{# Snowflake load-only TYPE1 materialization: the governed destination is never created or altered. #}
{% materialization enterprise_load, adapter='snowflake' %}

    {% if flags.FULL_REFRESH %}
        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: --full-refresh is not supported for governed targets.'
        ) }}
    {% endif %}

    {% set meta_cfg = config.get('meta', {}) %}
    {% set load_cfg = meta_cfg.get('enterprise_load', {}) %}
    {% set cfg = validate_enterprise_config(load_cfg) %}
    {% if cfg['load_type'] != 'TYPE1' %}
        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: Only TYPE1 is implemented in the current demo.'
        ) }}
    {% endif %}

    {# Start the per-model audit BEFORE relation preparation or pre-load DQ. #}
    {% set etl_nr = get_current_etl_nr() %}
    {% set model_execution_id = start_model_stats(
        etl_nr, model.unique_id, this, cfg['load_type']
    ) %}

    {# No CREATE OR REPLACE/ALTER on the governed destination. #}
    {% set target_relation = adapter.get_relation(
        database=this.database, schema=this.schema, identifier=this.identifier
    ) %}

    {% if target_relation is none or target_relation.type | lower != 'table' %}
        {% do record_pre_load_check(model_execution_id, etl_nr,
            'TARGET_EXISTS_AND_IS_TABLE', false, 0, 1,
            'The governed target must be deployed separately: ' ~ this) %}
        {% do mark_pre_load_failed(
            model_execution_id, ['TARGET_EXISTS_AND_IS_TABLE']
        ) %}
        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: Target table missing or not a physical table: ' ~ this
        ) }}
    {% endif %}

    {# Source is a temporary snapshot in the writable DBT_WORK schema. #}
    {% set temp_name = 'EL_SRC_' ~ (model.name | upper)
        ~ '_' ~ (invocation_id | replace('-', '') | upper) %}
    {% set source_relation = api.Relation.create(
        database=target.database,
        schema=target.schema,
        identifier=temp_name,
        type='table'
    ) %}

    {{ run_hooks(pre_hooks) }}

    {% call statement('prepare_source', auto_begin=False) %}
        CREATE TEMPORARY TABLE {{ source_relation }} AS
        {{ compiled_code }}
    {% endcall %}

    {% do set_model_stage(model_execution_id, 'PRE_LOAD_RUNNING') %}

    {# Returns evidence and failed checks instead of raising early. #}
    {% set pre_results = check_pre_load(
        source_relation,
        target_relation,
        cfg['natural_keys'],
        cfg['pre_load']['require_nonempty'],
        model_execution_id,
        etl_nr
    ) %}

    {% if not pre_results['passed'] %}
        {% do mark_pre_load_failed(
            model_execution_id,
            pre_results['failed_checks'],
            pre_results['source_row_count']
        ) %}
        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: Pre-load validation blocked the load. '
            ~ 'Failed checks: ' ~ (pre_results['failed_checks'] | join(', '))
            ~ '. Target table was not modified.'
        ) }}
    {% endif %}

    {% set before_result = run_query('SELECT COUNT(*) FROM ' ~ target_relation) %}
    {% set target_before = before_result.rows[0][0] | int %}

    {% do mark_pre_load_passed(
        model_execution_id, pre_results['source_row_count'], target_before
    ) %}

    {% set merge_sql = generate_type1_merge_sql(
        source_relation,
        target_relation,
        cfg['natural_keys'],
        etl_nr,
        'enterprise_dbt_framework'
    ) %}

    {% do set_model_stage(model_execution_id, 'MERGE_RUNNING') %}
    {% set dml_stats = execute_snowflake_merge(merge_sql, 'main') %}

    {% set after_result = run_query('SELECT COUNT(*) FROM ' ~ target_relation) %}
    {% set target_after = after_result.rows[0][0] | int %}
    {% do finish_model_stats(model_execution_id, dml_stats, target_after) %}

    {# Retain the independently verified TYPE1 post-load checks. #}
    {% set dq_result = run_type1_post_load_checks(
        source_relation,
        target_relation,
        cfg['natural_keys'],
        model_execution_id,
        etl_nr,
        target_before,
        target_after,
        dml_stats
    ) %}

    {% call statement('cleanup_source', auto_begin=False) %}
        DROP TABLE IF EXISTS {{ source_relation }}
    {% endcall %}

    {{ run_hooks(post_hooks) }}
    {% do adapter.commit() %}

    {% if not dq_result['passed'] %}
        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: Post-load DQ failed after data commit. '
            ~ 'Failed checks: ' ~ (dq_result['failed_checks'] | join(', '))
        ) }}
    {% endif %}

    {% do log('ENTERPRISE_LOAD SUCCESS | Target=' ~ target_relation
        ~ ' | ETL_NR=' ~ etl_nr
        ~ ' | Inserted=' ~ dml_stats['rows_inserted']
        ~ ' | Updated=' ~ dml_stats['rows_updated'], info=true) %}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
