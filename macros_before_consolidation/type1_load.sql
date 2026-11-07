
{# TYPE1 execution path for enterprise_load.
   Governed target must already exist.
   No target CREATE OR REPLACE or ALTER.
#}

{% macro execute_enterprise_type1(
    cfg, load_cfg, model_id, model_name,
    target_relation_name, model_sql,
    pre_hooks_to_run, post_hooks_to_run
) %}

    {# Start model audit before source preparation. #}
    {% set etl_nr = get_current_etl_nr() %}
    {% set model_execution_id = start_model_stats(
        etl_nr, model_id, target_relation_name, cfg['load_type']
    ) %}

    {# Validate the governed target. #}
    {% set target_relation = adapter.get_relation(
        database=target_relation_name.database,
        schema=target_relation_name.schema,
        identifier=target_relation_name.identifier
    ) %}

    {% if target_relation is none
        or target_relation.type | lower != 'table' %}

        {% do record_pre_load_check(
            model_execution_id, etl_nr,
            'TARGET_EXISTS_AND_IS_TABLE',
            false, 0, 1,
            'The governed target must be deployed separately: '
            ~ target_relation_name
        ) %}

        {% do mark_pre_load_failed(
            model_execution_id, ['TARGET_EXISTS_AND_IS_TABLE']
        ) %}

        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: Target table missing or not '
            ~ 'a physical table: ' ~ target_relation_name
        ) }}
    {% endif %}

    {# Prepare temporary source in writable workspace. #}
    {% set temp_name = 'EL_SRC_' ~ (model_name | upper)
        ~ '_' ~ (invocation_id | replace('-', '') | upper) %}

    {% set source_relation = api.Relation.create(
        database=target.database,
        schema=target.schema,
        identifier=temp_name,
        type='table'
    ) %}

    {{ run_hooks(pre_hooks_to_run) }}

    {% call statement('prepare_source', auto_begin=False) %}
        CREATE TEMPORARY TABLE {{ source_relation }} AS
        {{ model_sql }}
    {% endcall %}

    {# Execute pre-load validations. #}
    {% do set_model_stage(
        model_execution_id, 'PRE_LOAD_RUNNING'
    ) %}

    {% set pre_results = check_pre_load(
        source_relation,
        target_relation,
        cfg['natural_keys'],
        cfg['pre_load']['require_nonempty'],
        model_execution_id,
        etl_nr
    ) %}

    {# Preserve existing TYPE1 volume-threshold behavior. #}
    {% if pre_results['passed'] %}

        {% set pre_load_cfg = load_cfg.get('pre_load', {}) or {} %}
        {% set volume_cfg = pre_load_cfg.get(
            'volume_threshold', {}
        ) %}

        {% set volume_result = check_source_volume_threshold(
            model_execution_id,
            etl_nr,
            model_id,
            pre_results['source_row_count'],
            volume_cfg
        ) %}

        {% if not volume_result['passed'] %}

            {% for check_name in volume_result['failed_checks'] %}
                {% do pre_results['failed_checks'].append(
                    check_name
                ) %}
            {% endfor %}

            {% do pre_results.update({'passed': false}) %}

        {% endif %}
    {% endif %}

    {# Reject invalid source before DML. #}
    {% if not pre_results['passed'] %}

        {% do mark_pre_load_failed(
            model_execution_id,
            pre_results['failed_checks'],
            pre_results['source_row_count']
        ) %}

        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: Pre-load validation blocked '
            ~ 'the load. Failed checks: '
            ~ (pre_results['failed_checks'] | join(', '))
            ~ '. Target table was not modified.'
        ) }}

    {% endif %}

    {# Capture target count before execution. #}
    {% set before_result = run_query(
        'SELECT COUNT(*) FROM ' ~ target_relation
    ) %}

    {% set target_before = before_result.rows[0][0] | int %}

    {% do mark_pre_load_passed(
        model_execution_id,
        pre_results['source_row_count'],
        target_before
    ) %}

    {# Generate and execute existing TYPE1 MERGE. #}
    {% set merge_sql = generate_type1_merge_sql(
        source_relation,
        target_relation,
        cfg['natural_keys'],
        etl_nr,
        'enterprise_dbt_framework'
    ) %}

    {% do set_model_stage(
        model_execution_id, 'MERGE_RUNNING'
    ) %}

    {% set dml_stats = execute_snowflake_merge(
        merge_sql, 'main'
    ) %}

    {# Capture statistics after DML. #}
    {% set after_result = run_query(
        'SELECT COUNT(*) FROM ' ~ target_relation
    ) %}

    {% set target_after = after_result.rows[0][0] | int %}

    {% do finish_model_stats(
        model_execution_id,
        dml_stats,
        target_after
    ) %}

    {# Existing TYPE1 post-load checks. #}
    {% set dq_result = run_type1_post_load_checks(
        source_relation,
        target_relation,
        cfg['natural_keys'],
        model_execution_id,
        etl_nr,
        target_before,
        target_after,
        dml_stats,
        cfg['post_load']
    ) %}

    {# Clean up temporary source. #}
    {% call statement('cleanup_source', auto_begin=False) %}
        DROP TABLE IF EXISTS {{ source_relation }}
    {% endcall %}

    {{ run_hooks(post_hooks_to_run) }}

    {% do adapter.commit() %}

    {% if not dq_result['passed'] %}

        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: Post-load DQ failed after '
            ~ 'data commit. Failed checks: '
            ~ (dq_result['failed_checks'] | join(', '))
        ) }}

    {% endif %}

    {% do log(
        'ENTERPRISE_LOAD SUCCESS | Target=' ~ target_relation
        ~ ' | ETL_NR=' ~ etl_nr
        ~ ' | Inserted=' ~ dml_stats['rows_inserted']
        ~ ' | Updated=' ~ dml_stats['rows_updated'],
        info=true
    ) %}

    {% do return(target_relation) %}

{% endmacro %}
