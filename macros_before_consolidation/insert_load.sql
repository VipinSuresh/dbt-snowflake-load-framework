
{# INSERT-only strategy for existing governed Snowflake targets. #}

{% macro execute_enterprise_insert(
    cfg, load_cfg, model_id, model_name,
    target_relation_name, model_sql,
    pre_hooks_to_run, post_hooks_to_run
) %}

    {# 1. Start model audit. #}

    {% set etl_nr = get_current_etl_nr() %}

    {% set model_execution_id = start_model_stats(
        etl_nr, model_id, target_relation_name, 'INSERT'
    ) %}

    {# 2. Validate governed target existence. #}

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
            'Governed target must already exist: '
            ~ target_relation_name
        ) %}

        {% do mark_pre_load_failed(
            model_execution_id,
            ['TARGET_EXISTS_AND_IS_TABLE']
        ) %}

        {{ exceptions.raise_compiler_error(
            'INSERT: Governed target missing or not a table: '
            ~ target_relation_name
        ) }}

    {% endif %}

    {# 3. Prepare source snapshot. #}

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

    {# 4. Execute reusable pre-load validations. #}

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

    {# 5. Evaluate source-volume thresholds. #}

    {% if pre_results['passed'] %}

        {% set pre_load_cfg =
            load_cfg.get('pre_load', {}) or {} %}

        {% set volume_cfg =
            pre_load_cfg.get('volume_threshold', {}) %}

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

    {# 6. Stop execution if pre-load fails. #}

    {% if not pre_results['passed'] %}

        {% do mark_pre_load_failed(
            model_execution_id,
            pre_results['failed_checks'],
            pre_results['source_row_count']
        ) %}

        {{ exceptions.raise_compiler_error(
            'INSERT: Pre-load validation blocked the load. '
            ~ 'Failed checks: '
            ~ (pre_results['failed_checks'] | join(', '))
            ~ '. Target table was not modified.'
        ) }}

    {% endif %}

    {# 7. Record target state before DML. #}

    {% set before_result = run_query(
        'SELECT COUNT(*) FROM ' ~ target_relation
    ) %}

    {% set target_before =
        before_result.rows[0][0] | int %}

    {% do mark_pre_load_passed(
        model_execution_id,
        pre_results['source_row_count'],
        target_before
    ) %}

    {# 8. Generate INSERT-only MERGE. #}

    {% set merge_sql = generate_insert_merge_sql(
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
    merge_sql,
    'main',
    insert_only=true
    ) %}

    {# 9. Record post-DML statistics. #}

    {% set after_result = run_query(
        'SELECT COUNT(*) FROM ' ~ target_relation
    ) %}

    {% set target_after =
        after_result.rows[0][0] | int %}

    {% do finish_model_stats(
        model_execution_id,
        dml_stats,
        target_after
    ) %}

    {# 10. INSERT-specific post-load checks. #}

    {% set dq_result = run_insert_post_load_checks(
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

    {# 11. Cleanup and finalization. #}

    {% call statement('cleanup_source', auto_begin=False) %}

        DROP TABLE IF EXISTS {{ source_relation }}

    {% endcall %}

    {{ run_hooks(post_hooks_to_run) }}

    {% do adapter.commit() %}

    {% if not dq_result['passed'] %}

        {{ exceptions.raise_compiler_error(
            'INSERT: Post-load DQ failed after commit. '
            ~ 'Failed checks: '
            ~ (dq_result['failed_checks'] | join(', '))
        ) }}

    {% endif %}

    {% do log(
        'ENTERPRISE_LOAD INSERT SUCCESS'
        ~ ' | Target=' ~ target_relation
        ~ ' | ETL_NR=' ~ etl_nr
        ~ ' | Inserted=' ~ dml_stats['rows_inserted']
        ~ ' | Updated=' ~ dml_stats['rows_updated']
        ~ ' | Deleted=' ~ dml_stats['rows_deleted'],
        info=true
    ) %}

    {% do return(target_relation) %}

{% endmacro %}
