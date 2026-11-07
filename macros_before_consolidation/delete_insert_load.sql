
{# Atomic key-scoped DELETE + INSERT for governed Snowflake targets. #}

{% macro execute_enterprise_delete_insert(
    cfg, load_cfg, model_id, model_name,
    target_relation_name, model_sql,
    pre_hooks_to_run, post_hooks_to_run
) %}

    {# 1. Initialize model audit. #}

    {% set etl_nr = get_current_etl_nr() %}

    {% set model_execution_id = start_model_stats(
        etl_nr, model_id, target_relation_name, 'DELETE_INSERT'
    ) %}


    {# 2. Resolve existing governed target. #}

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
            'DELETE_INSERT: Target missing or not a physical table: '
            ~ target_relation_name
        ) }}

    {% endif %}


    {# 3. Prepare temporary source snapshot. #}

    {% set temp_name = 'EL_SRC_' ~ (model_name | upper)
        ~ '_' ~ (invocation_id | replace('-', '') | upper) %}

    {% set source_relation = api.Relation.create(
        database=target_relation.database,
        schema=target_relation.schema,
        identifier=temp_name,
        type='table'
    ) %}

    {{ run_hooks(pre_hooks_to_run) }}

    {% call statement('prepare_source', auto_begin=False) %}

        CREATE TEMPORARY TABLE {{ source_relation }} AS
        {{ model_sql }}

    {% endcall %}


    {# 4. Run shared pre-load validations. #}

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


    {# 5. Historical source-volume validation. #}

    {% if pre_results['passed'] %}

        {% set volume_result = check_source_volume_threshold(
            model_execution_id,
            etl_nr,
            model_id,
            pre_results['source_row_count'],
            cfg['pre_load']['volume_threshold']
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


    {# 6. Reject invalid prepared source before any target DML. #}

    {% if not pre_results['passed'] %}

        {% do mark_pre_load_failed(
            model_execution_id,
            pre_results['failed_checks'],
            pre_results['source_row_count']
        ) %}

        {{ exceptions.raise_compiler_error(
            'DELETE_INSERT: Pre-load validation blocked the load. '
            ~ 'Failed checks: '
            ~ (pre_results['failed_checks'] | join(', '))
            ~ '. Governed target was not modified.'
        ) }}

    {% endif %}


    {# 7. Capture target state and expected matching-key count. #}

    {% set before_result = run_query(
        'SELECT COUNT(*) FROM ' ~ target_relation
    ) %}

    {% set target_before = before_result.rows[0][0] | int %}

    {% set join_condition = generate_delete_insert_key_match(
        cfg['natural_keys'],
        'tgt',
        'src'
    ) %}

    {% set overlap_sql %}

        SELECT COUNT(*)
        FROM {{ target_relation }} AS tgt
        INNER JOIN {{ source_relation }} AS src
            ON {{ join_condition }}

    {% endset %}

    {% set overlap_result = run_query(overlap_sql) %}

    {% set expected_deleted =
        overlap_result.rows[0][0] | int %}

    {% do mark_pre_load_passed(
        model_execution_id,
        pre_results['source_row_count'],
        target_before
    ) %}


    {# 8. Generate key-scoped DELETE and full-row INSERT. #}

    {% set delete_sql = generate_delete_insert_delete_sql(
        source_relation,
        target_relation,
        cfg['natural_keys']
    ) %}

    {% set insert_sql = generate_replace_insert_sql(
        source_relation,
        target_relation,
        etl_nr,
        'enterprise_dbt_framework'
    ) %}


    {# 9. Execute DELETE and INSERT in one transaction. #}

    {% do set_model_stage(
        model_execution_id, 'MERGE_RUNNING'
    ) %}

    {% call statement('main', fetch_result=True, auto_begin=False) %}

        EXECUTE IMMEDIATE $$
        DECLARE
            v_deleted NUMBER DEFAULT 0;
            v_inserted NUMBER DEFAULT 0;
        BEGIN

            BEGIN TRANSACTION;

            {{ delete_sql }};

            v_deleted := SQLROWCOUNT;

            {{ insert_sql }};

            v_inserted := SQLROWCOUNT;

            COMMIT;

            RETURN TO_VARCHAR(v_deleted)
                || '|' || TO_VARCHAR(v_inserted);

        EXCEPTION
            WHEN OTHER THEN
                ROLLBACK;
                RAISE;
        END;
        $$;

    {% endcall %}


    {# 10. Extract actual DELETE and INSERT counts. #}

    {% set result = load_result('main') %}

    {% set result_table = result['table'] %}

    {% if result_table is none
        or result_table.rows | length == 0 %}

        {{ exceptions.raise_compiler_error(
            'DELETE_INSERT: Transaction returned no DML counts.'
        ) }}

    {% endif %}

    {% set returned_value = result_table.rows[0][0] | string %}
    {% set parts = returned_value.split('|') %}

    {% if parts | length != 2 %}

        {{ exceptions.raise_compiler_error(
            'DELETE_INSERT: Unexpected transaction result: '
            ~ returned_value
        ) }}

    {% endif %}

    {% set dml_stats = {
        'rows_inserted': parts[1] | int,
        'rows_updated': 0,
        'rows_deleted': parts[0] | int
    } %}

    {% do log(
        'DELETE_INSERT DML COMMITTED'
        ~ ' | Deleted=' ~ dml_stats['rows_deleted']
        ~ ' | Inserted=' ~ dml_stats['rows_inserted'],
        info=true
    ) %}


    {# 11. Record committed DML statistics. #}

    {% set after_result = run_query(
        'SELECT COUNT(*) FROM ' ~ target_relation
    ) %}

    {% set target_after = after_result.rows[0][0] | int %}

    {% do finish_model_stats(
        model_execution_id,
        dml_stats,
        target_after
    ) %}


    {# 12. Execute DELETE_INSERT post-load validations. #}

    {% set dq_result = run_delete_insert_post_load_checks(
        source_relation,
        target_relation,
        cfg['natural_keys'],
        model_execution_id,
        etl_nr,
        pre_results['source_row_count'],
        target_before,
        target_after,
        expected_deleted,
        dml_stats,
        cfg['post_load']
    ) %}


    {# 13. Cleanup and finalize. #}

    {% call statement('cleanup_source', auto_begin=False) %}
        DROP TABLE IF EXISTS {{ source_relation }}
    {% endcall %}

    {{ run_hooks(post_hooks_to_run) }}

    {% do adapter.commit() %}

    {% if not dq_result['passed'] %}

        {{ exceptions.raise_compiler_error(
            'DELETE_INSERT: Post-load DQ failed after commit. '
            ~ 'Failed checks: '
            ~ (dq_result['failed_checks'] | join(', '))
        ) }}

    {% endif %}

    {% do log(
        'ENTERPRISE_LOAD DELETE_INSERT SUCCESS'
        ~ ' | Target=' ~ target_relation
        ~ ' | ETL_NR=' ~ etl_nr
        ~ ' | Deleted=' ~ dml_stats['rows_deleted']
        ~ ' | Inserted=' ~ dml_stats['rows_inserted']
        ~ ' | Updated=' ~ dml_stats['rows_updated'],
        info=true
    ) %}

    {% do return(target_relation) %}

{% endmacro %}
