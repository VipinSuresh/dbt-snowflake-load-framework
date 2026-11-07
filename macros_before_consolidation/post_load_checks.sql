{# Post-load DQ evidence; SKIP is explicit for disabled optional checks. #}
{% macro record_skipped_post_load_check(model_execution_id, etl_nr, check_name) %}
    {% set skip_sql %}
        INSERT INTO DBT_DEMO.ETL_CONTROL.ETL_DQ_RESULTS
        (
            DQ_RESULT_ID,
            MODEL_EXECUTION_ID,
            ETL_NR,
            CHECK_NAME,
            CHECK_PHASE,
            CHECK_STATUS,
            OBSERVED_VALUE,
            REFERENCE_VALUE,
            DETAILS,
            CHECKED_AT
        )
        SELECT
            UUID_STRING(),
            '{{ model_execution_id }}',
            {{ etl_nr }},
            '{{ check_name }}',
            'POST_LOAD',
            'SKIP',
            NULL,
            NULL,
            OBJECT_CONSTRUCT('reason', 'Disabled by YAML configuration', 'load_type', 'TYPE1'),
            CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)
    {% endset %}
    {% do run_query(skip_sql) %}
    {% do log('POST_LOAD | ' ~ check_name ~ ' | SKIP | Disabled by YAML', info=true) %}
{% endmacro %}


{% macro run_type1_post_load_checks(
    source_relation,
    target_relation,
    natural_keys,
    model_execution_id,
    etl_nr,
    target_before,
    target_after,
    dml_stats,
    post_load_cfg=none
) %}

    {# Backward-compatible defaults if a test macro calls this directly. #}
    {% if post_load_cfg is none %}
        {% set post_load_cfg = {
            'reconcile_counts': true,
            'duplicate_check': true,
            'source_target_match': true
        } %}
    {% endif %}

    {% set failures = [] %}
    {% set keys = [] %}
    {% set join_conditions = [] %}
    {% set null_conditions = [] %}

    {% for key in natural_keys %}
        {% set q = adapter.quote(key | upper) %}
        {% do keys.append(q) %}
        {% do join_conditions.append('src.' ~ q ~ ' = tgt.' ~ q) %}
        {% do null_conditions.append('tgt.' ~ q ~ ' IS NULL') %}
    {% endfor %}

    {# Optional: target row count after = before + inserts - deletes. #}
    {% if post_load_cfg['reconcile_counts'] %}
        {% set expected_count = target_before
            + dml_stats['rows_inserted'] - dml_stats['rows_deleted'] %}
        {% set status = record_post_load_check(
            model_execution_id, etl_nr,
            'TYPE1_ROW_COUNT_DELTA', target_after, expected_count
        ) %}
        {% if status == 'FAIL' %}
            {% do failures.append('ROW_COUNT_DELTA') %}
        {% endif %}
    {% else %}
        {% do record_skipped_post_load_check(
            model_execution_id, etl_nr, 'TYPE1_ROW_COUNT_DELTA'
        ) %}
    {% endif %}

    {# Mandatory: target natural keys cannot contain NULLs. #}
    {% set null_sql %}
        SELECT COUNT(*)
        FROM {{ target_relation }} tgt
        WHERE {{ null_conditions | join(' OR ') }}
    {% endset %}
    {% set null_result = run_query(null_sql) %}
    {% set null_rows = null_result.rows[0][0] | int %}
    {% set status = record_post_load_check(
        model_execution_id, etl_nr,
        'TYPE1_TARGET_NULL_KEYS', null_rows, 0
    ) %}
    {% if status == 'FAIL' %}
        {% do failures.append('TARGET_NULL_KEYS') %}
    {% endif %}

    {# Mandatory: target natural keys must be unique. #}
    {% set dup_sql %}
        SELECT COUNT(*)
        FROM (
            SELECT {{ keys | join(', ') }}
            FROM {{ target_relation }}
            GROUP BY {{ keys | join(', ') }}
            HAVING COUNT(*) > 1
        ) duplicate_groups
    {% endset %}
    {% set dup_result = run_query(dup_sql) %}
    {% set duplicate_groups = dup_result.rows[0][0] | int %}
    {% set status = record_post_load_check(
        model_execution_id, etl_nr,
        'TYPE1_TARGET_DUPLICATES', duplicate_groups, 0
    ) %}
    {% if status == 'FAIL' %}
        {% do failures.append('TARGET_DUPLICATES') %}
    {% endif %}

    {# Optional: compare business values for loaded keys, including NULL-safe differences. #}
    {% if post_load_cfg['source_target_match'] %}
        {% set source_columns = adapter.get_columns_in_relation(source_relation) %}
        {% set normalized_keys = [] %}
        {% for key in natural_keys %}
            {% do normalized_keys.append(key | upper) %}
        {% endfor %}
        {% set differences = [] %}
        {% for col in source_columns %}
            {% if col.name | upper not in normalized_keys %}
                {% set q = adapter.quote(col.name) %}
                {% do differences.append(
                    'src.' ~ q ~ ' IS DISTINCT FROM tgt.' ~ q
                ) %}
            {% endif %}
        {% endfor %}
        {% set first_key = keys[0] %}
        {% set comparison_sql %}
            SELECT COUNT(*)
            FROM {{ source_relation }} src
            LEFT JOIN {{ target_relation }} tgt
                ON {{ join_conditions | join(' AND ') }}
            WHERE tgt.{{ first_key }} IS NULL
            {% if differences | length > 0 %}
                OR {{ differences | join(' OR ') }}
            {% endif %}
        {% endset %}
        {% set comparison_result = run_query(comparison_sql) %}
        {% set mismatch_rows = comparison_result.rows[0][0] | int %}
        {% set status = record_post_load_check(
            model_execution_id, etl_nr,
            'TYPE1_SOURCE_TARGET_MATCH', mismatch_rows, 0
        ) %}
        {% if status == 'FAIL' %}
            {% do failures.append('SOURCE_TARGET_MATCH') %}
        {% endif %}
    {% else %}
        {% do record_skipped_post_load_check(
            model_execution_id, etl_nr, 'TYPE1_SOURCE_TARGET_MATCH'
        ) %}
    {% endif %}

    {% set passed = failures | length == 0 %}
    {% do finalize_model_dq(model_execution_id, passed) %}
    {% do return({'passed': passed, 'failed_checks': failures}) %}
{% endmacro %}
