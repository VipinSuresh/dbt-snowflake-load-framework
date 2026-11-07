
{# TRUNCATE_LOAD post-load DQ. #}

{% macro record_truncate_post_check(
    model_execution_id, etl_nr, check_name, observed, expected
) %}

    {% set status = 'PASS' if observed == expected else 'FAIL' %}

    {% set sql %}
        INSERT INTO DBT_DEMO.ETL_CONTROL.ETL_DQ_RESULTS
        (
            DQ_RESULT_ID, MODEL_EXECUTION_ID, ETL_NR,
            CHECK_NAME, CHECK_PHASE, CHECK_STATUS,
            OBSERVED_VALUE, REFERENCE_VALUE,
            DETAILS, CHECKED_AT
        )
        SELECT
            UUID_STRING(),
            '{{ model_execution_id }}',
            {{ etl_nr }},
            '{{ check_name }}',
            'POST_LOAD',
            '{{ status }}',
            {{ observed }},
            {{ expected }},
            OBJECT_CONSTRUCT('load_type', 'TRUNCATE_LOAD'),
            CONVERT_TIMEZONE(
                'UTC', CURRENT_TIMESTAMP()
            )::TIMESTAMP_NTZ(9)
    {% endset %}

    {% do run_query(sql) %}
    {% do log(
        'POST_LOAD | ' ~ check_name ~ ' | ' ~ status,
        info=true
    ) %}

    {% do return(status) %}

{% endmacro %}


{% macro record_truncate_skip(
    model_execution_id, etl_nr, check_name
) %}

    {% set sql %}
        INSERT INTO DBT_DEMO.ETL_CONTROL.ETL_DQ_RESULTS
        (
            DQ_RESULT_ID, MODEL_EXECUTION_ID, ETL_NR,
            CHECK_NAME, CHECK_PHASE, CHECK_STATUS,
            OBSERVED_VALUE, REFERENCE_VALUE,
            DETAILS, CHECKED_AT
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
            OBJECT_CONSTRUCT(
                'load_type', 'TRUNCATE_LOAD',
                'reason', 'Disabled by YAML configuration'
            ),
            CONVERT_TIMEZONE(
                'UTC', CURRENT_TIMESTAMP()
            )::TIMESTAMP_NTZ(9)
    {% endset %}

    {% do run_query(sql) %}
    {% do log(
        'POST_LOAD | ' ~ check_name ~ ' | SKIP',
        info=true
    ) %}

{% endmacro %}


{% macro run_truncate_post_load_checks(
    source_relation,
    target_relation,
    natural_keys,
    model_execution_id,
    etl_nr,
    source_count,
    target_before,
    target_after,
    dml_stats,
    post_cfg
) %}

    {% set failures = [] %}
    {% set keys = [] %}
    {% set null_conditions = [] %}
    {% set join_conditions = [] %}
    {% set normalized_keys = [] %}

    {% for key in natural_keys %}
        {% set name = key | upper %}
        {% set q = adapter.quote(name) %}

        {% do normalized_keys.append(name) %}
        {% do keys.append(q) %}
        {% do null_conditions.append('tgt.' ~ q ~ ' IS NULL') %}
        {% do join_conditions.append('src.' ~ q ~ ' = tgt.' ~ q) %}
    {% endfor %}


    {# 1. Target after replacement equals prepared source count. #}

    {% set status = record_truncate_post_check(
        model_execution_id, etl_nr,
        'REPLACE_TARGET_ROW_COUNT',
        target_after, source_count
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('REPLACE_TARGET_ROW_COUNT') %}
    {% endif %}


    {# 2. Verify INSERT, DELETE and UPDATE statistics. #}

    {% set counts_ok =
        dml_stats['rows_deleted'] == target_before
        and dml_stats['rows_inserted'] == source_count
        and dml_stats['rows_updated'] == 0
        and target_after == (
            target_before
            - dml_stats['rows_deleted']
            + dml_stats['rows_inserted']
        )
    %}

    {% set status = record_truncate_post_check(
        model_execution_id, etl_nr,
        'REPLACE_DML_COUNTS',
        1 if counts_ok else 0, 1
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('REPLACE_DML_COUNTS') %}
    {% endif %}


    {# 3. Mandatory target NULL-key check. #}

    {% set null_sql %}
        SELECT COUNT(*)
        FROM {{ target_relation }} tgt
        WHERE {{ null_conditions | join(' OR ') }}
    {% endset %}

    {% set null_count = run_query(null_sql).rows[0][0] | int %}

    {% set status = record_truncate_post_check(
        model_execution_id, etl_nr,
        'REPLACE_TARGET_NULL_KEYS',
        null_count, 0
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('REPLACE_TARGET_NULL_KEYS') %}
    {% endif %}


    {# 4. Mandatory target duplicate-key check. #}

    {% set duplicate_sql %}
        SELECT COUNT(*)
        FROM (
            SELECT {{ keys | join(', ') }}
            FROM {{ target_relation }}
            GROUP BY {{ keys | join(', ') }}
            HAVING COUNT(*) > 1
        ) d
    {% endset %}

    {% set duplicate_count =
        run_query(duplicate_sql).rows[0][0] | int %}

    {% set status = record_truncate_post_check(
        model_execution_id, etl_nr,
        'REPLACE_TARGET_DUPLICATES',
        duplicate_count, 0
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('REPLACE_TARGET_DUPLICATES') %}
    {% endif %}


    {# 5. Every replacement row belongs to this ETL run. #}

    {% set audit_sql %}
        SELECT COUNT(*)
        FROM {{ target_relation }}
        WHERE ETL_CREATED_RUN_NUMBER = {{ etl_nr }}
          AND ETL_UPDATED_RUN_NUMBER IS NULL
    {% endset %}

    {% set audit_count = run_query(audit_sql).rows[0][0] | int %}

    {% set status = record_truncate_post_check(
        model_execution_id, etl_nr,
        'REPLACE_AUDIT_RUN_COUNT',
        audit_count, source_count
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('REPLACE_AUDIT_RUN_COUNT') %}
    {% endif %}


    {# 6. All source business attributes must match target. #}

    {% if post_cfg['source_target_match'] %}

        {% set source_columns =
            adapter.get_columns_in_relation(source_relation) %}

        {% set differences = [] %}

        {% for col in source_columns %}
            {% if col.name | upper not in normalized_keys %}

                {% set q = adapter.quote(col.name) %}

                {% do differences.append(
                    'src.' ~ q ~ ' IS DISTINCT FROM tgt.' ~ q
                ) %}

            {% endif %}
        {% endfor %}

        {% set match_sql %}
            SELECT COUNT(*)
            FROM {{ source_relation }} src
            LEFT JOIN {{ target_relation }} tgt
                ON {{ join_conditions | join(' AND ') }}
            WHERE tgt.{{ keys[0] }} IS NULL
            {% if differences | length > 0 %}
                OR {{ differences | join(' OR ') }}
            {% endif %}
        {% endset %}

        {% set mismatches =
            run_query(match_sql).rows[0][0] | int %}

        {% set status = record_truncate_post_check(
            model_execution_id, etl_nr,
            'REPLACE_SOURCE_TARGET_MATCH',
            mismatches, 0
        ) %}

        {% if status == 'FAIL' %}
            {% do failures.append('REPLACE_SOURCE_TARGET_MATCH') %}
        {% endif %}

    {% else %}

        {% do record_truncate_skip(
            model_execution_id, etl_nr,
            'REPLACE_SOURCE_TARGET_MATCH'
        ) %}

    {% endif %}


    {% set passed = failures | length == 0 %}

    {% do finalize_model_dq(model_execution_id, passed) %}

    {% do return({
        'passed': passed,
        'failed_checks': failures
    }) %}

{% endmacro %}
