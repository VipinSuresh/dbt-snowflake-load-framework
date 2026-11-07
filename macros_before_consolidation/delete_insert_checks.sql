
{# DELETE_INSERT post-load checks and audit evidence. #}

{% macro record_delete_insert_post_check(
    model_execution_id,
    etl_nr,
    check_name,
    observed,
    expected,
    enabled=true
) %}

    {% set status = (
        'SKIP' if not enabled
        else ('PASS' if observed == expected else 'FAIL')
    ) %}

    {% set actual_sql = observed | string if enabled else 'NULL' %}
    {% set expected_sql = expected | string if enabled else 'NULL' %}

    {% set sql %}

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
            '{{ status }}',
            {{ actual_sql }},
            {{ expected_sql }},
            OBJECT_CONSTRUCT(
                'load_type', 'DELETE_INSERT'
            ),
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


{% macro run_delete_insert_post_load_checks(
    source_relation,
    target_relation,
    natural_keys,
    model_execution_id,
    etl_nr,
    source_count,
    target_before,
    target_after,
    expected_deleted,
    dml_stats,
    post_cfg
) %}

    {% set failures = [] %}

    {% set quoted_keys = [] %}
    {% set null_conditions = [] %}
    {% set normalized_keys = [] %}

    {% for key in natural_keys %}

        {% set key_name = key | upper %}
        {% set q = adapter.quote(key_name) %}

        {% do normalized_keys.append(key_name) %}
        {% do quoted_keys.append(q) %}
        {% do null_conditions.append(
            'tgt.' ~ q ~ ' IS NULL'
        ) %}

    {% endfor %}


    {# 1. DML must affect exactly the expected keys and rows. #}

    {% set dml_ok = (
        dml_stats['rows_deleted'] == expected_deleted
        and dml_stats['rows_inserted'] == source_count
        and dml_stats['rows_updated'] == 0
        and target_after == (
            target_before
            - dml_stats['rows_deleted']
            + dml_stats['rows_inserted']
        )
    ) %}

    {% set status = record_delete_insert_post_check(
        model_execution_id, etl_nr,
        'DELETE_INSERT_DML_COUNTS',
        1 if dml_ok else 0, 1
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('DELETE_INSERT_DML_COUNTS') %}
    {% endif %}


    {# 2. Reconcile final count with expected key-scoped replacement. #}

    {% set expected_after = (
        target_before - expected_deleted + source_count
    ) %}

    {% set status = record_delete_insert_post_check(
        model_execution_id, etl_nr,
        'DELETE_INSERT_TARGET_ROW_COUNT',
        target_after, expected_after,
        post_cfg['reconcile_counts']
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('DELETE_INSERT_TARGET_ROW_COUNT') %}
    {% endif %}


    {# 3. Target natural keys must not contain NULL. #}

    {% set null_sql %}

        SELECT COUNT(*)
        FROM {{ target_relation }} AS tgt
        WHERE {{ null_conditions | join(' OR ') }}

    {% endset %}

    {% set null_count = run_query(null_sql).rows[0][0] | int %}

    {% set status = record_delete_insert_post_check(
        model_execution_id, etl_nr,
        'DELETE_INSERT_TARGET_NULL_KEYS',
        null_count, 0
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('DELETE_INSERT_TARGET_NULL_KEYS') %}
    {% endif %}


    {# 4. Target natural keys must remain unique. #}

    {% set duplicate_sql %}

        SELECT COUNT(*)
        FROM (
            SELECT {{ quoted_keys | join(', ') }}
            FROM {{ target_relation }}
            GROUP BY {{ quoted_keys | join(', ') }}
            HAVING COUNT(*) > 1
        ) d

    {% endset %}

    {% set duplicate_count =
        run_query(duplicate_sql).rows[0][0] | int %}

    {% set status = record_delete_insert_post_check(
        model_execution_id, etl_nr,
        'DELETE_INSERT_TARGET_DUPLICATES',
        duplicate_count, 0
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('DELETE_INSERT_TARGET_DUPLICATES') %}
    {% endif %}


    {# 5. Every inserted row must carry this ETL creation number. #}

    {% set audit_sql %}

        SELECT COUNT(*)
        FROM {{ target_relation }}
        WHERE ETL_CREATED_RUN_NUMBER = {{ etl_nr }}
          AND ETL_UPDATED_RUN_NUMBER IS NULL

    {% endset %}

    {% set audit_count = run_query(audit_sql).rows[0][0] | int %}

    {% set status = record_delete_insert_post_check(
        model_execution_id, etl_nr,
        'DELETE_INSERT_AUDIT_RUN_COUNT',
        audit_count, source_count
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('DELETE_INSERT_AUDIT_RUN_COUNT') %}
    {% endif %}


    {# 6. Source rows must match target business attributes. #}

    {% if post_cfg['source_target_match'] %}

        {% set join_condition = generate_delete_insert_key_match(
            natural_keys,
            'tgt',
            'src'
        ) %}

        {% set source_columns =
            adapter.get_columns_in_relation(source_relation) %}

        {% set differences = [] %}

        {% for col in source_columns %}

            {% if col.name | upper not in normalized_keys %}

                {% set q = adapter.quote(col.name) %}

                {% do differences.append(
                    'src.' ~ q
                    ~ ' IS DISTINCT FROM tgt.' ~ q
                ) %}

            {% endif %}

        {% endfor %}

        {% set match_sql %}

            SELECT COUNT(*)
            FROM {{ source_relation }} AS src
            LEFT JOIN {{ target_relation }} AS tgt
                ON {{ join_condition }}

            WHERE tgt.{{ quoted_keys[0] }} IS NULL

            {% if differences | length > 0 %}
                OR (
                    {{ differences | join(' OR ') }}
                )
            {% endif %}

        {% endset %}

        {% set mismatches =
            run_query(match_sql).rows[0][0] | int %}

        {% set status = record_delete_insert_post_check(
            model_execution_id, etl_nr,
            'DELETE_INSERT_SOURCE_TARGET_MATCH',
            mismatches, 0
        ) %}

    {% else %}

        {% set status = record_delete_insert_post_check(
            model_execution_id, etl_nr,
            'DELETE_INSERT_SOURCE_TARGET_MATCH',
            0, 0, false
        ) %}

    {% endif %}

    {% if status == 'FAIL' %}
        {% do failures.append('DELETE_INSERT_SOURCE_TARGET_MATCH') %}
    {% endif %}


    {% set passed = failures | length == 0 %}

    {% do finalize_model_dq(
        model_execution_id,
        passed
    ) %}

    {% do return({
        'passed': passed,
        'failed_checks': failures
    }) %}

{% endmacro %}
