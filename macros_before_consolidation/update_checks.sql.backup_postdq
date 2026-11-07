
{# UPDATE-only post-load DQ checks. #}

{% macro record_update_post_check(
    model_execution_id, etl_nr, check_name, observed, expected
) %}

    {% set status = 'PASS' if observed == expected else 'FAIL' %}

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
            {{ observed }},
            {{ expected }},
            OBJECT_CONSTRUCT('load_type', 'UPDATE'),
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


{% macro record_update_skip(
    model_execution_id, etl_nr, check_name
) %}

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
            'SKIP',
            NULL,
            NULL,
            OBJECT_CONSTRUCT(
                'load_type', 'UPDATE',
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


{% macro run_update_post_load_checks(
    source_relation,
    target_relation,
    natural_keys,
    model_execution_id,
    etl_nr,
    target_before,
    target_after,
    dml_stats,
    post_cfg
) %}

    {% set failures = [] %}
    {% set keys = [] %}
    {% set join_conditions = [] %}
    {% set null_conditions = [] %}
    {% set normalized_keys = [] %}

    {% for key in natural_keys %}

        {% set key_name = key | upper %}
        {% set q = adapter.quote(key_name) %}

        {% do normalized_keys.append(key_name) %}
        {% do keys.append(q) %}

        {% do join_conditions.append(
            'src.' ~ q ~ ' = tgt.' ~ q
        ) %}

        {% do null_conditions.append(
            'tgt.' ~ q ~ ' IS NULL'
        ) %}

    {% endfor %}


    {# 1. UPDATE must preserve total target row count. #}

    {% if post_cfg['reconcile_counts'] %}

        {% set status = record_update_post_check(
            model_execution_id,
            etl_nr,
            'UPDATE_ROW_COUNT_UNCHANGED',
            target_after,
            target_before
        ) %}

        {% if status == 'FAIL' %}
            {% do failures.append(
                'UPDATE_ROW_COUNT_UNCHANGED'
            ) %}
        {% endif %}

    {% else %}

        {% do record_update_skip(
            model_execution_id,
            etl_nr,
            'UPDATE_ROW_COUNT_UNCHANGED'
        ) %}

    {% endif %}


    {# 2. UPDATE must never insert or delete records. #}

    {% set unexpected_mutations =
        dml_stats['rows_inserted'] + dml_stats['rows_deleted'] %}

    {% set status = record_update_post_check(
        model_execution_id,
        etl_nr,
        'UPDATE_NO_INSERT_DELETE',
        unexpected_mutations,
        0
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('UPDATE_NO_INSERT_DELETE') %}
    {% endif %}


    {# 3. Target natural keys must not contain NULL. #}

    {% set null_sql %}
        SELECT COUNT(*)
        FROM {{ target_relation }} tgt
        WHERE {{ null_conditions | join(' OR ') }}
    {% endset %}

    {% set null_count =
        run_query(null_sql).rows[0][0] | int %}

    {% set status = record_update_post_check(
        model_execution_id,
        etl_nr,
        'UPDATE_TARGET_NULL_KEYS',
        null_count,
        0
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('UPDATE_TARGET_NULL_KEYS') %}
    {% endif %}


    {# 4. Target natural keys must remain unique. #}

    {% set duplicate_sql %}
        SELECT COUNT(*)
        FROM (
            SELECT {{ keys | join(', ') }}
            FROM {{ target_relation }}
            GROUP BY {{ keys | join(', ') }}
            HAVING COUNT(*) > 1
        ) duplicate_groups
    {% endset %}

    {% set duplicate_count =
        run_query(duplicate_sql).rows[0][0] | int %}

    {% set status = record_update_post_check(
        model_execution_id,
        etl_nr,
        'UPDATE_TARGET_DUPLICATES',
        duplicate_count,
        0
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('UPDATE_TARGET_DUPLICATES') %}
    {% endif %}


    {# 5. Reconcile updated rows with ETL run metadata. #}

    {% set run_count_sql %}
        SELECT COUNT(*)
        FROM {{ target_relation }}
        WHERE ETL_UPDATED_RUN_NUMBER = {{ etl_nr }}
    {% endset %}

    {% set run_update_count =
        run_query(run_count_sql).rows[0][0] | int %}

    {% set status = record_update_post_check(
        model_execution_id,
        etl_nr,
        'UPDATE_RUN_ROW_COUNT',
        run_update_count,
        dml_stats['rows_updated']
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('UPDATE_RUN_ROW_COUNT') %}
    {% endif %}


    {# 6. Validate only matching source/target natural keys.
       Unmatched source keys are intentionally ignored.
    #}

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
            INNER JOIN {{ target_relation }} tgt
                ON {{ join_conditions | join(' AND ') }}

            WHERE
                {% if differences | length > 0 %}
                    {{ differences | join(' OR ') }}
                {% else %}
                    1 = 0
                {% endif %}
        {% endset %}

        {% set mismatch_count =
            run_query(match_sql).rows[0][0] | int %}

        {% set status = record_update_post_check(
            model_execution_id,
            etl_nr,
            'UPDATE_SOURCE_TARGET_MATCH',
            mismatch_count,
            0
        ) %}

        {% if status == 'FAIL' %}
            {% do failures.append(
                'UPDATE_SOURCE_TARGET_MATCH'
            ) %}
        {% endif %}

    {% else %}

        {% do record_update_skip(
            model_execution_id,
            etl_nr,
            'UPDATE_SOURCE_TARGET_MATCH'
        ) %}

    {% endif %}


    {# Finalize model-level DQ status. #}

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
