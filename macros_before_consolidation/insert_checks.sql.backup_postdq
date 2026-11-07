
{# INSERT strategy post-load DQ checks. #}

{% macro record_insert_skip(model_execution_id, etl_nr, check_name) %}

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
                'load_type', 'INSERT',
                'reason', 'Disabled by YAML configuration'
            ),
            CONVERT_TIMEZONE(
                'UTC', CURRENT_TIMESTAMP()
            )::TIMESTAMP_NTZ(9)
    {% endset %}

    {% do run_query(sql) %}
    {% do log('POST_LOAD | ' ~ check_name ~ ' | SKIP', info=true) %}

{% endmacro %}


{% macro run_insert_post_load_checks(
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
        {% do join_conditions.append('src.' ~ q ~ ' = tgt.' ~ q) %}
        {% do null_conditions.append('tgt.' ~ q ~ ' IS NULL') %}
    {% endfor %}

    {# 1. Target row-count reconciliation. #}

    {% if post_cfg['reconcile_counts'] %}

        {% set expected_count =
            target_before + dml_stats['rows_inserted'] %}

        {% set status = record_post_load_check(
            model_execution_id, etl_nr,
            'INSERT_ROW_COUNT_DELTA',
            target_after,
            expected_count
        ) %}

        {% if status == 'FAIL' %}
            {% do failures.append('INSERT_ROW_COUNT_DELTA') %}
        {% endif %}

    {% else %}

        {% do record_insert_skip(
            model_execution_id, etl_nr,
            'INSERT_ROW_COUNT_DELTA'
        ) %}

    {% endif %}

    {# 2. INSERT must never update or delete target rows. #}

    {% set mutation_count =
        dml_stats['rows_updated'] + dml_stats['rows_deleted'] %}

    {% set status = record_post_load_check(
        model_execution_id, etl_nr,
        'INSERT_NO_MUTATIONS',
        mutation_count,
        0
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('INSERT_NO_MUTATIONS') %}
    {% endif %}

    {# 3. Mandatory target NULL-key validation. #}

    {% set null_sql %}
        SELECT COUNT(*)
        FROM {{ target_relation }} tgt
        WHERE {{ null_conditions | join(' OR ') }}
    {% endset %}

    {% set null_count = run_query(null_sql).rows[0][0] | int %}

    {% set status = record_post_load_check(
        model_execution_id, etl_nr,
        'INSERT_TARGET_NULL_KEYS',
        null_count,
        0
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('INSERT_TARGET_NULL_KEYS') %}
    {% endif %}

    {# 4. Mandatory target duplicate-key validation. #}

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

    {% set status = record_post_load_check(
        model_execution_id, etl_nr,
        'INSERT_TARGET_DUPLICATES',
        duplicate_count,
        0
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('INSERT_TARGET_DUPLICATES') %}
    {% endif %}

    {# 5. Reconcile inserted count against ETL audit metadata. #}

    {% set run_count_sql %}
        SELECT COUNT(*)
        FROM {{ target_relation }}
        WHERE ETL_CREATED_RUN_NUMBER = {{ etl_nr }}
    {% endset %}

    {% set run_insert_count =
        run_query(run_count_sql).rows[0][0] | int %}

    {% set status = record_post_load_check(
        model_execution_id, etl_nr,
        'INSERT_RUN_ROW_COUNT',
        run_insert_count,
        dml_stats['rows_inserted']
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('INSERT_RUN_ROW_COUNT') %}
    {% endif %}

    {# 6. Only newly inserted rows must match source. #}

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
            FROM {{ target_relation }} tgt

            LEFT JOIN {{ source_relation }} src
                ON {{ join_conditions | join(' AND ') }}

            WHERE tgt.ETL_CREATED_RUN_NUMBER = {{ etl_nr }}
              AND (
                  src.{{ keys[0] }} IS NULL

                  {% if differences | length > 0 %}
                    OR {{ differences | join(' OR ') }}
                  {% endif %}
              )
        {% endset %}

        {% set mismatches =
            run_query(match_sql).rows[0][0] | int %}

        {% set status = record_post_load_check(
            model_execution_id, etl_nr,
            'INSERT_SOURCE_MATCH_NEW_ROWS',
            mismatches,
            0
        ) %}

        {% if status == 'FAIL' %}
            {% do failures.append('INSERT_SOURCE_MATCH_NEW_ROWS') %}
        {% endif %}

    {% else %}

        {% do record_insert_skip(
            model_execution_id, etl_nr,
            'INSERT_SOURCE_MATCH_NEW_ROWS'
        ) %}

    {% endif %}

    {# Finalize audit after all checks. #}

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
