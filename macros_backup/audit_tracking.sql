
{% macro get_current_etl_nr() %}

    {% set sql %}
        SELECT ETL_NR
        FROM DBT_DEMO.ETL_CONTROL.ETL_RUN_CONTROL
        WHERE DBT_INVOCATION_ID = '{{ invocation_id }}'
    {% endset %}

    {% set result = run_query(sql) %}

    {% if result is none or result.rows | length != 1 %}
        {{ exceptions.raise_compiler_error(
            'ETL: Expected exactly one run-control record '
            ~ 'for invocation ' ~ invocation_id
        ) }}
    {% endif %}

    {% do return(result.rows[0][0] | int) %}

{% endmacro %}


{% macro start_model_stats(
    etl_nr,
    model_id,
    target_relation,
    load_type,
    source_count,
    target_before
) %}

    {% set id_result = run_query('SELECT UUID_STRING()') %}
    {% set model_execution_id = id_result.rows[0][0] %}

    {% set safe_model = model_id | replace("'", "''") %}
    {% set safe_target = (target_relation | string) | replace("'", "''") %}
    {% set now_utc =
        "CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)" %}

    {% set sql %}
        INSERT INTO DBT_DEMO.ETL_CONTROL.ETL_MODEL_STATS
        (
            MODEL_EXECUTION_ID,
            ETL_NR,
            DBT_MODEL_ID,
            TARGET_RELATION,
            LOAD_TYPE,
            SOURCE_ROW_COUNT,
            TARGET_ROWS_BEFORE,
            LOAD_STATUS,
            DQ_STATUS,
            STARTED_AT
        )
        SELECT
            '{{ model_execution_id }}',
            {{ etl_nr }},
            '{{ safe_model }}',
            '{{ safe_target }}',
            '{{ load_type }}',
            {{ source_count }},
            {{ target_before }},
            'RUNNING',
            'PENDING',
            {{ now_utc }}
    {% endset %}

    {% do run_query(sql) %}

    {% do return(model_execution_id) %}

{% endmacro %}


{% macro finish_model_stats(
    model_execution_id,
    dml_stats,
    target_after
) %}

    {% set now_utc =
        "CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)" %}

    {% set sql %}
        UPDATE DBT_DEMO.ETL_CONTROL.ETL_MODEL_STATS
        SET
            ROWS_INSERTED = {{ dml_stats['rows_inserted'] | int }},
            ROWS_UPDATED = {{ dml_stats['rows_updated'] | int }},
            ROWS_DELETED = {{ dml_stats['rows_deleted'] | int }},
            TARGET_ROWS_AFTER = {{ target_after }},
            LOAD_STATUS = 'SUCCESS',
            DQ_STATUS = 'NOT_RUN',
            FINISHED_AT = {{ now_utc }}
        WHERE MODEL_EXECUTION_ID = '{{ model_execution_id }}'
    {% endset %}

    {% do run_query(sql) %}

{% endmacro %}


{% macro record_post_load_check(
    model_execution_id,
    etl_nr,
    check_name,
    observed_value,
    reference_value
) %}

    {% set check_status =
        'PASS' if observed_value == reference_value
        else 'FAIL'
    %}

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
            '{{ check_status }}',
            {{ observed_value }},
            {{ reference_value }},
            OBJECT_CONSTRUCT('load_type', 'TYPE1'),
            CONVERT_TIMEZONE(
                'UTC', CURRENT_TIMESTAMP()
            )::TIMESTAMP_NTZ(9)
    {% endset %}

    {% do run_query(sql) %}

    {% do log(
        'POST_LOAD | ' ~ check_name
        ~ ' | ' ~ check_status
        ~ ' | Actual=' ~ observed_value
        ~ ' | Expected=' ~ reference_value,
        info=true
    ) %}

    {% do return(check_status) %}

{% endmacro %}


{% macro finalize_model_dq(
    model_execution_id,
    passed
) %}

    {% set dq_status = 'PASS' if passed else 'FAIL' %}

    {% set load_status =
        'SUCCESS' if passed
        else 'LOAD_COMMITTED_DQ_FAILED'
    %}

    {% set sql %}
        UPDATE DBT_DEMO.ETL_CONTROL.ETL_MODEL_STATS
        SET
            DQ_STATUS = '{{ dq_status }}',
            LOAD_STATUS = '{{ load_status }}',
            FINISHED_AT =
                CONVERT_TIMEZONE(
                    'UTC', CURRENT_TIMESTAMP()
                )::TIMESTAMP_NTZ(9)
        WHERE MODEL_EXECUTION_ID = '{{ model_execution_id }}'
    {% endset %}

    {% do run_query(sql) %}

{% endmacro %}
