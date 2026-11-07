{# Public demo: model-level ETL statistics and DQ evidence. #}

{% macro get_current_etl_nr() %}
    {% set sql %}
        SELECT ETL_NR
        FROM DBT_DEMO.ETL_CONTROL.ETL_RUN_CONTROL
        WHERE DBT_INVOCATION_ID = '{{ invocation_id }}'
    {% endset %}
    {% set result = run_query(sql) %}
    {% if result is none or result.rows | length != 1 %}
        {{ exceptions.raise_compiler_error(
            'ETL: Expected one run record for invocation ' ~ invocation_id
        ) }}
    {% endif %}
    {% do return(result.rows[0][0] | int) %}
{% endmacro %}


{# Start BEFORE preparing source or running DQ. Unknown counts remain NULL. #}
{% macro start_model_stats(
    etl_nr, model_id, target_relation, load_type,
    source_count=none, target_before=none
) %}
    {% set id_result = run_query('SELECT UUID_STRING()') %}
    {% set model_execution_id = id_result.rows[0][0] %}
    {% set safe_model = (model_id | string) | replace("'", "''") %}
    {% set safe_target = (target_relation | string) | replace("'", "''") %}
    {% set safe_load_type = (load_type | string) | replace("'", "''") %}
    {% set source_sql = 'NULL' if source_count is none else (source_count | int | string) %}
    {% set target_sql = 'NULL' if target_before is none else (target_before | int | string) %}
    {% set now_utc = "CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)" %}
    {% set sql %}
        INSERT INTO DBT_DEMO.ETL_CONTROL.ETL_MODEL_STATS
        (
            MODEL_EXECUTION_ID, ETL_NR, DBT_MODEL_ID, TARGET_RELATION,
            LOAD_TYPE, SOURCE_ROW_COUNT, TARGET_ROWS_BEFORE,
            LOAD_STATUS, DQ_STATUS, STARTED_AT
        )
        SELECT
            '{{ model_execution_id }}', {{ etl_nr }},
            '{{ safe_model }}', '{{ safe_target }}', '{{ safe_load_type }}',
            {{ source_sql }}, {{ target_sql }},
            'PREPARING', 'PENDING', {{ now_utc }}
    {% endset %}
    {% do run_query(sql) %}
    {% do return(model_execution_id) %}
{% endmacro %}


{% macro set_model_stage(model_execution_id, stage) %}
    {% set allowed = ['PREPARING', 'PRE_LOAD_RUNNING', 'PRE_LOAD_PASSED', 'MERGE_RUNNING'] %}
    {% if stage not in allowed %}
        {{ exceptions.raise_compiler_error('ETL: Invalid model stage: ' ~ stage) }}
    {% endif %}
    {% set sql %}
        UPDATE DBT_DEMO.ETL_CONTROL.ETL_MODEL_STATS
        SET LOAD_STATUS = '{{ stage }}'
        WHERE MODEL_EXECUTION_ID = '{{ model_execution_id }}'
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}


{% macro mark_pre_load_passed(model_execution_id, source_count, target_before) %}
    {% set sql %}
        UPDATE DBT_DEMO.ETL_CONTROL.ETL_MODEL_STATS
        SET
            SOURCE_ROW_COUNT = {{ source_count | int }},
            TARGET_ROWS_BEFORE = {{ target_before | int }},
            LOAD_STATUS = 'PRE_LOAD_PASSED'
        WHERE MODEL_EXECUTION_ID = '{{ model_execution_id }}'
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}


{% macro mark_pre_load_failed(model_execution_id, failed_checks, source_count=none) %}
    {% set error_text = 'Blocked by pre-load checks: ' ~ (failed_checks | join(', ')) %}
    {% set safe_error = (error_text | replace("'", "''"))[:3900] %}
    {% set source_sql = 'NULL' if source_count is none else (source_count | int | string) %}
    {% set sql %}
        UPDATE DBT_DEMO.ETL_CONTROL.ETL_MODEL_STATS
        SET
            SOURCE_ROW_COUNT = {{ source_sql }},
            LOAD_STATUS = 'PRE_LOAD_FAILED',
            DQ_STATUS = 'FAIL',
            ERROR_MESSAGE = '{{ safe_error }}',
            FINISHED_AT = CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)
        WHERE MODEL_EXECUTION_ID = '{{ model_execution_id }}'
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}


{# The explicit status supports non-numeric checks such as schema compatibility. #}
{% macro record_pre_load_check(
    model_execution_id, etl_nr, check_name, passed,
    observed=none, reference=none, detail=''
) %}
    {% set check_status = 'PASS' if passed else 'FAIL' %}
    {% set actual_sql = 'NULL' if observed is none else (observed | string) %}
    {% set expected_sql = 'NULL' if reference is none else (reference | string) %}
    {% set safe_name = (check_name | string) | replace("'", "''") %}
    {% set safe_detail = (detail | string | replace("'", "''"))[:1900] %}
    {% set sql %}
        INSERT INTO DBT_DEMO.ETL_CONTROL.ETL_DQ_RESULTS
        (
            DQ_RESULT_ID, MODEL_EXECUTION_ID, ETL_NR,
            CHECK_NAME, CHECK_PHASE, CHECK_STATUS,
            OBSERVED_VALUE, REFERENCE_VALUE, DETAILS, CHECKED_AT
        )
        SELECT
            UUID_STRING(), '{{ model_execution_id }}', {{ etl_nr }},
            '{{ safe_name }}', 'PRE_LOAD', '{{ check_status }}',
            {{ actual_sql }}, {{ expected_sql }},
            OBJECT_CONSTRUCT('message', '{{ safe_detail }}'),
            CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)
    {% endset %}
    {% do run_query(sql) %}
    {% do log('PRE_LOAD ' ~ check_name ~ ' | ' ~ check_status ~ ' | ' ~ detail, info=true) %}
    {% do return(check_status) %}
{% endmacro %}


{# Existing model-load statistics logic, unchanged in meaning. #}
{% macro finish_model_stats(model_execution_id, dml_stats, target_after) %}
    {% set now_utc = "CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)" %}
    {% set sql %}
        UPDATE DBT_DEMO.ETL_CONTROL.ETL_MODEL_STATS
        SET
            ROWS_INSERTED = {{ dml_stats['rows_inserted'] | int }},
            ROWS_UPDATED = {{ dml_stats['rows_updated'] | int }},
            ROWS_DELETED = {{ dml_stats['rows_deleted'] | int }},
            TARGET_ROWS_AFTER = {{ target_after | int }},
            LOAD_STATUS = 'DML_COMMITTED',
            DQ_STATUS = 'NOT_RUN',
            FINISHED_AT = {{ now_utc }}
        WHERE MODEL_EXECUTION_ID = '{{ model_execution_id }}'
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}


{# Retains the existing post-load check interface and numeric evidence. #}
{% macro record_post_load_check(
    model_execution_id, etl_nr, check_name,
    observed_value, reference_value
) %}
    {% set check_status = 'PASS' if observed_value == reference_value else 'FAIL' %}
    {% set sql %}
        INSERT INTO DBT_DEMO.ETL_CONTROL.ETL_DQ_RESULTS
        (
            DQ_RESULT_ID, MODEL_EXECUTION_ID, ETL_NR,
            CHECK_NAME, CHECK_PHASE, CHECK_STATUS,
            OBSERVED_VALUE, REFERENCE_VALUE, DETAILS, CHECKED_AT
        )
        SELECT
            UUID_STRING(), '{{ model_execution_id }}', {{ etl_nr }},
            '{{ check_name }}', 'POST_LOAD', '{{ check_status }}',
            {{ observed_value }}, {{ reference_value }},
            OBJECT_CONSTRUCT('load_type', 'TYPE1'),
            CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)
    {% endset %}
    {% do run_query(sql) %}
    {% do log('POST_LOAD | ' ~ check_name ~ ' | ' ~ check_status, info=true) %}
    {% do return(check_status) %}
{% endmacro %}


{% macro finalize_model_dq(model_execution_id, passed) %}
    {% set dq_status = 'PASS' if passed else 'FAIL' %}
    {% set load_status = 'SUCCESS' if passed else 'LOAD_COMMITTED_DQ_FAILED' %}
    {% set sql %}
        UPDATE DBT_DEMO.ETL_CONTROL.ETL_MODEL_STATS
        SET
            DQ_STATUS = '{{ dq_status }}',
            LOAD_STATUS = '{{ load_status }}',
            FINISHED_AT = CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)
        WHERE MODEL_EXECUTION_ID = '{{ model_execution_id }}'
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
