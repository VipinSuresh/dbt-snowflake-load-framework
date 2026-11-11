{% macro regression_verify_schema_drift_failure(
    invocation_id,
    model_name,
    target_identifier,
    check_name
) %}

    {% if execute %}
        {% set expected_model_id = 'model.' ~ project_name ~ '.' ~ model_name %}
        {% set safe_invocation = invocation_id | replace("'", "''") %}
        {% set safe_target = target_identifier | replace("'", "''") %}
        {% set safe_check = check_name | replace("'", "''") %}
        {% set safe_model = expected_model_id | replace("'", "''") %}

        {% set sql %}
            SELECT
                COUNT(DISTINCT s.MODEL_EXECUTION_ID) AS MODEL_RUNS,
                COALESCE(COUNT_IF(
                    d.CHECK_NAME = '{{ safe_check }}'
                    AND d.CHECK_PHASE = 'PRE_LOAD'
                    AND d.CHECK_STATUS = 'FAIL'
                ), 0) AS MATCHING_FAILURES,
                COALESCE(COUNT_IF(
                    s.LOAD_STATUS = 'PRE_LOAD_FAILED'
                    AND s.DQ_STATUS = 'FAIL'
                ), 0) AS FAILED_MODEL_STATS
            FROM {{ enterprise_control_relation('ETL_RUN_CONTROL') }} r
            JOIN {{ enterprise_control_relation('ETL_MODEL_STATS') }} s
              ON r.ETL_NR = s.ETL_NR
            LEFT JOIN {{ enterprise_control_relation('ETL_DQ_RESULTS') }} d
              ON s.MODEL_EXECUTION_ID = d.MODEL_EXECUTION_ID
            WHERE r.DBT_INVOCATION_ID = '{{ safe_invocation }}'
              AND s.DBT_MODEL_ID = '{{ safe_model }}'
              AND s.TARGET_RELATION ILIKE '%{{ safe_target }}%'
        {% endset %}

        {% set result = run_query(sql) %}
        {% set model_runs = result.rows[0][0] | int %}
        {% set failures = result.rows[0][1] | int %}
        {% set failed_stats = result.rows[0][2] | int %}

        {% if model_runs != 1 or failures != 1 or failed_stats < 1 %}
            {{ exceptions.raise_compiler_error(
                'SCHEMA DRIFT FAILURE AUDIT FAILED: model_runs=' ~ model_runs
                ~ ', matching_failures=' ~ failures
                ~ ', failed_model_stats=' ~ failed_stats
                ~ ', check_name=' ~ check_name
            ) }}
        {% endif %}

        {% do log('SCHEMA_DRIFT_FAILURE_AUDIT_VERIFIED | ' ~ check_name, info=true) %}
    {% endif %}

{% endmacro %}
