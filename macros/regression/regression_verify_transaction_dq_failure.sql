{% macro regression_verify_transaction_dq_failure(
    invocation_id,
    model_name,
    target_identifier,
    check_name,
    expected_reason='REGRESSION_FORCED_FAILURE'
) %}

    {% if execute %}

        {% set expected_model_id = "model." ~ project_name ~ "." ~ model_name %}

        {% set sql %}
            SELECT
                COUNT(DISTINCT s.MODEL_EXECUTION_ID) AS MODEL_RUNS,
                COUNT(DISTINCT IFF(
                    s.LOAD_STATUS = 'TRANSACTION_DQ_FAILED'
                    AND s.DQ_STATUS = 'FAIL'
                    AND s.ERROR_MESSAGE ILIKE
                        '%{{ expected_reason | replace("'", "''") }}%',
                    s.MODEL_EXECUTION_ID,
                    NULL
                )) AS MODEL_FAILURE_ROWS,
                COALESCE(COUNT_IF(
                    d.CHECK_NAME = '{{ check_name | replace("'", "''") }}'
                    AND d.CHECK_PHASE = 'TRANSACTION'
                    AND d.CHECK_STATUS = 'FAIL'
                    AND d.OBSERVED_VALUE = 0
                    AND d.REFERENCE_VALUE = 1
                    AND d.DETAILS:reason::string =
                        '{{ expected_reason | replace("'", "''") }}'
                ), 0) AS MATCHING_DQ_FAILURES

            FROM {{ enterprise_control_relation('ETL_RUN_CONTROL') }} r

            JOIN {{ enterprise_control_relation('ETL_MODEL_STATS') }} s
              ON r.ETL_NR = s.ETL_NR

            LEFT JOIN {{ enterprise_control_relation('ETL_DQ_RESULTS') }} d
              ON s.MODEL_EXECUTION_ID = d.MODEL_EXECUTION_ID

            WHERE r.DBT_INVOCATION_ID =
              '{{ invocation_id | replace("'", "''") }}'

              AND s.DBT_MODEL_ID =
              '{{ expected_model_id | replace("'", "''") }}'

              AND s.TARGET_RELATION ILIKE
              '%{{ target_identifier | replace("'", "''") }}%'
        {% endset %}

        {% set result = run_query(sql) %}
        {% set model_runs = result.rows[0][0] | int %}
        {% set model_failures = result.rows[0][1] | int %}
        {% set dq_failures = result.rows[0][2] | int %}

        {% if model_runs != 1 or model_failures != 1 or dq_failures != 1 %}
            {{ exceptions.raise_compiler_error(
                'TRANSACTION DQ AUDIT FAILED: model_runs=' ~ model_runs
                ~ ', model_failures=' ~ model_failures
                ~ ', dq_failures=' ~ dq_failures
            ) }}
        {% endif %}

        {% do log('TRANSACTION_DQ_AUDIT_VERIFIED', info=true) %}

    {% endif %}

{% endmacro %}
