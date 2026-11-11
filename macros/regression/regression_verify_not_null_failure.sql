{% macro regression_verify_not_null_failure(
    invocation_id,
    model_name,
    target_identifier
) %}

    {% if execute %}

        {% set expected_model_id = "model." ~ project_name ~ "." ~ model_name %}

        {% set sql %}
            SELECT
                COUNT(DISTINCT s.MODEL_EXECUTION_ID)
                    AS MODEL_RUNS,

                COALESCE(COUNT_IF(
                    d.CHECK_NAME = 'SOURCE_REQUIRED_NOT_NULL'
                    AND d.CHECK_STATUS = 'FAIL'
                    AND d.OBSERVED_VALUE = 1
                    AND d.DETAILS:message::string
                        ILIKE '%CUSTOMER_NAME%'
                ), 0) AS MATCHING_FAILURES

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
        {% set failures = result.rows[0][1] | int %}

        {% if model_runs != 1 or failures != 1 %}
            {{ exceptions.raise_compiler_error(
                'REGRESSION AUDIT FAILED: model_runs='
                ~ model_runs
                ~ ', failures=' ~ failures
            ) }}
        {% endif %}

        {% do log('REGRESSION_AUDIT_VERIFIED', info=true) %}

    {% endif %}

{% endmacro %}