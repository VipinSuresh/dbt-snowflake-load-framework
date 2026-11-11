{# Regression-only verifier for unexpected SQL/runtime failures. #}
{% macro regression_verify_runtime_failure_audit(model_name, target_identifier) %}
    {% if not execute %}
        {% do return(none) %}
    {% endif %}

    {% set safe_model = (model_name | string) | replace("'", "''") %}
    {% set safe_target = (target_identifier | string) | replace("'", "''") %}

    {% set sql %}
        SELECT
            m.LOAD_STATUS,
            m.DQ_STATUS,
            m.ERROR_MESSAGE,
            m.FINISHED_AT,
            r.RUN_STATUS
        FROM {{ enterprise_control_relation('ETL_MODEL_STATS') }} m
        JOIN {{ enterprise_control_relation('ETL_RUN_CONTROL') }} r
          ON r.ETL_NR = m.ETL_NR
        WHERE m.DBT_MODEL_ID ILIKE '%.' || '{{ safe_model }}'
          AND m.TARGET_RELATION ILIKE '%' || '{{ safe_target }}' || '%'
        ORDER BY m.STARTED_AT DESC
        LIMIT 1
    {% endset %}

    {% set result = run_query(sql) %}
    {% if result is none or result.rows | length != 1 %}
        {{ exceptions.raise_compiler_error(
            'RUNTIME_FAILURE_AUDIT_NOT_FOUND | model=' ~ model_name
            ~ ' target=' ~ target_identifier
        ) }}
    {% endif %}

    {% set row = result.rows[0] %}
    {% set load_status = row[0] | string %}
    {% set dq_status = row[1] | string %}
    {% set error_message = '' if row[2] is none else (row[2] | string) %}
    {% set finished_at = row[3] %}
    {% set run_status = row[4] | string %}

    {# Unexpected failures are conservatively repaired by on-run-end because
       dbt cannot infer transaction outcome from a generic adapter/database error. #}
    {% if load_status != 'LOAD_OUTCOME_UNKNOWN' %}
        {{ exceptions.raise_compiler_error(
            'RUNTIME_FAILURE_AUDIT_BAD_LOAD_STATUS | expected=LOAD_OUTCOME_UNKNOWN'
            ~ ' observed=' ~ load_status
        ) }}
    {% endif %}

    {% if dq_status != 'PENDING' %}
        {{ exceptions.raise_compiler_error(
            'RUNTIME_FAILURE_AUDIT_BAD_DQ_STATUS | expected=PENDING'
            ~ ' observed=' ~ dq_status
        ) }}
    {% endif %}

    {% if error_message | length == 0 %}
        {{ exceptions.raise_compiler_error(
            'RUNTIME_FAILURE_AUDIT_MISSING_ERROR_MESSAGE'
        ) }}
    {% endif %}

    {% if finished_at is none %}
        {{ exceptions.raise_compiler_error(
            'RUNTIME_FAILURE_AUDIT_MISSING_FINISHED_AT'
        ) }}
    {% endif %}

    {% if run_status != 'FAILED' %}
        {{ exceptions.raise_compiler_error(
            'RUNTIME_FAILURE_AUDIT_BAD_RUN_STATUS | expected=FAILED'
            ~ ' observed=' ~ run_status
        ) }}
    {% endif %}

    {% do log(
        'RUNTIME_FAILURE_AUDIT_VERIFIED'
        ~ ' | LOAD_STATUS=' ~ load_status
        ~ ' | DQ_STATUS=' ~ dq_status
        ~ ' | RUN_STATUS=' ~ run_status,
        info=true
    ) %}
{% endmacro %}
