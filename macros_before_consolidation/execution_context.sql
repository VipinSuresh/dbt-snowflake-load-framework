{# dbt invocation/run control. One sequence-generated ETL_NR per command. #}

{% macro start_etl_run() %}
    INSERT INTO DBT_DEMO.ETL_CONTROL.ETL_RUN_CONTROL
    (
        ETL_NR, DBT_INVOCATION_ID, EXECUTION_SOURCE,
        JOB_NAME, DBT_TARGET_NAME, RUN_STARTED_AT, RUN_STATUS
    )
    SELECT
        DBT_DEMO.ETL_CONTROL.ETL_NR_SEQ.NEXTVAL,
        '{{ invocation_id }}',
        'DBT_CORE',
        '{{ project_name }}',
        '{{ target.name }}',
        CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9),
        'RUNNING'
    WHERE NOT EXISTS (
        SELECT 1
        FROM DBT_DEMO.ETL_CONTROL.ETL_RUN_CONTROL
        WHERE DBT_INVOCATION_ID = '{{ invocation_id }}'
    )
{% endmacro %}


{# End hook repairs model status on dbt errors AFTER individual SQL commands. #}
{% macro finish_etl_run() %}
    {% set failed = [] %}
    {% set failure_messages = [] %}

    {% for result in results %}
        {% set status = result.status | string | lower %}
        {% if status in ['error', 'fail'] %}
            {% do failed.append(result.node.unique_id) %}
            {% set node_id = result.node.unique_id | string %}
            {% set reason = result.message | default(
                'dbt execution failed without an error message', true
            ) | string %}

            {% do failure_messages.append(node_id ~ ': ' ~ reason) %}
            {% if result.node.resource_type == 'model' %}
                {% set safe_model = (result.node.unique_id | string) | replace("'", "''") %}
                {% set reason = (result.message | default('dbt model execution failed', true)) | string %}
                {% set safe_reason = (reason | replace("'", "''"))[:3900] %}
                {% set repair_sql %}
                    UPDATE DBT_DEMO.ETL_CONTROL.ETL_MODEL_STATS
                    SET
                        LOAD_STATUS = CASE
                            WHEN LOAD_STATUS IN ('PREPARING', 'PRE_LOAD_RUNNING')
                                THEN 'PREPARATION_FAILED'
                            WHEN LOAD_STATUS = 'PRE_LOAD_PASSED'
                                THEN 'EXECUTION_FAILED_BEFORE_DML'
                            WHEN LOAD_STATUS = 'MERGE_RUNNING'
                                THEN 'LOAD_OUTCOME_UNKNOWN'
                            WHEN LOAD_STATUS = 'DML_COMMITTED'
                                THEN 'LOAD_COMMITTED_DBT_FAILED'
                            WHEN LOAD_STATUS = 'SUCCESS'
                                THEN 'LOAD_COMMITTED_DBT_FAILED'
                            ELSE LOAD_STATUS
                        END,
                        DQ_STATUS = CASE
                            WHEN LOAD_STATUS IN ('PREPARING', 'PRE_LOAD_RUNNING')
                                THEN 'FAIL'
                            ELSE DQ_STATUS
                        END,
                        ERROR_MESSAGE = COALESCE(ERROR_MESSAGE, '{{ safe_reason }}'),
                        FINISHED_AT = COALESCE(
                            FINISHED_AT,
                            CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)
                        )
                    WHERE ETL_NR = (
                        SELECT ETL_NR
                        FROM DBT_DEMO.ETL_CONTROL.ETL_RUN_CONTROL
                        WHERE DBT_INVOCATION_ID = '{{ invocation_id }}'
                    )
                    AND DBT_MODEL_ID = '{{ safe_model }}'
                {% endset %}
                {% do run_query(repair_sql) %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {% set combined_error = failure_messages | join(' | ') %}
    {% set safe_run_error = (combined_error | replace("'", "''"))[:3900] %}

    UPDATE DBT_DEMO.ETL_CONTROL.ETL_RUN_CONTROL
    SET
    RUN_ENDED_AT = CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9),
    RUN_STATUS = '{{ "FAILED" if failed | length > 0 else "SUCCESS" }}',
    ERROR_MESSAGE = {% if failed | length > 0 %}
        '{{ safe_run_error }}'
    {% else %}
        NULL
    {% endif %}
    WHERE DBT_INVOCATION_ID = '{{ invocation_id }}'
{% endmacro %}
