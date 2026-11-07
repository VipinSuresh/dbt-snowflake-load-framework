
{% macro start_etl_run() %}

    INSERT INTO DBT_DEMO.ETL_CONTROL.ETL_RUN_CONTROL
    (
        ETL_NR,
        DBT_INVOCATION_ID,
        EXECUTION_SOURCE,
        JOB_NAME,
        DBT_TARGET_NAME,
        RUN_STARTED_AT,
        RUN_STATUS
    )
    SELECT
        DBT_DEMO.ETL_CONTROL.ETL_NR_SEQ.NEXTVAL,
        '{{ invocation_id }}',
        'DBT_CORE',
        '{{ project_name }}',
        '{{ target.name }}',
        CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ,
        'RUNNING'
    WHERE NOT EXISTS (
        SELECT 1
        FROM DBT_DEMO.ETL_CONTROL.ETL_RUN_CONTROL
        WHERE DBT_INVOCATION_ID = '{{ invocation_id }}'
    )

{% endmacro %}


{% macro finish_etl_run() %}

    {% set failures = [] %}

    {% for result in results %}
        {% if result.status | string | lower in ['error', 'fail'] %}
            {% do failures.append(result.node.unique_id) %}
        {% endif %}
    {% endfor %}

    UPDATE DBT_DEMO.ETL_CONTROL.ETL_RUN_CONTROL
    SET
        RUN_ENDED_AT =
            CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ,
        RUN_STATUS =
            '{{ "FAILED" if failures | length > 0 else "SUCCESS" }}'
    WHERE DBT_INVOCATION_ID = '{{ invocation_id }}'

{% endmacro %}