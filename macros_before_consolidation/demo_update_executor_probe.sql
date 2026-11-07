
{% macro demo_update_executor_probe() %}

    {% set table_name =
        'DBT_DEMO.DBT_WORK.EL_UPDATE_EXEC_'
        ~ (invocation_id | replace('-', '') | upper)
    %}

    {% do run_query(
        'CREATE TEMPORARY TABLE ' ~ table_name
        ~ ' (ID NUMBER, VAL VARCHAR)'
    ) %}

    {% do run_query(
        "INSERT INTO " ~ table_name
        ~ " VALUES (1, 'Original'), (2, 'Same')"
    ) %}

    {% set merge_sql %}

        MERGE INTO {{ table_name }} tgt

        USING (
            SELECT
                COLUMN1::NUMBER AS ID,
                COLUMN2::VARCHAR AS VAL
            FROM VALUES
                (1, 'Changed'),
                (2, 'Same'),
                (3, 'New')
        ) src

        ON tgt.ID = src.ID

        WHEN MATCHED
          AND tgt.VAL IS DISTINCT FROM src.VAL
        THEN UPDATE SET
            VAL = src.VAL

    {% endset %}

    {# First run: one actual update. #}

    {% set first = execute_snowflake_merge(
        merge_sql,
        'update_executor_first',
        update_only=true
    ) %}

    {# Second run: no changes. #}

    {% set second = execute_snowflake_merge(
        merge_sql,
        'update_executor_second',
        update_only=true
    ) %}

    {% set check_sql %}
        SELECT
            COUNT(*) AS TOTAL_ROWS,
            COALESCE(COUNT_IF(
                ID = 1 AND VAL = 'Changed'
            ), 0) AS UPDATED_ROW,
            COALESCE(COUNT_IF(
                ID = 3
            ), 0) AS UNEXPECTED_INSERT
        FROM {{ table_name }}
    {% endset %}

    {% set result = run_query(check_sql) %}

    {% set total = result.rows[0][0] | int %}
    {% set updated = result.rows[0][1] | int %}
    {% set unexpected = result.rows[0][2] | int %}

    {% do log(
        'UPDATE EXECUTOR FIRST | inserted='
        ~ first['rows_inserted']
        ~ ' | updated=' ~ first['rows_updated']
        ~ ' | deleted=' ~ first['rows_deleted'],
        info=true
    ) %}

    {% do log(
        'UPDATE EXECUTOR SECOND | inserted='
        ~ second['rows_inserted']
        ~ ' | updated=' ~ second['rows_updated']
        ~ ' | deleted=' ~ second['rows_deleted'],
        info=true
    ) %}

    {% do run_query(
        'DROP TABLE IF EXISTS ' ~ table_name
    ) %}

    {% if first['rows_inserted'] != 0
       or first['rows_updated'] != 1
       or first['rows_deleted'] != 0
       or second['rows_inserted'] != 0
       or second['rows_updated'] != 0
       or second['rows_deleted'] != 0
       or total != 2
       or updated != 1
       or unexpected != 0 %}

        {{ exceptions.raise_compiler_error(
            'UPDATE EXECUTOR PROBE FAILED'
        ) }}

    {% endif %}

    {% do log('UPDATE EXECUTOR PROBE PASSED', info=true) %}

{% endmacro %}
