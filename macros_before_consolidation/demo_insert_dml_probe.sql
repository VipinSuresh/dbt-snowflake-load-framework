
{% macro demo_insert_dml_probe() %}

    {# Unique temporary table; governed targets are untouched. #}
    {% set table_name =
        'DBT_DEMO.DBT_WORK.EL_INSERT_PROBE_'
        ~ (invocation_id | replace('-', '') | upper)
    %}

    {% do run_query(
        'CREATE TEMPORARY TABLE ' ~ table_name
        ~ ' (ID NUMBER, CUSTOMER_NAME VARCHAR)'
    ) %}

    {# Existing row must never be updated. #}
    {% do run_query(
        "INSERT INTO " ~ table_name
        ~ " (ID, CUSTOMER_NAME) VALUES (1, 'Original')"
    ) %}

    {% set merge_sql %}

        MERGE INTO {{ table_name }} AS tgt

        USING (
            SELECT
                COLUMN1::NUMBER AS ID,
                COLUMN2::VARCHAR AS CUSTOMER_NAME
            FROM VALUES
                (1, 'Changed'),
                (2, 'New')
        ) AS src

        ON tgt.ID = src.ID

        WHEN NOT MATCHED THEN
            INSERT (ID, CUSTOMER_NAME)
            VALUES (src.ID, src.CUSTOMER_NAME)

    {% endset %}

    {# Call the actual shared executor. #}
    {% set stats = execute_snowflake_merge(
        merge_sql, 'insert_probe_merge'
    ) %}

    {% set result = run_query(
        'SELECT COUNT(*), '
        ~ "COALESCE(COUNT_IF(ID = 1 AND CUSTOMER_NAME = 'Original'), 0), "
        ~ "COALESCE(COUNT_IF(ID = 2 AND CUSTOMER_NAME = 'New'), 0) "
        ~ 'FROM ' ~ table_name
    ) %}

    {% set total = result.rows[0][0] | int %}
    {% set original_preserved = result.rows[0][1] | int %}
    {% set new_inserted = result.rows[0][2] | int %}

    {% do log(
        'INSERT PROBE | inserted=' ~ stats['rows_inserted']
        ~ ' | updated=' ~ stats['rows_updated']
        ~ ' | deleted=' ~ stats['rows_deleted']
        ~ ' | target_rows=' ~ total
        ~ ' | original_preserved=' ~ original_preserved
        ~ ' | new_inserted=' ~ new_inserted,
        info=true
    ) %}

    {% set passed =
        stats['rows_inserted'] == 1
        and stats['rows_updated'] == 0
        and stats['rows_deleted'] == 0
        and total == 2
        and original_preserved == 1
        and new_inserted == 1
    %}

    {% do run_query('DROP TABLE IF EXISTS ' ~ table_name) %}

    {% if not passed %}
        {{ exceptions.raise_compiler_error(
            'INSERT PROBE FAILED: Row counts or target data mismatch.'
        ) }}
    {% endif %}

    {% do log('INSERT PROBE PASSED', info=true) %}

{% endmacro %}
