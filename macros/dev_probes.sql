{# ===== From: macros/demo_insert_dml_probe.sql | definition bodies unchanged ===== #}

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


{# ===== From: macros/demo_insert_noop_metadata.sql | definition bodies unchanged ===== #}

{% macro demo_insert_noop_metadata() %}

    {% set table_name =
        'DBT_DEMO.DBT_WORK.EL_NOOP_'
        ~ (invocation_id | replace('-', '') | upper)
    %}

    {% do run_query(
        'CREATE TEMPORARY TABLE ' ~ table_name
        ~ ' (ID NUMBER, VAL VARCHAR)'
    ) %}

    {% do run_query(
        "INSERT INTO " ~ table_name
        ~ " VALUES (1, 'Original')"
    ) %}

    {# Source key already exists: no INSERT should occur. #}
    {% call statement(
        'noop_merge',
        fetch_result=True,
        auto_begin=False
    ) %}

        MERGE INTO {{ table_name }} tgt
        USING (
            SELECT 1 AS ID, 'Changed' AS VAL
        ) src
        ON tgt.ID = src.ID

        WHEN NOT MATCHED THEN
            INSERT (ID, VAL)
            VALUES (src.ID, src.VAL)

    {% endcall %}

    {% set result = load_result('noop_merge') %}
    {% set response = result['response'] %}
    {% set result_table = result['table'] %}

    {% do log(
        'NOOP RESPONSE | ' ~ (response | string),
        info=true
    ) %}

    {% if response.rows_affected is defined %}
        {% do log(
            'NOOP rows_affected=' ~ response.rows_affected,
            info=true
        ) %}
    {% endif %}

    {% if response.rows_inserted is defined %}
        {% do log(
            'NOOP rows_inserted=' ~ response.rows_inserted,
            info=true
        ) %}
    {% endif %}

    {% if response.rows_updated is defined %}
        {% do log(
            'NOOP rows_updated=' ~ response.rows_updated,
            info=true
        ) %}
    {% endif %}

    {% if result_table is not none %}

        {% do log(
            'NOOP COLUMNS | '
            ~ (result_table.column_names | join(' | ')),
            info=true
        ) %}

        {% do log(
            'NOOP RESULT ROW COUNT | '
            ~ (result_table.rows | length),
            info=true
        ) %}

        {% for row in result_table.rows %}

            {% set values = [] %}

            {% for col in result_table.column_names %}
                {% do values.append(
                    row[loop.index0] | string
                ) %}
            {% endfor %}

            {% do log(
                'NOOP VALUES | ' ~ (values | join(' | ')),
                info=true
            ) %}

        {% endfor %}

    {% else %}

        {% do log('NOOP RESULT TABLE | None', info=true) %}

    {% endif %}

    {% set check = run_query(
        "SELECT COUNT(*), "
        ~ "COUNT_IF(ID = 1 AND VAL = 'Original') "
        ~ 'FROM ' ~ table_name
    ) %}

    {% do log(
        'NOOP DATA | total=' ~ check.rows[0][0]
        ~ ' | original_preserved=' ~ check.rows[0][1],
        info=true
    ) %}

    {% do run_query(
        'DROP TABLE IF EXISTS ' ~ table_name
    ) %}

{% endmacro %}


{# ===== From: macros/demo_update_dml_probe.sql | definition bodies unchanged ===== #}

{% macro demo_update_dml_probe() %}

    {% set table_name =
        'DBT_DEMO.DBT_WORK.EL_UPDATE_PROBE_'
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

    {% set update_sql %}

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
        THEN UPDATE SET VAL = src.VAL

    {% endset %}

    {# First execution: expect one updated row. #}

    {% call statement(
        'update_probe_first',
        fetch_result=True,
        auto_begin=False
    ) %}
        {{ update_sql }}
    {% endcall %}

    {% set first = load_result('update_probe_first') %}

    {% do log(
        'UPDATE FIRST RESPONSE | ' ~ first['response'],
        info=true
    ) %}

    {% if first['table'] is not none %}
        {% do log(
            'UPDATE FIRST COLUMNS | '
            ~ (first['table'].column_names | join(', ')),
            info=true
        ) %}
        {% do log(
            'UPDATE FIRST VALUES | '
            ~ (first['table'].rows[0] | string),
            info=true
        ) %}
    {% endif %}

    {# Second execution: expect zero changes. #}

    {% call statement(
        'update_probe_second',
        fetch_result=True,
        auto_begin=False
    ) %}
        {{ update_sql }}
    {% endcall %}

    {% set second = load_result('update_probe_second') %}

    {% do log(
        'UPDATE SECOND RESPONSE | ' ~ second['response'],
        info=true
    ) %}

    {% if second['table'] is not none %}
        {% do log(
            'UPDATE SECOND COLUMNS | '
            ~ (second['table'].column_names | join(', ')),
            info=true
        ) %}
        {% do log(
            'UPDATE SECOND VALUES | '
            ~ (second['table'].rows[0] | string),
            info=true
        ) %}
    {% endif %}

    {% set result = run_query(
        'SELECT COUNT(*), '
        ~ "COALESCE(COUNT_IF(ID = 1 AND VAL = 'Changed'), 0), "
        ~ 'COALESCE(COUNT_IF(ID = 3), 0) '
        ~ 'FROM ' ~ table_name
    ) %}

    {% set total = result.rows[0][0] | int %}
    {% set updated = result.rows[0][1] | int %}
    {% set unexpected_insert = result.rows[0][2] | int %}

    {% do log(
        'UPDATE DATA | total=' ~ total
        ~ ' | updated=' ~ updated
        ~ ' | unexpected_insert=' ~ unexpected_insert,
        info=true
    ) %}

    {% do run_query('DROP TABLE IF EXISTS ' ~ table_name) %}

    {% if total != 2 or updated != 1 or unexpected_insert != 0 %}
        {{ exceptions.raise_compiler_error(
            'UPDATE PROBE: Unexpected target state.'
        ) }}
    {% endif %}

    {% do log('UPDATE DATA PROBE PASSED', info=true) %}

{% endmacro %}


{# ===== From: macros/demo_update_executor_probe.sql | definition bodies unchanged ===== #}

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
