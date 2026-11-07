
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
