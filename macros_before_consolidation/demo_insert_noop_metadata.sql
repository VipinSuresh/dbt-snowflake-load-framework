
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
