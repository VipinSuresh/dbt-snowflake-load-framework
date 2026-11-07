
{# Generate a full-replacement INSERT for a governed Snowflake target.
   This macro generates SQL only; it never executes DELETE or INSERT.
#}

{% macro generate_replace_insert_sql(
    source_relation,
    target_relation,
    etl_nr,
    process_name='enterprise_dbt_framework'
) %}

    {% set source_columns =
        adapter.get_columns_in_relation(source_relation) %}

    {% if source_columns | length == 0 %}
        {{ exceptions.raise_compiler_error(
            'TRUNCATE_LOAD: No business columns found in source.'
        ) }}
    {% endif %}

    {% set insert_columns = [] %}
    {% set select_values = [] %}

    {# Copy all validated business columns. #}

    {% for col in source_columns %}

        {% set q = adapter.quote(col.name) %}

        {% do insert_columns.append(q) %}
        {% do select_values.append('src.' ~ q) %}

    {% endfor %}

    {# New replacement rows receive new creation audit metadata. #}

    {% set run_timestamp =
        "CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)" %}

    {% set safe_process = process_name | replace("'", "''") %}
    {% set process_literal = "'" ~ safe_process ~ "'" %}

    {% set audit_values = {
        'CREATED_BY_PROCESS': process_literal,
        'CREATED_AT': run_timestamp,
        'ETL_CREATED_RUN_NUMBER': etl_nr | string,
        'ETL_CREATED_AT': run_timestamp,
        'UPDATED_BY_PROCESS': 'NULL',
        'UPDATED_AT': 'NULL',
        'ETL_UPDATED_RUN_NUMBER': 'NULL',
        'ETL_UPDATED_AT': 'NULL'
    } %}

    {% for col, value in audit_values.items() %}

        {% do insert_columns.append(adapter.quote(col)) %}
        {% do select_values.append(value) %}

    {% endfor %}

    {# INSERT only. DELETE is executed separately inside
       the same Snowflake transaction by truncate_load.sql.
    #}

    {% set insert_sql %}

        INSERT INTO {{ target_relation }}
        (
            {{ insert_columns | join(', ') }}
        )

        SELECT
            {{ select_values | join(',\n            ') }}

        FROM {{ source_relation }} AS src

    {% endset %}

    {% do return(insert_sql) %}

{% endmacro %}
