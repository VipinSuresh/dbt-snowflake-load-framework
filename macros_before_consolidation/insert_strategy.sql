
{# Generate INSERT-only MERGE for an existing governed Snowflake target. #}

{% macro generate_insert_merge_sql(
    source_relation,
    target_relation,
    natural_keys,
    etl_nr,
    process_name='enterprise_dbt_framework'
) %}

    {% set source_columns =
        adapter.get_columns_in_relation(source_relation) %}

    {% set business_columns = [] %}
    {% set normalized_keys = [] %}

    {% for col in source_columns %}
        {% do business_columns.append(col.name | upper) %}
    {% endfor %}

    {% for key in natural_keys %}
        {% do normalized_keys.append(key | upper) %}
    {% endfor %}

    {% if normalized_keys | length == 0 %}
        {{ exceptions.raise_compiler_error(
            'INSERT: At least one natural key is required.'
        ) }}
    {% endif %}

    {% for key in normalized_keys %}
        {% if key not in business_columns %}
            {{ exceptions.raise_compiler_error(
                'INSERT: Natural key missing from source: ' ~ key
            ) }}
        {% endif %}
    {% endfor %}

    {# Match existing rows by natural keys. #}

    {% set join_conditions = [] %}

    {% for key in normalized_keys %}
        {% set q = adapter.quote(key) %}
        {% do join_conditions.append(
            'tgt.' ~ q ~ ' = src.' ~ q
        ) %}
    {% endfor %}

    {# Build explicit business-column INSERT mapping. #}

    {% set insert_columns = [] %}
    {% set insert_values = [] %}

    {% for col in business_columns %}
        {% set q = adapter.quote(col) %}
        {% do insert_columns.append(q) %}
        {% do insert_values.append('src.' ~ q) %}
    {% endfor %}

    {# Populate the standard eight audit columns. #}

    {% set run_timestamp =
        "CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)" %}

    {% set safe_process = process_name | replace("'", "''") %}
    {% set process_literal = "'" ~ safe_process ~ "'" %}

    {% set audit_insert_values = {
        'CREATED_BY_PROCESS': process_literal,
        'CREATED_AT': run_timestamp,
        'ETL_CREATED_RUN_NUMBER': etl_nr | string,
        'ETL_CREATED_AT': run_timestamp,
        'UPDATED_BY_PROCESS': 'NULL',
        'UPDATED_AT': 'NULL',
        'ETL_UPDATED_RUN_NUMBER': 'NULL',
        'ETL_UPDATED_AT': 'NULL'
    } %}

    {% for col, value in audit_insert_values.items() %}
        {% do insert_columns.append(adapter.quote(col)) %}
        {% do insert_values.append(value) %}
    {% endfor %}

    {# No WHEN MATCHED clause: existing keys remain untouched. #}

    {% set merge_sql %}

        MERGE INTO {{ target_relation }} AS tgt

        USING {{ source_relation }} AS src

        ON {{ join_conditions | join(' AND ') }}

        WHEN NOT MATCHED
        THEN INSERT (
            {{ insert_columns | join(', ') }}
        )
        VALUES (
            {{ insert_values | join(', ') }}
        )

    {% endset %}

    {% do return(merge_sql) %}

{% endmacro %}
