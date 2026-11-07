
{% macro generate_type1_merge_sql(
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

    {% for key in natural_keys %}
        {% do normalized_keys.append(key | upper) %}
    {% endfor %}

    {% for col in source_columns %}
        {% do business_columns.append(col.name | upper) %}
    {% endfor %}

    {# Validate keys before generating SQL #}

    {% if normalized_keys | length == 0 %}
        {{ exceptions.raise_compiler_error(
            'TYPE1: At least one natural key is required.'
        ) }}
    {% endif %}

    {% for key in normalized_keys %}
        {% if key not in business_columns %}
            {{ exceptions.raise_compiler_error(
                'TYPE1: Natural key missing from source: ' ~ key
            ) }}
        {% endif %}
    {% endfor %}

    {# Identify non-key business columns #}

    {% set value_columns = [] %}

    {% for col in business_columns %}
        {% if col not in normalized_keys %}
            {% do value_columns.append(col) %}
        {% endif %}
    {% endfor %}

    {% if value_columns | length == 0 %}
        {{ exceptions.raise_compiler_error(
            'TYPE1: No business attributes to update.'
        ) }}
    {% endif %}

    {# Build natural-key matching conditions #}

    {% set join_conditions = [] %}

    {% for key in normalized_keys %}
        {% set q = adapter.quote(key) %}

        {% do join_conditions.append(
            'tgt.' ~ q ~ ' = src.' ~ q
        ) %}
    {% endfor %}

    {# NULL-safe change detection #}

    {% set change_conditions = [] %}
    {% set update_assignments = [] %}

    {% for col in value_columns %}
        {% set q = adapter.quote(col) %}

        {% do change_conditions.append(
            'tgt.' ~ q ~ ' IS DISTINCT FROM src.' ~ q
        ) %}

        {% do update_assignments.append(
            q ~ ' = src.' ~ q
        ) %}
    {% endfor %}

    {# Consistent execution metadata #}

    {% set run_timestamp =
        "CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)" %}

    {% set safe_process = process_name | replace("'", "''") %}
    {% set process_literal = "'" ~ safe_process ~ "'" %}

    {% do update_assignments.append(
        '"UPDATED_BY_PROCESS" = ' ~ process_literal
    ) %}

    {% do update_assignments.append(
        '"UPDATED_AT" = ' ~ run_timestamp
    ) %}

    {% do update_assignments.append(
        '"ETL_UPDATED_RUN_NUMBER" = ' ~ (etl_nr | string)
    ) %}

    {% do update_assignments.append(
        '"ETL_UPDATED_AT" = ' ~ run_timestamp
    ) %}

    {# Build explicit INSERT column/value lists #}

    {% set insert_columns = [] %}
    {% set insert_values = [] %}

    {% for col in business_columns %}
        {% set q = adapter.quote(col) %}

        {% do insert_columns.append(q) %}
        {% do insert_values.append('src.' ~ q) %}
    {% endfor %}

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

    {# Generate Snowflake TYPE1 MERGE #}

    {% set merge_sql %}

        MERGE INTO {{ target_relation }} AS tgt

        USING {{ source_relation }} AS src

        ON {{ join_conditions | join(' AND ') }}

        WHEN MATCHED AND (
            {{ change_conditions | join(' OR ') }}
        )
        THEN UPDATE SET
            {{ update_assignments | join(',\n            ') }}

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


{# Preview wrapper — generates SQL without executing it #}

{% macro demo_preview_type1_merge() %}

    {% set source_relation = adapter.get_relation(
        database='DBT_DEMO',
        schema='DBT_WORK',
        identifier='INT_CUSTOMERS'
    ) %}

    {% set target_relation = adapter.get_relation(
        database='DBT_DEMO',
        schema='MARTS',
        identifier='DIM_CUSTOMER'
    ) %}

    {% if source_relation is none or target_relation is none %}
        {{ exceptions.raise_compiler_error(
            'DEMO: Source or target relation is missing.'
        ) }}
    {% endif %}

    {# Reuse the pre-load validation already tested #}

    {% do check_pre_load(
        source_relation,
        target_relation,
        ['CUSTOMER_ID'],
        true
    ) %}

    {# ETL number is a preview placeholder here #}

    {% set merge_sql = generate_type1_merge_sql(
        source_relation,
        target_relation,
        ['CUSTOMER_ID'],
        1001,
        'enterprise_dbt_framework'
    ) %}

    {% do log(
        'TYPE1 MERGE SQL PREVIEW — NOT EXECUTED:\n'
        ~ merge_sql,
        info=true
    ) %}

{% endmacro %}
