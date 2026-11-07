
{# UPDATE-only MERGE for an existing governed Snowflake target. #}

{% macro generate_update_merge_sql(
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

    {# Validate natural keys. #}

    {% if normalized_keys | length == 0 %}
        {{ exceptions.raise_compiler_error(
            'UPDATE: At least one natural key is required.'
        ) }}
    {% endif %}

    {% for key in normalized_keys %}
        {% if key not in business_columns %}
            {{ exceptions.raise_compiler_error(
                'UPDATE: Natural key missing from source: ' ~ key
            ) }}
        {% endif %}
    {% endfor %}

    {# Identify business attributes excluding natural keys. #}

    {% set value_columns = [] %}

    {% for col in business_columns %}
        {% if col not in normalized_keys %}
            {% do value_columns.append(col) %}
        {% endif %}
    {% endfor %}

    {% if value_columns | length == 0 %}
        {{ exceptions.raise_compiler_error(
            'UPDATE: No non-key business attributes to update.'
        ) }}
    {% endif %}

    {# Build natural-key matching conditions. #}

    {% set join_conditions = [] %}

    {% for key in normalized_keys %}
        {% set q = adapter.quote(key) %}

        {% do join_conditions.append(
            'tgt.' ~ q ~ ' = src.' ~ q
        ) %}
    {% endfor %}

    {# NULL-safe change detection and update assignments. #}

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

    {# Populate update audit metadata. #}

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

    {# UPDATE-only MERGE: no INSERT or DELETE actions. #}

    {% set merge_sql %}

        MERGE INTO {{ target_relation }} AS tgt

        USING {{ source_relation }} AS src

        ON {{ join_conditions | join(' AND ') }}

        WHEN MATCHED AND (
            {{ change_conditions | join(' OR ') }}
        )

        THEN UPDATE SET
            {{ update_assignments | join(',\n            ') }}

    {% endset %}

    {% do return(merge_sql) %}

{% endmacro %}
