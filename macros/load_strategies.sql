{# ===== From: macros/load_strategies.sql | definition bodies unchanged ===== #}

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

{# ===== From: macros/insert_strategy.sql | definition bodies unchanged ===== #}

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


{# ===== From: macros/update_strategy.sql | definition bodies unchanged ===== #}

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


{# ===== From: macros/replace_strategy.sql | definition bodies unchanged ===== #}

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


{# ===== From: macros/delete_insert_strategy.sql | definition bodies unchanged ===== #}

{# Shared natural-key predicate for DELETE_INSERT. #}

{% macro generate_delete_insert_key_match(
    natural_keys,
    target_alias,
    source_alias
) %}

    {% if natural_keys | length == 0 %}
        {{ exceptions.raise_compiler_error(
            'DELETE_INSERT: Natural keys are required.'
        ) }}
    {% endif %}

    {% set conditions = [] %}

    {% for key in natural_keys %}

        {% set quoted_key = adapter.quote(key | upper) %}

        {% do conditions.append(
            target_alias ~ '.' ~ quoted_key
            ~ ' = '
            ~ source_alias ~ '.' ~ quoted_key
        ) %}

    {% endfor %}

    {% do return(conditions | join(' AND ')) %}

{% endmacro %}


{# Delete only target records matching prepared source keys. #}

{% macro generate_delete_insert_delete_sql(
    source_relation,
    target_relation,
    natural_keys
) %}

    {% set condition = generate_delete_insert_key_match(
        natural_keys,
        adapter.quote(target_relation.identifier),
        adapter.quote(source_relation.identifier)
    ) %}

    {% set delete_sql %}

        DELETE FROM {{ target_relation }}

        USING {{ source_relation }}

        WHERE {{ condition }}

    {% endset %}

    {% do return(delete_sql) %}

{% endmacro %}
