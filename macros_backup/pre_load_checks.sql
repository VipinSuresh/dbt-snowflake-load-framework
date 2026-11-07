
{% macro check_pre_load(
    source_relation,
    target_relation,
    natural_keys,
    require_nonempty=true
) %}

    {% if not execute %}
        {% do return({}) %}
    {% endif %}

    {# 1. Validate source and target existence #}

    {% if source_relation is none %}
        {{ exceptions.raise_compiler_error(
            'PRE_LOAD: Source relation does not exist.'
        ) }}
    {% endif %}

    {% if target_relation is none %}
        {{ exceptions.raise_compiler_error(
            'PRE_LOAD: Target table does not exist. Deploy DDL first.'
        ) }}
    {% endif %}

    {% if target_relation.type | lower != 'table' %}
        {{ exceptions.raise_compiler_error(
            'PRE_LOAD: Target must be an existing physical table.'
        ) }}
    {% endif %}

    {# 2. Retrieve source and target metadata #}

    {% set src_cols = adapter.get_columns_in_relation(source_relation) %}
    {% set tgt_cols = adapter.get_columns_in_relation(target_relation) %}

    {% set src_names = [] %}
    {% set tgt_names = [] %}
    {% set src_map = {} %}
    {% set tgt_map = {} %}

    {% for c in src_cols %}
        {% do src_names.append(c.name | upper) %}
        {% do src_map.update({c.name | upper: c}) %}
    {% endfor %}

    {% for c in tgt_cols %}
        {% do tgt_names.append(c.name | upper) %}
        {% do tgt_map.update({c.name | upper: c}) %}
    {% endfor %}

    {# 3. Framework-managed audit columns #}

    {% set audit_types = {
        'CREATED_BY_PROCESS': 'STRING',
        'CREATED_AT': 'TIMESTAMP_NTZ',
        'ETL_CREATED_RUN_NUMBER': 'NUMBER',
        'ETL_CREATED_AT': 'TIMESTAMP_NTZ',
        'UPDATED_BY_PROCESS': 'STRING',
        'UPDATED_AT': 'TIMESTAMP_NTZ',
        'ETL_UPDATED_RUN_NUMBER': 'NUMBER',
        'ETL_UPDATED_AT': 'TIMESTAMP_NTZ'
    } %}

    {% set business_tgt_names = [] %}

    {% for name in tgt_names %}
        {% if name not in audit_types %}
            {% do business_tgt_names.append(name) %}
        {% endif %}
    {% endfor %}

    {# 4. Exact business-column name matching #}

    {% if (src_names | sort) != (business_tgt_names | sort) %}
        {{ exceptions.raise_compiler_error(
            'PRE_LOAD: Source business columns do not match target.'
            ~ ' Source=' ~ (src_names | join(','))
            ~ ' Target=' ~ (business_tgt_names | join(','))
        ) }}
    {% endif %}

    {# 5. Validate audit columns exist #}

    {% for name in audit_types.keys() %}
        {% if name not in tgt_names %}
            {{ exceptions.raise_compiler_error(
                'PRE_LOAD: Missing target audit column: ' ~ name
            ) }}
        {% endif %}
    {% endfor %}

    {# 6. Check basic data-type compatibility #}

    {% set string_types = [
        'VARCHAR',
        'CHARACTER VARYING',
        'CHAR',
        'CHARACTER',
        'TEXT',
        'STRING'
    ]%}

    {% set number_types = [
        'NUMBER',
        'DECIMAL',
        'NUMERIC',
        'INTEGER',
        'INT',
        'BIGINT',
        'FIXED'
    ] %}

    {% for name in src_names %}

        {% set src_type =
            (src_map[name].data_type | string | upper).split('(')[0] | trim %}
        {% set tgt_type =
            (tgt_map[name].data_type | string | upper).split('(')[0] | trim %}

        {% set compatible =
            src_type == tgt_type
            or (src_type in string_types and tgt_type in string_types)
            or (src_type in number_types and tgt_type in number_types)
        %}

        {% if not compatible %}
            {{ exceptions.raise_compiler_error(
                'PRE_LOAD: Type mismatch for ' ~ name
                ~ ': source=' ~ src_type
                ~ ', target=' ~ tgt_type
            ) }}
        {% endif %}

    {% endfor %}

    {# Validate framework audit-column type families #}

    {% for name, expected in audit_types.items() %}

        {% set actual =
            (tgt_map[name].data_type | string | upper).split('(')[0] | trim %}

        {% set valid =
            (expected == 'STRING' and actual in string_types)
            or (expected == 'NUMBER' and actual in number_types)
            or (expected == 'TIMESTAMP_NTZ' and actual in ['TIMESTAMP_NTZ','TIMESTAMP WITHOUT TIME ZONE'])
        %}

        {% if not valid %}
            {{ exceptions.raise_compiler_error(
                'PRE_LOAD: Invalid audit-column type for '
                ~ name ~ ': ' ~ actual
            ) }}
        {% endif %}

    {% endfor %}

    {# 7. Validate natural-key configuration #}

    {% if natural_keys is string or natural_keys is not sequence
          or natural_keys | length == 0 %}
        {{ exceptions.raise_compiler_error(
            'PRE_LOAD: At least one natural key is required.'
        ) }}
    {% endif %}

    {% set quoted_keys = [] %}
    {% set null_conditions = [] %}

    {% for key in natural_keys %}
        {% set normalized_key = key | upper %}

        {% if normalized_key not in src_names %}
            {{ exceptions.raise_compiler_error(
                'PRE_LOAD: Natural key not found: ' ~ key
            ) }}
        {% endif %}

        {% set quoted_key = adapter.quote(normalized_key) %}
        {% do quoted_keys.append(quoted_key) %}
        {% do null_conditions.append(quoted_key ~ ' IS NULL') %}
    {% endfor %}

    {# 8. Source count and NULL-key checks #}

    {% set count_sql %}
        SELECT
            COUNT(*) AS SOURCE_ROWS,
            COALESCE(
                SUM(
                    CASE
                        WHEN {{ null_conditions | join(' OR ') }}
                        THEN 1 ELSE 0
                    END
                ), 0
            ) AS NULL_KEY_ROWS
        FROM {{ source_relation }}
    {% endset %}

    {% set counts = run_query(count_sql) %}
    {% set source_count = counts.rows[0][0] | int %}
    {% set null_count = counts.rows[0][1] | int %}

    {% if require_nonempty and source_count == 0 %}
        {{ exceptions.raise_compiler_error(
            'PRE_LOAD: Source contains zero records.'
        ) }}
    {% endif %}

    {% if null_count > 0 %}
        {{ exceptions.raise_compiler_error(
            'PRE_LOAD: Found ' ~ null_count
            ~ ' records with NULL natural keys.'
        ) }}
    {% endif %}

    {# 9. Duplicate natural-key checks #}

    {% set duplicate_sql %}
        SELECT COUNT(*)
        FROM (
            SELECT {{ quoted_keys | join(', ') }}
            FROM {{ source_relation }}
            GROUP BY {{ quoted_keys | join(', ') }}
            HAVING COUNT(*) > 1
        ) duplicate_groups
    {% endset %}

    {% set dup_result = run_query(duplicate_sql) %}
    {% set duplicate_count = dup_result.rows[0][0] | int %}

    {% if duplicate_count > 0 %}
        {{ exceptions.raise_compiler_error(
            'PRE_LOAD: Found ' ~ duplicate_count
            ~ ' duplicate natural-key groups.'
        ) }}
    {% endif %}

    {{ log(
        'PRE_LOAD PASSED | Source=' ~ source_relation
        ~ ' | Target=' ~ target_relation
        ~ ' | Rows=' ~ source_count
        ~ ' | NULL keys=' ~ null_count
        ~ ' | Duplicate key groups=' ~ duplicate_count,
        info=true
    ) }}

    {% do return({
        'source_row_count': source_count,
        'null_key_count': null_count,
        'duplicate_key_groups': duplicate_count
    }) %}

{% endmacro %}


{# Test wrapper only; not part of the generic load lifecycle #}

{% macro demo_check_pre_load(source_identifier='INT_CUSTOMERS') %}

    {% set source_relation = adapter.get_relation(
        database='DBT_DEMO',
        schema='DBT_WORK',
        identifier=source_identifier
    ) %}

    {% set target_relation = adapter.get_relation(
        database='DBT_DEMO',
        schema='MARTS',
        identifier='DIM_CUSTOMER'
    ) %}

    {% do check_pre_load(
        source_relation,
        target_relation,
        ['CUSTOMER_ID'],
        true
    ) %}

{% endmacro %}


{% macro check_type1_target_keys(target_relation, natural_keys) %}

    {% set quoted_keys = [] %}
    {% set null_conditions = [] %}

    {% for key in natural_keys %}
        {% set q = adapter.quote(key | upper) %}
        {% do quoted_keys.append(q) %}
        {% do null_conditions.append(q ~ ' IS NULL') %}
    {% endfor %}

    {% if quoted_keys | length == 0 %}
        {{ exceptions.raise_compiler_error(
            'TYPE1: Natural keys are required.'
        ) }}
    {% endif %}

    {% set sql %}
        SELECT
            (SELECT COUNT(*)
             FROM {{ target_relation }}
             WHERE {{ null_conditions | join(' OR ') }})
                AS NULL_KEY_ROWS,

            (SELECT COUNT(*)
             FROM (
                 SELECT {{ quoted_keys | join(', ') }}
                 FROM {{ target_relation }}
                 GROUP BY {{ quoted_keys | join(', ') }}
                 HAVING COUNT(*) > 1
             ))
                AS DUPLICATE_KEY_GROUPS
    {% endset %}

    {% set result = run_query(sql) %}
    {% set null_count = result.rows[0][0] | int %}
    {% set duplicate_count = result.rows[0][1] | int %}

    {% if null_count > 0 or duplicate_count > 0 %}
        {{ exceptions.raise_compiler_error(
            'TYPE1: Target key validation failed. '
            ~ 'NULL keys=' ~ null_count
            ~ ', duplicate groups=' ~ duplicate_count
        ) }}
    {% endif %}

    {% do log('TYPE1 TARGET KEY VALIDATION PASSED', info=true) %}

{% endmacro %}

