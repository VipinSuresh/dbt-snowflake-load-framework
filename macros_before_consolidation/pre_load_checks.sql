{# Pre-load checks return structured evidence; callers decide whether to abort. #}
{# For this v1 integration, target-key uniqueness applies to TYPE1 only. #}

{% macro check_pre_load(
    source_relation, target_relation, natural_keys,
    require_nonempty=true, model_execution_id=none, etl_nr=none
) %}
    {% if not execute %}
        {% do return({'passed': true, 'source_row_count': none, 'failed_checks': []}) %}
    {% endif %}

    {% set failures = [] %}
    {% set source_count = none %}
    {% set audited = model_execution_id is not none and etl_nr is not none %}

    {# 1. Validate relation existence and target type before metadata calls. #}
    {% set relation_ok = (
        source_relation is not none
        and target_relation is not none
        and target_relation.type | lower == 'table'
    ) %}
    {% if audited %}
        {% do record_pre_load_check(model_execution_id, etl_nr,
            'RELATION_CONTRACT', relation_ok, 1 if relation_ok else 0, 1,
            'Source and existing physical target must be available') %}
    {% endif %}
    {% if not relation_ok %}
        {% do failures.append('RELATION_CONTRACT') %}
        {% do return({'passed': false, 'source_row_count': none,
            'failed_checks': failures}) %}
    {% endif %}

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

    {% set audit_types = {
        'CREATED_BY_PROCESS': 'VARCHAR',
        'CREATED_AT': 'TIMESTAMP_NTZ',
        'ETL_CREATED_RUN_NUMBER': 'NUMBER',
        'ETL_CREATED_AT': 'TIMESTAMP_NTZ',
        'UPDATED_BY_PROCESS': 'VARCHAR',
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

    {# 2. Verify exact business-column set + eight mandatory audit columns. #}
    {% set names_match = (src_names | sort) == (business_tgt_names | sort) %}
    {% set missing_audit = [] %}
    {% for name in audit_types.keys() %}
        {% if name not in tgt_names %}
            {% do missing_audit.append(name) %}
        {% endif %}
    {% endfor %}
    {% set columns_ok = names_match and (missing_audit | length == 0) %}
    {% if audited %}
        {% do record_pre_load_check(model_execution_id, etl_nr,
            'COLUMN_CONTRACT', columns_ok, 1 if columns_ok else 0, 1,
            'Source=' ~ src_names | join(',') ~ '; target business=' ~
            (business_tgt_names | join(',')) ~ '; missing audit=' ~
            (missing_audit | join(','))) %}
    {% endif %}
    {% if not columns_ok %}
        {% do failures.append('COLUMN_CONTRACT') %}
    {% endif %}

    {# Normalize Snowflake/adapter aliases; this does not verify lengths/precision. #}
    {% set string_types = ['VARCHAR','CHAR','CHARACTER','TEXT','STRING','CHARACTER VARYING'] %}
    {% set number_types = ['NUMBER','DECIMAL','NUMERIC','INTEGER','INT','BIGINT','FIXED'] %}
    {% set timestamp_types = ['TIMESTAMP_NTZ','TIMESTAMP WITHOUT TIME ZONE'] %}
    {% set type_failures = [] %}
    {% for name in src_names %}
        {% if name in tgt_map %}
            {% set s = (src_map[name].data_type | string | upper).split('(')[0] | trim %}
            {% set t = (tgt_map[name].data_type | string | upper).split('(')[0] | trim %}
            {% set compatible = s == t
                or (s in string_types and t in string_types)
                or (s in number_types and t in number_types) %}
            {% if not compatible %}
                {% do type_failures.append(name ~ ':' ~ s ~ '->' ~ t) %}
            {% endif %}
        {% endif %}
    {% endfor %}
    {% for name, expected in audit_types.items() %}
        {% if name in tgt_map %}
            {% set t = (tgt_map[name].data_type | string | upper).split('(')[0] | trim %}
            {% set compatible =
                (expected == 'VARCHAR' and t in string_types)
                or (expected == 'NUMBER' and t in number_types)
                or (expected == 'TIMESTAMP_NTZ' and t in timestamp_types) %}
            {% if not compatible %}
                {% do type_failures.append(name ~ ':expected ' ~ expected ~ ', found ' ~ t) %}
            {% endif %}
        {% endif %}
    {% endfor %}
    {% set types_ok = type_failures | length == 0 %}
    {% if audited %}
        {% do record_pre_load_check(model_execution_id, etl_nr,
            'TYPE_CONTRACT', types_ok, 1 if types_ok else 0, 1,
            type_failures | join('; ')) %}
    {% endif %}
    {% if not types_ok %}
        {% do failures.append('TYPE_CONTRACT') %}
    {% endif %}

    {# 3. Confirm natural keys exist and are ordinary identifiers. #}
    {% set normalized_keys = [] %}
    {% set invalid_keys = [] %}
    {% set keys_are_list = natural_keys is sequence and natural_keys is not string and natural_keys | length > 0 %}
    {% if keys_are_list %}
        {% for key in natural_keys %}
            {% if key is not string or key | upper not in src_names %}
                {% do invalid_keys.append(key | string) %}
            {% else %}
                {% do normalized_keys.append(key | upper) %}
            {% endif %}
        {% endfor %}
    {% endif %}
    {% set keys_valid = keys_are_list and invalid_keys | length == 0 %}
    {% if audited %}
        {% do record_pre_load_check(model_execution_id, etl_nr,
            'KEY_CONFIGURATION', keys_valid, 1 if keys_valid else 0, 1,
            'Keys=' ~ (normalized_keys | join(','))) %}
    {% endif %}
    {% if not keys_valid %}
        {% do failures.append('KEY_CONFIGURATION') %}
    {% endif %}

    {# Stop before row-level SQL if source/target metadata is incompatible. #}
    {% if failures | length > 0 %}
        {% do return({'passed': false, 'source_row_count': none,
            'failed_checks': failures}) %}
    {% endif %}

    {% set quoted_keys = [] %}
    {% set src_null_conditions = [] %}
    {% set tgt_null_conditions = [] %}
    {% for key in normalized_keys %}
        {% set q = adapter.quote(key) %}
        {% do quoted_keys.append(q) %}
        {% do src_null_conditions.append('src.' ~ q ~ ' IS NULL') %}
        {% do tgt_null_conditions.append('tgt.' ~ q ~ ' IS NULL') %}
    {% endfor %}

    {# 4. Source volume / NULL / duplicate checks. #}
    {% set source_sql %}
        SELECT COUNT(*) AS SOURCE_ROWS,
               COALESCE(SUM(IFF({{ src_null_conditions | join(' OR ') }}, 1, 0)), 0)
                   AS NULL_KEY_ROWS
        FROM {{ source_relation }} src
    {% endset %}
    {% set source_result = run_query(source_sql) %}
    {% set source_count = source_result.rows[0][0] | int %}
    {% set src_nulls = source_result.rows[0][1] | int %}

    {% set nonempty_ok = (not require_nonempty) or source_count > 0 %}
    {% if audited %}
        {% do record_pre_load_check(model_execution_id, etl_nr,
            'SOURCE_NONEMPTY', nonempty_ok, source_count,
            1 if require_nonempty else 0, 'Prepared source row count') %}
        {% do record_pre_load_check(model_execution_id, etl_nr,
            'SOURCE_NULL_KEYS', src_nulls == 0, src_nulls, 0,
            'Rows containing a NULL natural-key component') %}
    {% endif %}
    {% if not nonempty_ok %}
        {% do failures.append('SOURCE_NONEMPTY') %}
    {% endif %}
    {% if src_nulls > 0 %}
        {% do failures.append('SOURCE_NULL_KEYS') %}
    {% endif %}

    {% set source_dup_sql %}
        SELECT COUNT(*) FROM (
            SELECT {{ quoted_keys | join(', ') }}
            FROM {{ source_relation }}
            GROUP BY {{ quoted_keys | join(', ') }}
            HAVING COUNT(*) > 1
        ) d
    {% endset %}
    {% set source_dups = run_query(source_dup_sql).rows[0][0] | int %}
    {% if audited %}
        {% do record_pre_load_check(model_execution_id, etl_nr,
            'SOURCE_DUPLICATE_KEYS', source_dups == 0, source_dups, 0,
            'Number of duplicate natural-key groups') %}
    {% endif %}
    {% if source_dups > 0 %}
        {% do failures.append('SOURCE_DUPLICATE_KEYS') %}
    {% endif %}

    {# 5. TYPE1 destination safety: don't merge into an already corrupt target. #}
    {% set target_null_sql %}
        SELECT COUNT(*) FROM {{ target_relation }} tgt
        WHERE {{ tgt_null_conditions | join(' OR ') }}
    {% endset %}
    {% set target_nulls = run_query(target_null_sql).rows[0][0] | int %}
    {% if audited %}
        {% do record_pre_load_check(model_execution_id, etl_nr,
            'TARGET_NULL_KEYS', target_nulls == 0, target_nulls, 0,
            'Rows containing a NULL natural-key component') %}
    {% endif %}
    {% if target_nulls > 0 %}
        {% do failures.append('TARGET_NULL_KEYS') %}
    {% endif %}

    {% set target_dup_sql %}
        SELECT COUNT(*) FROM (
            SELECT {{ quoted_keys | join(', ') }}
            FROM {{ target_relation }}
            GROUP BY {{ quoted_keys | join(', ') }}
            HAVING COUNT(*) > 1
        ) d
    {% endset %}
    {% set target_dups = run_query(target_dup_sql).rows[0][0] | int %}
    {% if audited %}
        {% do record_pre_load_check(model_execution_id, etl_nr,
            'TARGET_DUPLICATE_KEYS', target_dups == 0, target_dups, 0,
            'Number of existing target duplicate natural-key groups') %}
    {% endif %}
    {% if target_dups > 0 %}
        {% do failures.append('TARGET_DUPLICATE_KEYS') %}
    {% endif %}

    {% do log('PRE_LOAD SUMMARY | Rows=' ~ source_count
        ~ ' | failed checks=' ~ (failures | join(', ')), info=true) %}

    {% do return({
        'passed': failures | length == 0,
        'source_row_count': source_count,
        'null_key_count': src_nulls,
        'duplicate_key_groups': source_dups,
        'failed_checks': failures
    }) %}
{% endmacro %}


{# Standalone wrapper retained for isolated checks; raises errors like earlier demo. #}
{% macro demo_check_pre_load(source_identifier='INT_CUSTOMERS') %}
    {% set src = adapter.get_relation(
        database='DBT_DEMO', schema='DBT_WORK', identifier=source_identifier
    ) %}
    {% set tgt = adapter.get_relation(
        database='DBT_DEMO', schema='MARTS', identifier='DIM_CUSTOMER'
    ) %}
    {% set result = check_pre_load(src, tgt, ['CUSTOMER_ID'], true) %}
    {% if not result['passed'] %}
        {{ exceptions.raise_compiler_error(
            'PRE_LOAD: Failed checks: ' ~ (result['failed_checks'] | join(', '))
        ) }}
    {% endif %}
    {% do log('PRE_LOAD PASSED | Source=' ~ src ~ ' | Target=' ~ tgt
        ~ ' | Rows=' ~ result['source_row_count'], info=true) %}
{% endmacro %}
