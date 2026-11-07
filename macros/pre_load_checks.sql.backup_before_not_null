{# ===== From: macros/pre_load_checks.sql | definition bodies unchanged ===== #}
{# Pre-load checks return structured evidence; callers decide whether to abort. #}
{# For this v1 integration, target-key uniqueness applies to TYPE1 only. #}

{# Metadata-driven validation for non-key business columns declared NOT NULL.
   The source contains business fields only; framework-generated audit and SCD2
   metadata columns are excluded automatically. No YAML list is needed. #}
{% macro check_source_required_not_null(
    source_relation, target_relation, natural_keys,
    source_columns, target_columns,
    model_execution_id=none, etl_nr=none
) %}
    {% if not execute %}
        {% do return({'passed': true, 'failed_checks': []}) %}
    {% endif %}

    {% set audited = model_execution_id is not none and etl_nr is not none %}
    {% set source_name_map = {} %}
    {% set target_names = [] %}
    {% set normalized_keys = [] %}

    {% for col in source_columns %}
        {% do source_name_map.update({col.name | upper: col.name}) %}
    {% endfor %}
    {% for col in target_columns %}
        {% do target_names.append(col.name | upper) %}
    {% endfor %}
    {% for key in natural_keys %}
        {% do normalized_keys.append(key | upper) %}
    {% endfor %}

    {# Consult the governed physical table, not source inferred types.
       Fail closed if its metadata is missing or incomplete. #}
    {% set safe_schema = target_relation.schema | string | replace("'", "''") %}
    {% set safe_table = target_relation.identifier | string | replace("'", "''") %}
    {% set metadata_sql %}
        SELECT COLUMN_NAME, IS_NULLABLE
        FROM {{ adapter.quote(target_relation.database) }}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{{ safe_schema }}'
          AND TABLE_NAME = '{{ safe_table }}'
        ORDER BY ORDINAL_POSITION
    {% endset %}
    {% set metadata = run_query(metadata_sql) %}
    {% set metadata_names = [] %}
    {% if metadata is not none %}
        {% for row in metadata.rows %}
            {% do metadata_names.append(row[0] | string | upper) %}
        {% endfor %}
    {% endif %}

    {% if (metadata_names | sort) != (target_names | sort) %}
        {% if audited %}
            {% do record_pre_load_check(
                model_execution_id, etl_nr,
                'SOURCE_NULLABILITY_METADATA', false, 0, 1,
                'Target NULLability metadata unavailable or incomplete for ' ~ target_relation
            ) %}
        {% endif %}
        {% do return({
            'passed': false,
            'failed_checks': ['SOURCE_NULLABILITY_METADATA']
        }) %}
    {% endif %}

    {% set required_columns = [] %}
    {% for row in metadata.rows %}
        {% set name = row[0] | string | upper %}
        {% set nullable = row[1] | string | upper %}
        {% if nullable == 'NO'
            and name in source_name_map
            and name not in normalized_keys %}
            {% do required_columns.append(source_name_map[name]) %}
        {% endif %}
    {% endfor %}

    {% set violating_columns = [] %}
    {% set null_counts = [] %}
    {% if required_columns | length > 0 %}
        {# One aggregate query over the prepared source for all required fields. #}
        {% set expressions = [] %}
        {% for column in required_columns %}
            {% set q = adapter.quote(column) %}
            {% do expressions.append(
                'COALESCE(COUNT_IF(src.' ~ q ~ ' IS NULL), 0) AS NULL_COUNT_' ~ loop.index0
            ) %}
        {% endfor %}
        {% set check_sql %}
            SELECT {{ expressions | join(',\n                   ') }}
            FROM {{ source_relation }} src
        {% endset %}
        {% set counts_result = run_query(check_sql) %}
        {% for column in required_columns %}
            {% set null_count = counts_result.rows[0][loop.index0] | int %}
            {% do null_counts.append(null_count) %}
            {% if null_count > 0 %}
                {% do violating_columns.append(column ~ '=' ~ null_count) %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {% set total_null_cells = null_counts | sum %}
    {% set passed = total_null_cells == 0 %}
    {% set detail = 'Required business columns=' ~ (required_columns | join(','))
        ~ '; NULL counts=' ~ (violating_columns | join(',')) %}
    {% if audited %}
        {% do record_pre_load_check(
            model_execution_id, etl_nr,
            'SOURCE_REQUIRED_NOT_NULL', passed, total_null_cells, 0, detail
        ) %}
    {% endif %}

    {% do return({
        'passed': passed,
        'failed_checks': [] if passed else ['SOURCE_REQUIRED_NOT_NULL']
    }) %}
{% endmacro %}


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

    {# Validate non-key source business columns marked NOT NULL in target DDL. #}
    {% set required_null_result = check_source_required_not_null(
        source_relation, target_relation, normalized_keys,
        src_cols, tgt_cols, model_execution_id, etl_nr
    ) %}
    {% if not required_null_result['passed'] %}
        {% for failed_check in required_null_result['failed_checks'] %}
            {% do failures.append(failed_check) %}
        {% endfor %}
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


{# ===== From: macros/source_volume_threshold.sql | definition bodies unchanged ===== #}
{# Optional pre-load source volume guard using the immediately preceding successful model run. #}
{# Failure blocks the MERGE. Baseline is a completed successful run with passed DQ. #}

{% macro record_source_volume_baseline_skip(model_execution_id, etl_nr, detail) %}
    {% set safe_detail = (detail | string | replace("'", "''"))[:1900] %}
    {% set sql %}
        INSERT INTO DBT_DEMO.ETL_CONTROL.ETL_DQ_RESULTS
        (
            DQ_RESULT_ID, MODEL_EXECUTION_ID, ETL_NR,
            CHECK_NAME, CHECK_PHASE, CHECK_STATUS,
            OBSERVED_VALUE, REFERENCE_VALUE, DETAILS, CHECKED_AT
        )
        SELECT
            UUID_STRING(), '{{ model_execution_id }}', {{ etl_nr }},
            'SOURCE_VOLUME_BASELINE', 'PRE_LOAD', 'SKIP',
            NULL, NULL, OBJECT_CONSTRUCT('message', '{{ safe_detail }}'),
            CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)
    {% endset %}
    {% do run_query(sql) %}
    {% do log('PRE_LOAD SOURCE_VOLUME_BASELINE | SKIP | ' ~ detail, info=true) %}
{% endmacro %}

{% macro check_source_volume_threshold(
    model_execution_id, etl_nr, model_id, current_source_count, threshold_cfg
) %}
    {% if threshold_cfg is none or threshold_cfg == {} %}
        {% do return({'passed': true, 'failed_checks': []}) %}
    {% endif %}
    {% if threshold_cfg is not mapping %}
        {{ exceptions.raise_compiler_error(
            'VOLUME_THRESHOLD: volume_threshold must be a YAML mapping.'
        ) }}
    {% endif %}
    {% set enabled = threshold_cfg.get('enabled', false) %}
    {% if enabled is not boolean %}
        {{ exceptions.raise_compiler_error(
            'VOLUME_THRESHOLD: enabled must be true or false.'
        ) }}
    {% endif %}
    {% if not enabled %}
        {% do return({'passed': true, 'failed_checks': []}) %}
    {% endif %}

    {% set max_drop = threshold_cfg.get('max_drop_pct') %}
    {% set max_growth = threshold_cfg.get('max_growth_pct') %}
    {% if max_drop is none and max_growth is none %}
        {{ exceptions.raise_compiler_error(
            'VOLUME_THRESHOLD: specify max_drop_pct and/or max_growth_pct when enabled.'
        ) }}
    {% endif %}
    {% if max_drop is not none %}
        {% if max_drop is not number or max_drop is boolean or max_drop < 0 or max_drop > 100 %}
            {{ exceptions.raise_compiler_error(
                'VOLUME_THRESHOLD: max_drop_pct must be a number between 0 and 100.'
            ) }}
        {% endif %}
    {% endif %}
    {% if max_growth is not none %}
        {% if max_growth is not number or max_growth is boolean or max_growth < 0 %}
            {{ exceptions.raise_compiler_error(
                'VOLUME_THRESHOLD: max_growth_pct must be a nonnegative number.'
            ) }}
        {% endif %}
    {% endif %}

    {# Current execution is still PRE_LOAD_RUNNING, so it cannot be its own baseline. #}
    {% set safe_model = (model_id | string) | replace("'", "''") %}
    {% set baseline_sql %}
        SELECT m.SOURCE_ROW_COUNT
        FROM DBT_DEMO.ETL_CONTROL.ETL_MODEL_STATS m
        JOIN DBT_DEMO.ETL_CONTROL.ETL_RUN_CONTROL r
          ON m.ETL_NR = r.ETL_NR
        WHERE m.DBT_MODEL_ID = '{{ safe_model }}'
          AND m.MODEL_EXECUTION_ID <> '{{ model_execution_id }}'
          AND m.SOURCE_ROW_COUNT IS NOT NULL
          AND m.LOAD_STATUS = 'SUCCESS'
          AND m.DQ_STATUS = 'PASS'
          AND r.RUN_STATUS = 'SUCCESS'
        ORDER BY m.FINISHED_AT DESC, m.STARTED_AT DESC
        LIMIT 1
    {% endset %}
    {% set baseline = run_query(baseline_sql) %}
    {% if baseline is none or baseline.rows | length == 0 %}
        {% do record_source_volume_baseline_skip(model_execution_id, etl_nr,
            'No previous fully successful execution for model ' ~ model_id
            ~ '; historical thresholds were not evaluated.') %}
        {% do return({'passed': true, 'failed_checks': [], 'baseline': none}) %}
    {% endif %}
    {% set previous_count = baseline.rows[0][0] | int %}
    {% set current_count = current_source_count | int %}

    {# Percent change is undefined for a zero baseline; report an explicit SKIP. #}
    {% if previous_count == 0 %}
        {% do record_source_volume_baseline_skip(model_execution_id, etl_nr,
            'Previous successful source count is zero; percent change undefined.') %}
        {% do return({'passed': true, 'failed_checks': [], 'baseline': 0}) %}
    {% endif %}

    {% set drop_pct = (100.0 * ([previous_count - current_count, 0] | max)
                        / previous_count) | round(2) %}
    {% set growth_pct = (100.0 * ([current_count - previous_count, 0] | max)
                          / previous_count) | round(2) %}
    {% set failures = [] %}

    {% if max_drop is not none %}
        {% set drop_ok = drop_pct <= max_drop %}
        {% do record_pre_load_check(model_execution_id, etl_nr,
            'SOURCE_VOLUME_DROP_PCT', drop_ok, drop_pct, max_drop,
            'Previous successful source rows=' ~ previous_count
            ~ '; current source rows=' ~ current_count ~ '; units=percent') %}
        {% if not drop_ok %}
            {% do failures.append('SOURCE_VOLUME_DROP_PCT') %}
        {% endif %}
    {% endif %}

    {% if max_growth is not none %}
        {% set growth_ok = growth_pct <= max_growth %}
        {% do record_pre_load_check(model_execution_id, etl_nr,
            'SOURCE_VOLUME_GROWTH_PCT', growth_ok, growth_pct, max_growth,
            'Previous successful source rows=' ~ previous_count
            ~ '; current source rows=' ~ current_count ~ '; units=percent') %}
        {% if not growth_ok %}
            {% do failures.append('SOURCE_VOLUME_GROWTH_PCT') %}
        {% endif %}
    {% endif %}

    {% do return({
        'passed': failures | length == 0,
        'failed_checks': failures,
        'baseline': previous_count,
        'current_source_count': current_count,
        'drop_pct': drop_pct,
        'growth_pct': growth_pct
    }) %}
{% endmacro %}


{# ===== From: macros/type2_checks.sql :: check_type2_pre_load | definition bodies unchanged ===== #}
{# TYPE2-specific validation: history naturally contains repeated natural keys. #}
{% macro check_type2_pre_load(source_relation, target_relation, natural_keys, require_nonempty, model_execution_id, etl_nr) %}
  {% set failures = [] %}
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
    'CREATED_BY_PROCESS':'VARCHAR','CREATED_AT':'TIMESTAMP_NTZ',
    'ETL_CREATED_RUN_NUMBER':'NUMBER','ETL_CREATED_AT':'TIMESTAMP_NTZ',
    'UPDATED_BY_PROCESS':'VARCHAR','UPDATED_AT':'TIMESTAMP_NTZ',
    'ETL_UPDATED_RUN_NUMBER':'NUMBER','ETL_UPDATED_AT':'TIMESTAMP_NTZ'
  } %}
  {% set scd_types = {
    'VALID_FROM':'TIMESTAMP_NTZ','VALID_TO':'TIMESTAMP_NTZ',
    'IS_CURRENT':'BOOLEAN','VERSION_NO':'NUMBER'
  } %}
  {% set metadata = dict(audit_types, **scd_types) %}
  {% set business_target = [] %}
  {% set missing = [] %}
  {% for name in tgt_names %}
    {% if name not in metadata %}{% do business_target.append(name) %}{% endif %}
  {% endfor %}
  {% for name in metadata.keys() %}
    {% if name not in tgt_names %}{% do missing.append(name) %}{% endif %}
  {% endfor %}
  {% set reserved = [] %}
  {% for col in ['EL_ACTION','EL_VERSION'] %}
    {% if col in src_names %}{% do reserved.append(col) %}{% endif %}
  {% endfor %}
  {% set columns_ok = (src_names | sort) == (business_target | sort) and missing | length == 0 and reserved | length == 0 %}
  {% do record_pre_load_check(model_execution_id, etl_nr, 'TYPE2_COLUMN_CONTRACT',
      columns_ok, 1 if columns_ok else 0, 1,
      'Missing metadata=' ~ (missing | join(',')) ~ '; reserved source columns=' ~ (reserved | join(','))) %}
  {% if not columns_ok %}
    {% do failures.append('TYPE2_COLUMN_CONTRACT') %}
    {% do return({'passed': false, 'source_row_count': none, 'failed_checks': failures}) %}
  {% endif %}

  {# Check broad Snowflake type compatibility; precision/length enforcement remains a v2 enhancement. #}
  {% set strings = ['VARCHAR','CHAR','CHARACTER','TEXT','STRING','CHARACTER VARYING'] %}
  {% set nums = ['NUMBER','DECIMAL','NUMERIC','INTEGER','INT','BIGINT','FIXED'] %}
  {% set timestamps = ['TIMESTAMP_NTZ','TIMESTAMP WITHOUT TIME ZONE'] %}
  {% set bools = ['BOOLEAN'] %}
  {% set type_errors = [] %}
  {% for name in src_names %}
    {% set s = ((src_map[name].data_type | string | upper).split('(')[0]) | trim %}
    {% set t = ((tgt_map[name].data_type | string | upper).split('(')[0]) | trim %}
    {% if not (s == t or (s in strings and t in strings) or (s in nums and t in nums)) %}
      {% do type_errors.append(name ~ ':' ~ s ~ '->' ~ t) %}
    {% endif %}
  {% endfor %}
  {% for name, expected in metadata.items() %}
    {% set t = ((tgt_map[name].data_type | string | upper).split('(')[0]) | trim %}
    {% if not ((expected == 'VARCHAR' and t in strings)
             or (expected == 'NUMBER' and t in nums)
             or (expected == 'TIMESTAMP_NTZ' and t in timestamps)
             or (expected == 'BOOLEAN' and t in bools)) %}
      {% do type_errors.append(name ~ ': expected ' ~ expected ~ ' found ' ~ t) %}
    {% endif %}
  {% endfor %}
  {% do record_pre_load_check(model_execution_id, etl_nr, 'TYPE2_TYPE_CONTRACT',
      type_errors | length == 0, 1 if type_errors | length == 0 else 0, 1, type_errors | join('; ')) %}
  {% if type_errors | length > 0 %}
    {% do failures.append('TYPE2_TYPE_CONTRACT') %}
    {% do return({'passed': false, 'source_row_count': none, 'failed_checks': failures}) %}
  {% endif %}

  {% set keys = [] %}
  {% set null_conditions = [] %}
  {% set t_null_conditions = [] %}
  {% set src_tgt_join = [] %}
  {% for key in natural_keys %}
    {% if key not in src_names %}
      {% do failures.append('TYPE2_KEY_CONFIGURATION') %}
    {% else %}
      {% set q = adapter.quote(key) %}
      {% do keys.append(q) %}
      {% do null_conditions.append('s.' ~ q ~ ' IS NULL') %}
      {% do t_null_conditions.append('t.' ~ q ~ ' IS NULL') %}
      {% do src_tgt_join.append('s.' ~ q ~ ' = h.' ~ q) %}
    {% endif %}
  {% endfor %}
  {% do record_pre_load_check(model_execution_id, etl_nr, 'TYPE2_KEY_CONFIGURATION',
      failures | length == 0, 1 if failures | length == 0 else 0, 1, 'Keys=' ~ natural_keys | join(',')) %}
  {% if failures | length > 0 %}
    {% do return({'passed': false, 'source_row_count': none, 'failed_checks': failures}) %}
  {% endif %}

  {% set count_sql %}
    SELECT COUNT(*), COUNT_IF({{ null_conditions | join(' OR ') }})
    FROM {{ source_relation }} s
  {% endset %}
  {% set count_result = run_query(count_sql) %}
  {% set source_count = count_result.rows[0][0] | int %}
  {% set nulls = count_result.rows[0][1] | default(0, true) | int %}
  {% set nonempty_ok = not require_nonempty or source_count > 0 %}
  {% do record_pre_load_check(model_execution_id, etl_nr, 'TYPE2_SOURCE_NONEMPTY',
      nonempty_ok, source_count, 1 if require_nonempty else 0, 'Prepared source count') %}
  {% do record_pre_load_check(model_execution_id, etl_nr, 'TYPE2_SOURCE_NULL_KEYS',
      nulls == 0, nulls, 0, 'NULL natural keys in prepared source') %}
  {% if not nonempty_ok %}{% do failures.append('TYPE2_SOURCE_NONEMPTY') %}{% endif %}
  {% if nulls > 0 %}{% do failures.append('TYPE2_SOURCE_NULL_KEYS') %}{% endif %}

  {% set duplicate_source_sql %}
    SELECT COUNT(*) FROM (
      SELECT {{ keys | join(', ') }} FROM {{ source_relation }}
      GROUP BY {{ keys | join(', ') }} HAVING COUNT(*) > 1
    ) bad
  {% endset %}
  {% set duplicate_source = run_query(duplicate_source_sql).rows[0][0] | int %}
  {% do record_pre_load_check(model_execution_id, etl_nr, 'TYPE2_SOURCE_DUPLICATES',
      duplicate_source == 0, duplicate_source, 0, 'Duplicate natural-key groups') %}
  {% if duplicate_source > 0 %}{% do failures.append('TYPE2_SOURCE_DUPLICATES') %}{% endif %}

  {# Same required-business-column NULL rule as the other five strategies. #}
  {% set required_null_result = check_source_required_not_null(
      source_relation, target_relation, natural_keys,
      src_cols, tgt_cols, model_execution_id, etl_nr
  ) %}
  {% if not required_null_result['passed'] %}
    {% for failed_check in required_null_result['failed_checks'] %}
      {% do failures.append(failed_check) %}
    {% endfor %}
  {% endif %}

  {% set bad_target_keys_sql %}
    SELECT COUNT(*) FROM {{ target_relation }} t
    WHERE {{ t_null_conditions | join(' OR ') }}
  {% endset %}
  {% set bad_target_keys = run_query(bad_target_keys_sql).rows[0][0] | int %}
  {% do record_pre_load_check(model_execution_id, etl_nr, 'TYPE2_HISTORY_NULL_KEYS',
      bad_target_keys == 0, bad_target_keys, 0, 'NULL natural keys in target history') %}
  {% if bad_target_keys > 0 %}{% do failures.append('TYPE2_HISTORY_NULL_KEYS') %}{% endif %}

  {# Multiple historical versions per key are VALID; multiple current ones are not. #}
  {% set duplicate_current_sql %}
    SELECT COUNT(*) FROM (
      SELECT {{ keys | join(', ') }} FROM {{ target_relation }}
      WHERE IS_CURRENT = TRUE GROUP BY {{ keys | join(', ') }} HAVING COUNT(*) > 1
    ) bad
  {% endset %}
  {% set dup_curr = run_query(duplicate_current_sql).rows[0][0] | int %}
  {% do record_pre_load_check(model_execution_id, etl_nr, 'TYPE2_DUPLICATE_CURRENT',
      dup_curr == 0, dup_curr, 0, 'Multiple current versions for one natural key') %}
  {% if dup_curr > 0 %}{% do failures.append('TYPE2_DUPLICATE_CURRENT') %}{% endif %}

  {% set invalid_states_sql %}
    SELECT COUNT(*) FROM {{ target_relation }}
    WHERE (IS_CURRENT = TRUE AND VALID_TO IS NOT NULL)
       OR (IS_CURRENT = FALSE AND VALID_TO IS NULL)
       OR (VALID_TO IS NOT NULL AND VALID_TO <= VALID_FROM)
       OR VERSION_NO IS NULL OR VERSION_NO < 1
  {% endset %}
  {% set invalid_states = run_query(invalid_states_sql).rows[0][0] | int %}
  {% do record_pre_load_check(model_execution_id, etl_nr, 'TYPE2_INVALID_VERSION_STATE',
      invalid_states == 0, invalid_states, 0, 'Malformed validity windows/current flag/version') %}
  {% if invalid_states > 0 %}{% do failures.append('TYPE2_INVALID_VERSION_STATE') %}{% endif %}

  {% set duplicate_versions_sql %}
    SELECT COUNT(*) FROM (
      SELECT {{ keys | join(', ') }}, VERSION_NO FROM {{ target_relation }}
      GROUP BY {{ keys | join(', ') }}, VERSION_NO HAVING COUNT(*) > 1
    ) bad
  {% endset %}
  {% set dup_ver = run_query(duplicate_versions_sql).rows[0][0] | int %}
  {% do record_pre_load_check(model_execution_id, etl_nr, 'TYPE2_DUPLICATE_VERSION_NUMBER',
      dup_ver == 0, dup_ver, 0, 'Version number must be unique per natural key') %}
  {% if dup_ver > 0 %}{% do failures.append('TYPE2_DUPLICATE_VERSION_NUMBER') %}{% endif %}

  {# Every known history key must have one current version, even if absent from today's source. #}
  {% set missing_current_sql %}
    SELECT COUNT(*) FROM (
      SELECT {{ keys | join(', ') }} FROM {{ target_relation }}
      GROUP BY {{ keys | join(', ') }}
      HAVING COALESCE(COUNT_IF(IS_CURRENT = TRUE), 0) <> 1
    ) bad
  {% endset %}
  {% set missing_current = run_query(missing_current_sql).rows[0][0] | int %}
  {% do record_pre_load_check(model_execution_id, etl_nr, 'TYPE2_CURRENT_STATE',
      missing_current == 0, missing_current, 0, 'Every known history key requires exactly one current version') %}
  {% if missing_current > 0 %}{% do failures.append('TYPE2_CURRENT_STATE') %}{% endif %}

  {# Validate overlapping and missing TYPE2 history boundaries. #}
  {% set history_boundaries_sql %}
    SELECT
      COALESCE(COUNT_IF(
        NEXT_FROM IS NOT NULL
        AND VALID_TO IS NOT NULL
        AND VALID_TO > NEXT_FROM
      ), 0) AS OVERLAP_COUNT,

      COALESCE(COUNT_IF(
        NEXT_FROM IS NOT NULL
        AND (
          VALID_TO IS NULL
          OR VALID_TO < NEXT_FROM
        )
      ), 0) AS GAP_COUNT

    FROM (
      SELECT
        {{ keys | join(', ') }},
        VALID_FROM,
        VALID_TO,
        VERSION_NO,
        LEAD(VALID_FROM) OVER (
          PARTITION BY {{ keys | join(', ') }}
          ORDER BY VERSION_NO
        ) AS NEXT_FROM
      FROM {{ target_relation }}
    ) h
  {% endset %}

  {% set boundary_result = run_query(history_boundaries_sql) %}
  {% set overlaps = boundary_result.rows[0][0] | int %}
  {% set gaps = boundary_result.rows[0][1] | int %}

  {% do record_pre_load_check(
    model_execution_id, etl_nr,
    'TYPE2_HISTORY_OVERLAP',
    overlaps == 0, overlaps, 0,
    'Validity windows must not overlap'
  ) %}

  {% if overlaps > 0 %}
    {% do failures.append('TYPE2_HISTORY_OVERLAP') %}
  {% endif %}

  {% do record_pre_load_check(
    model_execution_id, etl_nr,
    'TYPE2_HISTORY_GAPS',
    gaps == 0, gaps, 0,
    'Consecutive history versions must have continuous validity windows'
  ) %}

  {% if gaps > 0 %}
    {% do failures.append('TYPE2_HISTORY_GAPS') %}
  {% endif %}

  {% set orphan_sql %}
    SELECT COUNT(*) FROM {{ source_relation }} s
    WHERE EXISTS (
      SELECT 1 FROM {{ target_relation }} h WHERE {{ src_tgt_join | join(' AND ') }}
    )
    AND NOT EXISTS (
      SELECT 1 FROM {{ target_relation }} h
      WHERE {{ src_tgt_join | join(' AND ') }} AND h.IS_CURRENT = TRUE
    )
  {% endset %}
  {% set orphans = run_query(orphan_sql).rows[0][0] | int %}
  {% do record_pre_load_check(model_execution_id, etl_nr, 'TYPE2_MISSING_CURRENT_FOR_EXISTING_KEY',
      orphans == 0, orphans, 0, 'Historical key in source without a current version') %}
  {% if orphans > 0 %}{% do failures.append('TYPE2_MISSING_CURRENT_FOR_EXISTING_KEY') %}{% endif %}

  {% do return({'passed': failures | length == 0, 'source_row_count': source_count, 'failed_checks': failures}) %}
{% endmacro %}
