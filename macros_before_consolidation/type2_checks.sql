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


{% macro record_type2_post_check(model_execution_id, etl_nr, check_name, observed, expected) %}
  {% set status = 'PASS' if observed == expected else 'FAIL' %}
  {% set sql %}
    INSERT INTO DBT_DEMO.ETL_CONTROL.ETL_DQ_RESULTS
    (DQ_RESULT_ID, MODEL_EXECUTION_ID, ETL_NR, CHECK_NAME, CHECK_PHASE,
     CHECK_STATUS, OBSERVED_VALUE, REFERENCE_VALUE, DETAILS, CHECKED_AT)
    SELECT UUID_STRING(), '{{ model_execution_id }}', {{ etl_nr }},
      '{{ check_name }}', 'POST_LOAD', '{{ status }}', {{ observed }}, {{ expected }},
      OBJECT_CONSTRUCT('load_type', 'TYPE2'),
      CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)
  {% endset %}
  {% do run_query(sql) %}
  {% do log('POST_LOAD | ' ~ check_name ~ ' | ' ~ status, info=true) %}
  {% do return(status) %}
{% endmacro %}


{% macro record_type2_skip(model_execution_id, etl_nr, check_name) %}
  {% set sql %}
    INSERT INTO DBT_DEMO.ETL_CONTROL.ETL_DQ_RESULTS
    (DQ_RESULT_ID, MODEL_EXECUTION_ID, ETL_NR, CHECK_NAME, CHECK_PHASE,
     CHECK_STATUS, OBSERVED_VALUE, REFERENCE_VALUE, DETAILS, CHECKED_AT)
    SELECT UUID_STRING(), '{{ model_execution_id }}', {{ etl_nr }},
      '{{ check_name }}', 'POST_LOAD', 'SKIP', NULL, NULL,
      OBJECT_CONSTRUCT('load_type','TYPE2','reason','Disabled by YAML'),
      CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)
  {% endset %}
  {% do run_query(sql) %}
  {% do log('POST_LOAD | ' ~ check_name ~ ' | SKIP', info=true) %}
{% endmacro %}


{% macro run_type2_post_load_checks(source_relation, target_relation, natural_keys,
      model_execution_id, etl_nr, target_before, target_after, dml_stats, post_cfg) %}
  {% set failures = [] %}
  {% set keys = [] %}
  {% set join_on = [] %}
  {% for key in natural_keys %}
    {% set q = adapter.quote(key) %}
    {% do keys.append(q) %}
    {% do join_on.append('s.' ~ q ~ ' = t.' ~ q) %}
  {% endfor %}

  {% if post_cfg['reconcile_counts'] %}
    {% set expected = target_before + dml_stats['rows_inserted'] %}
    {% set status = record_type2_post_check(model_execution_id, etl_nr,
      'TYPE2_HISTORY_ROW_DELTA', target_after, expected) %}
    {% if status == 'FAIL' %}{% do failures.append('TYPE2_HISTORY_ROW_DELTA') %}{% endif %}
  {% else %}
    {% do record_type2_skip(model_execution_id, etl_nr, 'TYPE2_HISTORY_ROW_DELTA') %}
  {% endif %}

  {# Exactly one current record for every known natural key. #}
  {% set current_sql %}
    SELECT COUNT(*) FROM (
      SELECT {{ keys | join(', ') }} FROM {{ target_relation }}
      GROUP BY {{ keys | join(', ') }} HAVING COALESCE(COUNT_IF(IS_CURRENT = TRUE), 0) <> 1
    ) bad
  {% endset %}
  {% set invalid_current = run_query(current_sql).rows[0][0] | int %}
  {% set status = record_type2_post_check(model_execution_id, etl_nr,
    'TYPE2_EXACTLY_ONE_CURRENT', invalid_current, 0) %}
  {% if status == 'FAIL' %}{% do failures.append('TYPE2_EXACTLY_ONE_CURRENT') %}{% endif %}

  {% set windows_sql %}
    SELECT COUNT(*) FROM (
      SELECT {{ keys | join(', ') }}, VALID_FROM, VALID_TO, IS_CURRENT, VERSION_NO,
        LEAD(VALID_FROM) OVER (PARTITION BY {{ keys | join(', ') }} ORDER BY VERSION_NO) AS NEXT_FROM
      FROM {{ target_relation }}
    ) h
    WHERE (IS_CURRENT = TRUE AND VALID_TO IS NOT NULL)
       OR (IS_CURRENT = FALSE AND VALID_TO IS NULL)
       OR (VALID_TO IS NOT NULL AND VALID_TO <= VALID_FROM)
       OR (NEXT_FROM IS NOT NULL AND VALID_TO IS DISTINCT FROM NEXT_FROM)
  {% endset %}
  {% set invalid_windows = run_query(windows_sql).rows[0][0] | int %}
  {% set status = record_type2_post_check(model_execution_id, etl_nr,
    'TYPE2_HISTORY_WINDOWS', invalid_windows, 0) %}
  {% if status == 'FAIL' %}{% do failures.append('TYPE2_HISTORY_WINDOWS') %}{% endif %}

  {% if post_cfg['source_target_match'] %}
    {% set src_cols = adapter.get_columns_in_relation(source_relation) %}
    {% set nonkey = [] %}
    {% for c in src_cols %}
      {% if c.name | upper not in natural_keys %}
        {% set q = adapter.quote(c.name) %}
        {% do nonkey.append('s.' ~ q ~ ' IS DISTINCT FROM t.' ~ q) %}
      {% endif %}
    {% endfor %}
    {% set source_match_sql %}
      SELECT COUNT(*) FROM {{ source_relation }} s
      LEFT JOIN {{ target_relation }} t
        ON {{ join_on | join(' AND ') }} AND t.IS_CURRENT = TRUE
      WHERE t.{{ keys[0] }} IS NULL
      {% if nonkey | length > 0 %}OR {{ nonkey | join(' OR ') }}{% endif %}
    {% endset %}
    {% set mismatches = run_query(source_match_sql).rows[0][0] | int %}
    {% set status = record_type2_post_check(model_execution_id, etl_nr,
      'TYPE2_SOURCE_CURRENT_MATCH', mismatches, 0) %}
    {% if status == 'FAIL' %}{% do failures.append('TYPE2_SOURCE_CURRENT_MATCH') %}{% endif %}
  {% else %}
    {% do record_type2_skip(model_execution_id, etl_nr, 'TYPE2_SOURCE_CURRENT_MATCH') %}
  {% endif %}

  {% set passed = failures | length == 0 %}
  {% do finalize_model_dq(model_execution_id, passed) %}
  {% do return({'passed': passed, 'failed_checks': failures}) %}
{% endmacro %}
