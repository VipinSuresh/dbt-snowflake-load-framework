{# Snowflake TYPE2 path for enterprise_load. No DDL/ALTER against governed history table. #}
{% macro execute_enterprise_type2(cfg, model_id, model_name, target_relation_name, model_sql, pre_hooks_to_run, post_hooks_to_run) %}
  {% set etl_nr = get_current_etl_nr() %}
  {% set model_execution_id = start_model_stats(etl_nr, model_id, target_relation_name, 'TYPE2') %}
  {% set history_target = adapter.get_relation(
      database=target_relation_name.database,
      schema=target_relation_name.schema,
      identifier=target_relation_name.identifier
  ) %}
  {% if history_target is none or history_target.type | lower != 'table' %}
    {% do record_pre_load_check(model_execution_id, etl_nr,
      'TYPE2_TARGET_TABLE', false, 0, 1,
      'History target must already be a physical table: ' ~ target_relation_name) %}
    {% do mark_pre_load_failed(model_execution_id, ['TYPE2_TARGET_TABLE']) %}
    {{ exceptions.raise_compiler_error('TYPE2: Governed history table is missing: ' ~ target_relation_name) }}
  {% endif %}

  {# Source staging only in DBT_WORK; this is not the governed MARTS schema. #}
  {% set suffix = invocation_id | replace('-', '') | upper %}
  {% set prepared_source = api.Relation.create(
      database=target.database,
      schema=target.schema,
      identifier='EL_T2_SRC_' ~ (model_name | upper) ~ '_' ~ suffix,
      type='table'
  ) %}
  {% set delta_relation = api.Relation.create(
      database=target.database,
      schema=target.schema,
      identifier='EL_T2_DELTA_' ~ (model_name | upper) ~ '_' ~ suffix,
      type='table'
  ) %}
  {{ run_hooks(pre_hooks_to_run) }}
  {% call statement('type2_prepare_source', auto_begin=False) %}
    CREATE TEMPORARY TABLE {{ prepared_source }} AS
    {{ model_sql }}
  {% endcall %}

  {% do set_model_stage(model_execution_id, 'PRE_LOAD_RUNNING') %}
  {% set pre = check_type2_pre_load(
      prepared_source, history_target, cfg['natural_keys'],
      cfg['pre_load']['require_nonempty'], model_execution_id, etl_nr
  ) %}
  {% if pre['passed'] %}
    {% set volume = cfg['pre_load'].get('volume_threshold', {}) %}
    {% set volume_result = check_source_volume_threshold(
        model_execution_id, etl_nr, model_id, pre['source_row_count'], volume
    ) %}
    {% if not volume_result['passed'] %}
      {% for failed in volume_result['failed_checks'] %}
        {% do pre['failed_checks'].append(failed) %}
      {% endfor %}
      {% do pre.update({'passed': false}) %}
    {% endif %}
  {% endif %}
  {% if not pre['passed'] %}
    {% do mark_pre_load_failed(model_execution_id, pre['failed_checks'], pre['source_row_count']) %}
    {{ exceptions.raise_compiler_error(
      'TYPE2: Pre-load checks blocked history changes: ' ~ pre['failed_checks'] | join(', ')
    ) }}
  {% endif %}

  {% set before_result = run_query('SELECT COUNT(*) FROM ' ~ history_target) %}
  {% set target_before = before_result.rows[0][0] | int %}
  {% do mark_pre_load_passed(model_execution_id, pre['source_row_count'], target_before) %}

  {% set src_cols = adapter.get_columns_in_relation(prepared_source) %}
  {% set business_cols = [] %}
  {% set nonkey_cols = [] %}
  {% set keys = [] %}
  {% set source_current_join = [] %}
  {% set source_hist_join = [] %}
  {% set delta_target_join = [] %}
  {% for key in cfg['natural_keys'] %}
    {% set q = adapter.quote(key) %}
    {% do keys.append(q) %}
    {% do source_current_join.append('s.' ~ q ~ ' = c.' ~ q) %}
    {% do source_hist_join.append('s.' ~ q ~ ' = h.' ~ q) %}
    {% do delta_target_join.append('d.' ~ q ~ ' = t.' ~ q) %}
  {% endfor %}
  {% for col in src_cols %}
    {% set q = adapter.quote(col.name) %}
    {% do business_cols.append(q) %}
    {% if col.name | upper not in cfg['natural_keys'] %}
      {% do nonkey_cols.append(q) %}
    {% endif %}
  {% endfor %}
  {% if nonkey_cols | length == 0 %}
    {{ exceptions.raise_compiler_error('TYPE2: At least one non-key attribute is required.') }}
  {% endif %}
  {% set diffs = [] %}
  {% for col in nonkey_cols %}
    {% do diffs.append('s.' ~ col ~ ' IS DISTINCT FROM c.' ~ col) %}
  {% endfor %}
  {% set hist_by_key = [] %}
  {% for key in keys %}{% do hist_by_key.append(key) %}{% endfor %}

  {# Snapshot only NEW and CHANGED records. UNCHANGED rows are excluded. #}
  {% call statement('type2_prepare_delta', auto_begin=False) %}
    CREATE TEMPORARY TABLE {{ delta_relation }} AS
    SELECT
      s.*,
      IFF(c.{{ keys[0] }} IS NULL, 'NEW', 'CHANGED') AS EL_ACTION,
      COALESCE(h.MAX_VERSION, 0) + 1 AS EL_VERSION
    FROM {{ prepared_source }} s
    LEFT JOIN {{ history_target }} c
      ON {{ source_current_join | join(' AND ') }} AND c.IS_CURRENT = TRUE
    LEFT JOIN (
      SELECT {{ hist_by_key | join(', ') }}, MAX(VERSION_NO) AS MAX_VERSION
      FROM {{ history_target }}
      GROUP BY {{ hist_by_key | join(', ') }}
    ) h ON {{ source_hist_join | join(' AND ') }}
    WHERE c.{{ keys[0] }} IS NULL OR ({{ diffs | join(' OR ') }})
  {% endcall %}

  {% set delta_sql %}
    SELECT COUNT(*), COUNT_IF(EL_ACTION = 'CHANGED')
    FROM {{ delta_relation }}
  {% endset %}
  {% set delta_info = run_query(delta_sql) %}
  {% set expected_insert = delta_info.rows[0][0] | int %}
  {% set expected_expire = delta_info.rows[0][1] | default(0, true) | int %}

  {# The first DML expires existing versions; the second creates new versions.
     Both are atomic in one Snowflake Scripting transaction and include ROLLBACK. #}
  {% set insert_fields = business_cols + [
     'VALID_FROM', 'VALID_TO', 'IS_CURRENT', 'VERSION_NO',
     'CREATED_BY_PROCESS', 'CREATED_AT', 'ETL_CREATED_RUN_NUMBER', 'ETL_CREATED_AT',
     'UPDATED_BY_PROCESS', 'UPDATED_AT', 'ETL_UPDATED_RUN_NUMBER', 'ETL_UPDATED_AT'
  ] %}
  {% set selected_fields = [] %}
  {% for col in business_cols %}{% do selected_fields.append('d.' ~ col) %}{% endfor %}
  {% set selected_fields = selected_fields + [
      ':v_ts', 'NULL', 'TRUE', 'd.EL_VERSION',
      "'enterprise_dbt_framework'", ':v_ts', (etl_nr | string), ':v_ts',
      'NULL', 'NULL', 'NULL', 'NULL'
  ] %}
  {% do set_model_stage(model_execution_id, 'MERGE_RUNNING') %}
  {% call statement('main', fetch_result=True, auto_begin=False) %}
    EXECUTE IMMEDIATE $$
    DECLARE
      v_expired NUMBER DEFAULT 0;
      v_inserted NUMBER DEFAULT 0;
      v_ts TIMESTAMP_NTZ DEFAULT CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9);
    BEGIN
      BEGIN TRANSACTION;

      UPDATE {{ history_target }} t
      SET VALID_TO = :v_ts,
          IS_CURRENT = FALSE,
          UPDATED_BY_PROCESS = 'enterprise_dbt_framework',
          UPDATED_AT = :v_ts,
          ETL_UPDATED_RUN_NUMBER = {{ etl_nr }},
          ETL_UPDATED_AT = :v_ts
      FROM {{ delta_relation }} d
      WHERE {{ delta_target_join | join(' AND ') }}
        AND t.IS_CURRENT = TRUE AND d.EL_ACTION = 'CHANGED';
      v_expired := SQLROWCOUNT;

      INSERT INTO {{ history_target }} ({{ insert_fields | join(', ') }})
      SELECT {{ selected_fields | join(', ') }}
      FROM {{ delta_relation }} d;
      v_inserted := SQLROWCOUNT;

      COMMIT;
      RETURN TO_VARCHAR(v_expired) || '|' || TO_VARCHAR(v_inserted);
    EXCEPTION
      WHEN OTHER THEN
        ROLLBACK;
        RAISE;
    END;
    $$;
  {% endcall %}

  {% set script_result = load_result('main') %}
  {% if script_result is none or script_result.table is none or script_result.table.rows | length == 0 %}
    {{ exceptions.raise_compiler_error('TYPE2: No DML row counts returned; verify history target and transaction before retry.') }}
  {% endif %}
  {% set parts = (script_result.table.rows[0][0] | string).split('|') %}
  {% if parts | length != 2 %}
    {{ exceptions.raise_compiler_error('TYPE2: Unexpected transaction result: ' ~ (script_result.table.rows[0][0] | string)) }}
  {% endif %}
  {% set dml = {
      'rows_inserted': parts[1] | int,
      'rows_updated': parts[0] | int,
      'rows_deleted': 0
  } %}
  {% do log('TYPE2 DML COMMITTED | inserted versions=' ~ dml['rows_inserted']
     ~ ' | expired versions=' ~ dml['rows_updated'], info=true) %}

  {% set after_result = run_query('SELECT COUNT(*) FROM ' ~ history_target) %}
  {% set target_after = after_result.rows[0][0] | int %}
  {% do finish_model_stats(model_execution_id, dml, target_after) %}

  {% set dq = run_type2_post_load_checks(prepared_source, history_target,
      cfg['natural_keys'], model_execution_id, etl_nr,
      target_before, target_after, dml, cfg['post_load']) %}
  {# Cross-check that the transaction affected exactly the prepared delta. #}
  {% set count_ok = dml['rows_inserted'] == expected_insert and dml['rows_updated'] == expected_expire %}
  {% set count_status = record_type2_post_check(model_execution_id, etl_nr,
      'TYPE2_DELTA_DML_COUNTS', 1 if count_ok else 0, 1) %}
  {% if count_status == 'FAIL' %}
    {% do finalize_model_dq(model_execution_id, false) %}
    {% do dq['failed_checks'].append('TYPE2_DELTA_DML_COUNTS') %}
    {% do dq.update({'passed': false}) %}
  {% endif %}

  {% call statement('type2_cleanup_delta', auto_begin=False) %}
    DROP TABLE IF EXISTS {{ delta_relation }}
  {% endcall %}
  {% call statement('type2_cleanup_source', auto_begin=False) %}
    DROP TABLE IF EXISTS {{ prepared_source }}
  {% endcall %}
  {{ run_hooks(post_hooks_to_run) }}
  {% do adapter.commit() %}
  {% if not dq['passed'] %}
    {{ exceptions.raise_compiler_error('TYPE2: Post-load DQ failed after commit: '
       ~ (dq['failed_checks'] | join(', '))) }}
  {% endif %}
  {% do log('ENTERPRISE_LOAD TYPE2 SUCCESS | Target=' ~ history_target
       ~ ' | ETL_NR=' ~ etl_nr ~ ' | versions inserted=' ~ dml['rows_inserted']
       ~ ' | versions expired=' ~ dml['rows_updated'], info=true) %}
  {% do return(history_target) %}
{% endmacro %}
