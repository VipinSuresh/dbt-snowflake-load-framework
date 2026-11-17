{# ===== From: macros/type1_load.sql | definition bodies unchanged ===== #}

{# TYPE1 execution path for enterprise_load.
   Governed target must already exist.
   No target CREATE OR REPLACE or ALTER.
#}

{% macro execute_enterprise_type1(
    cfg, load_cfg, model_id, model_name,
    target_relation_name, model_sql,
    pre_hooks_to_run, post_hooks_to_run
) %}

    {# Start model audit before source preparation. #}
    {% set etl_nr = get_current_etl_nr() %}
    {% set model_execution_id = start_model_stats(
        etl_nr, model_id, target_relation_name, cfg['load_type']
    ) %}

    {# Optional declared-window validation/audit; no SQL filtering is performed here. #}
    {% do enforce_processing_window(cfg['processing_window'], model_execution_id, etl_nr) %}

    {# Validate the governed target. #}
    {% set target_relation = adapter.get_relation(
        database=target_relation_name.database,
        schema=target_relation_name.schema,
        identifier=target_relation_name.identifier
    ) %}

    {% if target_relation is none
        or target_relation.type | lower != 'table' %}

        {% do record_pre_load_check(
            model_execution_id, etl_nr,
            'TARGET_EXISTS_AND_IS_TABLE',
            false, 0, 1,
            'The governed target must be deployed separately: '
            ~ target_relation_name
        ) %}

        {% do mark_pre_load_failed(
            model_execution_id, ['TARGET_EXISTS_AND_IS_TABLE']
        ) %}

        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: Target table missing or not '
            ~ 'a physical table: ' ~ target_relation_name
        ) }}
    {% endif %}

    {# Prepare temporary source in writable workspace. #}
    {% set temp_name = 'EL_SRC_' ~ (model_name | upper)
        ~ '_' ~ (invocation_id | replace('-', '') | upper) %}

    {% set source_relation = api.Relation.create(
        database=target.database,
        schema=target.schema,
        identifier=temp_name,
        type='table'
    ) %}

    {{ run_hooks(pre_hooks_to_run) }}

    {% call statement('prepare_source', auto_begin=False) %}
        CREATE TEMPORARY TABLE {{ source_relation }} AS
        {{ model_sql }}
    {% endcall %}

    {# Execute pre-load validations. #}
    {% do set_model_stage(
        model_execution_id, 'PRE_LOAD_RUNNING'
    ) %}

    {% set pre_results = check_pre_load(
        source_relation,
        target_relation,
        cfg['natural_keys'],
        cfg['pre_load']['require_nonempty'],
        model_execution_id,
        etl_nr
    ) %}

    {# Preserve existing TYPE1 volume-threshold behavior. #}
    {% if pre_results['passed'] %}

        {% set pre_load_cfg = load_cfg.get('pre_load', {}) or {} %}
        {% set volume_cfg = pre_load_cfg.get(
            'volume_threshold', {}
        ) %}

        {% set volume_result = check_source_volume_threshold(
            model_execution_id,
            etl_nr,
            model_id,
            pre_results['source_row_count'],
            volume_cfg
        ) %}

        {% if not volume_result['passed'] %}

            {% for check_name in volume_result['failed_checks'] %}
                {% do pre_results['failed_checks'].append(
                    check_name
                ) %}
            {% endfor %}

            {% do pre_results.update({'passed': false}) %}

        {% endif %}
    {% endif %}

    {# Reject invalid source before DML. #}
    {% if not pre_results['passed'] %}

        {% do mark_pre_load_failed(
            model_execution_id,
            pre_results['failed_checks'],
            pre_results['source_row_count']
        ) %}

        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: Pre-load validation blocked '
            ~ 'the load. Failed checks: '
            ~ (pre_results['failed_checks'] | join(', '))
            ~ '. Target table was not modified.'
        ) }}

    {% endif %}

    {# Capture target count before execution. #}
    {% set before_result = run_query(
        'SELECT COUNT(*) FROM ' ~ target_relation
    ) %}

    {% set target_before = before_result.rows[0][0] | int %}

    {% do mark_pre_load_passed(
        model_execution_id,
        pre_results['source_row_count'],
        target_before
    ) %}

    {# Generate and execute existing TYPE1 MERGE. #}
    {% set merge_sql = generate_type1_merge_sql(
        source_relation,
        target_relation,
        cfg['natural_keys'],
        etl_nr,
        'enterprise_dbt_framework'
    ) %}

    {% do set_model_stage(
        model_execution_id, 'MERGE_RUNNING'
    ) %}

    {% set dml_stats = execute_snowflake_merge(
        merge_sql, 'main'
    ) %}

    {# Capture statistics after DML. #}
    {% set after_result = run_query(
        'SELECT COUNT(*) FROM ' ~ target_relation
    ) %}

    {% set target_after = after_result.rows[0][0] | int %}

    {% do finish_model_stats(
        model_execution_id,
        dml_stats,
        target_after
    ) %}

    {# Existing TYPE1 post-load checks. #}
    {% set dq_result = run_type1_post_load_checks(
        source_relation,
        target_relation,
        cfg['natural_keys'],
        model_execution_id,
        etl_nr,
        target_before,
        target_after,
        dml_stats,
        cfg['post_load']
    ) %}

    {# Clean up temporary source. #}
    {% call statement('cleanup_source', auto_begin=False) %}
        DROP TABLE IF EXISTS {{ source_relation }}
    {% endcall %}

    {{ run_hooks(post_hooks_to_run) }}

    {% do adapter.commit() %}

    {% if not dq_result['passed'] %}

        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: Post-load DQ failed after '
            ~ 'data commit. Failed checks: '
            ~ (dq_result['failed_checks'] | join(', '))
        ) }}

    {% endif %}

    {% do log(
        'ENTERPRISE_LOAD SUCCESS | Target=' ~ target_relation
        ~ ' | ETL_NR=' ~ etl_nr
        ~ ' | Inserted=' ~ dml_stats['rows_inserted']
        ~ ' | Updated=' ~ dml_stats['rows_updated'],
        info=true
    ) %}

    {% do return(target_relation) %}

{% endmacro %}


{# ===== From: macros/type2_load.sql | definition bodies unchanged ===== #}
{# Snowflake TYPE2 path for enterprise_load. No DDL/ALTER against governed history table. #}
{% macro execute_enterprise_type2(cfg, model_id, model_name, target_relation_name, model_sql, pre_hooks_to_run, post_hooks_to_run) %}
  {% set etl_nr = get_current_etl_nr() %}
  {% set model_execution_id = start_model_stats(etl_nr, model_id, target_relation_name, 'TYPE2') %}

    {# Optional declared-window validation/audit; no SQL filtering is performed here. #}
    {% do enforce_processing_window(cfg['processing_window'], model_execution_id, etl_nr) %}
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

  {# Capture the pre-DML history size only for natural keys this invocation
     will change. Whole-target before/after counts are concurrency-unsafe: a
     legitimate writer on another key can change them while this run waits. #}
  {% set affected_before_join = [] %}
  {% for key in keys %}
    {% do affected_before_join.append('t.' ~ key ~ ' = d.' ~ key) %}
  {% endfor %}
  {% set affected_before_sql %}
    SELECT COUNT(*)
    FROM {{ history_target }} t
    WHERE EXISTS (
      SELECT 1 FROM {{ delta_relation }} d
      WHERE {{ affected_before_join | join(' AND ') }}
    )
  {% endset %}
  {% set affected_before_result = run_query(affected_before_sql) %}
  {% set affected_target_before = affected_before_result.rows[0][0] | int %}

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
  {# Only regression fixture may request an injected failure; never expose this
     switch to general production model configurations. #}
  {% set inject_critical_failure = (
      model_name == 'regression_customer_type2'
      and var('regression_type2_force_critical_failure', false) == true
  ) %}
  {# Regression-only concurrency delay. It runs after the TYPE2 UPDATE while
     the transaction still owns its write lock, making a second dbt process
     contend against the same governed target deterministically. #}
  {% set concurrency_wait_seconds = 0 %}
  {% if model_name == 'regression_customer_type2' %}
    {% set concurrency_wait_seconds = var('regression_type2_concurrency_wait_seconds', 0) | int %}
  {% endif %}
  {% if concurrency_wait_seconds > 0 %}
    {% do log('TYPE2 REGRESSION CONCURRENCY HOLD | seconds=' ~ concurrency_wait_seconds, info=true) %}
  {% endif %}
  {% set affected_key_join = [] %}
  {% for key in keys %}
    {% do affected_key_join.append('t.' ~ key ~ ' = d.' ~ key) %}
  {% endfor %}
  {% set affected_keys_predicate = 'EXISTS (SELECT 1 FROM ' ~ delta_relation
      ~ ' d WHERE ' ~ (affected_key_join | join(' AND ')) ~ ')' %}
  {% do set_model_stage(model_execution_id, 'MERGE_RUNNING') %}
  {% call statement('main', fetch_result=True, auto_begin=False) %}
    EXECUTE IMMEDIATE $$
    DECLARE
      v_expired NUMBER DEFAULT 0;
      v_inserted NUMBER DEFAULT 0;
      v_bad_current NUMBER DEFAULT 0;
      v_bad_versions NUMBER DEFAULT 0;
      v_bad_windows NUMBER DEFAULT 0;
      v_failure_reason VARCHAR DEFAULT NULL;
      type2_critical_dq_failed EXCEPTION (-20041, 'TYPE2_CRITICAL_DQ_FAILED: transaction rolled back');
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

      {# REGRESSION_RUNTIME_FAILURE_TYPE2_BEGIN #}
      {% if model_name == 'regression_customer_type2'
            and var('regression_type2_force_runtime_failure', false) %}
        LET regression_runtime_fault NUMBER :=
          TO_NUMBER('RUNTIME_FAULT_SENTINEL_' || UUID_STRING());
      {% endif %}
      {# REGRESSION_RUNTIME_FAILURE_TYPE2_END #}

      {# Deliberately hold the regression transaction after acquiring the
         UPDATE lock. SYSTEM$WAIT is never enabled for non-regression models. #}
      {% if concurrency_wait_seconds > 0 %}
        CALL SYSTEM$WAIT({{ concurrency_wait_seconds }}, 'SECONDS');
      {% endif %}

      INSERT INTO {{ history_target }} ({{ insert_fields | join(', ') }})
      SELECT {{ selected_fields | join(', ') }}
      FROM {{ delta_relation }} d;
      v_inserted := SQLROWCOUNT;

      -- These are transaction invariants, not full-target post-load DQ.
      -- DML counts must match the immutable delta prepared before BEGIN.
      IF (v_inserted <> {{ expected_insert }} OR v_expired <> {{ expected_expire }}) THEN
        v_failure_reason := 'DML_COUNT_MISMATCH';
        RAISE type2_critical_dq_failed;
      END IF;

      -- The delta is prepared before BEGIN, so a concurrent run can commit the
      -- same next VERSION_NO while this transaction is waiting on the target
      -- write lock. Re-check version uniqueness for affected keys *inside* the
      -- transaction before allowing COMMIT. This converts a stale-delta race
      -- into a rollback instead of a post-commit DQ failure.
      SELECT COUNT(*) INTO :v_bad_versions
      FROM (
        SELECT {{ keys | join(', ') }}, VERSION_NO
        FROM {{ history_target }} t
        WHERE {{ affected_keys_predicate }}
        GROUP BY {{ keys | join(', ') }}, VERSION_NO
        HAVING COUNT(*) > 1
      ) invalid_versions;
      IF (v_bad_versions <> 0) THEN
        v_failure_reason := 'AFFECTED_KEY_DUPLICATE_VERSION';
        RAISE type2_critical_dq_failed;
      END IF;

      -- Limit validation to natural keys impacted by this invocation.
      SELECT COUNT(*) INTO :v_bad_current
      FROM (
        SELECT {{ keys | join(', ') }}
        FROM {{ history_target }} t
        WHERE {{ affected_keys_predicate }}
        GROUP BY {{ keys | join(', ') }}
        HAVING COALESCE(COUNT_IF(IS_CURRENT = TRUE), 0) <> 1
      ) invalid_current;
      IF (v_bad_current <> 0) THEN
        v_failure_reason := 'AFFECTED_KEY_CURRENT_STATE';
        RAISE type2_critical_dq_failed;
      END IF;

      SELECT COUNT(*) INTO :v_bad_windows
      FROM (
        SELECT {{ keys | join(', ') }}, VALID_FROM, VALID_TO, IS_CURRENT, VERSION_NO,
          LEAD(VALID_FROM) OVER (
            PARTITION BY {{ keys | join(', ') }} ORDER BY VERSION_NO
          ) AS NEXT_FROM
        FROM {{ history_target }} t
        WHERE {{ affected_keys_predicate }}
      ) history_rows
      WHERE (IS_CURRENT = TRUE AND VALID_TO IS NOT NULL)
         OR (IS_CURRENT = FALSE AND VALID_TO IS NULL)
         OR (VALID_TO IS NOT NULL AND VALID_TO <= VALID_FROM)
         OR (NEXT_FROM IS NOT NULL AND VALID_TO IS DISTINCT FROM NEXT_FROM);
      IF (v_bad_windows <> 0) THEN
        v_failure_reason := 'AFFECTED_KEY_HISTORY_WINDOWS';
        RAISE type2_critical_dq_failed;
      END IF;

      {# Explicit failure injection proves *validation-triggered* rollback.
         The flag only works for the isolated regression model. #}
      {% if inject_critical_failure %}
        v_failure_reason := 'REGRESSION_FORCED_FAILURE';
        RAISE type2_critical_dq_failed;
      {% endif %}

      COMMIT;
      RETURN TO_VARCHAR(v_expired) || '|' || TO_VARCHAR(v_inserted);
    EXCEPTION
      WHEN type2_critical_dq_failed THEN
        ROLLBACK;
        -- Persist failure evidence in a separate transaction so the business
        -- rollback cannot erase the audit record. Audit failure must not mask
        -- the original load exception.
        BEGIN
          BEGIN TRANSACTION;
          {{ transaction_dq_failure_audit_sql(
              model_execution_id, etl_nr,
              'TYPE2_TRANSACTION_CRITICAL_DQ', 'TYPE2'
          ) }}
          COMMIT;
        EXCEPTION
          WHEN OTHER THEN
            ROLLBACK;
            NULL;
        END;
        RAISE;
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

  {# Reconcile this invocation's affected natural keys, not the entire target.
     The delta temp table remains available until post-load checks finish. #}
  {% set affected_after_join = [] %}
  {% for key in keys %}
    {% do affected_after_join.append('t.' ~ key ~ ' = d.' ~ key) %}
  {% endfor %}
  {% set affected_after_sql %}
    SELECT COUNT(*)
    FROM {{ history_target }} t
    WHERE EXISTS (
      SELECT 1 FROM {{ delta_relation }} d
      WHERE {{ affected_after_join | join(' AND ') }}
    )
  {% endset %}
  {% set affected_after_result = run_query(affected_after_sql) %}
  {% set affected_target_after = affected_after_result.rows[0][0] | int %}

  {% set dq = run_type2_post_load_checks(prepared_source, history_target, delta_relation,
      cfg['natural_keys'], model_execution_id, etl_nr,
      affected_target_before, affected_target_after, dml, cfg['post_load']) %}
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


{# ===== From: macros/insert_load.sql | definition bodies unchanged ===== #}

{# INSERT-only strategy for existing governed Snowflake targets. #}

{% macro execute_enterprise_insert(
    cfg, load_cfg, model_id, model_name,
    target_relation_name, model_sql,
    pre_hooks_to_run, post_hooks_to_run
) %}

    {# 1. Start model audit. #}

    {% set etl_nr = get_current_etl_nr() %}

    {% set model_execution_id = start_model_stats(
        etl_nr, model_id, target_relation_name, 'INSERT'
    ) %}

    {# Optional declared-window validation/audit; no SQL filtering is performed here. #}
    {% do enforce_processing_window(cfg['processing_window'], model_execution_id, etl_nr) %}

    {# 2. Validate governed target existence. #}

    {% set target_relation = adapter.get_relation(
        database=target_relation_name.database,
        schema=target_relation_name.schema,
        identifier=target_relation_name.identifier
    ) %}

    {% if target_relation is none
        or target_relation.type | lower != 'table' %}

        {% do record_pre_load_check(
            model_execution_id, etl_nr,
            'TARGET_EXISTS_AND_IS_TABLE',
            false, 0, 1,
            'Governed target must already exist: '
            ~ target_relation_name
        ) %}

        {% do mark_pre_load_failed(
            model_execution_id,
            ['TARGET_EXISTS_AND_IS_TABLE']
        ) %}

        {{ exceptions.raise_compiler_error(
            'INSERT: Governed target missing or not a table: '
            ~ target_relation_name
        ) }}

    {% endif %}

    {# 3. Prepare source snapshot. #}

    {% set temp_name = 'EL_SRC_' ~ (model_name | upper)
        ~ '_' ~ (invocation_id | replace('-', '') | upper) %}

    {% set source_relation = api.Relation.create(
        database=target.database,
        schema=target.schema,
        identifier=temp_name,
        type='table'
    ) %}

    {{ run_hooks(pre_hooks_to_run) }}

    {% call statement('prepare_source', auto_begin=False) %}

        CREATE TEMPORARY TABLE {{ source_relation }} AS
        {{ model_sql }}

    {% endcall %}

    {# 4. Execute reusable pre-load validations. #}

    {% do set_model_stage(
        model_execution_id, 'PRE_LOAD_RUNNING'
    ) %}

    {% set pre_results = check_pre_load(
        source_relation,
        target_relation,
        cfg['natural_keys'],
        cfg['pre_load']['require_nonempty'],
        model_execution_id,
        etl_nr
    ) %}

    {# 5. Evaluate source-volume thresholds. #}

    {% if pre_results['passed'] %}

        {% set pre_load_cfg =
            load_cfg.get('pre_load', {}) or {} %}

        {% set volume_cfg =
            pre_load_cfg.get('volume_threshold', {}) %}

        {% set volume_result = check_source_volume_threshold(
            model_execution_id,
            etl_nr,
            model_id,
            pre_results['source_row_count'],
            volume_cfg
        ) %}

        {% if not volume_result['passed'] %}

            {% for check_name in volume_result['failed_checks'] %}
                {% do pre_results['failed_checks'].append(
                    check_name
                ) %}
            {% endfor %}

            {% do pre_results.update({'passed': false}) %}

        {% endif %}

    {% endif %}

    {# 6. Stop execution if pre-load fails. #}

    {% if not pre_results['passed'] %}

        {% do mark_pre_load_failed(
            model_execution_id,
            pre_results['failed_checks'],
            pre_results['source_row_count']
        ) %}

        {{ exceptions.raise_compiler_error(
            'INSERT: Pre-load validation blocked the load. '
            ~ 'Failed checks: '
            ~ (pre_results['failed_checks'] | join(', '))
            ~ '. Target table was not modified.'
        ) }}

    {% endif %}

    {# 7. Record target state before DML. #}

    {% set before_result = run_query(
        'SELECT COUNT(*) FROM ' ~ target_relation
    ) %}

    {% set target_before =
        before_result.rows[0][0] | int %}

    {% do mark_pre_load_passed(
        model_execution_id,
        pre_results['source_row_count'],
        target_before
    ) %}

    {# 8. Generate INSERT-only MERGE. #}

    {% set merge_sql = generate_insert_merge_sql(
        source_relation,
        target_relation,
        cfg['natural_keys'],
        etl_nr,
        'enterprise_dbt_framework'
    ) %}

    {% do set_model_stage(
        model_execution_id, 'MERGE_RUNNING'
    ) %}

    {% set dml_stats = execute_snowflake_merge(
    merge_sql,
    'main',
    insert_only=true
    ) %}

    {# 9. Record post-DML statistics. #}

    {% set after_result = run_query(
        'SELECT COUNT(*) FROM ' ~ target_relation
    ) %}

    {% set target_after =
        after_result.rows[0][0] | int %}

    {% do finish_model_stats(
        model_execution_id,
        dml_stats,
        target_after
    ) %}

    {# 10. INSERT-specific post-load checks. #}

    {% set dq_result = run_insert_post_load_checks(
        source_relation,
        target_relation,
        cfg['natural_keys'],
        model_execution_id,
        etl_nr,
        target_before,
        target_after,
        dml_stats,
        cfg['post_load']
    ) %}

    {# 11. Cleanup and finalization. #}

    {% call statement('cleanup_source', auto_begin=False) %}

        DROP TABLE IF EXISTS {{ source_relation }}

    {% endcall %}

    {{ run_hooks(post_hooks_to_run) }}

    {% do adapter.commit() %}

    {% if not dq_result['passed'] %}

        {{ exceptions.raise_compiler_error(
            'INSERT: Post-load DQ failed after commit. '
            ~ 'Failed checks: '
            ~ (dq_result['failed_checks'] | join(', '))
        ) }}

    {% endif %}

    {% do log(
        'ENTERPRISE_LOAD INSERT SUCCESS'
        ~ ' | Target=' ~ target_relation
        ~ ' | ETL_NR=' ~ etl_nr
        ~ ' | Inserted=' ~ dml_stats['rows_inserted']
        ~ ' | Updated=' ~ dml_stats['rows_updated']
        ~ ' | Deleted=' ~ dml_stats['rows_deleted'],
        info=true
    ) %}

    {% do return(target_relation) %}

{% endmacro %}


{# ===== From: macros/update_load.sql | definition bodies unchanged ===== #}

{# UPDATE-only strategy for existing governed Snowflake targets. #}

{% macro execute_enterprise_update(
    cfg, load_cfg, model_id, model_name,
    target_relation_name, model_sql,
    pre_hooks_to_run, post_hooks_to_run
) %}

    {# 1. Start model audit. #}

    {% set etl_nr = get_current_etl_nr() %}

    {% set model_execution_id = start_model_stats(
        etl_nr, model_id, target_relation_name, 'UPDATE'
    ) %}

    {# Optional declared-window validation/audit; no SQL filtering is performed here. #}
    {% do enforce_processing_window(cfg['processing_window'], model_execution_id, etl_nr) %}

    {# 2. Validate governed target existence. #}

    {% set target_relation = adapter.get_relation(
        database=target_relation_name.database,
        schema=target_relation_name.schema,
        identifier=target_relation_name.identifier
    ) %}

    {% if target_relation is none
        or target_relation.type | lower != 'table' %}

        {% do record_pre_load_check(
            model_execution_id, etl_nr,
            'TARGET_EXISTS_AND_IS_TABLE',
            false, 0, 1,
            'Governed target must already exist: '
            ~ target_relation_name
        ) %}

        {% do mark_pre_load_failed(
            model_execution_id,
            ['TARGET_EXISTS_AND_IS_TABLE']
        ) %}

        {{ exceptions.raise_compiler_error(
            'UPDATE: Governed target missing or not a table: '
            ~ target_relation_name
        ) }}

    {% endif %}

    {# 3. Prepare source snapshot. #}

    {% set temp_name = 'EL_SRC_' ~ (model_name | upper)
        ~ '_' ~ (invocation_id | replace('-', '') | upper) %}

    {% set source_relation = api.Relation.create(
        database=target.database,
        schema=target.schema,
        identifier=temp_name,
        type='table'
    ) %}

    {{ run_hooks(pre_hooks_to_run) }}

    {% call statement('prepare_source', auto_begin=False) %}

        CREATE TEMPORARY TABLE {{ source_relation }} AS
        {{ model_sql }}

    {% endcall %}

    {# 4. Execute reusable pre-load validations. #}

    {% do set_model_stage(
        model_execution_id, 'PRE_LOAD_RUNNING'
    ) %}

    {% set pre_results = check_pre_load(
        source_relation,
        target_relation,
        cfg['natural_keys'],
        cfg['pre_load']['require_nonempty'],
        model_execution_id,
        etl_nr
    ) %}

    {# 5. Evaluate source-volume thresholds. #}

    {% if pre_results['passed'] %}

        {% set pre_load_cfg =
            load_cfg.get('pre_load', {}) or {} %}

        {% set volume_cfg =
            pre_load_cfg.get('volume_threshold', {}) %}

        {% set volume_result = check_source_volume_threshold(
            model_execution_id,
            etl_nr,
            model_id,
            pre_results['source_row_count'],
            volume_cfg
        ) %}

        {% if not volume_result['passed'] %}

            {% for check_name in volume_result['failed_checks'] %}
                {% do pre_results['failed_checks'].append(
                    check_name
                ) %}
            {% endfor %}

            {% do pre_results.update({'passed': false}) %}

        {% endif %}

    {% endif %}

    {# 6. Stop execution if pre-load fails. #}

    {% if not pre_results['passed'] %}

        {% do mark_pre_load_failed(
            model_execution_id,
            pre_results['failed_checks'],
            pre_results['source_row_count']
        ) %}

        {{ exceptions.raise_compiler_error(
            'UPDATE: Pre-load validation blocked the load. '
            ~ 'Failed checks: '
            ~ (pre_results['failed_checks'] | join(', '))
            ~ '. Target table was not modified.'
        ) }}

    {% endif %}

    {# 7. Record target state before DML. #}

    {% set before_result = run_query(
        'SELECT COUNT(*) FROM ' ~ target_relation
    ) %}

    {% set target_before =
        before_result.rows[0][0] | int %}

    {% do mark_pre_load_passed(
        model_execution_id,
        pre_results['source_row_count'],
        target_before
    ) %}

    {# 8. Generate and execute UPDATE-only MERGE. #}

    {% set merge_sql = generate_update_merge_sql(
        source_relation,
        target_relation,
        cfg['natural_keys'],
        etl_nr,
        'enterprise_dbt_framework'
    ) %}

    {% do set_model_stage(
        model_execution_id, 'MERGE_RUNNING'
    ) %}

    {% set dml_stats = execute_snowflake_merge(
        merge_sql,
        'main',
        update_only=true
    ) %}

    {# 9. Record post-DML statistics. #}

    {% set after_result = run_query(
        'SELECT COUNT(*) FROM ' ~ target_relation
    ) %}

    {% set target_after =
        after_result.rows[0][0] | int %}

    {% do finish_model_stats(
        model_execution_id,
        dml_stats,
        target_after
    ) %}

    {# 10. UPDATE-specific post-load checks. #}

    {% set dq_result = run_update_post_load_checks(
        source_relation,
        target_relation,
        cfg['natural_keys'],
        model_execution_id,
        etl_nr,
        target_before,
        target_after,
        dml_stats,
        cfg['post_load']
    ) %}

    {# 11. Cleanup and finalization. #}

    {% call statement('cleanup_source', auto_begin=False) %}

        DROP TABLE IF EXISTS {{ source_relation }}

    {% endcall %}

    {{ run_hooks(post_hooks_to_run) }}

    {% do adapter.commit() %}

    {% if not dq_result['passed'] %}

        {{ exceptions.raise_compiler_error(
            'UPDATE: Post-load DQ failed after commit. '
            ~ 'Failed checks: '
            ~ (dq_result['failed_checks'] | join(', '))
        ) }}

    {% endif %}

    {% do log(
        'ENTERPRISE_LOAD UPDATE SUCCESS'
        ~ ' | Target=' ~ target_relation
        ~ ' | ETL_NR=' ~ etl_nr
        ~ ' | Inserted=' ~ dml_stats['rows_inserted']
        ~ ' | Updated=' ~ dml_stats['rows_updated']
        ~ ' | Deleted=' ~ dml_stats['rows_deleted'],
        info=true
    ) %}

    {% do return(target_relation) %}

{% endmacro %}


{# ===== From: macros/truncate_load.sql | definition bodies unchanged ===== #}

{# Transactional full replacement for an existing governed Snowflake table.
   Configuration name: TRUNCATE_LOAD
   Physical implementation: DELETE + INSERT in one transaction.
#}

{% macro execute_enterprise_truncate(
    cfg, load_cfg, model_id, model_name,
    target_relation_name, model_sql,
    pre_hooks_to_run, post_hooks_to_run
) %}

    {# 1. Initialize execution audit. #}

    {% set etl_nr = get_current_etl_nr() %}

    {% set model_execution_id = start_model_stats(
        etl_nr, model_id, target_relation_name, 'TRUNCATE_LOAD'
    ) %}

    {# Optional declared-window validation/audit; no SQL filtering is performed here. #}
    {% do enforce_processing_window(cfg['processing_window'], model_execution_id, etl_nr) %}


    {# 2. Require an existing governed physical table. #}

    {% set target_relation = adapter.get_relation(
        database=target_relation_name.database,
        schema=target_relation_name.schema,
        identifier=target_relation_name.identifier
    ) %}

    {% if target_relation is none
        or target_relation.type | lower != 'table' %}

        {% do record_pre_load_check(
            model_execution_id, etl_nr,
            'TARGET_EXISTS_AND_IS_TABLE',
            false, 0, 1,
            'Governed target must already exist: ' ~ target_relation_name
        ) %}

        {% do mark_pre_load_failed(
            model_execution_id,
            ['TARGET_EXISTS_AND_IS_TABLE']
        ) %}

        {{ exceptions.raise_compiler_error(
            'TRUNCATE_LOAD: Target missing or not a physical table: '
            ~ target_relation_name
        ) }}

    {% endif %}


    {# 3. Prepare temporary source snapshot. #}

    {% set temp_name = 'EL_SRC_' ~ (model_name | upper)
        ~ '_' ~ (invocation_id | replace('-', '') | upper) %}

    {% set source_relation = api.Relation.create(
    database=target_relation.database,
    schema=target_relation.schema,
    identifier=temp_name,
    type='table'
    ) %}

    {{ run_hooks(pre_hooks_to_run) }}

    {% call statement('prepare_source', auto_begin=False) %}
        CREATE TEMPORARY TABLE {{ source_relation }} AS
        {{ model_sql }}
    {% endcall %}


    {# 4. Run shared pre-load checks. #}

    {% do set_model_stage(
        model_execution_id, 'PRE_LOAD_RUNNING'
    ) %}

    {% set pre_results = check_pre_load(
        source_relation,
        target_relation,
        cfg['natural_keys'],
        cfg['pre_load']['require_nonempty'],
        model_execution_id,
        etl_nr
    ) %}

    {% set volume_cfg = cfg['pre_load']['volume_threshold'] %}


    {# 5. Compare source volume with previous successful runs. #}

    {% if pre_results['passed'] %}

        {% set volume_result = check_source_volume_threshold(
            model_execution_id,
            etl_nr,
            model_id,
            pre_results['source_row_count'],
            volume_cfg
        ) %}

        {% if not volume_result['passed'] %}

            {% for check_name in volume_result['failed_checks'] %}
                {% do pre_results['failed_checks'].append(check_name) %}
            {% endfor %}

            {% do pre_results.update({'passed': false}) %}

        {% endif %}

    {% endif %}


    {# 6. Capture current target state and optionally guard major drops.
       Row count alone is not a sufficient concurrency token for full-table
       replacement, because a concurrent writer can leave the same row count
       with different contents. Capture an order-independent content fingerprint
       over the validated business columns plus the creation-run audit token.
    #}

    {% if pre_results['passed'] %}

        {% set replace_source_columns =
            adapter.get_columns_in_relation(source_relation) %}

        {% set replace_fingerprint_exprs = [] %}

        {% for col in replace_source_columns %}
            {% do replace_fingerprint_exprs.append(
                'TO_VARCHAR(' ~ adapter.quote(col.name) ~ ')'
            ) %}
        {% endfor %}

        {% do replace_fingerprint_exprs.append(
            'TO_VARCHAR(' ~ adapter.quote('ETL_CREATED_RUN_NUMBER') ~ ')'
        ) %}

        {% set replace_fingerprint_sql =
            'HASH_AGG(' ~ (replace_fingerprint_exprs | join(', ')) ~ ')' %}

        {% set before_sql %}
            SELECT
                COUNT(*) AS ROW_COUNT,
                {{ replace_fingerprint_sql }} AS CONTENT_FINGERPRINT
            FROM {{ target_relation }}
        {% endset %}

        {% set before_result = run_query(before_sql) %}

        {% set target_before = before_result.rows[0][0] | int %}
        {% set target_fingerprint_before = before_result.rows[0][1] | int %}

        {% if volume_cfg.get('enabled', false)
            and target_before > 0 %}

            {% set source_count = pre_results['source_row_count'] | int %}
            {% set drop_pct = 0 %}

            {% if source_count < target_before %}
                {% set drop_pct =
                    (target_before - source_count) * 100.0 / target_before
                %}
            {% endif %}

            {% set max_drop_pct = volume_cfg['max_drop_pct'] %}
            {% set drop_ok = drop_pct <= max_drop_pct %}

            {% do record_pre_load_check(
                model_execution_id, etl_nr,
                'REPLACE_TARGET_DROP_PCT',
                drop_ok,
                drop_pct,
                max_drop_pct,
                'Source row count compared with current target; units=percent'
            ) %}

            {% if not drop_ok %}
                {% do pre_results['failed_checks'].append(
                    'REPLACE_TARGET_DROP_PCT'
                ) %}
                {% do pre_results.update({'passed': false}) %}
            {% endif %}

        {% endif %}

    {% endif %}


    {# 6b. Full-table replacement cannot depend on a row in the governed target for
        locking because the target may legitimately be empty. The control row is
        provisioned by setup/create_etl_control_objects.sql and is shared by all
        TRUNCATE_LOAD executions. The row is only a mutual-exclusion token:
        LOCK_VERSION is not used for stale-target detection, because it is
        global and is bumped by replacements of other targets too. Stale-target
        detection is done per target by the count + content fingerprint re-check
        inside the transaction.
    #}

    {% set truncate_lock_relation =
        enterprise_control_relation('ETL_FRAMEWORK_LOCKS') %}

    {% if pre_results['passed'] %}

        {% set truncate_lock_actual = adapter.get_relation(
            database=truncate_lock_relation.database,
            schema=truncate_lock_relation.schema,
            identifier=truncate_lock_relation.identifier
        ) %}

        {% if truncate_lock_actual is none
            or truncate_lock_actual.type | lower != 'table' %}

            {% do record_pre_load_check(
                model_execution_id, etl_nr,
                'TRUNCATE_LOAD_SERIALIZATION_LOCK',
                false, 0, 1,
                'ETL_FRAMEWORK_LOCKS must exist before TRUNCATE_LOAD can run'
            ) %}

            {% do pre_results['failed_checks'].append(
                'TRUNCATE_LOAD_SERIALIZATION_LOCK'
            ) %}
            {% do pre_results.update({'passed': false}) %}

        {% else %}

            {% set truncate_lock_state_sql %}
                SELECT
                    COUNT(*) AS LOCK_ROWS
                FROM {{ truncate_lock_relation }}
                WHERE LOCK_NAME = 'TRUNCATE_LOAD_GLOBAL'
            {% endset %}

            {% set truncate_lock_state = run_query(truncate_lock_state_sql) %}
            {% set truncate_lock_rows =
                truncate_lock_state.rows[0][0] | int %}

            {% set truncate_lock_ok = truncate_lock_rows == 1 %}

            {% do record_pre_load_check(
                model_execution_id, etl_nr,
                'TRUNCATE_LOAD_SERIALIZATION_LOCK',
                truncate_lock_ok,
                truncate_lock_rows,
                1,
                'Exactly one TRUNCATE_LOAD_GLOBAL control row is required'
            ) %}

            {% if not truncate_lock_ok %}
                {% do pre_results['failed_checks'].append(
                    'TRUNCATE_LOAD_SERIALIZATION_LOCK'
                ) %}
                {% do pre_results.update({'passed': false}) %}
            {% endif %}

        {% endif %}

    {% endif %}


    {# 7. Reject the replacement if any pre-load check failed. #}

    {% if not pre_results['passed'] %}

        {% do mark_pre_load_failed(
            model_execution_id,
            pre_results['failed_checks'],
            pre_results['source_row_count']
        ) %}

        {{ exceptions.raise_compiler_error(
            'TRUNCATE_LOAD: Pre-load validation blocked replacement. '
            ~ 'Failed checks: '
            ~ (pre_results['failed_checks'] | join(', '))
            ~ '. Governed target was not modified.'
        ) }}

    {% endif %}

    {% do mark_pre_load_passed(
        model_execution_id,
        pre_results['source_row_count'],
        target_before
    ) %}

    {# TRUNCATE_LOAD serialization is provided by ETL_FRAMEWORK_LOCKS. #}


    {# 8. Generate INSERT SQL from prepared source. #}

    {% set insert_sql = generate_replace_insert_sql(
        source_relation,
        target_relation,
        etl_nr,
        'enterprise_dbt_framework'
    ) %}


    {# 9. Execute DELETE and INSERT atomically.
       SQLROWCOUNT is captured immediately after each DML and validated before
       COMMIT. Broader replacement DQ remains post-commit.
    #}

    {% set inject_critical_failure = (
        model_name == 'regression_customer_replace'
        and var('regression_truncate_force_critical_failure', false) == true
    ) %}

    {# Regression-only concurrency delay. Hold the transaction after the
       framework control lock is acquired so a second full replacement must
       wait before target revalidation and destructive work. #}
    {% set truncate_concurrency_wait_seconds = 0 %}
    {% if model_name == 'regression_customer_replace' %}
        {% set truncate_concurrency_wait_seconds = var(
            'regression_truncate_concurrency_wait_seconds', 0
        ) | int %}
    {% endif %}

    {% if truncate_concurrency_wait_seconds > 0 %}
        {% do log(
            'TRUNCATE_LOAD REGRESSION CONCURRENCY HOLD | seconds='
            ~ truncate_concurrency_wait_seconds,
            info=true
        ) %}
    {% endif %}

    {% do set_model_stage(
        model_execution_id, 'MERGE_RUNNING'
    ) %}

    {% call statement('main', fetch_result=True, auto_begin=False) %}

        EXECUTE IMMEDIATE $$
        DECLARE
            v_deleted NUMBER DEFAULT 0;
            v_inserted NUMBER DEFAULT 0;
            v_lock_rows NUMBER DEFAULT 0;
            v_current_count NUMBER DEFAULT 0;
            v_current_fingerprint NUMBER(19,0) DEFAULT 0;
            v_failure_reason VARCHAR DEFAULT NULL;
            truncate_critical_dq_failed EXCEPTION
                (-20043, 'TRUNCATE_LOAD_CRITICAL_DQ_FAILED: transaction rolled back');
        BEGIN

            BEGIN TRANSACTION;

            -- Serialize every TRUNCATE_LOAD through a framework-owned control
            -- row. Unlike the governed target, this row exists even when the
            -- target is empty. Updating LOCK_VERSION is a real DML mutation, so
            -- concurrent framework replacements contend on the same resource.
            UPDATE {{ truncate_lock_relation }}
            SET
                LOCK_VERSION = LOCK_VERSION + 1,
                LAST_ETL_NR = {{ etl_nr }},
                LAST_MODEL_EXECUTION_ID = '{{ model_execution_id }}',
                LAST_ACQUIRED_AT = CURRENT_TIMESTAMP()
            WHERE LOCK_NAME = 'TRUNCATE_LOAD_GLOBAL';

            v_lock_rows := SQLROWCOUNT;

            IF (v_lock_rows <> 1) THEN
                v_failure_reason := 'SERIALIZATION_LOCK_UNAVAILABLE';
                RAISE truncate_critical_dq_failed;
            END IF;

            -- The lock row is global, so LOCK_VERSION also moves when another
            -- target is replaced. Do not compare it to a pre-load snapshot;
            -- the per-target count + fingerprint re-check below is the
            -- stale-target guard.

            {# Regression-only hold after the serialization lock is owned. #}
            {% if truncate_concurrency_wait_seconds > 0 %}
                CALL SYSTEM$WAIT({{ truncate_concurrency_wait_seconds }}, 'SECONDS');
            {% endif %}

            -- Re-read target state only after the framework serialization lock
            -- is owned. This catches out-of-band target changes as well as any
            -- stale framework writer and fails before DELETE.
            SELECT
                COUNT(*),
                {{ replace_fingerprint_sql }}
            INTO
                :v_current_count,
                :v_current_fingerprint
            FROM {{ target_relation }};

            IF (v_current_count <> {{ target_before }}
                OR v_current_fingerprint <> {{ target_fingerprint_before }}) THEN
                v_failure_reason := 'TARGET_CHANGED_SINCE_PRECHECK';
                RAISE truncate_critical_dq_failed;
            END IF;

            {# Regression-only hold after stale-target recheck. This is used by
               the empty-baseline concurrency test to force two sessions to
               both observe the empty target before either reaches DELETE. #}
            {% if model_name == 'regression_customer_replace' %}
                {% set truncate_post_recheck_wait_seconds = var(
                    'regression_truncate_post_recheck_wait_seconds', 0
                ) | int %}
            {% else %}
                {% set truncate_post_recheck_wait_seconds = 0 %}
            {% endif %}
            {% if truncate_post_recheck_wait_seconds > 0 %}
                CALL SYSTEM$WAIT({{ truncate_post_recheck_wait_seconds }}, 'SECONDS');
            {% endif %}

            DELETE FROM {{ target_relation }};

            v_deleted := SQLROWCOUNT;

            {# REGRESSION_RUNTIME_FAILURE_TRUNCATE_BEGIN #}
            {% if model_name == 'regression_customer_replace'
                  and var('regression_truncate_force_runtime_failure', false) %}
                LET regression_runtime_fault NUMBER :=
                    TO_NUMBER('RUNTIME_FAULT_SENTINEL_' || UUID_STRING());
            {% endif %}
            {# REGRESSION_RUNTIME_FAILURE_TRUNCATE_END #}

            {{ insert_sql }};

            v_inserted := SQLROWCOUNT;

            -- Commit gate: the winning replacement must remove the complete
            -- locked target and insert the complete immutable prepared source.
            IF (v_deleted <> {{ target_before }}
                OR v_inserted <> {{ pre_results['source_row_count'] | int }}) THEN
                v_failure_reason := 'DML_COUNT_MISMATCH';
                RAISE truncate_critical_dq_failed;
            END IF;

            {# Regression-only failure injection proves validation-triggered rollback. #}
            {% if inject_critical_failure %}
                v_failure_reason := 'REGRESSION_FORCED_FAILURE';
                RAISE truncate_critical_dq_failed;
            {% endif %}

            COMMIT;

            RETURN TO_VARCHAR(v_deleted)
                || '|' || TO_VARCHAR(v_inserted);

        EXCEPTION
            WHEN truncate_critical_dq_failed THEN
                ROLLBACK;
                BEGIN
                    BEGIN TRANSACTION;
                    {{ transaction_dq_failure_audit_sql(
                        model_execution_id, etl_nr,
                        'TRUNCATE_LOAD_TRANSACTION_CRITICAL_DQ',
                        'TRUNCATE_LOAD'
                    ) }}
                    COMMIT;
                EXCEPTION
                    WHEN OTHER THEN
                        ROLLBACK;
                        NULL;
                END;
                RAISE;
            WHEN OTHER THEN
                ROLLBACK;
                RAISE;
        END;
        $$;

    {% endcall %}


    {# 10. Read actual transaction results. #}

    {% set result = load_result('main') %}
    {% set result_table = result['table'] %}

    {% if result_table is none
        or result_table.rows | length == 0 %}

        {{ exceptions.raise_compiler_error(
            'TRUNCATE_LOAD: Transaction returned no DML counts.'
        ) }}

    {% endif %}

    {% set returned_value = result_table.rows[0][0] | string %}
    {% set parts = returned_value.split('|') %}

    {% if parts | length != 2 %}

        {{ exceptions.raise_compiler_error(
            'TRUNCATE_LOAD: Unexpected transaction result: '
            ~ returned_value
        ) }}

    {% endif %}

    {% set dml_stats = {
        'rows_inserted': parts[1] | int,
        'rows_updated': 0,
        'rows_deleted': parts[0] | int
    } %}

    {% do log(
        'TRUNCATE_LOAD DML COMMITTED'
        ~ ' | Deleted=' ~ dml_stats['rows_deleted']
        ~ ' | Inserted=' ~ dml_stats['rows_inserted'],
        info=true
    ) %}


    {# 11. Record post-commit statistics. #}

    {% set after_result = run_query(
        'SELECT COUNT(*) FROM ' ~ target_relation
    ) %}

    {% set target_after = after_result.rows[0][0] | int %}

    {% do finish_model_stats(
        model_execution_id,
        dml_stats,
        target_after
    ) %}


    {# 12. Run replacement-specific post-load checks. #}

    {% set dq_result = run_truncate_post_load_checks(
        source_relation,
        target_relation,
        cfg['natural_keys'],
        model_execution_id,
        etl_nr,
        pre_results['source_row_count'],
        target_before,
        target_after,
        dml_stats,
        cfg['post_load']
    ) %}


    {# 13. Cleanup and finalize. #}

    {% call statement('cleanup_source', auto_begin=False) %}
        DROP TABLE IF EXISTS {{ source_relation }}
    {% endcall %}

    {{ run_hooks(post_hooks_to_run) }}

    {% do adapter.commit() %}

    {% if not dq_result['passed'] %}

        {{ exceptions.raise_compiler_error(
            'TRUNCATE_LOAD: Post-load DQ failed after commit. '
            ~ 'Failed checks: '
            ~ (dq_result['failed_checks'] | join(', '))
        ) }}

    {% endif %}

    {% do log(
        'ENTERPRISE_LOAD TRUNCATE_LOAD SUCCESS'
        ~ ' | Target=' ~ target_relation
        ~ ' | ETL_NR=' ~ etl_nr
        ~ ' | Deleted=' ~ dml_stats['rows_deleted']
        ~ ' | Inserted=' ~ dml_stats['rows_inserted'],
        info=true
    ) %}

    {% do return(target_relation) %}

{% endmacro %}


{# ===== From: macros/delete_insert_load.sql | definition bodies unchanged ===== #}

{# Atomic key-scoped DELETE + INSERT for governed Snowflake targets. #}

{% macro execute_enterprise_delete_insert(
    cfg, load_cfg, model_id, model_name,
    target_relation_name, model_sql,
    pre_hooks_to_run, post_hooks_to_run
) %}

    {# 1. Initialize model audit. #}

    {% set etl_nr = get_current_etl_nr() %}

    {% set model_execution_id = start_model_stats(
        etl_nr, model_id, target_relation_name, 'DELETE_INSERT'
    ) %}

    {# Optional declared-window validation/audit; no SQL filtering is performed here. #}
    {% do enforce_processing_window(cfg['processing_window'], model_execution_id, etl_nr) %}


    {# 2. Resolve existing governed target. #}

    {% set target_relation = adapter.get_relation(
        database=target_relation_name.database,
        schema=target_relation_name.schema,
        identifier=target_relation_name.identifier
    ) %}

    {% if target_relation is none
        or target_relation.type | lower != 'table' %}

        {% do record_pre_load_check(
            model_execution_id, etl_nr,
            'TARGET_EXISTS_AND_IS_TABLE',
            false, 0, 1,
            'Governed target must already exist: '
            ~ target_relation_name
        ) %}

        {% do mark_pre_load_failed(
            model_execution_id,
            ['TARGET_EXISTS_AND_IS_TABLE']
        ) %}

        {{ exceptions.raise_compiler_error(
            'DELETE_INSERT: Target missing or not a physical table: '
            ~ target_relation_name
        ) }}

    {% endif %}


    {# 3. Prepare temporary source snapshot. #}

    {% set temp_name = 'EL_SRC_' ~ (model_name | upper)
        ~ '_' ~ (invocation_id | replace('-', '') | upper) %}

    {% set source_relation = api.Relation.create(
        database=target_relation.database,
        schema=target_relation.schema,
        identifier=temp_name,
        type='table'
    ) %}

    {{ run_hooks(pre_hooks_to_run) }}

    {% call statement('prepare_source', auto_begin=False) %}

        CREATE TEMPORARY TABLE {{ source_relation }} AS
        {{ model_sql }}

    {% endcall %}


    {# 4. Run shared pre-load validations. #}

    {% do set_model_stage(
        model_execution_id, 'PRE_LOAD_RUNNING'
    ) %}

    {% set pre_results = check_pre_load(
        source_relation,
        target_relation,
        cfg['natural_keys'],
        cfg['pre_load']['require_nonempty'],
        model_execution_id,
        etl_nr
    ) %}


    {# 5. Historical source-volume validation. #}

    {% if pre_results['passed'] %}

        {% set volume_result = check_source_volume_threshold(
            model_execution_id,
            etl_nr,
            model_id,
            pre_results['source_row_count'],
            cfg['pre_load']['volume_threshold']
        ) %}

        {% if not volume_result['passed'] %}

            {% for check_name in volume_result['failed_checks'] %}

                {% do pre_results['failed_checks'].append(
                    check_name
                ) %}

            {% endfor %}

            {% do pre_results.update({'passed': false}) %}

        {% endif %}

    {% endif %}


    {# 6. Reject invalid prepared source before any target DML. #}

    {% if not pre_results['passed'] %}

        {% do mark_pre_load_failed(
            model_execution_id,
            pre_results['failed_checks'],
            pre_results['source_row_count']
        ) %}

        {{ exceptions.raise_compiler_error(
            'DELETE_INSERT: Pre-load validation blocked the load. '
            ~ 'Failed checks: '
            ~ (pre_results['failed_checks'] | join(', '))
            ~ '. Governed target was not modified.'
        ) }}

    {% endif %}


    {# 7. Capture target state and expected matching-key count. #}

    {% set before_result = run_query(
        'SELECT COUNT(*) FROM ' ~ target_relation
    ) %}

    {% set target_before = before_result.rows[0][0] | int %}

    {% set join_condition = generate_delete_insert_key_match(
        cfg['natural_keys'],
        'tgt',
        'src'
    ) %}

    {% set overlap_sql %}

        SELECT COUNT(*)
        FROM {{ target_relation }} AS tgt
        INNER JOIN {{ source_relation }} AS src
            ON {{ join_condition }}

    {% endset %}

    {% set overlap_result = run_query(overlap_sql) %}

    {% set expected_deleted =
        overlap_result.rows[0][0] | int %}

    {% do mark_pre_load_passed(
        model_execution_id,
        pre_results['source_row_count'],
        target_before
    ) %}


    {# 8. Generate key-scoped DELETE and full-row INSERT. #}

    {% set delete_sql = generate_delete_insert_delete_sql(
        source_relation,
        target_relation,
        cfg['natural_keys']
    ) %}

    {% set insert_sql = generate_replace_insert_sql(
        source_relation,
        target_relation,
        etl_nr,
        'enterprise_dbt_framework'
    ) %}


    {# 9. Execute DELETE and INSERT in one transaction.
       The DML-count invariant is enforced before COMMIT; broader DQ remains
       post-commit so expensive scans do not extend the critical section. #}

    {% set inject_critical_failure = (
        model_name == 'regression_customer_delete_insert'
        and var('regression_delete_insert_force_critical_failure', false) == true
    ) %}

    {# Regression-only concurrency delay. Hold the transaction after DELETE
       has acquired the target write lock so a second independent dbt process
       evaluates its stale pre-transaction overlap count under contention. #}
    {% set concurrency_wait_seconds = 0 %}
    {% if model_name == 'regression_customer_delete_insert' %}
        {% set concurrency_wait_seconds = var(
            'regression_delete_insert_concurrency_wait_seconds', 0
        ) | int %}
    {% endif %}

    {% if concurrency_wait_seconds > 0 %}
        {% do log(
            'DELETE_INSERT REGRESSION CONCURRENCY HOLD | seconds='
            ~ concurrency_wait_seconds,
            info=true
        ) %}
    {% endif %}

    {% do set_model_stage(
        model_execution_id, 'MERGE_RUNNING'
    ) %}

    {% call statement('main', fetch_result=True, auto_begin=False) %}

        EXECUTE IMMEDIATE $$
        DECLARE
            v_deleted NUMBER DEFAULT 0;
            v_inserted NUMBER DEFAULT 0;
            v_failure_reason VARCHAR DEFAULT NULL;
            delete_insert_critical_dq_failed EXCEPTION
                (-20042, 'DELETE_INSERT_CRITICAL_DQ_FAILED: transaction rolled back');
        BEGIN

            BEGIN TRANSACTION;

            {{ delete_sql }};

            v_deleted := SQLROWCOUNT;

            {# REGRESSION_RUNTIME_FAILURE_DELETE_INSERT_BEGIN #}
            {% if model_name == 'regression_customer_delete_insert'
                  and var('regression_delete_insert_force_runtime_failure', false) %}
                LET regression_runtime_fault NUMBER :=
                    TO_NUMBER('RUNTIME_FAULT_SENTINEL_' || UUID_STRING());
            {% endif %}
            {# REGRESSION_RUNTIME_FAILURE_DELETE_INSERT_END #}

            {# Regression-only hold after DELETE has taken the target write lock. #}
            {% if concurrency_wait_seconds > 0 %}
                CALL SYSTEM$WAIT({{ concurrency_wait_seconds }}, 'SECONDS');
            {% endif %}

            {{ insert_sql }};

            v_inserted := SQLROWCOUNT;

            -- Commit gate: the immutable prepared source and pre-DML overlap
            -- define exactly how many rows this transaction may affect.
            IF (v_deleted <> {{ expected_deleted }}
                OR v_inserted <> {{ pre_results['source_row_count'] | int }}) THEN
                v_failure_reason := 'DML_COUNT_MISMATCH';
                RAISE delete_insert_critical_dq_failed;
            END IF;

            {# Regression-only failure injection proves validation-triggered rollback. #}
            {% if inject_critical_failure %}
                v_failure_reason := 'REGRESSION_FORCED_FAILURE';
                RAISE delete_insert_critical_dq_failed;
            {% endif %}

            COMMIT;

            RETURN TO_VARCHAR(v_deleted)
                || '|' || TO_VARCHAR(v_inserted);

        EXCEPTION
            WHEN delete_insert_critical_dq_failed THEN
                ROLLBACK;
                BEGIN
                    BEGIN TRANSACTION;
                    {{ transaction_dq_failure_audit_sql(
                        model_execution_id, etl_nr,
                        'DELETE_INSERT_TRANSACTION_CRITICAL_DQ',
                        'DELETE_INSERT'
                    ) }}
                    COMMIT;
                EXCEPTION
                    WHEN OTHER THEN
                        ROLLBACK;
                        NULL;
                END;
                RAISE;
            WHEN OTHER THEN
                ROLLBACK;
                RAISE;
        END;
        $$;

    {% endcall %}


    {# 10. Extract actual DELETE and INSERT counts. #}

    {% set result = load_result('main') %}

    {% set result_table = result['table'] %}

    {% if result_table is none
        or result_table.rows | length == 0 %}

        {{ exceptions.raise_compiler_error(
            'DELETE_INSERT: Transaction returned no DML counts.'
        ) }}

    {% endif %}

    {% set returned_value = result_table.rows[0][0] | string %}
    {% set parts = returned_value.split('|') %}

    {% if parts | length != 2 %}

        {{ exceptions.raise_compiler_error(
            'DELETE_INSERT: Unexpected transaction result: '
            ~ returned_value
        ) }}

    {% endif %}

    {% set dml_stats = {
        'rows_inserted': parts[1] | int,
        'rows_updated': 0,
        'rows_deleted': parts[0] | int
    } %}

    {% do log(
        'DELETE_INSERT DML COMMITTED'
        ~ ' | Deleted=' ~ dml_stats['rows_deleted']
        ~ ' | Inserted=' ~ dml_stats['rows_inserted'],
        info=true
    ) %}


    {# 11. Record committed DML statistics. #}

    {% set after_result = run_query(
        'SELECT COUNT(*) FROM ' ~ target_relation
    ) %}

    {% set target_after = after_result.rows[0][0] | int %}

    {% do finish_model_stats(
        model_execution_id,
        dml_stats,
        target_after
    ) %}


    {# 12. Execute DELETE_INSERT post-load validations. #}

    {% set dq_result = run_delete_insert_post_load_checks(
        source_relation,
        target_relation,
        cfg['natural_keys'],
        model_execution_id,
        etl_nr,
        pre_results['source_row_count'],
        target_before,
        target_after,
        expected_deleted,
        dml_stats,
        cfg['post_load']
    ) %}


    {# 13. Cleanup and finalize. #}

    {% call statement('cleanup_source', auto_begin=False) %}
        DROP TABLE IF EXISTS {{ source_relation }}
    {% endcall %}

    {{ run_hooks(post_hooks_to_run) }}

    {% do adapter.commit() %}

    {% if not dq_result['passed'] %}

        {{ exceptions.raise_compiler_error(
            'DELETE_INSERT: Post-load DQ failed after commit. '
            ~ 'Failed checks: '
            ~ (dq_result['failed_checks'] | join(', '))
        ) }}

    {% endif %}

    {% do log(
        'ENTERPRISE_LOAD DELETE_INSERT SUCCESS'
        ~ ' | Target=' ~ target_relation
        ~ ' | ETL_NR=' ~ etl_nr
        ~ ' | Deleted=' ~ dml_stats['rows_deleted']
        ~ ' | Inserted=' ~ dml_stats['rows_inserted']
        ~ ' | Updated=' ~ dml_stats['rows_updated'],
        info=true
    ) %}

    {% do return(target_relation) %}

{% endmacro %}
