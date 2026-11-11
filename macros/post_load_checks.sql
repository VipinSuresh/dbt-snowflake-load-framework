{# ===== From: macros/post_load_checks.sql | definition bodies unchanged ===== #}
{# Post-load DQ evidence; SKIP is explicit for disabled optional checks. #}
{% macro record_skipped_post_load_check(model_execution_id, etl_nr, check_name) %}
    {% set skip_sql %}
        INSERT INTO {{ enterprise_control_relation('ETL_DQ_RESULTS') }}
        (
            DQ_RESULT_ID,
            MODEL_EXECUTION_ID,
            ETL_NR,
            CHECK_NAME,
            CHECK_PHASE,
            CHECK_STATUS,
            OBSERVED_VALUE,
            REFERENCE_VALUE,
            DETAILS,
            CHECKED_AT
        )
        SELECT
            UUID_STRING(),
            '{{ model_execution_id }}',
            {{ etl_nr }},
            '{{ check_name }}',
            'POST_LOAD',
            'SKIP',
            NULL,
            NULL,
            OBJECT_CONSTRUCT('reason', 'Disabled by YAML configuration', 'load_type', 'TYPE1'),
            CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)
    {% endset %}
    {% do run_query(skip_sql) %}
    {% do log('POST_LOAD | ' ~ check_name ~ ' | SKIP | Disabled by YAML', info=true) %}
{% endmacro %}


{% macro run_type1_post_load_checks(
    source_relation,
    target_relation,
    natural_keys,
    model_execution_id,
    etl_nr,
    target_before,
    target_after,
    dml_stats,
    post_load_cfg=none
) %}

    {# Backward-compatible defaults if a test macro calls this directly. #}
    {% if post_load_cfg is none %}
        {% set post_load_cfg = {
            'reconcile_counts': true,
            'duplicate_check': true,
            'source_target_match': true
        } %}
    {% endif %}

    {% set failures = [] %}
    {% set keys = [] %}
    {% set join_conditions = [] %}
    {% set null_conditions = [] %}

    {% for key in natural_keys %}
        {% set q = adapter.quote(key | upper) %}
        {% do keys.append(q) %}
        {% do join_conditions.append('src.' ~ q ~ ' = tgt.' ~ q) %}
        {% do null_conditions.append('tgt.' ~ q ~ ' IS NULL') %}
    {% endfor %}

    {# Optional: target row count after = before + inserts - deletes. #}
    {% if post_load_cfg['reconcile_counts'] %}
        {% set expected_count = target_before
            + dml_stats['rows_inserted'] - dml_stats['rows_deleted'] %}
        {% set status = record_post_load_check(
            model_execution_id, etl_nr,
            'TYPE1_ROW_COUNT_DELTA', target_after, expected_count
        ) %}
        {% if status == 'FAIL' %}
            {% do failures.append('ROW_COUNT_DELTA') %}
        {% endif %}
    {% else %}
        {% do record_skipped_post_load_check(
            model_execution_id, etl_nr, 'TYPE1_ROW_COUNT_DELTA'
        ) %}
    {% endif %}

    {# Mandatory: target natural keys cannot contain NULLs. #}
    {% set null_sql %}
        SELECT COUNT(*)
        FROM {{ target_relation }} tgt
        WHERE {{ null_conditions | join(' OR ') }}
    {% endset %}
    {% set null_result = run_query(null_sql) %}
    {% set null_rows = null_result.rows[0][0] | int %}
    {% set status = record_post_load_check(
        model_execution_id, etl_nr,
        'TYPE1_TARGET_NULL_KEYS', null_rows, 0
    ) %}
    {% if status == 'FAIL' %}
        {% do failures.append('TARGET_NULL_KEYS') %}
    {% endif %}

    {# Mandatory: target natural keys must be unique. #}
    {% set dup_sql %}
        SELECT COUNT(*)
        FROM (
            SELECT {{ keys | join(', ') }}
            FROM {{ target_relation }}
            GROUP BY {{ keys | join(', ') }}
            HAVING COUNT(*) > 1
        ) duplicate_groups
    {% endset %}
    {% set dup_result = run_query(dup_sql) %}
    {% set duplicate_groups = dup_result.rows[0][0] | int %}
    {% set status = record_post_load_check(
        model_execution_id, etl_nr,
        'TYPE1_TARGET_DUPLICATES', duplicate_groups, 0
    ) %}
    {% if status == 'FAIL' %}
        {% do failures.append('TARGET_DUPLICATES') %}
    {% endif %}

    {# Optional: compare business values for loaded keys, including NULL-safe differences. #}
    {% if post_load_cfg['source_target_match'] %}
        {% set source_columns = adapter.get_columns_in_relation(source_relation) %}
        {% set normalized_keys = [] %}
        {% for key in natural_keys %}
            {% do normalized_keys.append(key | upper) %}
        {% endfor %}
        {% set differences = [] %}
        {% for col in source_columns %}
            {% if col.name | upper not in normalized_keys %}
                {% set q = adapter.quote(col.name) %}
                {% do differences.append(
                    'src.' ~ q ~ ' IS DISTINCT FROM tgt.' ~ q
                ) %}
            {% endif %}
        {% endfor %}
        {% set first_key = keys[0] %}
        {% set comparison_sql %}
            SELECT COUNT(*)
            FROM {{ source_relation }} src
            LEFT JOIN {{ target_relation }} tgt
                ON {{ join_conditions | join(' AND ') }}
            WHERE tgt.{{ first_key }} IS NULL
            {% if differences | length > 0 %}
                OR {{ differences | join(' OR ') }}
            {% endif %}
        {% endset %}
        {% set comparison_result = run_query(comparison_sql) %}
        {% set mismatch_rows = comparison_result.rows[0][0] | int %}
        {% set status = record_post_load_check(
            model_execution_id, etl_nr,
            'TYPE1_SOURCE_TARGET_MATCH', mismatch_rows, 0
        ) %}
        {% if status == 'FAIL' %}
            {% do failures.append('SOURCE_TARGET_MATCH') %}
        {% endif %}
    {% else %}
        {% do record_skipped_post_load_check(
            model_execution_id, etl_nr, 'TYPE1_SOURCE_TARGET_MATCH'
        ) %}
    {% endif %}

    {% set passed = failures | length == 0 %}
    {% do finalize_model_dq(model_execution_id, passed) %}
    {% do return({'passed': passed, 'failed_checks': failures}) %}
{% endmacro %}


{# ===== From: macros/type2_checks.sql :: post_load macros | definition bodies unchanged ===== #}



{% macro record_type2_post_check(model_execution_id, etl_nr, check_name, observed, expected) %}
  {% set status = 'PASS' if observed == expected else 'FAIL' %}
  {% set sql %}
    INSERT INTO {{ enterprise_control_relation('ETL_DQ_RESULTS') }}
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
    INSERT INTO {{ enterprise_control_relation('ETL_DQ_RESULTS') }}
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


{% macro run_type2_post_load_checks(source_relation, target_relation, affected_relation, natural_keys,
      model_execution_id, etl_nr, affected_before, affected_after, dml_stats, post_cfg) %}
  {% set failures = [] %}
  {% set keys = [] %}
  {% set join_on = [] %}
  {% set source_affected_join = [] %}
  {% set target_affected_join = [] %}
  {% for key in natural_keys %}
    {% set q = adapter.quote(key) %}
    {% do keys.append(q) %}
    {% do join_on.append('s.' ~ q ~ ' = t.' ~ q) %}
    {% do source_affected_join.append('s.' ~ q ~ ' = d.' ~ q) %}
    {% do target_affected_join.append('t.' ~ q ~ ' = d.' ~ q) %}
  {% endfor %}
  {% set target_affected_predicate = 'EXISTS (SELECT 1 FROM ' ~ affected_relation
      ~ ' d WHERE ' ~ (target_affected_join | join(' AND ')) ~ ')' %}
  {% set source_affected_predicate = 'EXISTS (SELECT 1 FROM ' ~ affected_relation
      ~ ' d WHERE ' ~ (source_affected_join | join(' AND ')) ~ ')' %}

  {# Post-load success is execution-scoped. Whole-table row counts are not
     stable under legitimate concurrent writers, so reconcile only history
     rows belonging to natural keys in this invocation's immutable delta. #}
  {% if post_cfg['reconcile_counts'] %}
    {% set expected = affected_before + dml_stats['rows_inserted'] %}
    {% set status = record_type2_post_check(model_execution_id, etl_nr,
      'TYPE2_HISTORY_ROW_DELTA', affected_after, expected) %}
    {% if status == 'FAIL' %}{% do failures.append('TYPE2_HISTORY_ROW_DELTA') %}{% endif %}
  {% else %}
    {% do record_type2_skip(model_execution_id, etl_nr, 'TYPE2_HISTORY_ROW_DELTA') %}
  {% endif %}

  {# The whole target was validated before DML. After commit, validate the
     history this invocation owned so unrelated concurrent keys cannot make
     this run fail after its own transaction succeeded. #}
  {% set current_sql %}
    SELECT COUNT(*) FROM (
      SELECT {{ keys | join(', ') }}
      FROM {{ target_relation }} t
      WHERE {{ target_affected_predicate }}
      GROUP BY {{ keys | join(', ') }}
      HAVING COALESCE(COUNT_IF(IS_CURRENT = TRUE), 0) <> 1
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
      FROM {{ target_relation }} t
      WHERE {{ target_affected_predicate }}
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
      SELECT COUNT(*)
      FROM {{ source_relation }} s
      LEFT JOIN {{ target_relation }} t
        ON {{ join_on | join(' AND ') }} AND t.IS_CURRENT = TRUE
      WHERE {{ source_affected_predicate }}
        AND (
          t.{{ keys[0] }} IS NULL
          {% if nonkey | length > 0 %}OR {{ nonkey | join(' OR ') }}{% endif %}
        )
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


{# ===== From: macros/insert_checks.sql | definition bodies unchanged ===== #}

{# INSERT strategy post-load DQ checks. #}

{% macro record_insert_skip(model_execution_id, etl_nr, check_name) %}

    {% set sql %}
        INSERT INTO {{ enterprise_control_relation('ETL_DQ_RESULTS') }}
        (
            DQ_RESULT_ID, MODEL_EXECUTION_ID, ETL_NR,
            CHECK_NAME, CHECK_PHASE, CHECK_STATUS,
            OBSERVED_VALUE, REFERENCE_VALUE,
            DETAILS, CHECKED_AT
        )
        SELECT
            UUID_STRING(),
            '{{ model_execution_id }}',
            {{ etl_nr }},
            '{{ check_name }}',
            'POST_LOAD',
            'SKIP',
            NULL,
            NULL,
            OBJECT_CONSTRUCT(
                'load_type', 'INSERT',
                'reason', 'Disabled by YAML configuration'
            ),
            CONVERT_TIMEZONE(
                'UTC', CURRENT_TIMESTAMP()
            )::TIMESTAMP_NTZ(9)
    {% endset %}

    {% do run_query(sql) %}
    {% do log('POST_LOAD | ' ~ check_name ~ ' | SKIP', info=true) %}

{% endmacro %}


{% macro run_insert_post_load_checks(
    source_relation,
    target_relation,
    natural_keys,
    model_execution_id,
    etl_nr,
    target_before,
    target_after,
    dml_stats,
    post_cfg
) %}

    {% set failures = [] %}
    {% set keys = [] %}
    {% set join_conditions = [] %}
    {% set null_conditions = [] %}
    {% set normalized_keys = [] %}

    {% for key in natural_keys %}
        {% set key_name = key | upper %}
        {% set q = adapter.quote(key_name) %}

        {% do normalized_keys.append(key_name) %}
        {% do keys.append(q) %}
        {% do join_conditions.append('src.' ~ q ~ ' = tgt.' ~ q) %}
        {% do null_conditions.append('tgt.' ~ q ~ ' IS NULL') %}
    {% endfor %}

    {# 1. Target row-count reconciliation. #}

    {% if post_cfg['reconcile_counts'] %}

        {% set expected_count =
            target_before + dml_stats['rows_inserted'] %}

        {% set status = record_post_load_check(
            model_execution_id, etl_nr,
            'INSERT_ROW_COUNT_DELTA',
            target_after,
            expected_count
        ) %}

        {% if status == 'FAIL' %}
            {% do failures.append('INSERT_ROW_COUNT_DELTA') %}
        {% endif %}

    {% else %}

        {% do record_insert_skip(
            model_execution_id, etl_nr,
            'INSERT_ROW_COUNT_DELTA'
        ) %}

    {% endif %}

    {# 2. INSERT must never update or delete target rows. #}

    {% set mutation_count =
        dml_stats['rows_updated'] + dml_stats['rows_deleted'] %}

    {% set status = record_post_load_check(
        model_execution_id, etl_nr,
        'INSERT_NO_MUTATIONS',
        mutation_count,
        0
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('INSERT_NO_MUTATIONS') %}
    {% endif %}

    {# 3. Mandatory target NULL-key validation. #}

    {% set null_sql %}
        SELECT COUNT(*)
        FROM {{ target_relation }} tgt
        WHERE {{ null_conditions | join(' OR ') }}
    {% endset %}

    {% set null_count = run_query(null_sql).rows[0][0] | int %}

    {% set status = record_post_load_check(
        model_execution_id, etl_nr,
        'INSERT_TARGET_NULL_KEYS',
        null_count,
        0
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('INSERT_TARGET_NULL_KEYS') %}
    {% endif %}

    {# 4. Mandatory target duplicate-key validation. #}

    {% set duplicate_sql %}
        SELECT COUNT(*)
        FROM (
            SELECT {{ keys | join(', ') }}
            FROM {{ target_relation }}
            GROUP BY {{ keys | join(', ') }}
            HAVING COUNT(*) > 1
        ) d
    {% endset %}

    {% set duplicate_count =
        run_query(duplicate_sql).rows[0][0] | int %}

    {% set status = record_post_load_check(
        model_execution_id, etl_nr,
        'INSERT_TARGET_DUPLICATES',
        duplicate_count,
        0
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('INSERT_TARGET_DUPLICATES') %}
    {% endif %}

    {# 5. Reconcile inserted count against ETL audit metadata. #}

    {% set run_count_sql %}
        SELECT COUNT(*)
        FROM {{ target_relation }}
        WHERE ETL_CREATED_RUN_NUMBER = {{ etl_nr }}
    {% endset %}

    {% set run_insert_count =
        run_query(run_count_sql).rows[0][0] | int %}

    {% set status = record_post_load_check(
        model_execution_id, etl_nr,
        'INSERT_RUN_ROW_COUNT',
        run_insert_count,
        dml_stats['rows_inserted']
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('INSERT_RUN_ROW_COUNT') %}
    {% endif %}

    {# 6. Only newly inserted rows must match source. #}

    {% if post_cfg['source_target_match'] %}

        {% set source_columns =
            adapter.get_columns_in_relation(source_relation) %}

        {% set differences = [] %}

        {% for col in source_columns %}

            {% if col.name | upper not in normalized_keys %}

                {% set q = adapter.quote(col.name) %}

                {% do differences.append(
                    'src.' ~ q ~ ' IS DISTINCT FROM tgt.' ~ q
                ) %}

            {% endif %}

        {% endfor %}

        {% set match_sql %}
            SELECT COUNT(*)
            FROM {{ target_relation }} tgt

            LEFT JOIN {{ source_relation }} src
                ON {{ join_conditions | join(' AND ') }}

            WHERE tgt.ETL_CREATED_RUN_NUMBER = {{ etl_nr }}
              AND (
                  src.{{ keys[0] }} IS NULL

                  {% if differences | length > 0 %}
                    OR {{ differences | join(' OR ') }}
                  {% endif %}
              )
        {% endset %}

        {% set mismatches =
            run_query(match_sql).rows[0][0] | int %}

        {% set status = record_post_load_check(
            model_execution_id, etl_nr,
            'INSERT_SOURCE_MATCH_NEW_ROWS',
            mismatches,
            0
        ) %}

        {% if status == 'FAIL' %}
            {% do failures.append('INSERT_SOURCE_MATCH_NEW_ROWS') %}
        {% endif %}

    {% else %}

        {% do record_insert_skip(
            model_execution_id, etl_nr,
            'INSERT_SOURCE_MATCH_NEW_ROWS'
        ) %}

    {% endif %}

    {# Finalize audit after all checks. #}

    {% set passed = failures | length == 0 %}

    {% do finalize_model_dq(
        model_execution_id,
        passed
    ) %}

    {% do return({
        'passed': passed,
        'failed_checks': failures
    }) %}

{% endmacro %}


{# ===== From: macros/update_checks.sql | definition bodies unchanged ===== #}

{# UPDATE-only post-load DQ checks. #}

{% macro record_update_post_check(
    model_execution_id, etl_nr, check_name, observed, expected
) %}

    {% set status = 'PASS' if observed == expected else 'FAIL' %}

    {% set sql %}
        INSERT INTO {{ enterprise_control_relation('ETL_DQ_RESULTS') }}
        (
            DQ_RESULT_ID,
            MODEL_EXECUTION_ID,
            ETL_NR,
            CHECK_NAME,
            CHECK_PHASE,
            CHECK_STATUS,
            OBSERVED_VALUE,
            REFERENCE_VALUE,
            DETAILS,
            CHECKED_AT
        )
        SELECT
            UUID_STRING(),
            '{{ model_execution_id }}',
            {{ etl_nr }},
            '{{ check_name }}',
            'POST_LOAD',
            '{{ status }}',
            {{ observed }},
            {{ expected }},
            OBJECT_CONSTRUCT('load_type', 'UPDATE'),
            CONVERT_TIMEZONE(
                'UTC', CURRENT_TIMESTAMP()
            )::TIMESTAMP_NTZ(9)
    {% endset %}

    {% do run_query(sql) %}

    {% do log(
        'POST_LOAD | ' ~ check_name ~ ' | ' ~ status,
        info=true
    ) %}

    {% do return(status) %}

{% endmacro %}


{% macro record_update_skip(
    model_execution_id, etl_nr, check_name
) %}

    {% set sql %}
        INSERT INTO {{ enterprise_control_relation('ETL_DQ_RESULTS') }}
        (
            DQ_RESULT_ID,
            MODEL_EXECUTION_ID,
            ETL_NR,
            CHECK_NAME,
            CHECK_PHASE,
            CHECK_STATUS,
            OBSERVED_VALUE,
            REFERENCE_VALUE,
            DETAILS,
            CHECKED_AT
        )
        SELECT
            UUID_STRING(),
            '{{ model_execution_id }}',
            {{ etl_nr }},
            '{{ check_name }}',
            'POST_LOAD',
            'SKIP',
            NULL,
            NULL,
            OBJECT_CONSTRUCT(
                'load_type', 'UPDATE',
                'reason', 'Disabled by YAML configuration'
            ),
            CONVERT_TIMEZONE(
                'UTC', CURRENT_TIMESTAMP()
            )::TIMESTAMP_NTZ(9)
    {% endset %}

    {% do run_query(sql) %}

    {% do log(
        'POST_LOAD | ' ~ check_name ~ ' | SKIP',
        info=true
    ) %}

{% endmacro %}


{% macro run_update_post_load_checks(
    source_relation,
    target_relation,
    natural_keys,
    model_execution_id,
    etl_nr,
    target_before,
    target_after,
    dml_stats,
    post_cfg
) %}

    {% set failures = [] %}
    {% set keys = [] %}
    {% set join_conditions = [] %}
    {% set null_conditions = [] %}
    {% set normalized_keys = [] %}

    {% for key in natural_keys %}

        {% set key_name = key | upper %}
        {% set q = adapter.quote(key_name) %}

        {% do normalized_keys.append(key_name) %}
        {% do keys.append(q) %}

        {% do join_conditions.append(
            'src.' ~ q ~ ' = tgt.' ~ q
        ) %}

        {% do null_conditions.append(
            'tgt.' ~ q ~ ' IS NULL'
        ) %}

    {% endfor %}


    {# 1. UPDATE must preserve total target row count. #}

    {% if post_cfg['reconcile_counts'] %}

        {% set status = record_update_post_check(
            model_execution_id,
            etl_nr,
            'UPDATE_ROW_COUNT_UNCHANGED',
            target_after,
            target_before
        ) %}

        {% if status == 'FAIL' %}
            {% do failures.append(
                'UPDATE_ROW_COUNT_UNCHANGED'
            ) %}
        {% endif %}

    {% else %}

        {% do record_update_skip(
            model_execution_id,
            etl_nr,
            'UPDATE_ROW_COUNT_UNCHANGED'
        ) %}

    {% endif %}


    {# 2. UPDATE must never insert or delete records. #}

    {% set unexpected_mutations =
        dml_stats['rows_inserted'] + dml_stats['rows_deleted'] %}

    {% set status = record_update_post_check(
        model_execution_id,
        etl_nr,
        'UPDATE_NO_INSERT_DELETE',
        unexpected_mutations,
        0
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('UPDATE_NO_INSERT_DELETE') %}
    {% endif %}


    {# 3. Target natural keys must not contain NULL. #}

    {% set null_sql %}
        SELECT COUNT(*)
        FROM {{ target_relation }} tgt
        WHERE {{ null_conditions | join(' OR ') }}
    {% endset %}

    {% set null_count =
        run_query(null_sql).rows[0][0] | int %}

    {% set status = record_update_post_check(
        model_execution_id,
        etl_nr,
        'UPDATE_TARGET_NULL_KEYS',
        null_count,
        0
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('UPDATE_TARGET_NULL_KEYS') %}
    {% endif %}


    {# 4. Target natural keys must remain unique. #}

    {% set duplicate_sql %}
        SELECT COUNT(*)
        FROM (
            SELECT {{ keys | join(', ') }}
            FROM {{ target_relation }}
            GROUP BY {{ keys | join(', ') }}
            HAVING COUNT(*) > 1
        ) duplicate_groups
    {% endset %}

    {% set duplicate_count =
        run_query(duplicate_sql).rows[0][0] | int %}

    {% set status = record_update_post_check(
        model_execution_id,
        etl_nr,
        'UPDATE_TARGET_DUPLICATES',
        duplicate_count,
        0
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('UPDATE_TARGET_DUPLICATES') %}
    {% endif %}


    {# 5. Reconcile updated rows with ETL run metadata. #}

    {% set run_count_sql %}
        SELECT COUNT(*)
        FROM {{ target_relation }}
        WHERE ETL_UPDATED_RUN_NUMBER = {{ etl_nr }}
    {% endset %}

    {% set run_update_count =
        run_query(run_count_sql).rows[0][0] | int %}

    {% set status = record_update_post_check(
        model_execution_id,
        etl_nr,
        'UPDATE_RUN_ROW_COUNT',
        run_update_count,
        dml_stats['rows_updated']
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('UPDATE_RUN_ROW_COUNT') %}
    {% endif %}


    {# 6. Validate only matching source/target natural keys.
       Unmatched source keys are intentionally ignored.
    #}

    {% if post_cfg['source_target_match'] %}

        {% set source_columns =
            adapter.get_columns_in_relation(source_relation) %}

        {% set differences = [] %}

        {% for col in source_columns %}

            {% if col.name | upper not in normalized_keys %}

                {% set q = adapter.quote(col.name) %}

                {% do differences.append(
                    'src.' ~ q ~ ' IS DISTINCT FROM tgt.' ~ q
                ) %}

            {% endif %}

        {% endfor %}

        {% set match_sql %}
            SELECT COUNT(*)
            FROM {{ source_relation }} src
            INNER JOIN {{ target_relation }} tgt
                ON {{ join_conditions | join(' AND ') }}

            WHERE
                {% if differences | length > 0 %}
                    {{ differences | join(' OR ') }}
                {% else %}
                    1 = 0
                {% endif %}
        {% endset %}

        {% set mismatch_count =
            run_query(match_sql).rows[0][0] | int %}

        {% set status = record_update_post_check(
            model_execution_id,
            etl_nr,
            'UPDATE_SOURCE_TARGET_MATCH',
            mismatch_count,
            0
        ) %}

        {% if status == 'FAIL' %}
            {% do failures.append(
                'UPDATE_SOURCE_TARGET_MATCH'
            ) %}
        {% endif %}

    {% else %}

        {% do record_update_skip(
            model_execution_id,
            etl_nr,
            'UPDATE_SOURCE_TARGET_MATCH'
        ) %}

    {% endif %}


    {# Finalize model-level DQ status. #}

    {% set passed = failures | length == 0 %}

    {% do finalize_model_dq(
        model_execution_id,
        passed
    ) %}

    {% do return({
        'passed': passed,
        'failed_checks': failures
    }) %}

{% endmacro %}


{# ===== From: macros/truncate_checks.sql | definition bodies unchanged ===== #}

{# TRUNCATE_LOAD post-load DQ. #}

{% macro record_truncate_post_check(
    model_execution_id, etl_nr, check_name, observed, expected
) %}

    {% set status = 'PASS' if observed == expected else 'FAIL' %}

    {% set sql %}
        INSERT INTO {{ enterprise_control_relation('ETL_DQ_RESULTS') }}
        (
            DQ_RESULT_ID, MODEL_EXECUTION_ID, ETL_NR,
            CHECK_NAME, CHECK_PHASE, CHECK_STATUS,
            OBSERVED_VALUE, REFERENCE_VALUE,
            DETAILS, CHECKED_AT
        )
        SELECT
            UUID_STRING(),
            '{{ model_execution_id }}',
            {{ etl_nr }},
            '{{ check_name }}',
            'POST_LOAD',
            '{{ status }}',
            {{ observed }},
            {{ expected }},
            OBJECT_CONSTRUCT('load_type', 'TRUNCATE_LOAD'),
            CONVERT_TIMEZONE(
                'UTC', CURRENT_TIMESTAMP()
            )::TIMESTAMP_NTZ(9)
    {% endset %}

    {% do run_query(sql) %}
    {% do log(
        'POST_LOAD | ' ~ check_name ~ ' | ' ~ status,
        info=true
    ) %}

    {% do return(status) %}

{% endmacro %}


{% macro record_truncate_skip(
    model_execution_id, etl_nr, check_name
) %}

    {% set sql %}
        INSERT INTO {{ enterprise_control_relation('ETL_DQ_RESULTS') }}
        (
            DQ_RESULT_ID, MODEL_EXECUTION_ID, ETL_NR,
            CHECK_NAME, CHECK_PHASE, CHECK_STATUS,
            OBSERVED_VALUE, REFERENCE_VALUE,
            DETAILS, CHECKED_AT
        )
        SELECT
            UUID_STRING(),
            '{{ model_execution_id }}',
            {{ etl_nr }},
            '{{ check_name }}',
            'POST_LOAD',
            'SKIP',
            NULL,
            NULL,
            OBJECT_CONSTRUCT(
                'load_type', 'TRUNCATE_LOAD',
                'reason', 'Disabled by YAML configuration'
            ),
            CONVERT_TIMEZONE(
                'UTC', CURRENT_TIMESTAMP()
            )::TIMESTAMP_NTZ(9)
    {% endset %}

    {% do run_query(sql) %}
    {% do log(
        'POST_LOAD | ' ~ check_name ~ ' | SKIP',
        info=true
    ) %}

{% endmacro %}


{% macro run_truncate_post_load_checks(
    source_relation,
    target_relation,
    natural_keys,
    model_execution_id,
    etl_nr,
    source_count,
    target_before,
    target_after,
    dml_stats,
    post_cfg
) %}

    {% set failures = [] %}
    {% set keys = [] %}
    {% set null_conditions = [] %}
    {% set join_conditions = [] %}
    {% set normalized_keys = [] %}

    {% for key in natural_keys %}
        {% set name = key | upper %}
        {% set q = adapter.quote(name) %}

        {% do normalized_keys.append(name) %}
        {% do keys.append(q) %}
        {% do null_conditions.append('tgt.' ~ q ~ ' IS NULL') %}
        {% do join_conditions.append('src.' ~ q ~ ' = tgt.' ~ q) %}
    {% endfor %}


    {# 1. Target after replacement equals prepared source count. #}

    {% set status = record_truncate_post_check(
        model_execution_id, etl_nr,
        'REPLACE_TARGET_ROW_COUNT',
        target_after, source_count
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('REPLACE_TARGET_ROW_COUNT') %}
    {% endif %}


    {# 2. Verify INSERT, DELETE and UPDATE statistics. #}

    {% set counts_ok =
        dml_stats['rows_deleted'] == target_before
        and dml_stats['rows_inserted'] == source_count
        and dml_stats['rows_updated'] == 0
        and target_after == (
            target_before
            - dml_stats['rows_deleted']
            + dml_stats['rows_inserted']
        )
    %}

    {% set status = record_truncate_post_check(
        model_execution_id, etl_nr,
        'REPLACE_DML_COUNTS',
        1 if counts_ok else 0, 1
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('REPLACE_DML_COUNTS') %}
    {% endif %}


    {# 3. Mandatory target NULL-key check. #}

    {% set null_sql %}
        SELECT COUNT(*)
        FROM {{ target_relation }} tgt
        WHERE {{ null_conditions | join(' OR ') }}
    {% endset %}

    {% set null_count = run_query(null_sql).rows[0][0] | int %}

    {% set status = record_truncate_post_check(
        model_execution_id, etl_nr,
        'REPLACE_TARGET_NULL_KEYS',
        null_count, 0
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('REPLACE_TARGET_NULL_KEYS') %}
    {% endif %}


    {# 4. Mandatory target duplicate-key check. #}

    {% set duplicate_sql %}
        SELECT COUNT(*)
        FROM (
            SELECT {{ keys | join(', ') }}
            FROM {{ target_relation }}
            GROUP BY {{ keys | join(', ') }}
            HAVING COUNT(*) > 1
        ) d
    {% endset %}

    {% set duplicate_count =
        run_query(duplicate_sql).rows[0][0] | int %}

    {% set status = record_truncate_post_check(
        model_execution_id, etl_nr,
        'REPLACE_TARGET_DUPLICATES',
        duplicate_count, 0
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('REPLACE_TARGET_DUPLICATES') %}
    {% endif %}


    {# 5. Every replacement row belongs to this ETL run. #}

    {% set audit_sql %}
        SELECT COUNT(*)
        FROM {{ target_relation }}
        WHERE ETL_CREATED_RUN_NUMBER = {{ etl_nr }}
          AND ETL_UPDATED_RUN_NUMBER IS NULL
    {% endset %}

    {% set audit_count = run_query(audit_sql).rows[0][0] | int %}

    {% set status = record_truncate_post_check(
        model_execution_id, etl_nr,
        'REPLACE_AUDIT_RUN_COUNT',
        audit_count, source_count
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('REPLACE_AUDIT_RUN_COUNT') %}
    {% endif %}


    {# 6. All source business attributes must match target. #}

    {% if post_cfg['source_target_match'] %}

        {% set source_columns =
            adapter.get_columns_in_relation(source_relation) %}

        {% set differences = [] %}

        {% for col in source_columns %}
            {% if col.name | upper not in normalized_keys %}

                {% set q = adapter.quote(col.name) %}

                {% do differences.append(
                    'src.' ~ q ~ ' IS DISTINCT FROM tgt.' ~ q
                ) %}

            {% endif %}
        {% endfor %}

        {% set match_sql %}
            SELECT COUNT(*)
            FROM {{ source_relation }} src
            LEFT JOIN {{ target_relation }} tgt
                ON {{ join_conditions | join(' AND ') }}
            WHERE tgt.{{ keys[0] }} IS NULL
            {% if differences | length > 0 %}
                OR {{ differences | join(' OR ') }}
            {% endif %}
        {% endset %}

        {% set mismatches =
            run_query(match_sql).rows[0][0] | int %}

        {% set status = record_truncate_post_check(
            model_execution_id, etl_nr,
            'REPLACE_SOURCE_TARGET_MATCH',
            mismatches, 0
        ) %}

        {% if status == 'FAIL' %}
            {% do failures.append('REPLACE_SOURCE_TARGET_MATCH') %}
        {% endif %}

    {% else %}

        {% do record_truncate_skip(
            model_execution_id, etl_nr,
            'REPLACE_SOURCE_TARGET_MATCH'
        ) %}

    {% endif %}


    {% set passed = failures | length == 0 %}

    {% do finalize_model_dq(model_execution_id, passed) %}

    {% do return({
        'passed': passed,
        'failed_checks': failures
    }) %}

{% endmacro %}


{# ===== From: macros/delete_insert_checks.sql | definition bodies unchanged ===== #}

{# DELETE_INSERT post-load checks and audit evidence. #}

{% macro record_delete_insert_post_check(
    model_execution_id,
    etl_nr,
    check_name,
    observed,
    expected,
    enabled=true
) %}

    {% set status = (
        'SKIP' if not enabled
        else ('PASS' if observed == expected else 'FAIL')
    ) %}

    {% set actual_sql = observed | string if enabled else 'NULL' %}
    {% set expected_sql = expected | string if enabled else 'NULL' %}

    {% set sql %}

        INSERT INTO {{ enterprise_control_relation('ETL_DQ_RESULTS') }}
        (
            DQ_RESULT_ID,
            MODEL_EXECUTION_ID,
            ETL_NR,
            CHECK_NAME,
            CHECK_PHASE,
            CHECK_STATUS,
            OBSERVED_VALUE,
            REFERENCE_VALUE,
            DETAILS,
            CHECKED_AT
        )

        SELECT
            UUID_STRING(),
            '{{ model_execution_id }}',
            {{ etl_nr }},
            '{{ check_name }}',
            'POST_LOAD',
            '{{ status }}',
            {{ actual_sql }},
            {{ expected_sql }},
            OBJECT_CONSTRUCT(
                'load_type', 'DELETE_INSERT'
            ),
            CONVERT_TIMEZONE(
                'UTC', CURRENT_TIMESTAMP()
            )::TIMESTAMP_NTZ(9)

    {% endset %}

    {% do run_query(sql) %}

    {% do log(
        'POST_LOAD | ' ~ check_name ~ ' | ' ~ status,
        info=true
    ) %}

    {% do return(status) %}

{% endmacro %}


{% macro run_delete_insert_post_load_checks(
    source_relation,
    target_relation,
    natural_keys,
    model_execution_id,
    etl_nr,
    source_count,
    target_before,
    target_after,
    expected_deleted,
    dml_stats,
    post_cfg
) %}

    {% set failures = [] %}

    {% set quoted_keys = [] %}
    {% set null_conditions = [] %}
    {% set normalized_keys = [] %}

    {% for key in natural_keys %}

        {% set key_name = key | upper %}
        {% set q = adapter.quote(key_name) %}

        {% do normalized_keys.append(key_name) %}
        {% do quoted_keys.append(q) %}
        {% do null_conditions.append(
            'tgt.' ~ q ~ ' IS NULL'
        ) %}

    {% endfor %}

    {# Prepared source keys define the rows owned by this invocation.
       Whole-table before/after counts are not stable under legitimate
       concurrent DELETE_INSERT writers operating on disjoint keys. #}
    {% set owned_key_match = generate_delete_insert_key_match(
        natural_keys,
        'tgt',
        'src'
    ) %}


    {# 1. DML statement counts must match this invocation's immutable
       prepared source and pre-transaction overlap. Do not mix global
       target_after into this execution-scoped invariant. #}

    {% set dml_ok = (
        dml_stats['rows_deleted'] == expected_deleted
        and dml_stats['rows_inserted'] == source_count
        and dml_stats['rows_updated'] == 0
    ) %}

    {% set status = record_delete_insert_post_check(
        model_execution_id, etl_nr,
        'DELETE_INSERT_DML_COUNTS',
        1 if dml_ok else 0, 1
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('DELETE_INSERT_DML_COUNTS') %}
    {% endif %}


    {# 2. Reconcile only the natural keys owned by this invocation.
       A disjoint concurrent writer may legitimately change the whole-table
       row count between this run's before/after observations. #}

    {% set owned_count_sql %}
        SELECT COUNT(*)
        FROM {{ target_relation }} AS tgt
        WHERE EXISTS (
            SELECT 1
            FROM {{ source_relation }} AS src
            WHERE {{ owned_key_match }}
        )
    {% endset %}

    {% set owned_target_count =
        run_query(owned_count_sql).rows[0][0] | int %}

    {% set status = record_delete_insert_post_check(
        model_execution_id, etl_nr,
        'DELETE_INSERT_TARGET_ROW_COUNT',
        owned_target_count, source_count,
        post_cfg['reconcile_counts']
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('DELETE_INSERT_TARGET_ROW_COUNT') %}
    {% endif %}


    {# 3. Natural keys written by this invocation must not contain NULL.
       Whole-target key health was already validated pre-load. #}

    {% set null_sql %}

        SELECT COUNT(*)
        FROM {{ target_relation }} AS tgt
        WHERE ETL_CREATED_RUN_NUMBER = {{ etl_nr }}
          AND ({{ null_conditions | join(' OR ') }})

    {% endset %}

    {% set null_count = run_query(null_sql).rows[0][0] | int %}

    {% set status = record_delete_insert_post_check(
        model_execution_id, etl_nr,
        'DELETE_INSERT_TARGET_NULL_KEYS',
        null_count, 0
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('DELETE_INSERT_TARGET_NULL_KEYS') %}
    {% endif %}


    {# 4. Natural keys owned by this invocation must remain unique.
       Scope the check to the prepared source key set so unrelated writers
       cannot make this completed run fail post-commit. #}

    {% set duplicate_sql %}

        SELECT COUNT(*)
        FROM (
            SELECT {% for q in quoted_keys %}tgt.{{ q }}{% if not loop.last %}, {% endif %}{% endfor %}
            FROM {{ target_relation }} AS tgt
            WHERE EXISTS (
                SELECT 1
                FROM {{ source_relation }} AS src
                WHERE {{ owned_key_match }}
            )
            GROUP BY {% for q in quoted_keys %}tgt.{{ q }}{% if not loop.last %}, {% endif %}{% endfor %}
            HAVING COUNT(*) > 1
        ) d

    {% endset %}

    {% set duplicate_count =
        run_query(duplicate_sql).rows[0][0] | int %}

    {% set status = record_delete_insert_post_check(
        model_execution_id, etl_nr,
        'DELETE_INSERT_TARGET_DUPLICATES',
        duplicate_count, 0
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('DELETE_INSERT_TARGET_DUPLICATES') %}
    {% endif %}


    {# 5. Every inserted row must carry this ETL creation number. #}

    {% set audit_sql %}

        SELECT COUNT(*)
        FROM {{ target_relation }}
        WHERE ETL_CREATED_RUN_NUMBER = {{ etl_nr }}
          AND ETL_UPDATED_RUN_NUMBER IS NULL

    {% endset %}

    {% set audit_count = run_query(audit_sql).rows[0][0] | int %}

    {% set status = record_delete_insert_post_check(
        model_execution_id, etl_nr,
        'DELETE_INSERT_AUDIT_RUN_COUNT',
        audit_count, source_count
    ) %}

    {% if status == 'FAIL' %}
        {% do failures.append('DELETE_INSERT_AUDIT_RUN_COUNT') %}
    {% endif %}


    {# 6. Source rows must match target business attributes. #}

    {% if post_cfg['source_target_match'] %}

        {% set join_condition = generate_delete_insert_key_match(
            natural_keys,
            'tgt',
            'src'
        ) %}

        {% set source_columns =
            adapter.get_columns_in_relation(source_relation) %}

        {% set differences = [] %}

        {% for col in source_columns %}

            {% if col.name | upper not in normalized_keys %}

                {% set q = adapter.quote(col.name) %}

                {% do differences.append(
                    'src.' ~ q
                    ~ ' IS DISTINCT FROM tgt.' ~ q
                ) %}

            {% endif %}

        {% endfor %}

        {% set match_sql %}

            SELECT COUNT(*)
            FROM {{ source_relation }} AS src
            LEFT JOIN {{ target_relation }} AS tgt
                ON {{ join_condition }}

            WHERE tgt.{{ quoted_keys[0] }} IS NULL

            {% if differences | length > 0 %}
                OR (
                    {{ differences | join(' OR ') }}
                )
            {% endif %}

        {% endset %}

        {% set mismatches =
            run_query(match_sql).rows[0][0] | int %}

        {% set status = record_delete_insert_post_check(
            model_execution_id, etl_nr,
            'DELETE_INSERT_SOURCE_TARGET_MATCH',
            mismatches, 0
        ) %}

    {% else %}

        {% set status = record_delete_insert_post_check(
            model_execution_id, etl_nr,
            'DELETE_INSERT_SOURCE_TARGET_MATCH',
            0, 0, false
        ) %}

    {% endif %}

    {% if status == 'FAIL' %}
        {% do failures.append('DELETE_INSERT_SOURCE_TARGET_MATCH') %}
    {% endif %}


    {% set passed = failures | length == 0 %}

    {% do finalize_model_dq(
        model_execution_id,
        passed
    ) %}

    {% do return({
        'passed': passed,
        'failed_checks': failures
    }) %}

{% endmacro %}
