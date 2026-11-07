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
