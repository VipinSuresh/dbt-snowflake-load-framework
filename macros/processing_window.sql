{# Optional runtime window validation; does not alter model SQL or target DML. #}

{% macro enforce_processing_window(window_cfg, model_execution_id, etl_nr) %}
    {% if not window_cfg.get('enabled', false) %}
        {% do return(none) %}
    {% endif %}

    {% set date_column = window_cfg['date_column'] %}
    {% set start_value = var('start_date', none) %}
    {% set end_value = var('end_date', none) %}
    {% set reason = none %}

    {# Requiring strings avoids unpredictable date parsing of YAML number/bool values. #}
    {% if start_value is not string or (start_value | trim) == '' %}
        {% set reason = 'Missing start_date or start_date is not a string.' %}
    {% elif end_value is not string or (end_value | trim) == '' %}
        {% set reason = 'Missing end_date or end_date is not a string.' %}
    {% else %}
        {% set start_value = start_value | trim %}
        {% set end_value = end_value | trim %}

        {# Strict ISO dates, or timestamps without time-zone offsets.
           Examples: 2026-10-08; 2026-10-08T12:00:00; 2026-10-08 12:00:00. #}
        {% set safe_start = start_value | replace("'", "''") %}
        {% set safe_end = end_value | replace("'", "''") %}
        {% set shape = '^[0-9]{4}-[0-9]{2}-[0-9]{2}([ T][0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,9})?)?$' %}
        {% set check_sql %}
            SELECT
                IFF(
                    REGEXP_LIKE('{{ safe_start }}', '{{ shape }}'),
                    TRY_TO_TIMESTAMP_NTZ('{{ safe_start }}'), NULL
                ) AS START_TS,
                IFF(
                    REGEXP_LIKE('{{ safe_end }}', '{{ shape }}'),
                    TRY_TO_TIMESTAMP_NTZ('{{ safe_end }}'), NULL
                ) AS END_TS
        {% endset %}
        {% set checked = run_query(check_sql) %}
        {% set start_ts = checked.rows[0][0] %}
        {% set end_ts = checked.rows[0][1] %}
        {% if start_ts is none or end_ts is none %}
            {% set reason = 'Invalid start_date or end_date; use ISO YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS.' %}
        {% elif start_ts >= end_ts %}
            {% set reason = 'start_date must be earlier than end_date (exclusive).' %}
        {% endif %}
    {% endif %}

    {% set passed = (reason is none) %}
    {% if passed %}
        {% set reason = 'Declared window accepted; source filter is not verified.' %}
    {% endif %}
    {% do record_processing_window_check(
        model_execution_id, etl_nr, passed,
        date_column, start_value, end_value, reason
    ) %}

    {% if not passed %}
        {% do mark_pre_load_failed(model_execution_id, ['PROCESSING_WINDOW']) %}
        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: Processing window failed before DML. ' ~ reason
        ) }}
    {% endif %}
{% endmacro %}
