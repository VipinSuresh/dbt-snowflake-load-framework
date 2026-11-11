
{# Contract for the independent Snowflake enterprise_load demo. #}

{% macro validate_enterprise_config(load_cfg) %}

    {% if load_cfg is not mapping %}
        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: enterprise_load config must be a dictionary.'
        ) }}
    {% endif %}

    {# Reject typos rather than silently ignoring an intended safeguard. #}

    {% set allowed_root = [
        'load_type',
        'natural_keys',
        'pre_load',
        'post_load',
        'processing_window'
    ] %}

    {% for option in load_cfg.keys() %}
        {% if option not in allowed_root %}
            {{ exceptions.raise_compiler_error(
                'ENTERPRISE_LOAD: Unsupported enterprise_load option: ' ~ option
            ) }}
        {% endif %}
    {% endfor %}


    {# 1. Validate loading strategy. #}

    {% set raw_load_type = load_cfg.get('load_type', '') %}

    {% if raw_load_type is not string %}
        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: load_type must be a string.'
        ) }}
    {% endif %}

    {% set load_type = raw_load_type | upper | trim %}

    {% set allowed_load_types = [
        'TYPE1',
        'TYPE2',
        'INSERT',
        'TRUNCATE_LOAD',
        'UPDATE',
        'DELETE_INSERT'
    ] %}

    {% if load_type not in allowed_load_types %}
        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: Invalid load_type: ' ~ load_type
        ) }}
    {% endif %}


    {# 2. Validate and normalize natural keys. #}

    {% set natural_keys = load_cfg.get('natural_keys', []) %}

    {% if natural_keys is not sequence or natural_keys is string %}
        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: natural_keys must be a list.'
        ) }}
    {% endif %}

    {% set normalized_keys = [] %}

    {% for key in natural_keys %}

        {% if key is not string or (key | trim) == '' %}
            {{ exceptions.raise_compiler_error(
                'ENTERPRISE_LOAD: Every natural key must be a nonempty column name.'
            ) }}
        {% endif %}

        {% set normalized_key = key | trim | upper %}

        {% if normalized_key in normalized_keys %}
            {{ exceptions.raise_compiler_error(
                'ENTERPRISE_LOAD: Duplicate natural key: ' ~ normalized_key
            ) }}
        {% endif %}

        {% do normalized_keys.append(normalized_key) %}

    {% endfor %}

    {% if load_type in [
        'TYPE1',
        'TYPE2',
        'INSERT',
        'UPDATE',
        'TRUNCATE_LOAD',
        'DELETE_INSERT'
    ] and normalized_keys | length == 0 %}

        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: Natural keys are required for ' ~ load_type
        ) }}

    {% endif %}


    {# 3. Validate pre-load configuration. #}

    {% set pre_load = load_cfg.get('pre_load', {}) %}

    {% if pre_load is not mapping %}
        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: pre_load must be a dictionary.'
        ) }}
    {% endif %}

    {% set allowed_pre = [
        'column_match',
        'require_nonempty',
        'validate_keys',
        'volume_threshold'
    ] %}

    {% for option in pre_load.keys() %}
        {% if option not in allowed_pre %}
            {{ exceptions.raise_compiler_error(
                'ENTERPRISE_LOAD: Unsupported pre_load option: ' ~ option
            ) }}
        {% endif %}
    {% endfor %}

    {% set column_match = pre_load.get('column_match', true) %}
    {% set validate_keys = pre_load.get('validate_keys', true) %}
    {% set require_nonempty = pre_load.get('require_nonempty', true) %}

    {% if column_match is not sameas true %}
        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: pre_load.column_match is mandatory and must be true.'
        ) }}
    {% endif %}

    {% if validate_keys is not sameas true %}
        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: pre_load.validate_keys is mandatory and must be true.'
        ) }}
    {% endif %}

    {% if require_nonempty is not sameas true
        and require_nonempty is not sameas false %}

        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: pre_load.require_nonempty must be true or false.'
        ) }}

    {% endif %}

    {# TRUNCATE_LOAD cannot replace the target with an empty source. #}

    {% if load_type == 'TRUNCATE_LOAD'
        and require_nonempty is not sameas true %}

        {{ exceptions.raise_compiler_error(
            'TRUNCATE_LOAD: require_nonempty must be true to prevent empty replacement.'
        ) }}

    {% endif %}


    {# 4. Validate source-volume thresholds. #}

    {% set volume = pre_load.get('volume_threshold', {}) %}

    {% if volume is not mapping %}
        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: pre_load.volume_threshold must be a dictionary.'
        ) }}
    {% endif %}

    {% set allowed_volume = [
        'enabled',
        'max_drop_pct',
        'max_growth_pct'
    ] %}

    {% for option in volume.keys() %}
        {% if option not in allowed_volume %}
            {{ exceptions.raise_compiler_error(
                'ENTERPRISE_LOAD: Unsupported volume_threshold option: ' ~ option
            ) }}
        {% endif %}
    {% endfor %}

    {% set volume_enabled = volume.get('enabled', false) %}

    {% if volume_enabled is not sameas true
        and volume_enabled is not sameas false %}

        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: volume_threshold.enabled must be true or false.'
        ) }}

    {% endif %}

    {% if volume_enabled and (
        'max_drop_pct' not in volume
        or 'max_growth_pct' not in volume
    ) %}

        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: Enabled volume_threshold requires max_drop_pct and max_growth_pct.'
        ) }}

    {% endif %}

    {% for option in ['max_drop_pct', 'max_growth_pct'] %}

        {% if option in volume %}

            {% set value = volume[option] %}

            {% if value is not number
                or value is sameas true
                or value is sameas false %}

                {{ exceptions.raise_compiler_error(
                    'ENTERPRISE_LOAD: volume_threshold.' ~ option ~ ' must be numeric.'
                ) }}

            {% endif %}

            {% if value < 0
                or (option == 'max_drop_pct' and value > 100) %}

                {{ exceptions.raise_compiler_error(
                    'ENTERPRISE_LOAD: volume_threshold.' ~ option ~ ' is outside the allowed range.'
                ) }}

            {% endif %}

        {% endif %}

    {% endfor %}


    {# 5. Validate post-load configuration. #}

    {% set post_load = load_cfg.get('post_load', {}) %}

    {% if post_load is not mapping %}
        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: post_load must be a dictionary.'
        ) }}
    {% endif %}

    {% set allowed_post = [
        'reconcile_counts',
        'duplicate_check',
        'source_target_match'
    ] %}

    {% for option in post_load.keys() %}
        {% if option not in allowed_post %}
            {{ exceptions.raise_compiler_error(
                'ENTERPRISE_LOAD: Unsupported post_load option: ' ~ option
            ) }}
        {% endif %}
    {% endfor %}

    {% set reconcile_counts = post_load.get('reconcile_counts', true) %}
    {% set duplicate_check = post_load.get('duplicate_check', true) %}
    {% set source_target_match = post_load.get('source_target_match', true) %}

    {# Duplicate validation is mandatory across all loading strategies. #}

    {% if duplicate_check is not sameas true %}
        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: post_load.duplicate_check is mandatory and must be true.'
        ) }}
    {% endif %}

    {% for name, enabled in [
        ('reconcile_counts', reconcile_counts),
        ('source_target_match', source_target_match)
    ] %}

        {% if enabled is not sameas true
            and enabled is not sameas false %}

            {{ exceptions.raise_compiler_error(
                'ENTERPRISE_LOAD: post_load.' ~ name ~ ' must be true or false.'
            ) }}

        {% endif %}

    {% endfor %}

    {# Full replacement requires mandatory count reconciliation.
       Validate this AFTER reconcile_counts is initialized.
    #}

    {% if load_type == 'TRUNCATE_LOAD'
        and reconcile_counts is not sameas true %}

        {{ exceptions.raise_compiler_error(
            'TRUNCATE_LOAD: post_load.reconcile_counts must be true.'
        ) }}

    {% endif %}


    {# 6. Optional processing-window tracking (no filtering performed here). #}

    {% set window = load_cfg.get('processing_window', {}) %}

    {% if window is not mapping %}
        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: processing_window must be a dictionary.'
        ) }}
    {% endif %}

    {% for option in window.keys() %}
        {% if option not in ['enabled', 'date_column'] %}
            {{ exceptions.raise_compiler_error(
                'ENTERPRISE_LOAD: Unsupported processing_window option: ' ~ option
            ) }}
        {% endif %}
    {% endfor %}

    {% set window_enabled = window.get('enabled', false) %}
    {% if window_enabled is not sameas true and window_enabled is not sameas false %}
        {{ exceptions.raise_compiler_error(
            'ENTERPRISE_LOAD: processing_window.enabled must be true or false.'
        ) }}
    {% endif %}

    {% set window_date_column = window.get('date_column', none) %}
    {% if window_enabled %}
        {% if window_date_column is not string or (window_date_column | trim) == '' %}
            {{ exceptions.raise_compiler_error(
                'ENTERPRISE_LOAD: processing_window.date_column is required when enabled.'
            ) }}
        {% endif %}
        {% if load_type == 'TRUNCATE_LOAD' %}
            {{ exceptions.raise_compiler_error(
                'TRUNCATE_LOAD: processing_window.enabled=true is prohibited for full replacement.'
            ) }}
        {% endif %}
    {% endif %}

    {# 6. Return normalized enterprise configuration. #}

    {% set normalized = {
        'load_type': load_type,
        'natural_keys': normalized_keys,
        'pre_load': {
            'column_match': true,
            'require_nonempty': require_nonempty,
            'validate_keys': true,
            'volume_threshold': volume
        },
        'post_load': {
            'reconcile_counts': reconcile_counts,
            'duplicate_check': true,
            'source_target_match': source_target_match
        },
        'processing_window': {
            'enabled': window_enabled,
            'date_column': (window_date_column | trim | upper) if window_enabled else none
        }
    } %}

    {% do log(
        'ENTERPRISE CONFIG VALIDATED: ' ~ load_type,
        info=true
    ) %}

    {% do return(normalized) %}

{% endmacro %}
