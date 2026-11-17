{% macro regression_prepare_replace_distinct_targets() %}

    {% set regression_database =
        var('enterprise_regression_database', target.database) %}

    {% set regression_schema =
        var('enterprise_regression_schema', 'REGRESSION_TEST') %}

    {% set base_relation = adapter.get_relation(
        database=regression_database,
        schema=regression_schema,
        identifier='DIM_CUSTOMER_REPLACE_TEST'
    ) %}

    {% if base_relation is none
        or base_relation.type | lower != 'table' %}

        {{ exceptions.raise_compiler_error(
            'Distinct-target TRUNCATE_LOAD regression requires '
            ~ 'DIM_CUSTOMER_REPLACE_TEST to exist as a physical table. '
            ~ 'Run regression_reset_replace first.'
        ) }}

    {% endif %}

    {% set target_a = api.Relation.create(
        database=regression_database,
        schema=regression_schema,
        identifier='DIM_CUSTOMER_REPLACE_TEST_A',
        type='table'
    ) %}

    {% set target_b = api.Relation.create(
        database=regression_database,
        schema=regression_schema,
        identifier='DIM_CUSTOMER_REPLACE_TEST_B',
        type='table'
    ) %}

    {% call statement('prepare_distinct_target_a', auto_begin=False) %}
        CREATE OR REPLACE TABLE {{ target_a }}
        CLONE {{ base_relation }}
    {% endcall %}

    {% call statement('prepare_distinct_target_b', auto_begin=False) %}
        CREATE OR REPLACE TABLE {{ target_b }}
        CLONE {{ base_relation }}
    {% endcall %}

    {% do log(
        'TRUNCATE_LOAD_DISTINCT_TARGETS_READY'
        ~ ' | TargetA=' ~ target_a
        ~ ' | TargetB=' ~ target_b,
        info=true
    ) %}

{% endmacro %}
