{% macro regression_reset_replace_empty() %}

    {% if execute %}

        {% set target = regression_target_relation('DIM_CUSTOMER_REPLACE_TEST') %}

        {% if adapter.get_relation(
            database=target.database,
            schema=target.schema,
            identifier='DIM_CUSTOMER_REPLACE_TEST'
        ) is none %}

            {{ exceptions.raise_compiler_error(
                'TRUNCATE_LOAD regression target does not exist'
            ) }}

        {% endif %}

        {% do run_query('DELETE FROM ' ~ target) %}

        {% do log('TRUNCATE_LOAD_EMPTY_REGRESSION_RESET_OK', info=true) %}

    {% endif %}

{% endmacro %}
