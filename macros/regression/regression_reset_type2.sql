{% macro regression_reset_type2() %}

    {% if execute %}

        {% set target = regression_target_relation('DIM_CUSTOMER_TYPE2_TEST') %}

        {% if adapter.get_relation(
            database=target.database,
            schema=target.schema,
            identifier='DIM_CUSTOMER_TYPE2_TEST'
        ) is none %}

            {{ exceptions.raise_compiler_error(
                'TYPE2 regression target does not exist'
            ) }}

        {% endif %}

        {% do run_query('DELETE FROM ' ~ target) %}

        {% do log('TYPE2_REGRESSION_RESET_OK', info=true) %}

    {% endif %}

{% endmacro %}