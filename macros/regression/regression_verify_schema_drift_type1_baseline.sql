{% macro regression_verify_schema_drift_type1_baseline() %}
    {% if execute %}
        {% set target = regression_target_relation('DIM_CUSTOMER_TYPE1_TEST') %}
        {% set sql %}
            SELECT
                COUNT(*) AS TOTAL_ROWS,
                COUNT_IF(
                    CUSTOMER_ID = 101
                    AND CUSTOMER_NAME = 'Sheldon'
                    AND CITY = 'Dallas'
                    AND ETL_CREATED_RUN_NUMBER = 0
                ) AS CUSTOMER_101,
                COUNT_IF(
                    CUSTOMER_ID = 102
                    AND CUSTOMER_NAME = 'Leonard'
                    AND CITY = 'Plano'
                    AND ETL_CREATED_RUN_NUMBER = 0
                ) AS CUSTOMER_102
            FROM {{ target }}
        {% endset %}
        {% set result = run_query(sql) %}
        {% set total_rows = result.rows[0][0] | int %}
        {% set c101 = result.rows[0][1] | int %}
        {% set c102 = result.rows[0][2] | int %}

        {% if total_rows != 2 or c101 != 1 or c102 != 1 %}
            {{ exceptions.raise_compiler_error(
                'SCHEMA DRIFT TYPE1 BASELINE CHANGED: total=' ~ total_rows
                ~ ', customer_101=' ~ c101 ~ ', customer_102=' ~ c102
            ) }}
        {% endif %}

        {% do log('SCHEMA_DRIFT_TYPE1_TARGET_UNCHANGED', info=true) %}
    {% endif %}
{% endmacro %}
