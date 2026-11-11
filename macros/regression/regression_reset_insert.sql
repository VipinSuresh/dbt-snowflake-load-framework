{% macro regression_reset_insert() %}

    {% if execute %}

        {% set target = regression_target_relation('DIM_CUSTOMER_INSERT_TEST') %}

        {% if adapter.get_relation(
            database=target.database,
            schema=target.schema,
            identifier='DIM_CUSTOMER_INSERT_TEST'
        ) is none %}
            {{ exceptions.raise_compiler_error(
                'INSERT regression target does not exist'
            ) }}
        {% endif %}

        {% do run_query('DELETE FROM ' ~ target) %}

        {% set sql %}
            INSERT INTO {{ target }}
            (
                CUSTOMER_ID,
                CUSTOMER_NAME,
                EMAIL,
                CUSTOMER_STATUS,
                CITY,
                CREATED_BY_PROCESS,
                CREATED_AT,
                ETL_CREATED_RUN_NUMBER,
                ETL_CREATED_AT
            )
            SELECT
                COLUMN1,
                COLUMN2,
                COLUMN3,
                COLUMN4,
                COLUMN5,
                'REGRESSION_SETUP',
                CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                0,
                CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
            FROM VALUES
                (101, 'Sheldon', 'sheldon@example.com', 'ACTIVE', 'Dallas'),
                (102, 'Leonard', 'leonard@example.com', 'ACTIVE', 'Plano')
        {% endset %}

        {% do run_query(sql) %}
        {% do log('INSERT_REGRESSION_RESET_OK', info=true) %}

    {% endif %}

{% endmacro %}