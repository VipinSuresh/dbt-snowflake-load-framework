{% macro regression_reset_update() %}

    {% set target = regression_target_relation('DIM_CUSTOMER_UPDATE_TEST') %}

    {% if execute %}
        {% if adapter.get_relation(
            database=target.database,
            schema=target.schema,
            identifier='DIM_CUSTOMER_UPDATE_TEST'
        ) is none %}
            {{ exceptions.raise_compiler_error('Regression target table does not exist') }}
        {% endif %}

        {% do run_query('DELETE FROM ' ~ target) %}

        {% set sql %}
            INSERT INTO {{ target }}
            (
                CUSTOMER_ID, CUSTOMER_NAME, EMAIL,
                CUSTOMER_STATUS, CITY,
                CREATED_BY_PROCESS, CREATED_AT,
                ETL_CREATED_RUN_NUMBER, ETL_CREATED_AT
            )
            SELECT
                COLUMN1, COLUMN2, COLUMN3, COLUMN4, COLUMN5,
                'REGRESSION_SETUP',
                CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                0,
                CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
            FROM VALUES
                (101, 'Sheldon', 'sheldon@example.com', 'ACTIVE', 'Dallas'),
                (102, 'Leonard', 'leonard@example.com', 'ACTIVE', 'Plano'),
                (103, 'Raj', 'raj@example.com', 'ACTIVE', 'Frisco')
        {% endset %}

        {% do run_query(sql) %}
        {% do log('REGRESSION_RESET_OK', info=true) %}
    {% endif %}

{% endmacro %}


{% macro regression_null_failure_count() %}

    {% if execute %}
        {% set sql %}
            SELECT COUNT(*)
            FROM {{ enterprise_control_relation('ETL_DQ_RESULTS') }}
            WHERE CHECK_NAME = 'SOURCE_REQUIRED_NOT_NULL'
              AND CHECK_STATUS = 'FAIL'
              AND DETAILS:message::string ILIKE '%CUSTOMER_NAME%'
        {% endset %}

        {% set result = run_query(sql) %}
        {% do log(
            'REGRESSION_FAIL_COUNT=' ~ result.columns[0].values()[0],
            info=true
        ) %}
    {% endif %}

{% endmacro %}