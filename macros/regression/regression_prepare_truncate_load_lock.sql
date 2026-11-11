{% macro regression_prepare_truncate_load_lock() %}

    {% if execute %}

        {% set lock_relation = enterprise_control_relation('ETL_FRAMEWORK_LOCKS') %}

        {% do run_query(
            'CREATE TABLE IF NOT EXISTS ' ~ lock_relation ~ ' ('
            ~ 'LOCK_NAME VARCHAR(100) NOT NULL, '
            ~ 'LOCK_VERSION NUMBER(38,0) NOT NULL, '
            ~ 'LAST_ETL_NR NUMBER(38,0), '
            ~ 'LAST_MODEL_EXECUTION_ID VARCHAR(36), '
            ~ 'LAST_ACQUIRED_AT TIMESTAMP_NTZ(9)'
            ~ ')'
        ) %}

        {% set seed_sql %}
            MERGE INTO {{ lock_relation }} AS target
            USING (
                SELECT 'TRUNCATE_LOAD_GLOBAL' AS LOCK_NAME
            ) AS source
            ON target.LOCK_NAME = source.LOCK_NAME
            WHEN NOT MATCHED THEN
                INSERT (
                    LOCK_NAME,
                    LOCK_VERSION,
                    LAST_ETL_NR,
                    LAST_MODEL_EXECUTION_ID,
                    LAST_ACQUIRED_AT
                )
                VALUES (
                    source.LOCK_NAME,
                    0,
                    NULL,
                    NULL,
                    NULL
                )
        {% endset %}

        {% do run_query(seed_sql) %}

        {% set verify_sql %}
            SELECT COUNT(*)
            FROM {{ lock_relation }}
            WHERE LOCK_NAME = 'TRUNCATE_LOAD_GLOBAL'
        {% endset %}

        {% set verify = run_query(verify_sql) %}
        {% set lock_rows = verify.rows[0][0] | int %}

        {% if lock_rows != 1 %}
            {{ exceptions.raise_compiler_error(
                'TRUNCATE_LOAD regression requires exactly one '
                ~ 'TRUNCATE_LOAD_GLOBAL row in ETL_FRAMEWORK_LOCKS; found '
                ~ lock_rows
            ) }}
        {% endif %}

        {% do log('TRUNCATE_LOAD_FRAMEWORK_LOCK_READY', info=true) %}

    {% endif %}

{% endmacro %}
