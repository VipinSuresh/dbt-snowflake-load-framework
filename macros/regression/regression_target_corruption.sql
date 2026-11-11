{% macro regression_corrupt_target_state(corruption_case) %}

    {% if execute %}

        {% if corruption_case == 'type1_null_key' %}

            {% set target = regression_target_relation('DIM_CUSTOMER_TYPE1_TEST') %}
            {% do run_query('UPDATE ' ~ target ~ ' SET CUSTOMER_ID = NULL WHERE CUSTOMER_ID = 102') %}

        {% elif corruption_case == 'type1_duplicate_key' %}

            {% set target = regression_target_relation('DIM_CUSTOMER_TYPE1_TEST') %}
            {% do run_query('UPDATE ' ~ target ~ ' SET CUSTOMER_ID = 101 WHERE CUSTOMER_ID = 102') %}

        {% elif corruption_case == 'type2_duplicate_current' %}

            {% set target = regression_target_relation('DIM_CUSTOMER_TYPE2_TEST') %}
            {% set sql %}
                INSERT INTO {{ target }}
                SELECT * REPLACE (
                    VERSION_NO + 1000 AS VERSION_NO
                )
                FROM {{ target }}
                WHERE CUSTOMER_ID = 101
                  AND IS_CURRENT = TRUE
                QUALIFY ROW_NUMBER() OVER (ORDER BY VERSION_NO DESC) = 1
            {% endset %}
            {% do run_query(sql) %}

        {% elif corruption_case == 'type2_duplicate_version' %}

            {% set target = regression_target_relation('DIM_CUSTOMER_TYPE2_TEST') %}
            {% set sql %}
                INSERT INTO {{ target }}
                SELECT * REPLACE (
                    FALSE AS IS_CURRENT,
                    DATEADD(second, 1, VALID_FROM) AS VALID_TO
                )
                FROM {{ target }}
                WHERE CUSTOMER_ID = 101
                  AND IS_CURRENT = TRUE
                QUALIFY ROW_NUMBER() OVER (ORDER BY VERSION_NO DESC) = 1
            {% endset %}
            {% do run_query(sql) %}

        {% elif corruption_case == 'type2_history_overlap' %}

            {% set target = regression_target_relation('DIM_CUSTOMER_TYPE2_TEST') %}

            {% set expire_sql %}
                UPDATE {{ target }}
                SET IS_CURRENT = FALSE,
                    VALID_TO = DATEADD(second, 2, VALID_FROM)
                WHERE CUSTOMER_ID = 101
                  AND IS_CURRENT = TRUE
            {% endset %}
            {% do run_query(expire_sql) %}

            {% set insert_sql %}
                INSERT INTO {{ target }}
                SELECT * REPLACE (
                    VERSION_NO + 1 AS VERSION_NO,
                    TRUE AS IS_CURRENT,
                    DATEADD(second, 1, VALID_FROM) AS VALID_FROM,
                    CAST(NULL AS TIMESTAMP_NTZ) AS VALID_TO
                )
                FROM {{ target }}
                WHERE CUSTOMER_ID = 101
                  AND VERSION_NO = 1
                QUALIFY ROW_NUMBER() OVER (ORDER BY VALID_FROM) = 1
            {% endset %}
            {% do run_query(insert_sql) %}

        {% else %}
            {{ exceptions.raise_compiler_error(
                'Unknown target corruption case: ' ~ corruption_case
            ) }}
        {% endif %}

        {% do log('TARGET_CORRUPTION_CREATED | ' ~ corruption_case, info=true) %}

    {% endif %}

{% endmacro %}


{% macro regression_verify_target_corruption_state(corruption_case) %}

    {% if execute %}

        {% if corruption_case == 'type1_null_key' %}
            {% set target = regression_target_relation('DIM_CUSTOMER_TYPE1_TEST') %}
            {% set sql %}
                SELECT COUNT(*) AS TOTAL_ROWS,
                       COUNT_IF(CUSTOMER_ID IS NULL) AS BAD_ROWS
                FROM {{ target }}
            {% endset %}
            {% set result = run_query(sql) %}
            {% set total_rows = result.rows[0][0] | int %}
            {% set bad_rows = result.rows[0][1] | int %}
            {% if total_rows != 2 or bad_rows != 1 %}
                {{ exceptions.raise_compiler_error(
                    'TARGET CORRUPTION STATE CHANGED: type1_null_key | total=' ~ total_rows ~ ', null_rows=' ~ bad_rows
                ) }}
            {% endif %}

        {% elif corruption_case == 'type1_duplicate_key' %}
            {% set target = regression_target_relation('DIM_CUSTOMER_TYPE1_TEST') %}
            {% set sql %}
                SELECT
                    (SELECT COUNT(*) FROM {{ target }}) AS TOTAL_ROWS,
                    (SELECT COUNT(*) FROM (
                        SELECT CUSTOMER_ID FROM {{ target }}
                        GROUP BY CUSTOMER_ID HAVING COUNT(*) > 1
                    )) AS DUP_GROUPS
            {% endset %}
            {% set result = run_query(sql) %}
            {% set total_rows = result.rows[0][0] | int %}
            {% set dup_groups = result.rows[0][1] | int %}
            {% if total_rows != 2 or dup_groups != 1 %}
                {{ exceptions.raise_compiler_error(
                    'TARGET CORRUPTION STATE CHANGED: type1_duplicate_key | total=' ~ total_rows ~ ', duplicate_groups=' ~ dup_groups
                ) }}
            {% endif %}

        {% elif corruption_case == 'type2_duplicate_current' %}
            {% set target = regression_target_relation('DIM_CUSTOMER_TYPE2_TEST') %}
            {% set sql %}
                SELECT
                    (SELECT COUNT(*) FROM {{ target }}) AS TOTAL_ROWS,
                    (SELECT COUNT(*) FROM (
                        SELECT CUSTOMER_ID FROM {{ target }}
                        WHERE IS_CURRENT = TRUE
                        GROUP BY CUSTOMER_ID HAVING COUNT(*) > 1
                    )) AS DUP_CURRENT_GROUPS
            {% endset %}
            {% set result = run_query(sql) %}
            {% set total_rows = result.rows[0][0] | int %}
            {% set bad_groups = result.rows[0][1] | int %}
            {% if total_rows != 4 or bad_groups != 1 %}
                {{ exceptions.raise_compiler_error(
                    'TARGET CORRUPTION STATE CHANGED: type2_duplicate_current | total=' ~ total_rows ~ ', duplicate_current_groups=' ~ bad_groups
                ) }}
            {% endif %}

        {% elif corruption_case == 'type2_duplicate_version' %}
            {% set target = regression_target_relation('DIM_CUSTOMER_TYPE2_TEST') %}
            {% set sql %}
                SELECT
                    (SELECT COUNT(*) FROM {{ target }}) AS TOTAL_ROWS,
                    (SELECT COUNT(*) FROM (
                        SELECT CUSTOMER_ID, VERSION_NO FROM {{ target }}
                        GROUP BY CUSTOMER_ID, VERSION_NO HAVING COUNT(*) > 1
                    )) AS DUP_VERSION_GROUPS
            {% endset %}
            {% set result = run_query(sql) %}
            {% set total_rows = result.rows[0][0] | int %}
            {% set bad_groups = result.rows[0][1] | int %}
            {% if total_rows != 4 or bad_groups != 1 %}
                {{ exceptions.raise_compiler_error(
                    'TARGET CORRUPTION STATE CHANGED: type2_duplicate_version | total=' ~ total_rows ~ ', duplicate_version_groups=' ~ bad_groups
                ) }}
            {% endif %}

        {% elif corruption_case == 'type2_history_overlap' %}
            {% set target = regression_target_relation('DIM_CUSTOMER_TYPE2_TEST') %}
            {% set sql %}
                WITH history AS (
                    SELECT
                        CUSTOMER_ID,
                        VERSION_NO,
                        VALID_FROM,
                        VALID_TO,
                        LEAD(VALID_FROM) OVER (
                            PARTITION BY CUSTOMER_ID ORDER BY VERSION_NO
                        ) AS NEXT_FROM
                    FROM {{ target }}
                )
                SELECT
                    (SELECT COUNT(*) FROM {{ target }}) AS TOTAL_ROWS,
                    COUNT_IF(
                        NEXT_FROM IS NOT NULL
                        AND VALID_TO IS NOT NULL
                        AND VALID_TO > NEXT_FROM
                    ) AS OVERLAPS
                FROM history
            {% endset %}
            {% set result = run_query(sql) %}
            {% set total_rows = result.rows[0][0] | int %}
            {% set overlaps = result.rows[0][1] | int %}
            {% if total_rows != 4 or overlaps < 1 %}
                {{ exceptions.raise_compiler_error(
                    'TARGET CORRUPTION STATE CHANGED: type2_history_overlap | total=' ~ total_rows ~ ', overlaps=' ~ overlaps
                ) }}
            {% endif %}

        {% else %}
            {{ exceptions.raise_compiler_error(
                'Unknown target corruption verification case: ' ~ corruption_case
            ) }}
        {% endif %}

        {% do log('TARGET_CORRUPTION_STATE_PRESERVED | ' ~ corruption_case, info=true) %}

    {% endif %}

{% endmacro %}
