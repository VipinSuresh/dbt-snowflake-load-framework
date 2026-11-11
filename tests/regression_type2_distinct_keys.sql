WITH history AS (
    SELECT *
    FROM {{ ref('regression_customer_type2') }}
),

checks AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT_IF(IS_CURRENT = TRUE) AS current_rows,
        COUNT_IF(IS_CURRENT = FALSE) AS expired_rows,

        COUNT_IF(
            CUSTOMER_ID = 101
            AND CITY = 'Dallas'
            AND VERSION_NO = 1
            AND IS_CURRENT = FALSE
            AND VALID_TO IS NOT NULL
        ) AS customer_101_old,

        COUNT_IF(
            CUSTOMER_ID = 101
            AND CITY = 'Austin'
            AND VERSION_NO = 2
            AND IS_CURRENT = TRUE
            AND VALID_TO IS NULL
        ) AS customer_101_new,

        COUNT_IF(
            CUSTOMER_ID = 102
            AND CITY = 'Plano'
            AND VERSION_NO = 1
            AND IS_CURRENT = FALSE
            AND VALID_TO IS NOT NULL
        ) AS customer_102_old,

        COUNT_IF(
            CUSTOMER_ID = 102
            AND CITY = 'McKinney'
            AND VERSION_NO = 2
            AND IS_CURRENT = TRUE
            AND VALID_TO IS NULL
        ) AS customer_102_new,

        COUNT_IF(
            CUSTOMER_ID = 103
            AND CITY = 'Frisco'
            AND VERSION_NO = 1
            AND IS_CURRENT = TRUE
        ) AS customer_103
    FROM history
),

continuity AS (
    SELECT
        COUNT_IF(old.CUSTOMER_ID = 101) AS customer_101_windows,
        COUNT_IF(old.CUSTOMER_ID = 102) AS customer_102_windows
    FROM history old
    JOIN history new
      ON old.CUSTOMER_ID = new.CUSTOMER_ID
     AND old.VERSION_NO = 1
     AND new.VERSION_NO = 2
    WHERE old.CUSTOMER_ID IN (101, 102)
      AND old.VALID_TO = new.VALID_FROM
      AND old.ETL_UPDATED_RUN_NUMBER = new.ETL_CREATED_RUN_NUMBER
)

SELECT *
FROM checks CROSS JOIN continuity
WHERE total_rows <> 5
   OR COALESCE(current_rows, 0) <> 3
   OR COALESCE(expired_rows, 0) <> 2
   OR COALESCE(customer_101_old, 0) <> 1
   OR COALESCE(customer_101_new, 0) <> 1
   OR COALESCE(customer_102_old, 0) <> 1
   OR COALESCE(customer_102_new, 0) <> 1
   OR COALESCE(customer_103, 0) <> 1
   OR COALESCE(customer_101_windows, 0) <> 1
   OR COALESCE(customer_102_windows, 0) <> 1
