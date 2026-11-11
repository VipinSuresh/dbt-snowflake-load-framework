    WITH checks AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT_IF(IS_CURRENT = TRUE) AS current_rows,
        COUNT_IF(IS_CURRENT = FALSE) AS expired_rows,

        COUNT_IF(
            CUSTOMER_ID = 101 AND CITY = 'Dallas'
            AND VERSION_NO = 1 AND IS_CURRENT = TRUE
            AND VALID_FROM IS NOT NULL AND VALID_TO IS NULL
        ) AS customer_101,

        COUNT_IF(
            CUSTOMER_ID = 102 AND CITY = 'Plano'
            AND VERSION_NO = 1 AND IS_CURRENT = TRUE
        ) AS customer_102,

        COUNT_IF(
            CUSTOMER_ID = 103 AND CITY = 'Frisco'
            AND VERSION_NO = 1 AND IS_CURRENT = TRUE
        ) AS customer_103

    FROM {{ ref('regression_customer_type2') }}
)

SELECT *
FROM checks
WHERE total_rows <> 3
   OR COALESCE(current_rows, 0) <> 3
   OR COALESCE(expired_rows, 0) <> 0
   OR COALESCE(customer_101, 0) <> 1
   OR COALESCE(customer_102, 0) <> 1
   OR COALESCE(customer_103, 0) <> 1