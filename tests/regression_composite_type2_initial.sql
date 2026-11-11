WITH checks AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT_IF(IS_CURRENT = TRUE) AS current_rows,
        COUNT_IF(CUSTOMER_ID = 101 AND CITY = 'Dallas' AND VERSION_NO = 1 AND IS_CURRENT = TRUE) AS dallas,
        COUNT_IF(CUSTOMER_ID = 101 AND CITY = 'Austin' AND VERSION_NO = 1 AND IS_CURRENT = TRUE) AS austin,
        COUNT_IF(CUSTOMER_ID = 102 AND CITY = 'Plano' AND VERSION_NO = 1 AND IS_CURRENT = TRUE) AS plano
    FROM {{ ref('regression_customer_type2') }}
)
SELECT * FROM checks
WHERE total_rows <> 3 OR current_rows <> 3 OR dallas <> 1 OR austin <> 1 OR plano <> 1
