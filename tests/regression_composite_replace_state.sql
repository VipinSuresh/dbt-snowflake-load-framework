WITH checks AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT_IF(CUSTOMER_ID = 101 AND CITY = 'Dallas' AND CUSTOMER_NAME = 'Sheldon Dallas') AS dallas,
        COUNT_IF(CUSTOMER_ID = 101 AND CITY = 'Austin' AND CUSTOMER_NAME = 'Sheldon Austin') AS austin,
        COUNT_IF(CUSTOMER_ID = 102 AND CITY = 'Plano') AS plano
    FROM {{ ref('regression_customer_replace') }}
), dup AS (
    SELECT COUNT(*) AS duplicate_groups FROM (
        SELECT CUSTOMER_ID, CITY FROM {{ ref('regression_customer_replace') }}
        GROUP BY CUSTOMER_ID, CITY HAVING COUNT(*) > 1
    )
)
SELECT * FROM checks CROSS JOIN dup
WHERE total_rows <> 3 OR dallas <> 1 OR austin <> 1 OR plano <> 1 OR duplicate_groups <> 0
