WITH checks AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT_IF(CUSTOMER_ID = 101 AND CITY = 'Dallas' AND ETL_CREATED_RUN_NUMBER = 0) AS dallas_existing,
        COUNT_IF(CUSTOMER_ID = 101 AND CITY = 'Austin' AND CUSTOMER_NAME = 'Sheldon Austin'
                 AND ETL_CREATED_RUN_NUMBER > 0) AS austin_inserted,
        COUNT_IF(CUSTOMER_ID = 102 AND CITY = 'Plano' AND ETL_CREATED_RUN_NUMBER = 0) AS plano_existing
    FROM {{ ref('regression_customer_insert') }}
), dup AS (
    SELECT COUNT(*) AS duplicate_groups FROM (
        SELECT CUSTOMER_ID, CITY FROM {{ ref('regression_customer_insert') }}
        GROUP BY CUSTOMER_ID, CITY HAVING COUNT(*) > 1
    )
)
SELECT * FROM checks CROSS JOIN dup
WHERE total_rows <> 3 OR dallas_existing <> 1 OR austin_inserted <> 1
   OR plano_existing <> 1 OR duplicate_groups <> 0
