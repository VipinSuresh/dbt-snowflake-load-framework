WITH checks AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT_IF(CUSTOMER_ID = 101 AND CITY = 'Dallas' AND CUSTOMER_NAME = 'Sheldon Dallas'
                 AND ETL_UPDATED_RUN_NUMBER IS NOT NULL) AS dallas_updated,
        COUNT_IF(CUSTOMER_ID = 101 AND CITY = 'Austin' AND CUSTOMER_NAME = 'Sheldon Austin'
                 AND ETL_CREATED_RUN_NUMBER > 0) AS austin_inserted,
        COUNT_IF(CUSTOMER_ID = 102 AND CITY = 'Plano') AS plano_present
    FROM {{ ref('regression_customer_type1') }}
), dup AS (
    SELECT COUNT(*) AS duplicate_groups FROM (
        SELECT CUSTOMER_ID, CITY FROM {{ ref('regression_customer_type1') }}
        GROUP BY CUSTOMER_ID, CITY HAVING COUNT(*) > 1
    )
)
SELECT * FROM checks CROSS JOIN dup
WHERE total_rows <> 3 OR dallas_updated <> 1 OR austin_inserted <> 1
   OR plano_present <> 1 OR duplicate_groups <> 0
