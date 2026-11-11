WITH checks AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT_IF(CUSTOMER_ID = 101 AND CITY = 'Dallas' AND CUSTOMER_NAME = 'Sheldon Dallas'
                 AND ETL_CREATED_RUN_NUMBER > 0) AS dallas_reinserted,
        COUNT_IF(CUSTOMER_ID = 101 AND CITY = 'Austin' AND CUSTOMER_NAME = 'Sheldon Austin'
                 AND ETL_CREATED_RUN_NUMBER > 0) AS austin_inserted,
        COUNT_IF(CUSTOMER_ID = 102 AND CITY = 'Plano' AND ETL_CREATED_RUN_NUMBER > 0) AS plano_reinserted,
        COUNT_IF(CUSTOMER_ID = 103 AND CITY = 'Frisco' AND ETL_CREATED_RUN_NUMBER = 0) AS frisco_untouched
    FROM {{ ref('regression_customer_delete_insert') }}
), dup AS (
    SELECT COUNT(*) AS duplicate_groups FROM (
        SELECT CUSTOMER_ID, CITY FROM {{ ref('regression_customer_delete_insert') }}
        GROUP BY CUSTOMER_ID, CITY HAVING COUNT(*) > 1
    )
)
SELECT * FROM checks CROSS JOIN dup
WHERE total_rows <> 4 OR dallas_reinserted <> 1 OR austin_inserted <> 1
   OR plano_reinserted <> 1 OR frisco_untouched <> 1 OR duplicate_groups <> 0
