WITH checks AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT_IF(CUSTOMER_ID = 101 AND CITY = 'Dallas' AND CUSTOMER_NAME = 'Sheldon Dallas'
                 AND ETL_UPDATED_RUN_NUMBER IS NOT NULL) AS dallas_updated,
        COUNT_IF(CUSTOMER_ID = 101 AND CITY = 'Austin') AS austin_absent,
        COUNT_IF(CUSTOMER_ID = 102 AND CITY = 'Plano') AS plano_present,
        COUNT_IF(CUSTOMER_ID = 103 AND CITY = 'Frisco') AS frisco_present
    FROM {{ ref('regression_customer_update') }}
)
SELECT * FROM checks
WHERE total_rows <> 3 OR dallas_updated <> 1 OR austin_absent <> 0
   OR plano_present <> 1 OR frisco_present <> 1
