WITH checks AS (
    SELECT
        COUNT(*) AS total_rows,

        COUNT_IF(
            CUSTOMER_ID = 101
            AND CITY = 'Austin'
            AND ETL_UPDATED_RUN_NUMBER IS NOT NULL
            AND CREATED_BY_PROCESS = 'REGRESSION_SETUP'
            AND ETL_CREATED_RUN_NUMBER = 0
        ) AS updated_customer,

        COUNT_IF(
            CUSTOMER_ID = 102
            AND CITY = 'Plano'
            AND ETL_UPDATED_RUN_NUMBER IS NULL
        ) AS unchanged_customer,

        COUNT_IF(
            CUSTOMER_ID = 103
            AND CITY = 'Frisco'
            AND ETL_UPDATED_RUN_NUMBER IS NULL
        ) AS retained_customer,

        COUNT_IF(CUSTOMER_ID = 104) AS unexpected_customer

    FROM {{ ref('regression_customer_update') }}
)

SELECT *
FROM checks
WHERE total_rows <> 3
   OR COALESCE(updated_customer, 0) <> 1
   OR COALESCE(unchanged_customer, 0) <> 1
   OR COALESCE(retained_customer, 0) <> 1
   OR COALESCE(unexpected_customer, 0) <> 0