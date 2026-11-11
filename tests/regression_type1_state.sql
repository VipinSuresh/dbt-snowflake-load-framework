WITH checks AS (

    SELECT
        COUNT(*) AS total_rows,

        COUNT_IF(
            CUSTOMER_ID = 101
            AND CITY = 'Austin'
            AND ETL_CREATED_RUN_NUMBER = 0
            AND ETL_UPDATED_RUN_NUMBER > 0
        ) AS updated_customer,

        COUNT_IF(
            CUSTOMER_ID = 102
            AND CITY = 'Plano'
            AND ETL_CREATED_RUN_NUMBER = 0
            AND ETL_UPDATED_RUN_NUMBER IS NULL
        ) AS unchanged_customer,

        COUNT_IF(
            CUSTOMER_ID = 104
            AND CUSTOMER_NAME = 'Howard'
            AND CITY = 'Irving'
            AND ETL_CREATED_RUN_NUMBER > 0
        ) AS inserted_customer

    FROM {{ ref('regression_customer_type1') }}

)

SELECT *
FROM checks
WHERE total_rows <> 3
   OR COALESCE(updated_customer, 0) <> 1
   OR COALESCE(unchanged_customer, 0) <> 1
   OR COALESCE(inserted_customer, 0) <> 1