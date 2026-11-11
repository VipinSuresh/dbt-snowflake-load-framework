WITH checks AS (

    SELECT
        COUNT(*) AS total_rows,

        COUNT_IF(
            CUSTOMER_ID = 101
            AND CITY = 'Dallas'
            AND ETL_CREATED_RUN_NUMBER = 0
            AND ETL_UPDATED_RUN_NUMBER IS NULL
        ) AS existing_customer_101,

        COUNT_IF(
            CUSTOMER_ID = 102
            AND CITY = 'Plano'
            AND ETL_CREATED_RUN_NUMBER = 0
            AND ETL_UPDATED_RUN_NUMBER IS NULL
        ) AS existing_customer_102,

        COUNT_IF(
            CUSTOMER_ID = 104
            AND CUSTOMER_NAME = 'Howard'
            AND CITY = 'Irving'
            AND ETL_CREATED_RUN_NUMBER > 0
        ) AS inserted_customer

    FROM {{ ref('regression_customer_insert') }}

)

SELECT *
FROM checks
WHERE total_rows <> 3
   OR COALESCE(existing_customer_101, 0) <> 1
   OR COALESCE(existing_customer_102, 0) <> 1
   OR COALESCE(inserted_customer, 0) <> 1