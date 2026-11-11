WITH checks AS (

    SELECT
        COUNT(*) AS total_rows,

        COUNT_IF(
            CUSTOMER_ID = 101
            AND CITY = 'Austin'
            AND ETL_CREATED_RUN_NUMBER > 0
        ) AS replaced_customer,

        COUNT_IF(
            CUSTOMER_ID = 104
            AND CUSTOMER_NAME = 'Howard'
            AND CITY = 'Irving'
            AND ETL_CREATED_RUN_NUMBER > 0
        ) AS inserted_customer,

        COUNT_IF(
            CUSTOMER_ID = 102
            AND CITY = 'Plano'
            AND CREATED_BY_PROCESS = 'REGRESSION_SETUP'
            AND ETL_CREATED_RUN_NUMBER = 0
        ) AS preserved_customer_102,

        COUNT_IF(
            CUSTOMER_ID = 103
            AND CITY = 'Frisco'
            AND CREATED_BY_PROCESS = 'REGRESSION_SETUP'
            AND ETL_CREATED_RUN_NUMBER = 0
        ) AS preserved_customer_103

    FROM {{ ref('regression_customer_delete_insert') }}

)

SELECT *
FROM checks
WHERE total_rows <> 4
   OR COALESCE(replaced_customer, 0) <> 1
   OR COALESCE(inserted_customer, 0) <> 1
   OR COALESCE(preserved_customer_102, 0) <> 1
   OR COALESCE(preserved_customer_103, 0) <> 1