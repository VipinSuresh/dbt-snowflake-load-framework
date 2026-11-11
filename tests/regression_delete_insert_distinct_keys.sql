WITH checks AS (

    SELECT
        COUNT(*) AS total_rows,

        COUNT_IF(
            CUSTOMER_ID = 101
            AND CUSTOMER_NAME = 'Sheldon'
            AND CITY = 'Austin'
            AND ETL_CREATED_RUN_NUMBER > 0
        ) AS customer_101,

        COUNT_IF(
            CUSTOMER_ID = 102
            AND CUSTOMER_NAME = 'Leonard'
            AND CITY = 'McKinney'
            AND ETL_CREATED_RUN_NUMBER > 0
        ) AS customer_102,

        COUNT_IF(
            CUSTOMER_ID = 103
            AND CUSTOMER_NAME = 'Raj'
            AND CITY = 'Frisco'
            AND CREATED_BY_PROCESS = 'REGRESSION_SETUP'
            AND ETL_CREATED_RUN_NUMBER = 0
        ) AS preserved_103,

        COUNT_IF(
            CUSTOMER_ID = 104
            AND CUSTOMER_NAME = 'Howard'
            AND CITY = 'Irving'
            AND ETL_CREATED_RUN_NUMBER > 0
        ) AS customer_104,

        COUNT_IF(
            CUSTOMER_ID = 105
            AND CUSTOMER_NAME = 'Penny'
            AND CITY = 'Allen'
            AND ETL_CREATED_RUN_NUMBER > 0
        ) AS customer_105

    FROM {{ ref('regression_customer_delete_insert') }}

)

SELECT *
FROM checks
WHERE total_rows <> 5
   OR COALESCE(customer_101, 0) <> 1
   OR COALESCE(customer_102, 0) <> 1
   OR COALESCE(preserved_103, 0) <> 1
   OR COALESCE(customer_104, 0) <> 1
   OR COALESCE(customer_105, 0) <> 1
