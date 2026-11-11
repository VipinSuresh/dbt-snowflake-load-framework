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
            CUSTOMER_ID = 104
            AND CUSTOMER_NAME = 'Howard'
            AND CITY = 'Irving'
            AND ETL_CREATED_RUN_NUMBER > 0
        ) AS customer_104,

        COUNT_IF(
            CUSTOMER_ID IN (102, 103)
        ) AS old_customers,

        COUNT_IF(
            CREATED_BY_PROCESS = 'REGRESSION_SETUP'
            OR ETL_CREATED_RUN_NUMBER = 0
        ) AS old_audit_rows,

        COUNT(DISTINCT ETL_CREATED_RUN_NUMBER)
            AS distinct_load_runs

    FROM {{ ref('regression_customer_replace') }}

)

SELECT *
FROM checks
WHERE total_rows <> 2
   OR COALESCE(customer_101, 0) <> 1
   OR COALESCE(customer_104, 0) <> 1
   OR COALESCE(old_customers, 0) <> 0
   OR COALESCE(old_audit_rows, 0) <> 0
   OR distinct_load_runs <> 1