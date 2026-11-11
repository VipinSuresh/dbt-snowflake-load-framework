WITH checks AS (
    SELECT
        COUNT(*) AS total_rows,

        COUNT_IF(
            (CUSTOMER_ID = 101 AND CUSTOMER_NAME = 'Sheldon' AND CITY = 'Austin')
            OR (CUSTOMER_ID = 104 AND CUSTOMER_NAME = 'Howard' AND CITY = 'Irving')
            OR (CUSTOMER_ID = 106 AND CUSTOMER_NAME = 'Bernadette' AND CITY = 'Carrollton')
        ) AS snapshot_a_rows,

        COUNT_IF(
            (CUSTOMER_ID = 201 AND CUSTOMER_NAME = 'Penny' AND CITY = 'Allen')
            OR (CUSTOMER_ID = 204 AND CUSTOMER_NAME = 'Amy' AND CITY = 'McKinney')
            OR (CUSTOMER_ID = 206 AND CUSTOMER_NAME = 'Stuart' AND CITY = 'Prosper')
        ) AS snapshot_b_rows,

        COUNT_IF(
            CUSTOMER_ID NOT IN (101, 104, 106, 201, 204, 206)
        ) AS unexpected_rows,

        COUNT(DISTINCT ETL_CREATED_RUN_NUMBER) AS distinct_load_runs,
        COUNT_IF(ETL_CREATED_RUN_NUMBER <= 0) AS setup_rows

    FROM {{ ref('regression_customer_replace') }}
)

SELECT *
FROM checks
WHERE total_rows <> 3
   OR unexpected_rows <> 0
   OR setup_rows <> 0
   OR distinct_load_runs <> 1
   OR NOT (
       (snapshot_a_rows = 3 AND snapshot_b_rows = 0)
       OR
       (snapshot_a_rows = 0 AND snapshot_b_rows = 3)
   )
