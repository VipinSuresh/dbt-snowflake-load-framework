
{# DELETE_INSERT integration test. #}
{# Replace keys 101 and 103, insert 108. #}

WITH source_rows AS (

    SELECT
        CUSTOMER_ID,
        CUSTOMER_NAME,
        EMAIL,
        CUSTOMER_STATUS,
        CITY
    FROM {{ ref('int_customers') }}
    WHERE CUSTOMER_ID IN (101, 103)

)

SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    EMAIL,
    CUSTOMER_STATUS,
    CASE
        WHEN CUSTOMER_ID = 101 THEN 'Seattle'
        WHEN CUSTOMER_ID = 103 THEN 'Phoenix'
        ELSE CITY
    END AS CITY

FROM source_rows

UNION ALL

SELECT
    108 AS CUSTOMER_ID,
    'Stuart' AS CUSTOMER_NAME,
    'stuart@example.com' AS EMAIL,
    'ACTIVE' AS CUSTOMER_STATUS,
    'Dallas' AS CITY
