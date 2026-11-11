
{# UPDATE-only integration test. #}

WITH source_rows AS (

    SELECT
        CUSTOMER_ID,
        CUSTOMER_NAME,
        EMAIL,
        CUSTOMER_STATUS,
        CITY
    FROM {{ ref('int_customers') }}

)

SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    EMAIL,
    CUSTOMER_STATUS,

    CASE
        WHEN CUSTOMER_ID = 101 THEN 'San Jose'
        ELSE CITY
    END AS CITY

FROM source_rows

WHERE CUSTOMER_ID <> 104

UNION ALL

SELECT
    108 AS CUSTOMER_ID,
    'Stuart' AS CUSTOMER_NAME,
    'stuart@example.com' AS EMAIL,
    'ACTIVE' AS CUSTOMER_STATUS,
    'Dallas' AS CITY
