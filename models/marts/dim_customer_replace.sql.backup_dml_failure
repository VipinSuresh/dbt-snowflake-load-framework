
{# Full replacement integration test. #}
{# Expected source: 3 customers, replacing 5 existing rows. #}

WITH source_rows AS (

    SELECT
        CUSTOMER_ID,
        CUSTOMER_NAME,
        EMAIL,
        CUSTOMER_STATUS,
        CITY
    FROM {{ ref('int_customers') }}
    WHERE CUSTOMER_ID IN (101, 102, 103)

)

SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    EMAIL,
    CUSTOMER_STATUS,

    CASE
        WHEN CUSTOMER_ID = 101 THEN 'Austin'
        ELSE CITY
    END AS CITY

FROM source_rows
