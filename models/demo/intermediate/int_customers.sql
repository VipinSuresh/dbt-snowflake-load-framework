{{ config(materialized='view') }}

SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    EMAIL,

    CASE
        WHEN CUSTOMER_STATUS IN ('ACTIVE', 'INACTIVE')
            THEN CUSTOMER_STATUS
        ELSE 'UNKNOWN'
    END AS CUSTOMER_STATUS,

    CITY

FROM {{ ref('stg_customers') }}