{{ config(materialized='view') }}

SELECT
    CUSTOMER_ID,
    TRIM(CUSTOMER_NAME) AS CUSTOMER_NAME,
    LOWER(TRIM(EMAIL)) AS EMAIL,
    UPPER(TRIM(CUSTOMER_STATUS)) AS CUSTOMER_STATUS,
    INITCAP(TRIM(CITY)) AS CITY

FROM {{ source('demo_raw', 'src_customers') }}