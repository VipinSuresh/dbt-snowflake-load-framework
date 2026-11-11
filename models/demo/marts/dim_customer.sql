SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    EMAIL,
    CUSTOMER_STATUS,
    CITY
FROM {{ ref('int_customers') }}