SELECT *
FROM {{ ref('regression_customer_replace') }}
LIMIT 1
