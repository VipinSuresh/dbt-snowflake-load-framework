WITH history AS (
    SELECT * FROM {{ ref('regression_customer_type2') }}
), checks AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT_IF(IS_CURRENT = TRUE) AS current_rows,
        COUNT_IF(IS_CURRENT = FALSE) AS expired_rows,
        COUNT_IF(CUSTOMER_ID = 101 AND CITY = 'Dallas' AND VERSION_NO = 1
                 AND IS_CURRENT = FALSE AND CUSTOMER_STATUS = 'ACTIVE') AS dallas_v1,
        COUNT_IF(CUSTOMER_ID = 101 AND CITY = 'Dallas' AND VERSION_NO = 2
                 AND IS_CURRENT = TRUE AND CUSTOMER_STATUS = 'INACTIVE') AS dallas_v2,
        COUNT_IF(CUSTOMER_ID = 101 AND CITY = 'Austin' AND VERSION_NO = 1
                 AND IS_CURRENT = TRUE AND CUSTOMER_STATUS = 'ACTIVE') AS austin_unchanged,
        COUNT_IF(CUSTOMER_ID = 102 AND CITY = 'Plano' AND VERSION_NO = 1
                 AND IS_CURRENT = TRUE) AS plano_unchanged
    FROM history
), continuity AS (
    SELECT COUNT(*) AS matching_windows
    FROM history old
    JOIN history new
      ON old.CUSTOMER_ID = new.CUSTOMER_ID
     AND old.CITY = new.CITY
     AND old.VERSION_NO = 1
     AND new.VERSION_NO = 2
    WHERE old.CUSTOMER_ID = 101 AND old.CITY = 'Dallas'
      AND old.VALID_TO = new.VALID_FROM
)
SELECT * FROM checks CROSS JOIN continuity
WHERE total_rows <> 4 OR current_rows <> 3 OR expired_rows <> 1
   OR dallas_v1 <> 1 OR dallas_v2 <> 1 OR austin_unchanged <> 1
   OR plano_unchanged <> 1 OR matching_windows <> 1
