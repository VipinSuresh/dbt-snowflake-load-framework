{# COMPOSITE_KEY_EDGE_CASE_WRAPPER_V1 #}
{% set composite_case = var('regression_case', '') %}
{% if composite_case == 'composite_positive' %}
SELECT
    column1::NUMBER(38,0) AS CUSTOMER_ID,
    column2::VARCHAR(100) AS CUSTOMER_NAME,
    column3::VARCHAR(255) AS EMAIL,
    column4::VARCHAR(20) AS CUSTOMER_STATUS,
    column5::VARCHAR(100) AS CITY
FROM VALUES
    (101, 'Sheldon Dallas', 'sheldon.dallas@example.com', 'ACTIVE', 'Dallas'),
    (101, 'Sheldon Austin', 'sheldon.austin@example.com', 'ACTIVE', 'Austin'),
    (102, 'Leonard', 'leonard@example.com', 'ACTIVE', 'Plano')
{% elif composite_case == 'composite_null_key' %}
SELECT
    column1::NUMBER(38,0) AS CUSTOMER_ID,
    column2::VARCHAR(100) AS CUSTOMER_NAME,
    column3::VARCHAR(255) AS EMAIL,
    column4::VARCHAR(20) AS CUSTOMER_STATUS,
    column5::VARCHAR(100) AS CITY
FROM VALUES
    (101, 'Sheldon Dallas', 'sheldon.dallas@example.com', 'ACTIVE', 'Dallas'),
    (103, 'Raj', 'raj@example.com', 'ACTIVE', NULL),
    (102, 'Leonard', 'leonard@example.com', 'ACTIVE', 'Plano')
{% elif composite_case == 'composite_duplicate_key' %}
SELECT
    column1::NUMBER(38,0) AS CUSTOMER_ID,
    column2::VARCHAR(100) AS CUSTOMER_NAME,
    column3::VARCHAR(255) AS EMAIL,
    column4::VARCHAR(20) AS CUSTOMER_STATUS,
    column5::VARCHAR(100) AS CITY
FROM VALUES
    (101, 'Sheldon Dallas A', 'sheldon.a@example.com', 'ACTIVE', 'Dallas'),
    (101, 'Sheldon Dallas B', 'sheldon.b@example.com', 'ACTIVE', 'Dallas'),
    (102, 'Leonard', 'leonard@example.com', 'ACTIVE', 'Plano')
{% else %}
{% set regression_case = var('regression_case', 'positive') %}
{% set inject_null = regression_case == 'null_name' %}
{% set critical_failure_case = regression_case == 'critical_failure' %}
{% set distinct_a = regression_case == 'concurrency_distinct_a' %}
{% set distinct_b = regression_case == 'concurrency_distinct_b' %}

SELECT
    column1::NUMBER(38,0) AS CUSTOMER_ID,
    column2::VARCHAR(100) AS CUSTOMER_NAME,
    column3::VARCHAR(255) AS EMAIL,
    column4::VARCHAR(20) AS CUSTOMER_STATUS,
    column5::VARCHAR(100) AS CITY

FROM VALUES
{% if distinct_b %}
    (102, 'Leonard', 'leonard@example.com', 'ACTIVE', 'McKinney'),
    (105, 'Penny', 'penny@example.com', 'ACTIVE', 'Allen')
{% else %}
    (101, 'Sheldon', 'sheldon@example.com', 'ACTIVE', {% if critical_failure_case %}'Houston'{% else %}'Austin'{% endif %}),
    (
        104,
        {% if inject_null %}NULL{% elif critical_failure_case %}'Amy'{% else %}'Howard'{% endif %},
        'howard@example.com',
        'ACTIVE',
        'Irving'
    )
{% endif %}
{% endif %}
