{# SCHEMA_DRIFT_EDGE_CASE_WRAPPER_V1 #}
{% set schema_case = var('regression_case', '') %}
{% if schema_case == 'schema_missing_column' %}
SELECT
    column1::NUMBER(38,0) AS CUSTOMER_ID,
    column2::VARCHAR(100) AS CUSTOMER_NAME,
    column3::VARCHAR(255) AS EMAIL,
    column4::VARCHAR(20) AS CUSTOMER_STATUS
FROM VALUES
    (101, 'Sheldon', 'sheldon@example.com', 'ACTIVE'),
    (102, 'Leonard', 'leonard@example.com', 'ACTIVE'),
    (104, 'Howard', 'howard@example.com', 'ACTIVE')
{% elif schema_case == 'schema_extra_column' %}
SELECT
    column1::NUMBER(38,0) AS CUSTOMER_ID,
    column2::VARCHAR(100) AS CUSTOMER_NAME,
    column3::VARCHAR(255) AS EMAIL,
    column4::VARCHAR(20) AS CUSTOMER_STATUS,
    column5::VARCHAR(100) AS CITY,
    column6::VARCHAR(50) AS UNEXPECTED_FIELD
FROM VALUES
    (101, 'Sheldon', 'sheldon@example.com', 'ACTIVE', 'Austin', 'X'),
    (102, 'Leonard', 'leonard@example.com', 'ACTIVE', 'Plano', 'X'),
    (104, 'Howard', 'howard@example.com', 'ACTIVE', 'Irving', 'X')
{% elif schema_case == 'schema_type_mismatch' %}
SELECT
    column1::VARCHAR(50) AS CUSTOMER_ID,
    column2::VARCHAR(100) AS CUSTOMER_NAME,
    column3::VARCHAR(255) AS EMAIL,
    column4::VARCHAR(20) AS CUSTOMER_STATUS,
    column5::VARCHAR(100) AS CITY
FROM VALUES
    ('101', 'Sheldon', 'sheldon@example.com', 'ACTIVE', 'Austin'),
    ('102', 'Leonard', 'leonard@example.com', 'ACTIVE', 'Plano'),
    ('104', 'Howard', 'howard@example.com', 'ACTIVE', 'Irving')
{% else %}
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
{% set inject_null = var('regression_case', 'positive') == 'null_name' %}

SELECT
    column1::NUMBER(38,0) AS CUSTOMER_ID,
    column2::VARCHAR(100) AS CUSTOMER_NAME,
    column3::VARCHAR(255) AS EMAIL,
    column4::VARCHAR(20) AS CUSTOMER_STATUS,
    column5::VARCHAR(100) AS CITY

FROM VALUES
    (
        101,
        {% if inject_null %}NULL{% else %}'Sheldon'{% endif %},
        'sheldon@example.com',
        'ACTIVE',
        'Austin'
    ),
    (102, 'Leonard', 'leonard@example.com', 'ACTIVE', 'Plano'),
    (104, 'Howard', 'howard@example.com', 'ACTIVE', 'Irving')
{% endif %}
{% endif %}
