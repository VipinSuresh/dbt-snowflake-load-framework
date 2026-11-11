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
    (103, 'Raj', 'raj@example.com', 'ACTIVE')
{% elif schema_case == 'schema_type_mismatch' %}
SELECT
    column1::VARCHAR(50) AS CUSTOMER_ID,
    column2::VARCHAR(100) AS CUSTOMER_NAME,
    column3::VARCHAR(255) AS EMAIL,
    column4::VARCHAR(20) AS CUSTOMER_STATUS,
    column5::VARCHAR(100) AS CITY
FROM VALUES
    ('101', 'Sheldon', 'sheldon@example.com', 'ACTIVE', 'Dallas'),
    ('102', 'Leonard', 'leonard@example.com', 'ACTIVE', 'Plano'),
    ('103', 'Raj', 'raj@example.com', 'ACTIVE', 'Frisco')
{% elif schema_case == 'schema_reserved_column' %}
SELECT
    column1::NUMBER(38,0) AS CUSTOMER_ID,
    column2::VARCHAR(100) AS CUSTOMER_NAME,
    column3::VARCHAR(255) AS EMAIL,
    column4::VARCHAR(20) AS CUSTOMER_STATUS,
    column5::VARCHAR(100) AS CITY,
    column6::VARCHAR(20) AS EL_ACTION
FROM VALUES
    (101, 'Sheldon', 'sheldon@example.com', 'ACTIVE', 'Dallas', 'CHANGED'),
    (102, 'Leonard', 'leonard@example.com', 'ACTIVE', 'Plano', 'CHANGED'),
    (103, 'Raj', 'raj@example.com', 'ACTIVE', 'Frisco', 'CHANGED')
{% else %}
{# COMPOSITE_KEY_EDGE_CASE_WRAPPER_V1 #}
{% set composite_case = var('regression_case', '') %}
{% if composite_case in ['composite_positive', 'composite_changed'] %}
SELECT
    column1::NUMBER(38,0) AS CUSTOMER_ID,
    column2::VARCHAR(100) AS CUSTOMER_NAME,
    column3::VARCHAR(255) AS EMAIL,
    column4::VARCHAR(20) AS CUSTOMER_STATUS,
    column5::VARCHAR(100) AS CITY
FROM VALUES
    (101, 'Sheldon Dallas', 'sheldon.dallas@example.com', {% if composite_case == 'composite_changed' %}'INACTIVE'{% else %}'ACTIVE'{% endif %}, 'Dallas'),
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
{% set scenario = var('regression_case', 'initial') %}

SELECT
    column1::NUMBER(38,0) AS CUSTOMER_ID,
    column2::VARCHAR(100) AS CUSTOMER_NAME,
    column3::VARCHAR(255) AS EMAIL,
    column4::VARCHAR(20) AS CUSTOMER_STATUS,
    column5::VARCHAR(100) AS CITY

FROM VALUES
    (
        101,
        {% if scenario == 'null_name' %}
            NULL
        {% else %}
            'Sheldon'
        {% endif %},
        'sheldon@example.com',
        'ACTIVE',
        {% if scenario in ['changed', 'changed_101'] %}
            'Austin'
        {% else %}
            'Dallas'
        {% endif %}
    ),
    (
        102,
        'Leonard',
        'leonard@example.com',
        'ACTIVE',
        {% if scenario == 'changed_102' %}
            'McKinney'
        {% else %}
            'Plano'
        {% endif %}
    ),
    (103, 'Raj', 'raj@example.com', 'ACTIVE', 'Frisco')
{% endif %}
{% endif %}
