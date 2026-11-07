
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if node.config.materialized == 'enterprise_load' -%}

        {%- if custom_schema_name is none -%}
            {{ exceptions.raise_compiler_error(
                'ENTERPRISE_LOAD requires an explicit target schema.'
            ) }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}

    {%- elif custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
