
{# Shared natural-key predicate for DELETE_INSERT. #}

{% macro generate_delete_insert_key_match(
    natural_keys,
    target_alias,
    source_alias
) %}

    {% if natural_keys | length == 0 %}
        {{ exceptions.raise_compiler_error(
            'DELETE_INSERT: Natural keys are required.'
        ) }}
    {% endif %}

    {% set conditions = [] %}

    {% for key in natural_keys %}

        {% set quoted_key = adapter.quote(key | upper) %}

        {% do conditions.append(
            target_alias ~ '.' ~ quoted_key
            ~ ' = '
            ~ source_alias ~ '.' ~ quoted_key
        ) %}

    {% endfor %}

    {% do return(conditions | join(' AND ')) %}

{% endmacro %}


{# Delete only target records matching prepared source keys. #}

{% macro generate_delete_insert_delete_sql(
    source_relation,
    target_relation,
    natural_keys
) %}

    {% set condition = generate_delete_insert_key_match(
        natural_keys,
        adapter.quote(target_relation.identifier),
        adapter.quote(source_relation.identifier)
    ) %}

    {% set delete_sql %}

        DELETE FROM {{ target_relation }}

        USING {{ source_relation }}

        WHERE {{ condition }}

    {% endset %}

    {% do return(delete_sql) %}

{% endmacro %}
