{% macro regression_target_relation(identifier) %}

    {% set regression_database =
        var('enterprise_regression_database', target.database) %}

    {% set regression_schema =
        var('enterprise_regression_schema', 'REGRESSION_TEST') %}

    {% set relation = api.Relation.create(
        database=regression_database,
        schema=regression_schema,
        identifier=identifier
    ) %}

    {% do return(relation) %}

{% endmacro %}
