{% macro enterprise_control_relation(object_name) %}

    {% set allowed = [
        'ETL_RUN_CONTROL',
        'ETL_MODEL_STATS',
        'ETL_DQ_RESULTS',
        'ETL_FRAMEWORK_LOCKS',
        'ETL_NR_SEQ'
    ] %}

    {% if object_name not in allowed %}
        {{ exceptions.raise_compiler_error(
            'Unknown enterprise control object: ' ~ object_name
        ) }}
    {% endif %}

    {% set control_database =
        var('enterprise_control_database', target.database) %}

    {% set control_schema =
        var('enterprise_control_schema', 'ETL_CONTROL') %}

    {% set relation = api.Relation.create(
        database=control_database,
        schema=control_schema,
        identifier=object_name
    ) %}

    {% do return(relation) %}

{% endmacro %}
