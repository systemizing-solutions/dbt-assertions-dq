{% macro dbt_assertions__is_in(mapping) %}
    {% if mapping is none %}
        {{ exceptions.raise_compiler_error(
            '__is_in__ helper key must be followed by a mapping like '
            '{column_name: [allowed, values]}'
        ) }}
    {% endif %}

    {% set out = {} %}
    {% for col, allowed_vals in mapping.items() %}
        {% set allowed_list = allowed_vals if allowed_vals is iterable else [allowed_vals] %}

        {% set quoted = [] %}
        {% for val in allowed_list %}
            {% set escaped = val|string | replace("'", "''") %}
            {% set _ = quoted.append("'" ~ escaped ~ "'") %}
        {% endfor %}

        {% set expression = col ~ ' IN (' ~ quoted | join(', ') ~ ')' %}

        {% do out.update({
            (col ~ '__is_in'): {
                'description': col ~ ' must be in [' ~ allowed_list | join(', ') ~ ']',
                'expression':   expression
            }
        }) %}
    {% endfor %}
    {{ return(out) }}
{% endmacro %}
