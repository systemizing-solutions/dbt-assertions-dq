{# macros/dbt_assertions__is_in.sql #}
{% macro dbt_assertions__is_in(mapping) %}
    {# `mapping` example in YAML:
       __is_in__:
         payment_method: ['BANK_TRANSFER', 'CREDIT_CARD']
         status:          SUCCESS                 # scalar is OK too
    #}


    {% set out = {} %}

    {% for col, allowed in mapping.items() %}

        {# Normalise `allowed` to a list even if the author supplied a scalar #}
        {% if allowed is string %}
            {% set allowed_list = [allowed] %}
        {% else %}
            {% set allowed_list = allowed %}
        {% endif %}

        {# Quote & escape every element once #}
        {% set quoted_vals = [] %}
        {% for v in allowed_list %}
            {% set escaped = v|string | replace("'", "''") %}
            {% set _ = quoted_vals.append("'" ~ escaped ~ "'") %}
        {% endfor %}

        {% set expression = col ~ ' IN (' ~ quoted_vals | join(', ') ~ ')' %}

        {% do out.update({
            (col ~ '__is_in'): {
                'description': col ~ ' must be in [' ~ allowed_list | join(', ') ~ ']',
                'expression':   expression
            }
        }) %}

    {% endfor %}

    {{ return(out) }}
{% endmacro %}

