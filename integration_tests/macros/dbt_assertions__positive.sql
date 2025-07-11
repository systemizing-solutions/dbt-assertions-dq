{% macro dbt_assertions__positive(columns) %}
    {%- set out = {} %}
    {%- for col in columns %}
        {%- do out.update({
            (col ~ '__positive'): {
                'description': col ~ ' must be > 0',
                'expression': col ~ ' > 0'
            }
        }) %}
    {%- endfor %}
    {{ return(out) }}
{% endmacro %}
