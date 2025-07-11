{%- macro _get_is_positive_assertions(positive_columns) -%}
    {{- return(adapter.dispatch('_get_is_positive_assertions', 'dbt_assertions')(positive_columns)) }}
{%- endmacro %}

{# --------------------------------------------------------------------- #}
{%- macro default___get_is_positive_assertions(positive_columns) %}
{%- set result = {} %}

{# Re-use the existing flattening helper :contentReference[oaicite:0]{index=0} #}
{%- set layered_cols = dbt_assertions._extract_columns(positive_columns) %}

{%- for parent, layer in layered_cols.items() %}
    {%- set cols = layer.columns %}
    {%- set depends_on = layer.depends_on %}

    {# ── Top level (no nesting) ──────────────────────────────────── #}
    {%- if parent is none %}
        {%- for col in cols %}
            {%- do result.update({
                 col ~ '__positive': {
                     'description': col ~ ' must be strictly positive.',
                     'expression': col ~ ' > 0'
                 }
            }) %}
        {%- endfor %}

    {# ── Nested / repeated structures ───────────────────────────── #}
    {%- else %}
        {# Build a “NOT EXISTS” check similar to other helpers #}
        {%- set expr = ['NOT EXISTS (\n    SELECT 1'] %}

        {# UNNEST chain: depends_on … parent #}
        {%- for key in depends_on + [parent] %}
            {%- do expr.append('\n    ' ~ ('FROM' if loop.first else 'CROSS JOIN')) %}
            {%- do expr.append(' UNNEST(' ~ key ~ ') ' ~ key) %}
        {%- endfor %}

        {%- for col in cols %}
            {%- do result.update({
                '.'.join(depends_on + [parent, col]) ~ '__positive': {
                    'description': '.'.join(depends_on + [parent, col])
                                    ~ ' values must be strictly positive.',
                    'expression': ''.join(expr)
                                   ~ '\n    WHERE ' ~ parent ~ ' IS NOT NULL'
                                   ~ '\n      AND ' ~ parent ~ '.' ~ col ~ ' <= 0\n)'
                }
            }) %}
        {%- endfor %}
    {%- endif %}
{%- endfor %}

{{ return(result) }}
{%- endmacro %}
