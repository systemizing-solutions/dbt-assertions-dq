{# ======================================================================
#  Helper family :  __is_on_wednesday__
#  Ensures every value is a Wednesday.
#  Supports nested / repeated structures via the standard UNNEST pattern.
# ====================================================================== #}

{%- macro _get_is_on_wednesday_assertions(wed_columns) -%}
    {{ return(
        adapter.dispatch('_get_is_on_wednesday_assertions',
                         'dbt_assertions')(wed_columns)
    ) }}
{%- endmacro %}

{%- macro default___get_is_on_wednesday_assertions(wed_columns) -%}
    {%- set result        = {} %}
    {%- set layered_cols  = dbt_assertions._extract_columns(wed_columns) %}

    {%- for parent, layer in layered_cols.items() %}
        {%- set cols       = layer.columns %}
        {%- set depends_on = layer.depends_on %}

        {# ---------- TOP-LEVEL COLUMNS --------------------------------- #}
        {%- if parent is none %}
            {%- for col in cols %}
                {%- set pred = dbt_assertions._is_wednesday_predicate(
                                 'CAST(' ~ col ~ ' AS DATE)') %}
                {%- do result.update({
                    col ~ '__is_on_wednesday': {
                        'description': col ~ ' must be a Wednesday.',
                        'expression' : pred
                    }
                }) %}
            {%- endfor %}

        {# ---------- NESTED / REPEATED STRUCTURES ---------------------- #}
        {%- else %}
            {%- set base = ['NOT EXISTS (\\n    SELECT 1'] %}
            {%- for key in depends_on + [parent] %}
                {%- do base.append(
                    '\\n    ' ~ ('FROM' if loop.first else 'CROSS JOIN')
                    ~ ' UNNEST(' ~ key ~ ') ' ~ key) %}
            {%- endfor %}
            {%- do base.append('\\n    WHERE ' ~ parent ~ ' IS NOT NULL') %}

            {%- for col in cols %}
                {%- set field_expr = 'CAST(' ~ parent ~ '.' ~ col ~ ' AS DATE)' %}
                {%- set pred       = dbt_assertions._is_wednesday_predicate(field_expr) %}
                {%- set expr = base + [
                    '\\n      AND NOT ' ~ pred,
                    '\\n)'] %}
                {%- do result.update({
                    '.'.join(depends_on + [parent, col]) ~ '__is_on_wednesday': {
                        'description':
                            '.'.join(depends_on + [parent, col])
                            ~ ' must be a Wednesday.',
                        'expression' : expr | join('')
                    }
                }) %}
            {%- endfor %}
        {%- endif %}
    {%- endfor %}

    {{ return(result) }}
{%- endmacro %}
