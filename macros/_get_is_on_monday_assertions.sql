{# ======================================================================
#  Helper family :  __is_on_monday__
#  Ensures each value is a Monday (ISO weekday 1).
#  Supports nested / repeated structures with the same UNNEST pattern
#  used by the core dbt-assertions helpers.
# ====================================================================== #}

{%- macro _get_is_on_monday_assertions(mon_columns) -%}
    {{ return(
        adapter.dispatch('_get_is_on_monday_assertions',
                         'dbt_assertions')(mon_columns)
    ) }}
{%- endmacro %}

{%- macro default___get_is_on_monday_assertions(mon_columns) -%}
    {%- set result        = {} %}
    {%- set layered_cols  = dbt_assertions._extract_columns(mon_columns) %}

    {%- for parent, layer in layered_cols.items() %}
        {%- set cols       = layer.columns %}
        {%- set depends_on = layer.depends_on %}

        {# ----------- TOP-LEVEL COLUMNS -------------------------------- #}
        {%- if parent is none %}
            {%- for col in cols %}
                {%- set pred = dbt_assertions._is_monday_predicate(
                                 'CAST(' ~ col ~ ' AS DATE)') %}
                {%- do result.update({
                    col ~ '__is_on_monday': {
                        'description': col ~ ' must be a Monday.',
                        'expression' : pred
                    }
                }) %}
            {%- endfor %}

        {# ----------- NESTED / REPEATED STRUCTURES -------------------- #}
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
                {%- set pred       = dbt_assertions._is_monday_predicate(field_expr) %}
                {%- set expr = base + [
                    '\\n      AND NOT ' ~ pred,
                    '\\n)'] %}
                {%- do result.update({
                    '.'.join(depends_on + [parent, col]) ~ '__is_on_monday': {
                        'description':
                            '.'.join(depends_on + [parent, col])
                            ~ ' must be a Monday.',
                        'expression' : expr | join('')
                    }
                }) %}
            {%- endfor %}
        {%- endif %}
    {%- endfor %}

    {{ return(result) }}
{%- endmacro %}
