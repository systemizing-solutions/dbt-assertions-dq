{# =========================================================================
#  Helper family  :  __is_today__
#  Each column / field must equal today’s date.
#  Works on nested / repeated structures via the same UNNEST pattern used
#  by the core dbt-assertions helpers.
# ========================================================================= #}

{%- macro _get_is_today_assertions(today_cols) -%}
    {{ return(
        adapter.dispatch('_get_is_today_assertions',
                         'dbt_assertions')(today_cols)
    ) }}
{%- endmacro %}

{# ---------------------------------------------------------------------- #}
{%- macro default___get_is_today_assertions(today_cols) -%}
    {%- set result        = {} %}
    {%- set layered_cols  = dbt_assertions._extract_columns(today_cols) %}

    {%- set today_expr = dbt_assertions._today_date() %}

    {%- for parent, layer in layered_cols.items() %}
        {%- set cols       = layer.columns %}
        {%- set depends_on = layer.depends_on %}

        {# ---------- TOP-LEVEL COLUMNS -------------------------------- #}
        {%- if parent is none %}
            {%- for col in cols %}
                {%- do result.update({
                    col ~ '__is_today': {
                        'description': col ~ ' must equal today.',
                        'expression' : 'CAST(' ~ col ~ ' AS DATE) = ' ~ today_expr
                    }
                }) %}
            {%- endfor %}

        {# ---------- NESTED / REPEATED STRUCTURES --------------------- #}
        {%- else %}
            {%- set base = ['NOT EXISTS (\\n    SELECT 1'] %}
            {%- for key in depends_on + [parent] %}
                {%- do base.append(
                    '\\n    ' ~ ('FROM' if loop.first else 'CROSS JOIN')
                    ~ ' UNNEST(' ~ key ~ ') ' ~ key) %}
            {%- endfor %}
            {%- do base.append('\\n    WHERE ' ~ parent ~ ' IS NOT NULL') %}

            {%- for col in cols %}
                {%- set expr = base + [
                    '\\n      AND CAST(' ~ parent ~ '.' ~ col
                    ~ ' AS DATE) <> ' ~ today_expr,
                    '\\n)'] %}
                {%- do result.update({
                    '.'.join(depends_on + [parent, col]) ~ '__is_today': {
                        'description':
                            '.'.join(depends_on + [parent, col])
                            ~ ' must equal today.',
                        'expression' : expr | join('')
                    }
                }) %}
            {%- endfor %}
        {%- endif %}
    {%- endfor %}

    {{ return(result) }}
{%- endmacro %}
