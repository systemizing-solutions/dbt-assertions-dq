{# =========================================================================
#  Helper family : __is_yesterday__
#  Each column / field must equal yesterday’s date.
# ========================================================================= #}

{%- macro _get_is_yesterday_assertions(y_cols) -%}
    {{ return(
        adapter.dispatch('_get_is_yesterday_assertions',
                         'dbt_assertions')(y_cols)
    ) }}
{%- endmacro %}

{# ---------------------------------------------------------------------- #}
{%- macro default___get_is_yesterday_assertions(y_cols) -%}
    {%- set result        = {} %}
    {%- set layered_cols  = dbt_assertions._extract_columns(y_cols) %}
    {%- set yesterday_expr = dbt_assertions._yesterday_date() %}

    {%- for parent, layer in layered_cols.items() %}
        {%- set cols       = layer.columns %}
        {%- set depends_on = layer.depends_on %}

        {# ---- top-level columns -------------------------------------- #}
        {%- if parent is none %}
            {%- for col in cols %}
                {%- do result.update({
                    col ~ '__is_yesterday': {
                        'description': col ~ ' must equal yesterday.',
                        'expression' : 'CAST(' ~ col ~ ' AS DATE) = ' ~ yesterday_expr
                    }
                }) %}
            {%- endfor %}

        {# ---- nested / repeated structures --------------------------- #}
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
                    ~ ' AS DATE) <> ' ~ yesterday_expr,
                    '\\n)'] %}
                {%- do result.update({
                    '.'.join(depends_on + [parent, col]) ~ '__is_yesterday': {
                        'description':
                            '.'.join(depends_on + [parent, col])
                            ~ ' must equal yesterday.',
                        'expression' : expr | join('')
                    }
                }) %}
            {%- endfor %}
        {%- endif %}
    {%- endfor %}

    {{ return(result) }}
{%- endmacro %}
