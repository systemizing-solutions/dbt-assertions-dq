{# =======================================================================
#  Helper family :  __is_on_weekday__
#  Each column / field must be a weekday (Mon-Fri).
#  Works on nested / repeated structures just like the built-in helpers.
# ======================================================================= #}

{%- macro _get_is_on_weekday_assertions(wd_columns) -%}
    {{ return(
        adapter.dispatch('_get_is_on_weekday_assertions',
                         'dbt_assertions')(wd_columns)
    ) }}
{%- endmacro %}

{%- macro default___get_is_on_weekday_assertions(wd_columns) -%}
    {%- set result        = {} %}
    {%- set layered_cols  = dbt_assertions._extract_columns(wd_columns) %}

    {%- for parent, layer in layered_cols.items() %}
        {%- set cols       = layer.columns %}
        {%- set depends_on = layer.depends_on %}

        {# ---------- TOP-LEVEL COLUMNS --------------------------------- #}
        {%- if parent is none %}
            {%- for col in cols %}
                {%- set pred =  dbt_assertions._is_weekday_predicate(
                                   'CAST(' ~ col ~ ' AS DATE)') %}
                {%- do result.update({
                    col ~ '__is_on_weekday': {
                        'description': col ~ ' must be a weekday (Mon-Fri).',
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
                {%- set pred       = dbt_assertions._is_weekday_predicate(field_expr) %}
                {%- set expr = base + [
                    '\\n      AND NOT ' ~ pred,
                    '\\n)'] %}
                {%- do result.update({
                    '.'.join(depends_on + [parent, col]) ~ '__is_on_weekday': {
                        'description':
                            '.'.join(depends_on + [parent, col])
                            ~ ' must be a weekday (Mon-Fri).',
                        'expression' : expr | join('')
                    }
                }) %}
            {%- endfor %}
        {%- endif %}
    {%- endfor %}

    {{ return(result) }}
{%- endmacro %}
