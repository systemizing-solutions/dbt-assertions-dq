{# ======================================================================
#  Helper family : __is_on_friday__
#  Ensures every value is a Friday.  Works on nested / repeated fields.
# ====================================================================== #}

{%- macro _get_is_on_friday_assertions(fri_columns) -%}
    {{ return(
        adapter.dispatch('_get_is_on_friday_assertions',
                         'dbt_assertions')(fri_columns)
    ) }}
{%- endmacro %}

{%- macro default___get_is_on_friday_assertions(fri_columns) -%}
    {%- set result        = {} %}
    {%- set layered_cols  = dbt_assertions._extract_columns(fri_columns) %}

    {%- for parent, layer in layered_cols.items() %}
        {%- set cols       = layer.columns %}
        {%- set depends_on = layer.depends_on %}

        {# ── Top-level columns ───────────────────────────────────────── #}
        {%- if parent is none %}
            {%- for col in cols %}
                {%- set pred = dbt_assertions._is_friday_predicate(
                                 'CAST(' ~ col ~ ' AS DATE)') %}
                {%- do result.update({
                    col ~ '__is_on_friday': {
                        'description': col ~ ' must be a Friday.',
                        'expression' : pred
                    }
                }) %}
            {%- endfor %}

        {# ── Nested / repeated fields ─────────────────────────────────- #}
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
                {%- set pred       = dbt_assertions._is_friday_predicate(field_expr) %}
                {%- set expr = base + [
                    '\\n      AND NOT ' ~ pred,
                    '\\n)'] %}
                {%- do result.update({
                    '.'.join(depends_on + [parent, col]) ~ '__is_on_friday': {
                        'description':
                            '.'.join(depends_on + [parent, col]) ~
                            ' must be a Friday.',
                        'expression' : expr | join('')
                    }
                }) %}
            {%- endfor %}
        {%- endif %}
    {%- endfor %}

    {{ return(result) }}
{%- endmacro %}
