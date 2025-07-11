{# ======================================================================
#  Helper family : __is_on_saturday__
#  Validates that each value is a Saturday, handling nested / repeated
#  structures via the same UNNEST pattern used by dbt-assertions.
# ====================================================================== #}

{%- macro _get_is_on_saturday_assertions(sat_columns) -%}
    {{ return(
        adapter.dispatch('_get_is_on_saturday_assertions',
                         'dbt_assertions')(sat_columns)
    ) }}
{%- endmacro %}

{%- macro default___get_is_on_saturday_assertions(sat_columns) -%}
    {%- set result        = {} %}
    {%- set layered_cols  = dbt_assertions._extract_columns(sat_columns) %}

    {%- for parent, layer in layered_cols.items() %}
        {%- set cols       = layer.columns %}
        {%- set depends_on = layer.depends_on %}

        {# ── top-level columns ───────────────────────────────────────── #}
        {%- if parent is none %}
            {%- for col in cols %}
                {%- set pred = dbt_assertions._is_saturday_predicate(
                                 'CAST(' ~ col ~ ' AS DATE)') %}
                {%- do result.update({
                    col ~ '__is_on_saturday': {
                        'description': col ~ ' must be a Saturday.',
                        'expression' : pred
                    }
                }) %}
            {%- endfor %}

        {# ── nested / repeated fields ─────────────────────────────────- #}
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
                {%- set pred       = dbt_assertions._is_saturday_predicate(field_expr) %}
                {%- set expr = base + [
                    '\\n      AND NOT ' ~ pred,
                    '\\n)'] %}
                {%- do result.update({
                    '.'.join(depends_on + [parent, col]) ~ '__is_on_saturday': {
                        'description':
                            '.'.join(depends_on + [parent, col])
                            ~ ' must be a Saturday.',
                        'expression' : expr | join('')
                    }
                }) %}
            {%- endfor %}
        {%- endif %}
    {%- endfor %}

    {{ return(result) }}
{%- endmacro %}
