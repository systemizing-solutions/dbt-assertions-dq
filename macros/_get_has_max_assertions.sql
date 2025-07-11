{# =============================================================================
#  Helper family  :  __has_max__
#  Semantics      :  Each value must be <= the supplied maximum.
#
#  Example YAML
#    __has_max__:
#      price: 100
#      items:
#        qty: 10
# ============================================================================= #}

{%- macro _get_has_max_assertions(max_columns) -%}
    {{ return(
        adapter.dispatch('_get_has_max_assertions',
                         'dbt_assertions')(max_columns)
    ) }}
{%- endmacro %}

{%- macro default___get_has_max_assertions(max_columns) -%}
    {%- set result        = {} %}
    {%- set layered_cols  = dbt_assertions._extract_columns(max_columns) %}

    {%- for parent, layer in layered_cols.items() %}
        {%- set cols       = layer.columns %}
        {%- set depends_on = layer.depends_on %}

        {# ── top-level columns ───────────────────────────────────────── #}
        {%- if parent is none %}
            {%- for col in cols %}
                {%- set max_value = max_columns[col] %}
                {%- do result.update({
                    col ~ '__has_max': {
                        'description': col ~ ' must be ≤ ' ~ max_value ~ '.',
                        'expression' : col ~ ' <= ' ~ max_value
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
                {%- set max_value = layer.nested[col] %}
                {%- set expr = base + [
                    '\\n      AND ' ~ parent ~ '.' ~ col ~ ' > ' ~ max_value,
                    '\\n)'] %}
                {%- do result.update({
                    '.'.join(depends_on + [parent, col]) ~ '__has_max': {
                        'description':
                            '.'.join(depends_on + [parent, col])
                            ~ ' must be ≤ ' ~ max_value ~ '.',
                        'expression' : expr | join('')
                    }
                }) %}
            {%- endfor %}
        {%- endif %}
    {%- endfor %}

    {{ return(result) }}
{%- endmacro %}
