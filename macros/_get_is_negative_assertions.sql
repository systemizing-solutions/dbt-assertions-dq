{%- macro _get_is_negative_assertions(neg_columns) -%}
    {{ return(
        adapter.dispatch('_get_is_negative_assertions', 'dbt_assertions')(neg_columns)
    ) }}
{%- endmacro %}

{# ---------------------------------------------------------------------- #}
{%- macro default___get_is_negative_assertions(neg_columns) %}
{%- set result = {} %}
{# flatten the yaml structure just like the native helpers do :contentReference[oaicite:3]{index=3} #}
{%- set layered_cols = dbt_assertions._extract_columns(neg_columns) %}

{%- for parent, layer in layered_cols.items() %}
    {%- set cols       = layer.columns %}
    {%- set depends_on = layer.depends_on %}

    {# ── 0-depth columns ──────────────────────────────────────────────── #}
    {%- if parent is none %}
        {%- for col in cols %}
            {%- do result.update({
                col ~ '__is_negative': {
                    'description': col ~ ' must be < 0.',
                    'expression' : col ~ ' < 0'
                }
            }) %}
        {%- endfor %}

    {# ── nested / repeated structures ─────────────────────────────────── #}
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
                '\\n      AND ' ~ parent ~ '.' ~ col ~ ' >= 0\\n)'] %}
            {%- do result.update({
                '.'.join(depends_on + [parent, col]) ~ '__is_negative': {
                    'description':
                        '.'.join(depends_on + [parent, col]) ~ ' must be < 0.',
                    'expression': expr | join('')
                }
            }) %}
        {%- endfor %}
    {%- endif %}
{%- endfor %}

{{ return(result) }}
{%- endmacro %}
