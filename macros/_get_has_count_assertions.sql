{# =============================================================================
#  Helper family  :  __has_count__
#
#  Meaning
#  ▸ For a scalar / simple column      → its value must be **≤ limit**  
#    (useful if you store a precalculated “number_of_items” field).
#
#  ▸ For an array-of-struct property   → the **COUNT()** of that property
#    across the UNNEST-ed array must be **≤ limit**.
#
#  YAML Example
#    assertions:
#      __has_count__:
#        num_lines: 1_000          # scalar
#        items:                    # array<struct>
#          qty: 250                # per-row limit on element count
# ============================================================================= #}

{%- macro _get_has_count_assertions(cnt_columns) -%}
    {{ return(
        adapter.dispatch('_get_has_count_assertions',
                         'dbt_assertions')(cnt_columns)
    ) }}
{%- endmacro %}

{# ----------------------------------------------------------------------------- #}
{%- macro default___get_has_count_assertions(cnt_columns) -%}
    {%- set result       = {} %}
    {%- set layered_cols = dbt_assertions._extract_columns(cnt_columns) %}

    {%- for parent, layer in layered_cols.items() %}
        {%- set cols       = layer.columns %}
        {%- set depends_on = layer.depends_on %}

        {# ─────────── top-level (scalar) columns ─────────────────────── #}
        {%- if parent is none %}
            {%- for col in cols %}
                {%- set lim = cnt_columns[col] %}
                {%- do result.update({
                    col ~ '__has_count': {
                        'description': col ~ ' must be ≤ ' ~ lim ~ '.',
                        'expression' : col ~ ' <= ' ~ lim
                    }
                }) %}
            {%- endfor %}

        {# ─────────── nested / repeated structures ───────────────────── #}
        {%- else %}
            {%- set base = ['NOT EXISTS (\\n    SELECT 1'] %}
            {%- for key in depends_on + [parent] %}
                {%- do base.append(
                    '\\n    ' ~ ('FROM' if loop.first else 'CROSS JOIN')
                    ~ ' UNNEST(' ~ key ~ ') ' ~ key) %}
            {%- endfor %}
            {%- do base.append('\\n    WHERE ' ~ parent ~ ' IS NOT NULL') %}

            {%- for col in cols %}
                {%- set lim   = layer.nested[col] %}
                {%- set expr  = base
                    + ['\\n    HAVING COUNT(' ~ parent ~ '.' ~ col
                       ~ ') > ' ~ lim,
                       '\\n)'] %}
                {%- do result.update({
                    '.'.join(depends_on + [parent, col]) ~ '__has_count': {
                        'description':
                            'Count of ' ~ '.'.join(depends_on + [parent, col])
                            ~ ' must be ≤ ' ~ lim ~ '.',
                        'expression' : expr | join('')
                    }
                }) %}
            {%- endfor %}
        {%- endif %}
    {%- endfor %}

    {{ return(result) }}
{%- endmacro %}
