{# =============================================================================
#  Helper family :  __has_sum__
#  Semantics
#    • Nested (array-of-struct) field  →  Sum of <field> across the array
#                                         must be ≤ <configured-max>.
#    • Top-level scalar column        →  Value itself must be ≤ max
#      (handy when the model already contains a pre-computed total).
#
#  Example YAML
#    __has_sum__:
#      total_amount: 10_000          # scalar
#      items:                        # array<struct>
#        weight: 50
# ============================================================================= #}

{%- macro _get_has_sum_assertions(sum_columns) -%}
    {{ return(
        adapter.dispatch('_get_has_sum_assertions',
                         'dbt_assertions')(sum_columns)
    ) }}
{%- endmacro %}

{%- macro default___get_has_sum_assertions(sum_columns) -%}
    {%- set result        = {}         %}
    {%- set layered_cols  = dbt_assertions._extract_columns(sum_columns) %}

    {%- for parent, layer in layered_cols.items() %}
        {%- set cols       = layer.columns     %}
        {%- set depends_on = layer.depends_on  %}

        {# ─────────── Top-level scalar columns ───────────────────────── #}
        {%- if parent is none %}
            {%- for col in cols %}
                {%- set raw_val = sum_columns[col] %}
                {%- set max_val = dbt_assertions._as_sql_limit(raw_val) %}
                {%- do result.update({
                    col ~ '__has_sum': {
                        'description': col ~ ' must be ≤ ' ~ raw_val ~ '.',
                        'expression' : col ~ ' <= ' ~ max_val
                    }
                }) %}
            {%- endfor %}

        {# ─────────── Nested / repeated structures ───────────────────── #}
        {%- else %}
            {%- set base = ['NOT EXISTS (\\n    SELECT 1'] %}
            {%- for key in depends_on + [parent] %}
                {%- do base.append(
                    '\\n    ' ~ ('FROM' if loop.first else 'CROSS JOIN')
                    ~ ' UNNEST(' ~ key ~ ') ' ~ key) %}
            {%- endfor %}
            {%- do base.append('\\n    WHERE ' ~ parent ~ ' IS NOT NULL') %}

            {%- for col in cols %}
                {%- set raw_val = layer.nested[col] %}
                {%- set max_val = raw_val %}

                {# build the HAVING clause that flags sums above the limit #}
                {%- set expr = base + [
                    '\\n    HAVING SUM(' ~ parent ~ '.' ~ col ~ ') > ' ~ max_val,
                    '\\n)'] %}

                {%- do result.update({
                    '.'.join(depends_on + [parent, col]) ~ '__has_sum': {
                        'description':
                            'Sum of ' ~ '.'.join(depends_on + [parent, col])
                            ~ ' must be ≤ ' ~ raw_val ~ '.',
                        'expression' : expr | join('')
                    }
                }) %}
            {%- endfor %}
        {%- endif %}
    {%- endfor %}

    {{ return(result) }}
{%- endmacro %}
