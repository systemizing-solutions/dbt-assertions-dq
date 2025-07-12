{# =============================================================================
#  Helper family  :  __has_mean__
#
#  Behaviour
#  • Scalar column  ➜  value itself must be ≤ limit
#  • Array/struct   ➜  AVG(<field>) across the UNNEST-ed array
#                     must be ≤ limit for each parent row
#
#  Example YAML
#    __has_mean__:
#      avg_unit_price: 100         # scalar
#      items:                      # array<struct>
#        cost: 25                  # per-order limit on average item cost
# ============================================================================= #}

{%- macro _get_has_mean_assertions(mean_columns) -%}
    {{ return(
        adapter.dispatch('_get_has_mean_assertions',
                         'dbt_assertions')(mean_columns)
    ) }}
{%- endmacro %}

{# ------------------------------------------------------------------------- #}
{%- macro default___get_has_mean_assertions(mean_columns) -%}
    {%- set result        = {} %}
    {%- set layered_cols  = dbt_assertions._extract_columns(mean_columns) %}

    {%- for parent, layer in layered_cols.items() %}
        {%- set cols       = layer.columns %}
        {%- set depends_on = layer.depends_on %}

        {# ─────────── 1 │ top-level scalar columns ─────────────────────── #}
        {%- if parent is none %}
            {%- for col in cols %}
                {%- set raw_lim = mean_columns[col] %}
                {% set lim = (
                    limit_raw is number
                        and limit_raw
                    or (limit_raw | trim).startswith('(')
                        and limit_raw
                    or (limit_raw | trim)[:6] | lower == 'select'
                        and limit_raw
                    or "'" ~ limit_raw | replace("'", "''") ~ "'"
                ) %}
                {%- do result.update({
                    col ~ '__has_mean': {
                        'description': col ~ ' must be ≤ ' ~ raw_lim ~ '.',
                        'expression' : col ~ ' <= ' ~ lim
                    }
                }) %}
            {%- endfor %}

        {# ─────────── 2 │ nested / repeated fields ─────────────────────── #}
        {%- else %}
            {# base SELECT for UNNEST chain -------------------------------- #}
            {%- set base = ['NOT EXISTS (\\n    SELECT 1'] %}
            {%- for key in depends_on + [parent] %}
                {%- do base.append(
                    '\\n    ' ~ ('FROM' if loop.first else 'CROSS JOIN')
                    ~ ' UNNEST(' ~ key ~ ') ' ~ key) %}
            {%- endfor %}
            {%- do base.append('\\n    WHERE ' ~ parent ~ ' IS NOT NULL') %}

            {%- for col in cols %}
                {%- set raw_lim = layer.nested[col] %}
                {%- set expr = base + [
                    '\\n    HAVING AVG(' ~ parent ~ '.' ~ col
                    ~ ') > ' ~ raw_lim,
                    '\\n)'] %}
                {%- do result.update({
                    '.'.join(depends_on + [parent, col]) ~ '__has_mean': {
                        'description':
                            'Mean of ' ~ '.'.join(depends_on + [parent, col])
                            ~ ' must be ≤ ' ~ raw_lim ~ '.',
                        'expression' : expr | join('')
                    }
                }) %}
            {%- endfor %}
        {%- endif %}
    {%- endfor %}

    {{ return(result) }}
{%- endmacro %}
