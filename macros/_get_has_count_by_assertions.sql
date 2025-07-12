{# ─────────────────────────────────────────────────────────────────────────
  Helper family :  __has_count_by__
  Rule          :  “COUNT(value) ≤ limit”

  YAML patterns supported
    amount:                 1_000                    # scalar, no grouping
    orders: {count: 10_000, by: [region, year]}        # scalar + group-by
    items:                                           # array<struct>
      sku: 100                                       # array field (no group-by)
──────────────────────────────────────────────────────────────────────── #}

{%- macro _count_pred_scalar(col, lim, by_cols) -%}
    {%- if by_cols | length == 0 -%}
        COUNT({{ col }}) OVER () <= {{ lim }}
    {%- else -%}
        COUNT({{ col }}) OVER (PARTITION BY {{ by_cols | join(', ') }}) <= {{ lim }}
    {%- endif %}
{%- endmacro %}

{%- macro _walk_count(result, node, depends_on=[], parent=None) -%}
    {%- for key, val in node.items() %}

        {# Ignore meta/helper keys beginning with “__” #}
        {%- if key.startswith('__') %}
            {%- continue %}
        {%- endif %}

        {# Detect config form ------------------------------------------------ #}
        {%- if val is mapping and ('count' in val or 'by' in val) %}
            {%- set lim_raw = val.get('count', val) %}
            {%- set by_cols = val.get('by', []) %}
        {%- elif val is mapping %}
            {{ dbt_assertions._walk_count(result,
                                          val,
                                          depends_on + ([parent] if parent else []),
                                          key) }}
            {%- continue %}
        {%- else %}
            {%- set lim_raw = val %}
            {%- set by_cols = [] %}
        {%- endif %}

        {% set lim = (
            limit_raw is number
                and limit_raw
            or (limit_raw | trim).startswith('(')
                and limit_raw
            or (limit_raw | trim)[:6] | lower == 'select'
                and limit_raw
            or "'" ~ limit_raw | replace("'", "''") ~ "'"
        ) %}

        {# ───── scalar column ─────────────────────────────────────────────── #}
        {%- if parent is none %}
            {%- set pred = dbt_assertions._count_pred_scalar(key, lim, by_cols) %}
            {%- do result.update({
                key ~ '__has_count_by': {
                    'description':
                      by_cols | length == 0
                        and 'Count of ' ~ key ~ ' must be ≤ ' ~ lim_raw ~ '.'
                        or  'Count of ' ~ key ~ ' per '
                            ~ by_cols | join(', ')
                            ~ ' must be ≤ ' ~ lim_raw ~ '.',
                    'expression' : pred
                }
            }) %}

        {# ───── array / struct field (no extra group-by) ──────────────────── #}
        {%- else %}
            {%- set q = [] %}
            {%- do q.append('NOT EXISTS (') %}
            {%- do q.append('    SELECT 1') %}
            {%- for ref in depends_on + [parent] %}
                {%- do q.append(
                    '    ' ~ ('FROM' if loop.first else 'CROSS JOIN')
                    ~ ' UNNEST(' ~ ref ~ ') ' ~ ref) %}
            {%- endfor %}
            {%- do q.append('    WHERE ' ~ parent ~ ' IS NOT NULL') %}
            {%- do q.append('    HAVING COUNT(' ~ parent ~ '.' ~ key
                           ~ ') > ' ~ lim) %}
            {%- do q.append(')') %}

            {%- do result.update({
                (depends_on + [parent, key]) | join('.') ~ '__has_count_by': {
                    'description':
                        'Count of ' ~ (depends_on + [parent, key]) | join('.')
                        ~ ' must be ≤ ' ~ lim_raw ~ '.',
                    'expression' : q | join('\n')
                }
            }) %}
        {%- endif %}
    {%- endfor %}
{%- endmacro %}

{%- macro _get_has_count_by_assertions(cfg) -%}
    {%- set result = {} %}
    {{ dbt_assertions._walk_count(result, cfg) }}
    {{ return(result) }}
{%- endmacro %}
