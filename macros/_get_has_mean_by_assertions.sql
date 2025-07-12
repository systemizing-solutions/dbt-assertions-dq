{# ───────────────────────────────────────────────────────────────────────
  Helper family : __has_mean_by__
  Rule          :  “average value ≤ limit”

  YAML examples
    __has_mean_by__:
      unit_price:               100
      revenue:    {mean: 50000, by: [region, year]}
      items:
        cost:     25            # array field (no extra grouping)
──────────────────────────────────────────────────────────────────────── #}

{%- macro _mean_pred_scalar(col, lim, by_cols) -%}
    {%- if by_cols | length == 0 -%}
        {{ col }} <= {{ lim }}
    {%- else -%}
        AVG({{ col }}) OVER (PARTITION BY {{ by_cols | join(', ') }}) <= {{ lim }}
    {%- endif %}
{%- endmacro %}

{%- macro _walk_mean(result, node, depends_on=[], parent=None) -%}
    {%- for key, val in node.items() %}

        {# skip meta/helper keys (#xx) --------------------------------- #}
        {%- if key.startswith('__') %}
            {%- continue %}
        {%- endif %}

        {# detect config form ------------------------------------------ #}
        {%- if val is mapping and ('mean' in val or 'by' in val) %}
            {%- set lim_raw = val['mean'] %}
            {%- set by_cols = val.get('by', []) %}
        {%- elif val is mapping %}
            {{ dbt_assertions._walk_mean(result,
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

        {# ───────── scalar column ───────────────────────────────────── #}
        {%- if parent is none %}
            {%- set pred = dbt_assertions._mean_pred_scalar(key, lim, by_cols) %}
            {%- do result.update({
                key ~ '__has_mean_by': {
                    'description':
                        by_cols | length == 0
                          and key ~ ' mean must be ≤ ' ~ lim_raw ~ '.'
                          or  key ~ ' mean per ' ~ by_cols | join(', ')
                              ~ ' must be ≤ ' ~ lim_raw ~ '.',
                    'expression': pred
                }
            }) %}

        {# ───────── array / struct field (no extra group-by) ────────── #}
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
            {%- do q.append('    HAVING AVG(' ~ parent ~ '.' ~ key
                           ~ ') > ' ~ lim) %}
            {%- do q.append(')') %}

            {%- do result.update({
                (depends_on + [parent, key]) | join('.') ~ '__has_mean_by': {
                    'description':
                        'Mean of ' ~ (depends_on + [parent, key]) | join('.')
                        ~ ' must be ≤ ' ~ lim_raw ~ '.',
                    'expression': q | join('\n')
                }
            }) %}
        {%- endif %}
    {%- endfor %}
{%- endmacro %}

{%- macro _get_has_mean_by_assertions(cfg) -%}
    {%- set result = {} %}
    {{ dbt_assertions._walk_mean(result, cfg) }}
    {{ return(result) }}
{%- endmacro %}
