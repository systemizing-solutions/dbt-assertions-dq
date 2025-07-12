{# ────────────────────────────────────────────────────────────────────────
  Helper family : __has_max_by__
  • Scalar column
      price:            500
      revenue: {max: 1_000, by: [region, year]}
  • Array<struct> column
      items:
        cost:  50
        weight: 100          -- NO "by" here; grouping not supported on arrays
──────────────────────────────────────────────────────────────────────── #}

{%- macro _has_max_scalar(col, lim, by_cols) -%}
    {%- if by_cols | length == 0 -%}
        {{ col }} <= {{ lim }}
    {%- else -%}
        MAX({{ col }}) OVER (PARTITION BY {{ by_cols | join(', ') }}) <= {{ lim }}
    {%- endif %}
{%- endmacro %}

{%- macro _walk_has_max(result, node, depends_on=[], parent=None) -%}
    {%- for key, val in node.items() %}

        {# Skip helper-name keys accidentally passed in #}
        {%- if key.startswith('__') %}
            {%- continue %}
        {%- endif %}

        {# Work out “limit” and optional group-by #}
        {%- if val is mapping and ('max' in val or 'by' in val) %}
            {%- set limit_raw = val['max'] %}
            {%- set by_cols   = val.get('by', []) %}
        {%- elif val is mapping %}
            {{ dbt_assertions._walk_has_max(result,
                                            val,
                                            depends_on + ([parent] if parent else []),
                                            key) }}
            {%- continue %}
        {%- else %}
            {%- set limit_raw = val %}
            {%- set by_cols   = [] %}
        {%- endif %}
        

        {% set limit = (
            limit_raw is number
                and limit_raw
            or (limit_raw | trim).startswith('(')
                and limit_raw
            or (limit_raw | trim)[:6] | lower == 'select'
                and limit_raw
            or "'" ~ limit_raw | replace("'", "''") ~ "'"
        ) %}

        {# ───── top-level scalar field ──────────────────────────────── #}
        {%- if parent is none %}
            {%- set pred = dbt_assertions._has_max_scalar(key, limit, by_cols) %}
            {%- do result.update({
                key ~ '__has_max_by': {
                    'description':
                      by_cols | length == 0
                        and key ~ ' must be ≤ ' ~ limit_raw ~ '.'
                        or  key ~ ' max per ' ~ by_cols | join(', ')
                            ~ ' must be ≤ ' ~ limit_raw ~ '.',
                    'expression': pred
                }
            }) %}

        {# ───── array / struct field (NO group-by support) ──────────── #}
        {%- else %}
            {%- set lines = [] %}
            {%- do lines.append('NOT EXISTS (') %}
            {%- do lines.append('    SELECT 1') %}
            {%- for ref in depends_on + [parent] %}
                {%- do lines.append(
                    '    ' ~ ('FROM' if loop.first else 'CROSS JOIN')
                    ~ ' UNNEST(' ~ ref ~ ') ' ~ ref) %}
            {%- endfor %}
            {%- do lines.append('    WHERE ' ~ parent ~ ' IS NOT NULL') %}
            {%- do lines.append('    HAVING MAX(' ~ parent ~ '.' ~ key
                               ~ ') > ' ~ limit) %}
            {%- do lines.append(')') %}

            {%- do result.update({
                (depends_on + [parent, key]) | join('.') ~ '__has_max_by': {
                    'description':
                        'Max of ' ~ (depends_on + [parent, key]) | join('.')
                        ~ ' must be ≤ ' ~ limit_raw ~ '.',
                    'expression': lines | join('\n')
                }
            }) %}
        {%- endif %}
    {%- endfor %}
{%- endmacro %}

{%- macro _get_has_max_by_assertions(cfg) -%}
    {%- set result = {} %}
    {{ dbt_assertions._walk_has_max(result, cfg) }}
    {{ return(result) }}
{%- endmacro %}
