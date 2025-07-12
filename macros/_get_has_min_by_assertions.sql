{# ────────────────────────────────────────────────────────────────────────
  Helper family : __has_min_by__
  Rule          :  “MIN(value) ≥ limit”
    • Scalar column
        score:            10
        rating: {min: 3, by: [region, year]}
    • Array<struct>
        items:
          cost:   1                      # no group-by for arrays
──────────────────────────────────────────────────────────────────────── #}

{%- macro _has_min_scalar(col, lim, by_cols) -%}
    {%- if by_cols | length == 0 -%}
        {{ col }} >= {{ lim }}
    {%- else -%}
        MIN({{ col }}) OVER (PARTITION BY {{ by_cols | join(', ') }}) >= {{ lim }}
    {%- endif %}
{%- endmacro %}

{%- macro _walk_has_min(result, node, depends_on=[], parent=None) -%}
    {%- for key, val in node.items() %}

        {# skip meta/helper keys #}
        {%- if key.startswith('__') %}
            {%- continue %}
        {%- endif %}

        {# decide leaf vs branch #}
        {%- if val is mapping and ('min' in val or 'by' in val) %}
            {%- set limit_raw = val.get('min', val) %}
            {%- set by_cols   = val.get('by', []) %}
        {%- elif val is mapping %}
            {{ dbt_assertions._walk_has_min(result,
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

        {# ───────── top-level scalar field ──────────────────────────── #}
        {%- if parent is none %}
            {%- set pred = dbt_assertions._has_min_scalar(key, limit, by_cols) %}
            {%- do result.update({
                key ~ '__has_min_by': {
                    'description':
                      by_cols | length == 0
                        and key ~ ' must be ≥ ' ~ limit_raw ~ '.'
                        or  key ~ ' min per ' ~ by_cols | join(', ')
                            ~ ' must be ≥ ' ~ limit_raw ~ '.',
                    'expression' : pred
                }
            }) %}

        {# ───────── array / struct field (no group-by) ──────────────── #}
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
            {%- do lines.append('    HAVING MIN(' ~ parent ~ '.' ~ key
                               ~ ') < ' ~ limit) %}
            {%- do lines.append(')') %}

            {%- do result.update({
                (depends_on + [parent, key]) | join('.') ~ '__has_min_by': {
                    'description':
                        'Min of ' ~ (depends_on + [parent, key]) | join('.')
                        ~ ' must be ≥ ' ~ limit_raw ~ '.',
                    'expression' : lines | join('\n')
                }
            }) %}
        {%- endif %}
    {%- endfor %}
{%- endmacro %}

{%- macro _get_has_min_by_assertions(cfg) -%}
    {%- set result = {} %}
    {{ dbt_assertions._walk_has_min(result, cfg) }}
    {{ return(result) }}
{%- endmacro %}
