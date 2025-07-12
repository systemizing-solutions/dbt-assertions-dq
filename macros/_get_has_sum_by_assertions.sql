{# ───────────────────────────────────────────────────────────────────────
  Helper family : __has_sum_by__
  Rule          :  “SUM(value) ≤ limit”
──────────────────────────────────────────────────────────────────────── #}

{%- macro _has_sum_scalar(col, lim, by_cols) -%}
    {%- if by_cols | length == 0 -%}
        {{ col }} <= {{ lim }}
    {%- else -%}
        SUM({{ col }}) OVER (PARTITION BY {{ by_cols | join(', ') }}) <= {{ lim }}
    {%- endif %}
{%- endmacro %}

{%- macro _walk_has_sum(result, node, depends_on=[], parent=None) -%}
    {%- for key, val in node.items() %}

        {# skip helper / meta keys (#xx) -------------------------------- #}
        {%- if key.startswith('__') %}
            {%- continue %}
        {%- endif %}

        {# determine limit & group-by ----------------------------------- #}
        {%- if val is mapping and ('sum' in val or 'by' in val) %}
            {%- set limit_raw = val.get('sum', val) %}
            {%- set by_cols   = val.get('by', []) %}
        {%- elif val is mapping %}
            {{ dbt_assertions._walk_has_sum(result,
                                            val,
                                            depends_on + ([parent] if parent else []),
                                            key) }}
            {%- continue %}
        {%- else %}
            {%- set limit_raw = val %}
            {%- set by_cols   = [] %}
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

        {# ───────── scalar column ────────────────────────────────────── #}
        {%- if parent is none %}
            {%- set pred = dbt_assertions._has_sum_scalar(key, lim, by_cols) %}
            {%- do result.update({
                key ~ '__has_sum_by': {
                    'description':
                        by_cols | length == 0
                          and key ~ ' total must be ≤ ' ~ limit_raw ~ '.'
                          or  key ~ ' total per ' ~ by_cols | join(', ')
                              ~ ' must be ≤ ' ~ limit_raw ~ '.',
                    'expression': pred
                }
            }) %}

        {# ───────── array / struct field (no extra group-by) ─────────── #}
        {%- else %}
            {%- set query = [] %}
            {%- do query.append('NOT EXISTS (') %}
            {%- do query.append('    SELECT 1') %}
            {%- for ref in depends_on + [parent] %}
                {%- do query.append(
                    '    ' ~ ('FROM' if loop.first else 'CROSS JOIN')
                    ~ ' UNNEST(' ~ ref ~ ') ' ~ ref) %}
            {%- endfor %}
            {%- do query.append('    WHERE ' ~ parent ~ ' IS NOT NULL') %}
            {%- do query.append(
                    '    HAVING SUM(' ~ parent ~ '.' ~ key
                    ~ ') > ' ~ lim) %}
            {%- do query.append(')') %}

            {%- do result.update({
                (depends_on + [parent, key]) | join('.') ~ '__has_sum_by': {
                    'description':
                        'Sum of ' ~ (depends_on + [parent, key]) | join('.')
                        ~ ' must be ≤ ' ~ limit_raw ~ '.',
                    'expression': query | join('\n')
                }
            }) %}
        {%- endif %}
    {%- endfor %}
{%- endmacro %}

{%- macro _get_has_sum_by_assertions(cfg) -%}
    {%- set result = {} %}
    {{ dbt_assertions._walk_has_sum(result, cfg) }}
    {{ return(result) }}
{%- endmacro %}
