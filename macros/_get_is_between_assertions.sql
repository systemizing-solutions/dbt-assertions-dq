{%- macro _get_is_between_assertions(between_columns) -%}
    {{ return(
        adapter.dispatch('_get_is_between_assertions',
                         'dbt_assertions')(between_columns)
    ) }}
{%- endmacro %}

{# ─────────────────────────────────────────────────────────────────────── #}
{# Stand-alone recursive walker                                           #}
{%- macro _walk_is_between_assertions(result, node,
                                      depends_on=[], parent=None) %}
    {%- for key, value in node.items() %}

        {# ── branch : nested mapping ---------------------------------- #}
        {%- if value is mapping %}
            {{ dbt_assertions._walk_is_between_assertions(
                 result, value,
                 depends_on + ([parent] if parent else []),
                 key) }}

        {# ── leaf : two-item list [lower, upper] ---------------------- #}
        {%- elif value is sequence and value | length == 2 %}
            {%- set lower_raw = value[0] %}
            {%- set upper_raw = value[1] %}

            {# flip if bounds supplied in reverse order ---------------- #}
            {%- if upper_raw < lower_raw %}
                {%- set tmp = lower_raw %}
                {%- set lower_raw = upper_raw %}
                {%- set upper_raw = tmp %}
            {%- endif %}

            {# quote if necessary for SQL literal ---------------------- #}
            {% set lower = (
                lower_raw is number
                    and lower_raw
                or (lower_raw | trim).startswith('(')
                    and lower_raw
                or (lower_raw | trim)[:6] | lower == 'select'
                    and lower_raw
                or "'" ~ lower_raw | replace("'", "''") ~ "'"
            ) %}
            {% set upper = (
                upper_raw is number
                    and upper_raw
                or (upper_raw | trim).startswith('(')
                    and upper_raw
                or (upper_raw | trim)[:6] | lower == 'select'
                    and upper_raw
                or "'" ~ upper_raw | replace("'", "''") ~ "'"
            ) %}

            {# ---- top-level column ----------------------------------- #}
            {%- if parent is none %}
                {%- do result.update({
                    key ~ '__is_between': {
                        'description': key ~ ' must be between '
                                       ~ lower_raw ~ ' and ' ~ upper_raw ~
                                       ' (inclusive).',
                        'expression' : key ~ ' BETWEEN ' ~ lower
                                       ~ ' AND ' ~ upper
                    }
                }) %}

            {# ---- nested / repeated field ---------------------------- #}
            {%- else %}
                {%- set expr = ['NOT EXISTS (\\n    SELECT 1'] %}
                {%- for dep in depends_on + [parent] %}
                    {%- do expr.append(
                        '\\n    ' ~ ('FROM' if loop.first else 'CROSS JOIN')
                        ~ ' UNNEST(' ~ dep ~ ') ' ~ dep) %}
                {%- endfor %}
                {%- do expr.append('\\n    WHERE ' ~ parent ~ ' IS NOT NULL') %}
                {%- do expr.append(
                    '\\n      AND (' ~ parent ~ '.' ~ key
                    ~ ' < ' ~ lower ~ ' OR ' ~ parent ~ '.' ~ key
                    ~ ' > ' ~ upper ~ ')\\n)') %}

                {%- do result.update({
                    '.'.join(depends_on + [parent, key]) ~ '__is_between': {
                        'description':
                            '.'.join(depends_on + [parent, key])
                            ~ ' must be between ' ~ lower_raw
                            ~ ' and ' ~ upper_raw ~ ' (inclusive).',
                        'expression' : expr | join('')
                    }
                }) %}
            {%- endif %}
        {%- endif %}
    {%- endfor %}
{%- endmacro %}

{# ─────────────────────────────────────────────────────────────────────── #}
{%- macro default___get_is_between_assertions(between_columns) %}
    {%- set result = {} %}
    {%- do dbt_assertions._walk_is_between_assertions(result, between_columns) %}
    {{ return(result) }}
{%- endmacro %}
