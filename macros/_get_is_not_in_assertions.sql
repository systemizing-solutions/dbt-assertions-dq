{%- macro _get_is_not_in_assertions(not_in_columns) -%}
    {{ return(
        adapter.dispatch('_get_is_not_in_assertions',
                         'dbt_assertions')(not_in_columns)
    ) }}
{%- endmacro %}

{# ─────────────────────────────────────────────────────────────────────── #}
{# Stand-alone recursive walker                                           #}
{%- macro _walk_is_not_in_assertions(result, node, depends_on=[], parent=None) %}
    {%- for key, value in node.items() %}

        {# ── branch : nested mapping ---------------------------------- #}
        {%- if value is mapping %}
            {{ dbt_assertions._walk_is_not_in_assertions(
                 result, value,
                 depends_on + ([parent] if parent else []),
                 key) }}

        {# ── leaf : sequence of banned values ------------------------- #}
        {%- elif value is sequence and (value | length > 0)
              and (value[0] is not mapping) %}
            {# build the (quoted) value list --------------------------- #}
            {%- set quoted = [] %}
            {%- for v in value %}
                {%- do quoted.append(v if v is number else
                                     "'" ~ v|replace("'", "''") ~ "'") %}
            {%- endfor %}
            {%- set list_sql = quoted | join(', ') %}

            {# ---- top-level column ---------------------------------- #}
            {%- if parent is none %}
                {%- do result.update({
                    key ~ '__is_not_in': {
                        'description': key ~ ' must NOT be in (' ~ list_sql ~ ').',
                        'expression' : key ~ ' NOT IN (' ~ list_sql ~ ')'
                    }
                }) %}

            {# ---- repeated / nested field --------------------------- #}
            {%- else %}
                {%- set expr = ['NOT EXISTS (\\n    SELECT 1'] %}
                {%- for dep in depends_on + [parent] %}
                    {%- do expr.append(
                        '\\n    ' ~ ('FROM' if loop.first else 'CROSS JOIN')
                        ~ ' UNNEST(' ~ dep ~ ') ' ~ dep) %}
                {%- endfor %}
                {%- do expr.append('\\n    WHERE ' ~ parent ~ ' IS NOT NULL') %}
                {%- do expr.append(
                    '\\n      AND ' ~ parent ~ '.' ~ key
                    ~ ' IN (' ~ list_sql ~ ')\\n)') %}

                {%- do result.update({
                    '.'.join(depends_on + [parent, key]) ~ '__is_not_in': {
                        'description':
                            '.'.join(depends_on + [parent, key])
                            ~ ' must NOT be in (' ~ list_sql ~ ').',
                        'expression' : expr | join('')
                    }
                }) %}
            {%- endif %}
        {%- endif %}
    {%- endfor %}
{%- endmacro %}

{# ─────────────────────────────────────────────────────────────────────── #}
{%- macro default___get_is_not_in_assertions(not_in_columns) %}
    {%- set result = {} %}
    {%- do dbt_assertions._walk_is_not_in_assertions(result, not_in_columns) %}
    {{ return(result) }}
{%- endmacro %}
