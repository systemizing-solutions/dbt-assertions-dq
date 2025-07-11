{%- macro _get_is_in_assertions(is_in_columns) -%}
    {{ return(adapter.dispatch('_get_is_in_assertions',
                               'dbt_assertions')(is_in_columns)) }}
{%- endmacro %}

{# --------------------------------------------------------------------- #}
{# Stand-alone recursive walker – MUST live at top-level, not inside      #}
{# another macro                                                         #}
{%- macro _walk_is_in_assertions(result_dict, node, depends_on=[], parent=None) %}

    {%- for key, value in node.items() %}

        {# ── branch : drill deeper ───────────────────────────────────── #}
        {%- if value is mapping %}
            {{ dbt_assertions._walk_is_in_assertions(
                 result_dict,
                 value,
                 depends_on + ([parent] if parent else []),
                 key) }}

        {# ── leaf : we have the allowed list ─────────────────────────── #}
        {%- elif value is sequence and (value | length > 0)
              and (value[0] is not mapping) %}

            {# format the IN (…) list #}
            {%- set formatted = [] %}
            {%- for v in value %}
                {%- do formatted.append(v if v is number else
                                        "'" ~ v|replace("'", "''") ~ "'") %}
            {%- endfor %}
            {%- set in_list = formatted | join(', ') %}

            {%- if parent is none %}          {# top-level column #}
                {%- do result_dict.update({
                    key ~ '__is_in': {
                        'description': key ~ ' must be in (' ~ in_list ~ ').',
                        'expression' : key ~ ' IN (' ~ in_list ~ ')'
                    }
                }) %}

            {%- else %}                      {# repeated / nested field #}
                {%- set expr = ['NOT EXISTS (\\n    SELECT 1'] %}
                {%- for dep in depends_on + [parent] %}
                    {%- do expr.append('\\n    ' ~ ('FROM' if loop.first
                                   else 'CROSS JOIN')) %}
                    {%- do expr.append(' UNNEST(' ~ dep ~ ') ' ~ dep) %}
                {%- endfor %}
                {%- do expr.append('\\n    WHERE ' ~ parent ~ ' IS NOT NULL') %}
                {%- do expr.append('\\n      AND ' ~ parent ~ '.' ~ key
                                   ~ ' NOT IN (' ~ in_list ~ ')\\n)') %}

                {%- do result_dict.update({
                    '.'.join(depends_on + [parent, key]) ~ '__is_in': {
                        'description': '.'.join(depends_on + [parent, key])
                                       ~ ' values must be in (' ~ in_list ~ ').',
                        'expression' : expr | join('')
                    }
                }) %}
            {%- endif %}

        {%- endif %}
    {%- endfor %}
{%- endmacro %}

{# --------------------------------------------------------------------- #}
{%- macro default___get_is_in_assertions(is_in_columns) %}
    {%- set result = {} %}
    {%- do dbt_assertions._walk_is_in_assertions(result, is_in_columns) %}
    {{ return(result) }}
{%- endmacro %}
