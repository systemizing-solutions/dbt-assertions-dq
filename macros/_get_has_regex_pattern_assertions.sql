{# ------------------------------------------------------------------------
#  Helper family: __has_regex_pattern__
#  Generates assertions that require every value to match a supplied
#  regular-expression pattern (inclusive). Adapter-specific regex functions
#  are handled via dbt_assertions._regex_predicate().
# ------------------------------------------------------------------------ #}

{%- macro _get_has_regex_pattern_assertions(re_columns) -%}
    {{ return(
        adapter.dispatch('_get_has_regex_pattern_assertions',
                         'dbt_assertions')(re_columns)
    ) }}
{%- endmacro %}

{# ─────────────────────────────────────────────────────────────────────── #}
{# Recursive walker (must be top-level to avoid “nested macro” errors).   #}
{%- macro _walk_has_regex_pattern_assertions(result, node,
                                             depends_on=[], parent=None) -%}
    {%- for key, value in node.items() %}

        {# ── branch : nested mapping ---------------------------------- #}
        {%- if value is mapping -%}
            {{ dbt_assertions._walk_has_regex_pattern_assertions(
                 result, value,
                 depends_on + ([parent] if parent else []),
                 key) }}

        {# ── leaf : pattern string ------------------------------------ #}
        {%- else -%}
            {%- set pattern_raw     = value|string %}
            {%- set pattern_literal = "'" ~ pattern_raw.replace("'", "''") ~ "'" %}

            {# ---- top-level column ----------------------------------- #}
            {%- if parent is none -%}
                {%- do result.update({
                    key ~ '__has_regex_pattern': {
                        'description':
                            key ~ ' must match regex `' ~ pattern_raw ~ '`.',
                        'expression' :
                            dbt_assertions._regex_predicate(key, pattern_literal)
                    }
                }) %}

            {# ---- nested / repeated field ---------------------------- #}
            {%- else -%}
                {%- set expr = ['NOT EXISTS (\\n    SELECT 1'] %}
                {%- for dep in depends_on + [parent] %}
                    {%- do expr.append(
                        '\\n    ' ~ ('FROM' if loop.first else 'CROSS JOIN')
                        ~ ' UNNEST(' ~ dep ~ ') ' ~ dep) %}
                {%- endfor %}
                {%- do expr.append('\\n    WHERE ' ~ parent ~ ' IS NOT NULL') %}
                {%- do expr.append(
                    '\\n      AND NOT '
                    ~ dbt_assertions._regex_predicate(parent ~ '.' ~ key,
                                                      pattern_literal)
                    ~ '\\n)') %}

                {%- do result.update({
                    '.'.join(depends_on + [parent, key]) ~ '__has_regex_pattern': {
                        'description':
                            '.'.join(depends_on + [parent, key])
                            ~ ' must match regex `' ~ pattern_raw ~ '`.',
                        'expression' : expr | join('')
                    }
                }) %}
            {%- endif %}
        {%- endif %}
    {%- endfor %}
{%- endmacro %}

{# ─────────────────────────────────────────────────────────────────────── #}
{%- macro default___get_has_regex_pattern_assertions(re_columns) -%}
    {%- set result = {} %}
    {%- do dbt_assertions._walk_has_regex_pattern_assertions(result, re_columns) %}
    {{ return(result) }}
{%- endmacro %}
