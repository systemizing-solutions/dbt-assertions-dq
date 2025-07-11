{%- macro _get_is_greater_than_assertions(gt_columns) -%}
    {{ return(adapter.dispatch('_get_is_greater_than_assertions',
                               'dbt_assertions')(gt_columns)) }}
{%- endmacro %}

{# -------------------------------------------------------------------- #}
{# Stand-alone recursive walker (MUST be top-level)                     #}
{%- macro _walk_is_gt_assertions(result, node, depends_on=[], parent=None) %}
    {%- for key, value in node.items() %}

        {# ---- branch : nested mapping -------------------------------- #}
        {%- if value is mapping %}
            {{ dbt_assertions._walk_is_gt_assertions(
                 result, value,
                 depends_on + ([parent] if parent else []),
                 key) }}

        {# ---- leaf : scalar threshold -------------------------------- #}
        {%- else %}
            {%- set threshold = value %}

            {# ---- top-level (no nesting) ----------------------------- #}
            {%- if parent is none %}
                {%- do result.update({
                    key ~ '__is_greater_than': {
                        'description': key ~ ' must be > ' ~ threshold ~ '.',
                        'expression' : key ~ ' > ' ~ threshold
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
                    '\\n      AND ' ~ parent ~ '.' ~ key
                    ~ ' <= ' ~ threshold ~ '\\n)') %}

                {%- do result.update({
                    '.'.join(depends_on + [parent, key]) ~ '__is_greater_than': {
                        'description': '.'.join(depends_on + [parent, key])
                                       ~ ' must be > ' ~ threshold ~ '.',
                        'expression' : expr | join('')
                    }
                }) %}
            {%- endif %}
        {%- endif %}
    {%- endfor %}
{%- endmacro %}

{# -------------------------------------------------------------------- #}
{%- macro default___get_is_greater_than_assertions(gt_columns) %}
    {%- set result = {} %}
    {%- do dbt_assertions._walk_is_gt_assertions(result, gt_columns) %}
    {{ return(result) }}
{%- endmacro %}
