{# macros/_get_plugin_assertions.sql #}
{% macro _get_plugin_assertions(assertions_dict) %}
    {%- set plugin_rules = {} -%}
    {%- set built_ins = ['__unique__', '__not_null__'] -%}

    {# Copy keys first so we can mutate the dict safely #}
    {%- set helper_keys = assertions_dict.keys() | list -%}

    {%- for helper_key in helper_keys %}
        {%- if helper_key.startswith('__') and helper_key.endswith('__')
             and helper_key not in built_ins %}

            {# "__positive__" → "positive" → "dbt_assertions__positive" #}
            {%- set rule_name   = helper_key[2:-2] %}
            {%- set macro_name  = 'dbt_assertions__' ~ rule_name %}
            {%- set plugin_macro = context.get(macro_name) %}

            {%- if plugin_macro is none %}
                {{ exceptions.raise_compiler_error(
                    'Helper key ' ~ helper_key ~
                    ' needs a macro called ' ~ macro_name ~
                    ' (not found in project or packages).'
                ) }}
            {%- else %}
                {%- set cols = assertions_dict[helper_key] %}
                {%- set new_rules = plugin_macro(cols) %}
                {%- do plugin_rules.update(new_rules) %}
            {%- endif %}

            {# Remove the helper key so _assertions_expression never sees it #}
            {%- do assertions_dict.pop(helper_key) %}
        {%- endif %}
    {%- endfor %}

    {{ return(plugin_rules) }}
{% endmacro %}
