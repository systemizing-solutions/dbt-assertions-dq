{# macros/_get_plugin_assertions.sql #}
{% macro _get_plugin_assertions(assertions_dict) %}
    {%- set plugin_rules = {} %}
    {%- set built_ins    = ['__unique__', '__not_null__'] %}
    {%- set adapter      = target.adapter or '' %}

    {%- for helper_key, cols in assertions_dict.items() | list %}
        {%- if helper_key.startswith('__')
              and helper_key.endswith('__')
              and helper_key not in built_ins %}

            {# "__is_positive__" -> "is_positive" -> "dbt_assertions__is_positive" #}
            {%- set rule          = helper_key[2:-2] %}
            {%- set generic_name  = 'dbt_assertions__' ~ rule %}
            {%- set adapter_name  = adapter ~ '__' ~ generic_name if adapter else None %}

            {# Find first macro whose key ends with adapter_name or generic_name #}
            {%- set plugin_macro = None %}
            {%- for k, v in context.items() if v is mapping or v is callable %}
                {%- if plugin_macro is none %}
                    {%- if adapter_name and k.endswith(adapter_name) %}
                        {%- set plugin_macro = v %}
                    {%- elif k.endswith(generic_name) %}
                        {%- set plugin_macro = v %}
                    {%- endif %}
                {%- endif %}
            {%- endfor %}

            {%- if plugin_macro is none %}
                {{ exceptions.raise_compiler_error(
                    'Helper key ' ~ helper_key ~
                    ' could not find a macro ending with "' ~
                    (adapter_name or generic_name) ~ '"'
                ) }}
            {%- endif %}

            {%- do assertions_dict.pop(helper_key) %}
            {%- do plugin_rules.update(plugin_macro(cols)) %}
        {%- endif %}
    {%- endfor %}

    {{ return(plugin_rules) }}
{% endmacro %}
