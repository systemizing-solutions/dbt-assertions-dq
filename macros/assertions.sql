{# macros/assertions.sql #}
{%- macro assertions(column=var('dbt_assertions:default_column', 'exceptions'), _node=none) %}
{#-
    Generates row-level assertions based on the schema model YAML.

    This macro parses the schema model YAML to extract row-level assertions,
    including unique and not-null constraints. It then constructs an array of
    failed assertions (exceptions) for each row based on its assertions results.

    Args:
        column (optional[str]): Column to read the exceptions from.
        _node (dict): any other node to read the columns from.
            This argument is reserved to `dbt-assertions`'s developers.

    Returns:
        str: An ARRAY<STRING> SELECT expression exceptions from assertions.

    Example Usage:
        Suppose the schema model YAML is set as below,
        you can call the function in you model the following:

        ```
        WITH
            final AS (
                SELECT
                    my_key,
                    my_other_key,
                    nested_structure,
                FROM {{ source('my_source') }}
            )
        SELECT
            *,
            {{ dbt_assertions.assertions() | indent(4)}},
        FROM final
        ```

        Suppose the schema model YAML includes the following assertions:

        ```
            - name: exceptions
              description: This column contains exceptions of the row based on assertions.
              assertions:
                __unique__:
                  - my_key
                  - my_other_key
                  - nested_structure:
                    - sub_key

                __not_null__: __unique__

                other_field_greater_than_1:
                  description: The row must follow this condition.
                  expression: my_other_field > 1

                site_key_exists:
                  description: The site key exists.
                  expression: |
                    site_key IN (
                        SELECT DISTINCT
                            site_key
                        FROM {{ '{{' ref('my_sites_model') '}}' }}
                    )
                  null_as_exception: true
        ```

        The macro will generate an expression to check these assertions and return
        an array of assertion IDs for each row where the assertions are violated.

#}
    {{- adapter.dispatch('assertions', 'dbt_assertions') (column, _node) }}
{%- endmacro %}


{%- macro default__assertions(column, _node) %}

{#- Parses the assertions if exists. #}
{%- set model = model if _node is none else _node %}
{%- set columns = model.columns if ('columns' in model) else {} %}
{%- set assertions_column = columns[column] if (column in columns) else {} %}
{%- set assertions = assertions_column.assertions if ('assertions' in assertions_column) else {} %}

{#- Generate assertions from helpers. #}
{%- set __unique__ = {} %}
{%- set __not_null__ = {} %}

{#  NEW — Start additional rules #}

{% set __is_positive__ = {} %}
{% set __is_negative__ = {} %}

{% set __is_greater_than__ = {} %}
{% set __is_greater_or_equal_to__ = {} %}
{% set __is_less_than__ = {} %}
{% set __is_less_or_equal_to__ = {} %}

{% set __is_in__ = {} %}
{% set __is_not_in__ = {} %}
{% set __is_between__ = {} %}

{% set __has_regex_pattern__ = {} %}

{% set __has_max__ = {} %}
{% set __has_min__ = {} %}
{% set __has_sum__ = {} %}
{% set __has_count__ = {} %}
{% set __has_mean__ = {} %}
{% set __has_max_by__ = {} %}
{% set __has_min_by__ = {} %}
{% set __has_sum_by__ = {} %}
{% set __has_count_by__ = {} %}
{% set __has_mean_by__ = {} %}

{% set __is_today__ = {} %}
{% set __is_yesterday__ = {} %}
{% set __is_on_weekday__ = {} %}
{% set __is_on_weekend__ = {} %}
{% set __is_on_monday__ = {} %}
{% set __is_on_tuesday__ = {} %}
{% set __is_on_wednesday__ = {} %}
{% set __is_on_thursday__ = {} %}
{% set __is_on_friday__ = {} %}
{% set __is_on_saturday__ = {} %}
{% set __is_on_sunday__ = {} %}

{#  NEW — End additional rules #}

{%- if '__unique__' in assertions %}
    {%- set __unique__ = assertions.pop('__unique__', {}) %}
{%- endif %}

{%- if '__not_null__' in assertions %}
    {%- set __not_null__ = assertions.pop('__not_null__', {}) %}
    {%- set __not_null__ = __not_null__ if ('__unique__' not in __not_null__) else __unique__ %}
{%- endif %}

{#  NEW — Start additional rules #}

{% if '__is_positive__' in assertions %}
    {% set __is_positive__ = assertions.pop('__is_positive__') %}
{% endif %}

{% if '__is_negative__' in assertions %}
    {% set __is_negative__ = assertions.pop('__is_negative__') %}
{% endif %}


{% if '__is_greater_than__' in assertions %}
    {% set __is_greater_than__ = assertions.pop('__is_greater_than__') %}
{% endif %}

{% if '__is_greater_or_equal_to__' in assertions %}
    {% set __is_greater_or_equal_to__ = assertions.pop('__is_greater_or_equal_to__') %}
{% endif %}

{% if '__is_less_than__' in assertions %}
    {% set __is_less_than__ = assertions.pop('__is_less_than__') %}
{% endif %}

{% if '__is_less_or_equal_to__' in assertions %}
    {% set __is_less_or_equal_to__ = assertions.pop('__is_less_or_equal_to__') %}
{% endif %}


{% if '__is_in__' in assertions %}
    {% set __is_in__ = assertions.pop('__is_in__') %}
{% endif %}

{% if '__is_not_in__' in assertions %}
    {% set __is_not_in__ = assertions.pop('__is_not_in__') %}
{% endif %}

{% if '__is_between__' in assertions %}
    {% set __is_between__ = assertions.pop('__is_between__') %}
{% endif %}


{% if '__has_regex_pattern__' in assertions %}
    {% set __has_regex_pattern__ = assertions.pop('__has_regex_pattern__') %}
{% endif %}


{% if '__has_max__' in assertions %}
    {% set __has_max__ = assertions.pop('__has_max__') %}
{% endif %}

{% if '__has_min__' in assertions %}
    {% set __has_min__ = assertions.pop('__has_min__') %}
{% endif %}

{% if '__has_sum__' in assertions %}
    {% set __has_sum__ = assertions.pop('__has_sum__') %}
{% endif %}

{% if '__has_count__' in assertions %}
    {% set __has_count__ = assertions.pop('__has_count__') %}
{% endif %}

{% if '__has_mean__' in assertions %}
    {% set __has_mean__ = assertions.pop('__has_mean__') %}
{% endif %}

{% if '__has_max_by__' in assertions %}
    {% set __has_max_by__ = assertions.pop('__has_max_by__') %}
{% endif %}

{% if '__has_min_by__' in assertions %}
    {% set __has_min_by__ = assertions.pop('__has_min_by__') %}
{% endif %}

{% if '__has_sum_by__' in assertions %}
    {% set __has_sum_by__ = assertions.pop('__has_sum_by__') %}
{% endif %}

{% if '__has_count_by__' in assertions %}
    {% set __has_count_by__ = assertions.pop('__has_count_by__') %}
{% endif %}

{% if '__has_mean_by__' in assertions %}
    {% set __has_mean_by__ = assertions.pop('__has_mean_by__') %}
{% endif %}


{% if '__is_today__' in assertions %}
    {% set __is_today__ = assertions.pop('__is_today__') %}
{% endif %}

{% if '__is_yesterday__' in assertions %}
    {% set __is_yesterday__ = assertions.pop('__is_yesterday__') %}
{% endif %}

{% if '__is_on_weekday__' in assertions %}
    {% set __is_on_weekday__ = assertions.pop('__is_on_weekday__') %}
{% endif %}

{% if '__is_on_weekend__' in assertions %}
    {% set __is_on_weekend__ = assertions.pop('__is_on_weekend__') %}
{% endif %}

{% if '__is_on_monday__' in assertions %}
    {% set __is_on_monday__ = assertions.pop('__is_on_monday__') %}
{% endif %}

{% if '__is_on_tuesday__' in assertions %}
    {% set __is_on_tuesday__ = assertions.pop('__is_on_tuesday__') %}
{% endif %}

{% if '__is_on_wednesday__' in assertions %}
    {% set __is_on_wednesday__ = assertions.pop('__is_on_wednesday__') %}
{% endif %}

{% if '__is_on_thursday__' in assertions %}
    {% set __is_on_thursday__ = assertions.pop('__is_on_thursday__') %}
{% endif %}

{% if '__is_on_friday__' in assertions %}
    {% set __is_on_friday__ = assertions.pop('__is_on_friday__') %}
{% endif %}

{% if '__is_on_saturday__' in assertions %}
    {% set __is_on_saturday__ = assertions.pop('__is_on_saturday__') %}
{% endif %}

{% if '__is_on_sunday__' in assertions %}
    {% set __is_on_sunday__ = assertions.pop('__is_on_sunday__') %}
{% endif %}

{#  NEW — End additional rules #}

{%- do assertions.update(dbt_assertions._get_unique_assertions(__unique__)) %}
{%- do assertions.update(dbt_assertions._get_not_null_assertions(__not_null__)) %}

{#  NEW — Start additional rules #}

{% do assertions.update(dbt_assertions._get_is_positive_assertions(__positive__)) %}
{% do assertions.update(dbt_assertions._get_is_negative_assertions(__is_negative__)) %}

{% do assertions.update(dbt_assertions._get_is_greater_than_assertions(__is_greater_than__)) %}
{% do assertions.update(dbt_assertions._get_is_greater_or_equal_to_assertions(__is_greater_or_equal_to__)) %}
{% do assertions.update(dbt_assertions._get_is_less_than_assertions(__is_less_than__)) %}
{% do assertions.update(dbt_assertions._get_is_less_or_equal_to_assertions(__is_less_or_equal_to__)) %}

{% do assertions.update(dbt_assertions._get_is_in_assertions(__is_in__)) %}
{% do assertions.update(dbt_assertions._get_is_not_in_assertions(__is_not_in__)) %}
{% do assertions.update(dbt_assertions._get_is_between_assertions(__is_between__)) %}

{% do assertions.update(dbt_assertions._get_has_regex_pattern_assertions(__has_regex_pattern__)) %}

{% do assertions.update(dbt_assertions._get_has_max_assertions(__has_max__)) %}
{% do assertions.update(dbt_assertions._get_has_min_assertions(__has_min__)) %}
{% do assertions.update(dbt_assertions._get_has_sum_assertions(__has_sum__)) %}
{% do assertions.update(dbt_assertions._get_has_count_assertions(__has_count__)) %}
{% do assertions.update(dbt_assertions._get_has_mean_assertions(__has_mean__)) %}
{% do assertions.update(dbt_assertions._get_has_max_by_assertions(__has_max_by__)) %}
{% do assertions.update(dbt_assertions._get_has_min_by_assertions(__has_min_by__)) %}
{% do assertions.update(dbt_assertions._get_has_sum_by_assertions(__has_sum_by__)) %}
{% do assertions.update(dbt_assertions._get_has_count_by_assertions(__has_count_by__)) %}
{% do assertions.update(dbt_assertions._get_has_mean_by_assertions(__has_mean_by__)) %}

{% do assertions.update(dbt_assertions._get_is_today_assertions(__is_today__)) %}
{% do assertions.update(dbt_assertions._get_is_yesterday_assertions(__is_yesterday__)) %}
{% do assertions.update(dbt_assertions._get_is_on_weekday_assertions(__is_on_weekday__)) %}
{% do assertions.update(dbt_assertions._get_is_on_weekend_assertions(__is_on_weekend__)) %}
{% do assertions.update(dbt_assertions._get_is_on_monday_assertions(__is_on_monday__)) %}
{% do assertions.update(dbt_assertions._get_is_on_tuesday_assertions(__is_on_tuesday__)) %}
{% do assertions.update(dbt_assertions._get_is_on_wednesday_assertions(__is_on_wednesday__)) %}
{% do assertions.update(dbt_assertions._get_is_on_thursday_assertions(__is_on_thursday__)) %}
{% do assertions.update(dbt_assertions._get_is_on_friday_assertions(__is_on_friday__)) %}
{% do assertions.update(dbt_assertions._get_is_on_saturday_assertions(__is_on_saturday__)) %}
{% do assertions.update(dbt_assertions._get_is_on_sunday_assertions(__is_on_sunday__)) %}

{#  NEW — End additional rules #}


{#  NEW — merge dynamically-discovered plugin rules #}
{%- do assertions.update(dbt_assertions._get_plugin_assertions(assertions)) %}

{{- dbt_assertions._assertions_expression(column, assertions) }}

{%- endmacro %}
