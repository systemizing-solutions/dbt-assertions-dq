{# ─────────────────────────────────────────────────────────────────────────
#  TRUE when <field_sql> (DATE) is a Sunday.
#
#  Example:
#     {{ dbt_assertions._is_sunday_predicate('CAST(order_date AS DATE)') }}
# ───────────────────────────────────────────────────────────────────────── #}

{%- macro _is_sunday_predicate(field_sql) -%}
    {{ return(
        adapter.dispatch('_is_sunday_predicate', 'dbt_assertions')
               (field_sql)
    ) }}
{%- endmacro %}

{# ---------- DEFAULT  (DuckDB, Postgres/Redshift, Trino …) -------------- #}
{#  EXTRACT(DOW): 0 = Sunday                                              #}
{%- macro default___is_sunday_predicate(field_sql) -%}
    {{ return('EXTRACT(DOW FROM ' ~ field_sql ~ ') = 0') }}
{%- endmacro %}

{# ---------- BIGQUERY --------------------------------------------------- #}
{#  DAYOFWEEK(): 1 = Sunday                                               #}
{%- macro bigquery___is_sunday_predicate(field_sql) -%}
    {{ return('EXTRACT(DAYOFWEEK FROM ' ~ field_sql ~ ') = 1') }}
{%- endmacro %}

{# ---------- SNOWFLAKE / ORACLE ---------------------------------------- #}
{#  DAYOFWEEK(): 1 = Sunday                                               #}
{%- macro snowflake___is_sunday_predicate(field_sql) -%}
    {{ return('DAYOFWEEK(' ~ field_sql ~ ') = 1') }}
{%- endmacro %}

{# ---------- SPARK / DATABRICKS ---------------------------------------- #}
{#  date_format(...,'u'): 7 = Sunday                                      #}
{%- macro spark___is_sunday_predicate(field_sql) -%}
    {{ return("CAST(date_format(" ~ field_sql ~ ", 'u') AS INT) = 7") }}
{%- endmacro %}
