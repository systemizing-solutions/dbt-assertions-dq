{# =======================================================================
#  Return a SQL predicate that evaluates to TRUE when the supplied
#  <field_sql> (DATE) falls on Monday-Friday.
#
#  Usage inside other macros:
#      {{ dbt_assertions._is_weekday_predicate('CAST(order_date AS DATE)') }}
# ======================================================================= #}

{%- macro _is_weekday_predicate(field_sql) -%}
    {{ return(
        adapter.dispatch('_is_weekday_predicate', 'dbt_assertions')
               (field_sql)
    ) }}
{%- endmacro %}

{# ---------- DEFAULT  (DuckDB, Postgres ≥10, Trino, etc.) -------------- #}
{%- macro default___is_weekday_predicate(field_sql) -%}
    {{ return('EXTRACT(DOW FROM ' ~ field_sql ~ ') BETWEEN 1 AND 5') }}
{%- endmacro %}

{# ---------- BIGQUERY --------------------------------------------------- #}
{%- macro bigquery___is_weekday_predicate(field_sql) -%}
    {{ return('EXTRACT(DAYOFWEEK FROM ' ~ field_sql ~ ') BETWEEN 2 AND 6') }}
{%- endmacro %}

{# ---------- SNOWFLAKE / REDSHIFT / ORACLE ----------------------------- #}
{%- macro snowflake___is_weekday_predicate(field_sql) -%}
    {{ return('DAYOFWEEK(' ~ field_sql ~ ') BETWEEN 2 AND 6') }}
{%- endmacro %}
{%- macro redshift___is_weekday_predicate(field_sql) -%}
    {{ return('EXTRACT(DOW FROM ' ~ field_sql ~ ') BETWEEN 1 AND 5') }}
{%- endmacro %}

{# ---------- SPARK / DATABRICKS ---------------------------------------- #}
{%- macro spark___is_weekday_predicate(field_sql) -%}
    {{ return("CAST(date_format(" ~ field_sql ~ ", 'u') AS INT) BETWEEN 1 AND 5") }}
{%- endmacro %}
