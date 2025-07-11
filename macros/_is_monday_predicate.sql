{# ======================================================================
#  Return a SQL predicate that is TRUE when <field_sql> (DATE) is Monday.
#
#  Example call inside another macro:
#      {{ dbt_assertions._is_monday_predicate('CAST(order_date AS DATE)') }}
# ====================================================================== #}

{%- macro _is_monday_predicate(field_sql) -%}
    {{ return(
        adapter.dispatch('_is_monday_predicate', 'dbt_assertions')
               (field_sql)
    ) }}
{%- endmacro %}

{# ---------- DEFAULT (DuckDB, Postgres ≥10, Trino, Redshift …) --------- #}
{#  EXTRACT(DOW): 0=Sun, 1=Mon, … 6=Sat                                   #}
{%- macro default___is_monday_predicate(field_sql) -%}
    {{ return('EXTRACT(DOW FROM ' ~ field_sql ~ ') = 1') }}
{%- endmacro %}

{# ---------- BIGQUERY -------------------------------------------------- #}
{#  DAYOFWEEK(): 1=Sun, 2=Mon, … 7=Sat                                   #}
{%- macro bigquery___is_monday_predicate(field_sql) -%}
    {{ return('EXTRACT(DAYOFWEEK FROM ' ~ field_sql ~ ') = 2') }}
{%- endmacro %}

{# ---------- SNOWFLAKE / ORACLE --------------------------------------- #}
{#  DAYOFWEEK(): 1=Sun, 2=Mon, … 7=Sat                                   #}
{%- macro snowflake___is_monday_predicate(field_sql) -%}
    {{ return('DAYOFWEEK(' ~ field_sql ~ ') = 2') }}
{%- endmacro %}

{# ---------- SPARK / DATABRICKS --------------------------------------- #}
{#  date_format(...,'u'): 1=Mon, … 7=Sun                                 #}
{%- macro spark___is_monday_predicate(field_sql) -%}
    {{ return("CAST(date_format(" ~ field_sql ~ ", 'u') AS INT) = 1") }}
{%- endmacro %}
