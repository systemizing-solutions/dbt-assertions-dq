{# ======================================================================
#  Return TRUE when <field_sql> (DATE) is a Saturday.
#
#  Example call inside another macro:
#     {{ dbt_assertions._is_saturday_predicate('CAST(my_date AS DATE)') }}
# ====================================================================== #}

{%- macro _is_saturday_predicate(field_sql) -%}
    {{ return(
        adapter.dispatch('_is_saturday_predicate', 'dbt_assertions')
               (field_sql)
    ) }}
{%- endmacro %}

{# ---------- DEFAULT  (DuckDB, Postgres/Redshift, Trino …) ------------- #}
{#  EXTRACT(DOW): 0=Sun, … 6=Sat                                          #}
{%- macro default___is_saturday_predicate(field_sql) -%}
    {{ return('EXTRACT(DOW FROM ' ~ field_sql ~ ') = 6') }}
{%- endmacro %}

{# ---------- BIGQUERY -------------------------------------------------- #}
{#  DAYOFWEEK(): 1=Sun, … 7=Sat                                           #}
{%- macro bigquery___is_saturday_predicate(field_sql) -%}
    {{ return('EXTRACT(DAYOFWEEK FROM ' ~ field_sql ~ ') = 7') }}
{%- endmacro %}

{# ---------- SNOWFLAKE / ORACLE --------------------------------------- #}
{#  DAYOFWEEK(): same mapping as BigQuery                                 #}
{%- macro snowflake___is_saturday_predicate(field_sql) -%}
    {{ return('DAYOFWEEK(' ~ field_sql ~ ') = 7') }}
{%- endmacro %}

{# ---------- SPARK / DATABRICKS --------------------------------------- #}
{#  date_format(...,'u'): 1=Mon, … 6=Sat, 7=Sun                           #}
{%- macro spark___is_saturday_predicate(field_sql) -%}
    {{ return("CAST(date_format(" ~ field_sql ~ ", 'u') AS INT) = 6") }}
{%- endmacro %}
