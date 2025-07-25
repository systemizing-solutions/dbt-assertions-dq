{# ======================================================================
#  Return a SQL predicate that is TRUE when <field_sql> (DATE) is
#  Thursday.
#
#  Example use:
#     {{ dbt_assertions._is_thursday_predicate('CAST(my_date AS DATE)') }}
# ====================================================================== #}

{%- macro _is_thursday_predicate(field_sql) -%}
    {{ return(
        adapter.dispatch('_is_thursday_predicate', 'dbt_assertions')
               (field_sql)
    ) }}
{%- endmacro %}

{# ---------- DEFAULT  (DuckDB, Postgres/Redshift, Trino, …) ------------ #}
{#  EXTRACT(DOW): 0=Sun, 1=Mon, 2=Tue, 3=Wed, 4=Thu, 5=Fri, 6=Sat         #}
{%- macro default___is_thursday_predicate(field_sql) -%}
    {{ return('EXTRACT(DOW FROM ' ~ field_sql ~ ') = 4') }}
{%- endmacro %}

{# ---------- BIGQUERY -------------------------------------------------- #}
{#  DAYOFWEEK(): 1=Sun, 2=Mon, 3=Tue, 4=Wed, 5=Thu, 6=Fri, 7=Sat         #}
{%- macro bigquery___is_thursday_predicate(field_sql) -%}
    {{ return('EXTRACT(DAYOFWEEK FROM ' ~ field_sql ~ ') = 5') }}
{%- endmacro %}

{# ---------- SNOWFLAKE / ORACLE --------------------------------------- #}
{#  DAYOFWEEK(): same mapping as BigQuery                                #}
{%- macro snowflake___is_thursday_predicate(field_sql) -%}
    {{ return('DAYOFWEEK(' ~ field_sql ~ ') = 5') }}
{%- endmacro %}

{# ---------- SPARK / DATABRICKS --------------------------------------- #}
{#  date_format(...,'u'): 1=Mon, 2=Tue, 3=Wed, 4=Thu, 5=Fri, 6=Sat, 7=Sun #}
{%- macro spark___is_thursday_predicate(field_sql) -%}
    {{ return("CAST(date_format(" ~ field_sql ~ ", 'u') AS INT) = 4") }}
{%- endmacro %}
