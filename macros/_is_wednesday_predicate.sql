{# ======================================================================
#  Return a SQL predicate that is TRUE when <field_sql> (DATE) is a
#  Wednesday.
#
#  Example:
#     {{ dbt_assertions._is_wednesday_predicate('CAST(event_date AS DATE)') }}
# ====================================================================== #}

{%- macro _is_wednesday_predicate(field_sql) -%}
    {{ return(
        adapter.dispatch('_is_wednesday_predicate', 'dbt_assertions')
               (field_sql)
    ) }}
{%- endmacro %}

{# ---------- DEFAULT  (DuckDB, Postgres/Redshift, Trino, …) ------------ #}
{#  EXTRACT(DOW): 0=Sun, 1=Mon, 2=Tue, 3=Wed, … 6=Sat                    #}
{%- macro default___is_wednesday_predicate(field_sql) -%}
    {{ return('EXTRACT(DOW FROM ' ~ field_sql ~ ') = 3') }}
{%- endmacro %}

{# ---------- BIGQUERY -------------------------------------------------- #}
{#  DAYOFWEEK(): 1=Sun, 2=Mon, 3=Tue, 4=Wed, … 7=Sat                     #}
{%- macro bigquery___is_wednesday_predicate(field_sql) -%}
    {{ return('EXTRACT(DAYOFWEEK FROM ' ~ field_sql ~ ') = 4') }}
{%- endmacro %}

{# ---------- SNOWFLAKE / ORACLE --------------------------------------- #}
{#  DAYOFWEEK(): 1=Sun, 2=Mon, 3=Tue, 4=Wed, … 7=Sat                     #}
{%- macro snowflake___is_wednesday_predicate(field_sql) -%}
    {{ return('DAYOFWEEK(' ~ field_sql ~ ') = 4') }}
{%- endmacro %}

{# ---------- SPARK / DATABRICKS --------------------------------------- #}
{#  date_format(...,'u'): 1=Mon, 2=Tue, 3=Wed, … 7=Sun                   #}
{%- macro spark___is_wednesday_predicate(field_sql) -%}
    {{ return("CAST(date_format(" ~ field_sql ~ ", 'u') AS INT) = 3") }}
{%- endmacro %}
