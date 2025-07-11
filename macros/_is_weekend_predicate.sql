{# ======================================================================
#  Return a SQL predicate that is TRUE when <field_sql> (DATE) falls on a
#  weekend (Saturday or Sunday).
#
#  Example usage inside another macro:
#      {{ dbt_assertions._is_weekend_predicate('CAST(ship_date AS DATE)') }}
# ====================================================================== #}

{%- macro _is_weekend_predicate(field_sql) -%}
    {{ return(
        adapter.dispatch('_is_weekend_predicate', 'dbt_assertions')
               (field_sql)
    ) }}
{%- endmacro %}

{# ---------- DEFAULT  (DuckDB, Postgres ≥10, Trino, Redshift …) -------- #}
{#  EXTRACT(DOW): 0=Sunday, 6=Saturday on PG family                     #}
{%- macro default___is_weekend_predicate(field_sql) -%}
    {{ return('EXTRACT(DOW FROM ' ~ field_sql ~ ') IN (0, 6)') }}
{%- endmacro %}

{# ---------- BIGQUERY -------------------------------------------------- #}
{#  DAYOFWEEK(): 1=Sunday, 7=Saturday                                   #}
{%- macro bigquery___is_weekend_predicate(field_sql) -%}
    {{ return('EXTRACT(DAYOFWEEK FROM ' ~ field_sql ~ ') IN (1, 7)') }}
{%- endmacro %}

{# ---------- SNOWFLAKE / ORACLE --------------------------------------- #}
{#  DAYOFWEEK(): 1=Sunday, 7=Saturday                                   #}
{%- macro snowflake___is_weekend_predicate(field_sql) -%}
    {{ return('DAYOFWEEK(' ~ field_sql ~ ') IN (1, 7)') }}
{%- endmacro %}

{# ---------- SPARK / DATABRICKS --------------------------------------- #}
{#  date_format(...,'u'): 1=Mon … 7=Sun                                  #}
{%- macro spark___is_weekend_predicate(field_sql) -%}
    {{ return("CAST(date_format(" ~ field_sql ~ ", 'u') AS INT) IN (6, 7)") }}
{%- endmacro %}
