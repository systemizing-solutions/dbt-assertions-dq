{# =========================================================================
#  Return a SQL expression that evaluates to yesterday’s date (inclusive)
#  for the active adapter.
# ========================================================================= #}

{%- macro _yesterday_date() -%}
    {{ return(adapter.dispatch('_yesterday_date', 'dbt_assertions')()) }}
{%- endmacro %}

{# ---------- DEFAULT  (Snowflake, Postgres, Redshift, Oracle …) --------- #}
{%- macro default___yesterday_date() -%}
    {{ return('CURRENT_DATE - 1') }}
{%- endmacro %}

{# ---------- BIGQUERY --------------------------------------------------- #}
{%- macro bigquery___yesterday_date() -%}
    {{ return("DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)") }}
{%- endmacro %}

{# ---------- DUCKDB / TRINO / many ANSI engines ------------------------- #}
{%- macro duckdb___yesterday_date() -%}
    {{ return("current_date - INTERVAL 1 DAY") }}
{%- endmacro %}

{# ---------- SPARK / DATABRICKS ---------------------------------------- #}
{%- macro spark___yesterday_date() -%}
    {{ return("date_sub(current_date(), 1)") }}
{%- endmacro %}

{# Add more overrides if an engine needs special syntax (e.g. Presto)     #}
