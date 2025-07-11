{# =========================================================================
#  Return the SQL literal / expression that represents TODAY (UTC or the
#  database’s current session time-zone) for the current adapter.
# ========================================================================= #}

{%- macro _today_date() -%}
    {{ return(adapter.dispatch('_today_date', 'dbt_assertions')()) }}
{%- endmacro %}

{# -------------- DEFAULT  (works on Snowflake, Postgres, Redshift …) ---- #}
{%- macro default___today_date() -%}
    {{ return('CURRENT_DATE') }}
{%- endmacro %}

{# -------------- BIGQUERY ---------------------------------------------- #}
{%- macro bigquery___today_date() -%}
    {{ return('CURRENT_DATE()') }}
{%- endmacro %}

{# -------------- DUCKDB ------------------------------------------------- #}
{%- macro duckdb___today_date() -%}
    {{ return('current_date') }}
{%- endmacro %}

{# -------------- SPARK (Databricks, SparkSQL) --------------------------- #}
{%- macro spark___today_date() -%}
    {{ return('current_date()') }}
{%- endmacro %}
