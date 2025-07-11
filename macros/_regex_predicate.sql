{# ------------------------------------------------------------------------
# Return an adapter-specific SQL predicate that checks
#     <field expression> matches <regex literal>
# ------------------------------------------------------------------------ #}

{%- macro _regex_predicate(field_sql, pattern_sql) -%}
    {{ return(
        adapter.dispatch('_regex_predicate', 'dbt_assertions')
               (field_sql, pattern_sql)
    ) }}
{%- endmacro %}

{# ---------- DEFAULT (most adapters: Snowflake, Redshift, Oracle, etc.) -#}
{%- macro default___regex_predicate(field_sql, pattern_sql) -%}
    {{ return("REGEXP_LIKE(" ~ field_sql ~ ", " ~ pattern_sql ~ ")") }}
{%- endmacro %}

{# ---------- BIGQUERY --------------------------------------------------- #}
{%- macro bigquery___regex_predicate(field_sql, pattern_sql) -%}
    {{ return("REGEXP_CONTAINS(" ~ field_sql ~ ", " ~ pattern_sql ~ ")") }}
{%- endmacro %}

{# ---------- DUCKDB / POSTGRES ----------------------------------------- #}
{# DuckDB & PG 14+ have regexp_full_match(); PG also supports the ~ operator. #}
{%- macro duckdb___regex_predicate(field_sql, pattern_sql) -%}
    {{ return("regexp_full_match(" ~ field_sql ~ ", " ~ pattern_sql ~ ")") }}
{%- endmacro %}

{%- macro postgres___regex_predicate(field_sql, pattern_sql) -%}
    {{ return(field_sql ~ " ~ " ~ pattern_sql) }}
{%- endmacro %}
