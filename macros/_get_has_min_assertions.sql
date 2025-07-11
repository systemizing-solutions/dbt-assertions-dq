{# =============================================================================
#  Helper family  :  __has_min__
#  Semantics      :  Each value must be >= the supplied minimum.
#
#  Example YAML
#    __has_min__:
#      balance: 0
#      items:
#        qty: 1
# ============================================================================= #}

{%- macro _get_has_min_assertions(min_columns) -%}
    {{ return(
        adapter.dispatch('_get_has_min_assertions',
                         'dbt_assertions')(min_columns)
    ) }}
{%- endmacro %}

{%- macro default___get_has_min_assertions(min_columns) -%}
    {%- set result       = {} %}
    {%- set layered_cols = dbt_assertions._extract_columns(min_columns) %}

    {%- for parent, layer in layered_cols.items() %}
        {%- set cols       = layer.columns %}
        {%- set depends_on = layer.depends_on %}

        {# ── top-level columns ───────────────────────────────────────── #}
        {%- if parent is none %}
            {%- for col in cols %}
                {%- set raw_val = min_columns[col] %}
                {%- set min_val = raw_val if raw_val is number
                                else ("'" ~ raw_val|replace("'", "''") ~ "'") %}
                {%- do result.update({
                    col ~ '__has_min': {
                        'description': col ~ ' must be ≥ ' ~ raw_val ~ '.',
                        'expression' : col ~ ' >= ' ~ min_val
                    }
                }) %}
            {%- endfor %}

        {# ── nested / repeated fields ────────────────────────────────── #}
        {%- else %}
            {%- set base = ['NOT EXISTS (\\n    SELECT 1'] %}
            {%- for key in depends_on + [parent] %}
                {%- do base.append(
                    '\\n    ' ~ ('FROM' if loop.first else 'CROSS JOIN')
                    ~ ' UNNEST(' ~ key ~ ') ' ~ key) %}
            {%- endfor %}
            {%- do base.append('\\n    WHERE ' ~ parent ~ ' IS NOT NULL') %}

            {%- for col in cols %}
                {%- set raw_val = layer.nested[col] %}
                {%- set min_val = raw_val if raw_val is number
                                else ("'" ~ raw_val|replace("'", "''") ~ "'") %}
                {%- set expr = base + [
                    '\\n      AND ' ~ parent ~ '.' ~ col ~ ' < ' ~ min_val,
                    '\\n)'] %}
                {%- do result.update({
                    '.'.join(depends_on + [parent, col]) ~ '__has_min': {
                        'description':
                            '.'.join(depends_on + [parent, col])
                            ~ ' must be ≥ ' ~ raw_val ~ '.',
                        'expression' : expr | join('')
                    }
                }) %}
            {%- endfor %}
        {%- endif %}
    {%- endfor %}

    {{ return(result) }}
{%- endmacro %}
