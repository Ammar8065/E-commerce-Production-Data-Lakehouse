{#
    Use the custom schema name verbatim (so `+schema: gold` lands in `gold`,
    not the dbt default `<target>_gold`). Standard override; keeps the warehouse
    schemas clean: silver (published) / staging / gold.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
