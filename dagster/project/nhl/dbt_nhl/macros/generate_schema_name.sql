-- macros/generate_schema_name.sql

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none or custom_schema_name | trim == '' -%}
        {{ target.schema | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
