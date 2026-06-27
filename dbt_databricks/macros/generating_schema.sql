{% macro generate_schema_name(custom_schema_name, node) -%}

    {# If a custom schema is configured in dbt_project.yml, use it exactly as written #}
    {%- if custom_schema_name is not none -%}

        {{ custom_schema_name | trim }}

    {# Fall back to the default profile schema ONLY if no custom schema is specified #}
    {%- else -%}

        {{ target.schema }}

    {%- endif -%}

{%- endmacro %}