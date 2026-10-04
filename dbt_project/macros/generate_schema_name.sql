{#-
  Controls in which schema a model is built.

  By default dbt builds "<target schema>_<custom schema>", e.g. "staging_production", which is almost
  never what you want. This override uses the custom schema exactly as written in dbt_project.yml
  (staging, production) and falls back to the profile's schema when none is set.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
