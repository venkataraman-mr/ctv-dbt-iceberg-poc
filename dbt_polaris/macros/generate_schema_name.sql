{# Use a model's schema config verbatim (gold, silver, reference) instead of prefixing it. #}
{% macro generate_schema_name(custom_schema_name, node) %}
  {%- if custom_schema_name is none -%}{{ target.schema }}
  {%- else -%}{{ custom_schema_name | trim }}{%- endif -%}
{% endmacro %}
