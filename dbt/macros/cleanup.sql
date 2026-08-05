{#
  Job-scratch cleanup (Nessie has no views, so dbt intermediate models are TABLES — the analog of the
  Databricks jobwork temp tables the legacy code DROPs at end of run). This on-run-end hook drops every
  model carrying `tag`, but ONLY if NO model with that tag failed/was skipped this run — so a FAILED run
  leaves all of that tag's scratch in place for debugging (exactly when you want it). Works for a job
  with multiple terminals (e.g. Job B = first-seen + occurrence summary). Opt out per tag with
  `--vars 'keep_<tag>_tables: true'` (e.g. keep_job_a_tables / keep_job_b_tables).

  Persistent tables are NOT touched: creative_unique_urls / creative_autochaff /
  missing_digital_occurrence_for_summary (bronze) and silver.watermark_control are pre-DDL'd, not dbt
  models, so they never carry the tag.

  Wire-up (dbt_project.yml on-run-end): "{{ cleanup_tagged_models(results, 'job_a') }}" etc.
#}
{% macro cleanup_tagged_models(results, tag) %}
  {% if not execute %}{{ return('') }}{% endif %}
  {% if var('keep_' ~ tag ~ '_tables', false) %}
    {{ log(tag ~ ' cleanup: skipped (keep_' ~ tag ~ '_tables=true)', info=true) }}
    {{ return('') }}
  {% endif %}

  {#- gate: only clean up if at least one tagged model ran and NONE failed. (namespace — a plain
      {% set %} inside a {% for %} is loop-scoped and won't escape.) -#}
  {% set ns = namespace(any=false, bad=false, dropped=0) %}
  {% for r in results %}
    {% if r.node.resource_type == 'model' and tag in (r.node.config.tags or []) %}
      {% set ns.any = true %}
      {% if (r.status | string) != 'success' %}{% set ns.bad = true %}{% endif %}
    {% endif %}
  {% endfor %}
  {% if not ns.any or ns.bad %}{{ return('') }}{% endif %}

  {% for r in results %}
    {% set n = r.node %}
    {% if n.resource_type == 'model' and tag in (n.config.tags or []) and (r.status | string) == 'success' %}
      {% set rel = n.database ~ '.' ~ n.schema ~ '.' ~ n.identifier %}
      {% do run_query('drop table if exists ' ~ rel) %}
      {% set ns.dropped = ns.dropped + 1 %}
      {{ log(tag ~ ' cleanup: dropped ' ~ rel, info=true) }}
    {% endif %}
  {% endfor %}
  {{ log(tag ~ ' cleanup: dropped ' ~ ns.dropped ~ ' scratch table(s)', info=true) }}
{% endmacro %}
