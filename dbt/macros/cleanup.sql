{#
  Job-scratch cleanup (Nessie has no views, so dbt intermediate models are TABLES — the analog of the
  Databricks jobwork temp tables that the legacy code DROPs at end of run). This on-run-end hook drops
  every successfully-built model tagged 'job_a' — but ONLY when the terminal model crtv_staging_final
  succeeded, so a FAILED run leaves the intermediates in place for debugging (which is exactly when you
  want them). Opt out with `--vars 'keep_job_a_tables: true'` to inspect them after a good run.

  Persistent tables are NOT touched: creative_unique_urls / creative_autochaff (bronze registries) and
  silver.watermark_control are pre-DDL'd and not dbt models, so they never match the tag filter.

  Wire-up (dbt_project.yml):  on-run-end: ["{{ cleanup_tagged_models(results, 'job_a', 'crtv_staging_final') }}"]
#}
{% macro cleanup_tagged_models(results, tag, gate_model) %}
  {% if not execute %}{{ return('') }}{% endif %}
  {% if var('keep_job_a_tables', false) %}
    {{ log('Job A cleanup: skipped (keep_job_a_tables=true)', info=true) }}
    {{ return('') }}
  {% endif %}

  {#- only clean up if the gate model (the terminal push model) succeeded this run.
      NOTE: use a namespace — a plain {% set %} inside a {% for %} is loop-scoped and won't escape. -#}
  {% set ns = namespace(gate_ok=false, dropped=0) %}
  {% for r in results %}
    {% if r.node.resource_type == 'model' and r.node.name == gate_model and (r.status | string) == 'success' %}
      {% set ns.gate_ok = true %}
    {% endif %}
  {% endfor %}
  {% if not ns.gate_ok %}
    {{ log('Job A cleanup: skipped (gate model ' ~ gate_model ~ ' did not succeed this run)', info=true) }}
    {{ return('') }}
  {% endif %}

  {% for r in results %}
    {% set n = r.node %}
    {% if n.resource_type == 'model' and tag in (n.config.tags or []) and (r.status | string) == 'success' %}
      {% set rel = n.database ~ '.' ~ n.schema ~ '.' ~ n.identifier %}
      {% do run_query('drop table if exists ' ~ rel) %}
      {% set ns.dropped = ns.dropped + 1 %}
      {{ log('Job A cleanup: dropped ' ~ rel, info=true) }}
    {% endif %}
  {% endfor %}
  {{ log('Job A cleanup: dropped ' ~ ns.dropped ~ ' scratch table(s) tagged ' ~ tag, info=true) }}
{% endmacro %}
