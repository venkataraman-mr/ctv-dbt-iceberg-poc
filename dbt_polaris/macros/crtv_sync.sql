{#
  Piece 4 sync-back shared macros.

  p4_creative_proc_call(wm_name) — a RUN-TIME pre_hook (template string) that CALLs the cloned creative
  get_changes proc with the current watermark. It must be run-time (not baked at parse) because pre_hook
  is captured at PARSE, when the watermark isn't readable yet — same trap as watermark_ts_finish_from_relation.
  Reads the watermark's end_timestamp (previous high-water) as the creative flag; passes a FAR-FUTURE
  archive flag so the proc's archive branches return zero rows (archive parked). Tunnels the CALL to
  Postgres via pg_call (system.execute).

  Usage (pre_hook, AFTER the advert-hold CTAS): "{{ p4_creative_proc_call('CTV_SYNC_CREATIVE') }}"
#}
{% macro p4_creative_proc_call(wm_name, arch_flag='9999-12-31 00:00:00') %}
  {% if not execute %}{{ return("select 1") }}{% endif %}
  {% set r = run_query("select end_timestamp as s from " ~ source('control', 'watermark_control')
                       ~ " where watermark_name = '" ~ wm_name ~ "'").rows %}
  {% set start_ts = r[0]['s'] if r else none %}
  {% set start_ts_naive = (start_ts | string)[:19] if start_ts is not none else '1900-01-01 00:00:00' %}
  {{ return(pg_call(
     "call tempwork.sp_dbx_creative_get_changes_for_databricks_ctv_poc('"
     ~ start_ts_naive ~ "," ~ arch_flag ~ "')")) }}
{% endmacro %}

{#
  p4_component_proc_call(wm_name) — same idea for the component get_changes proc (task 4). Its proc takes a
  single timestamp (no archive flag).
#}
{% macro p4_component_proc_call(wm_name) %}
  {% if not execute %}{{ return("select 1") }}{% endif %}
  {% set r = run_query("select end_timestamp as s from " ~ source('control', 'watermark_control')
                       ~ " where watermark_name = '" ~ wm_name ~ "'").rows %}
  {% set start_ts = r[0]['s'] if r else none %}
  {% set start_ts_naive = (start_ts | string)[:19] if start_ts is not none else '1900-01-01 00:00:00' %}
  {{ return(pg_call(
     "call tempwork.sp_dbx_component_get_changes_for_databricks_ctv_poc('" ~ start_ts_naive ~ "')")) }}
{% endmacro %}
