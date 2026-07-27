{#
  Watermark framework (Iceberg + Trino), ported from the legacy Databricks watermark_control.
  ONE control table (iceberg.silver.watermark_control), one row per named process, shared by two
  styles — mirroring common/common_functions.py:

    * VERSION-based  (append-only sources, e.g. staging->raw): uses last_commit_version.
        table_changes cannot read snapshots that contain delete files, so version watermarks are
        only valid on append-only sources.
    * TIMESTAMP-based (MERGE/delete-written sources, e.g. Piece 4 creative sync-back): uses
        start_timestamp / end_timestamp.

  The version macros use current_commit_version (our addition to the legacy schema) as a two-phase
  pin: begin records the source's end snapshot there and marks 'InProgress'; finish promotes it into
  last_commit_version and marks 'SUCCEEDED'. This carries the exact end snapshot from begin to the
  post_hook (which can't see the model body's runtime values) with no "source advanced mid-run"
  re-read window.

  Seed one row per process before first use (see ddl/03_silver_watermark_control.sql).

  Piece 1 (digital_raw_occurrence) uses the VERSION pair with Trino's system.table_changes to read
  only new staging inserts each run. The TIMESTAMP pair is for the MERGE-written sources later.
#}

{% macro snapshots_table(source_relation) %}
  {{ return(source_relation.database ~ '.' ~ source_relation.schema ~ '."' ~ source_relation.identifier ~ '$snapshots"') }}
{% endmacro %}

{#- latest committed snapshot id of an Iceberg source (the "end" version). -#}
{% macro _latest_snapshot_sql(source_relation) %}
  {{ return('(select snapshot_id from ' ~ snapshots_table(source_relation) ~ ' order by committed_at desc limit 1)') }}
{% endmacro %}


{# ============================ VERSION-based (append-only) ============================ #}

{#- Body: pins the source's current end snapshot in current_commit_version, marks InProgress, and
    returns {start_version = last processed, end_version = pinned end}. -#}
{% macro watermark_version_begin(watermark_name, source_relation) %}
  {% if not execute %}{{ return({'start_version': none, 'end_version': none}) }}{% endif %}
  {% set read_sql %}
    select
      (select last_commit_version
         from {{ source('control', 'watermark_control') }}
        where watermark_name = '{{ watermark_name }}') as start_version,
      {{ _latest_snapshot_sql(source_relation) }} as end_version
  {% endset %}
  {% set row = run_query(read_sql).rows[0] %}
  {% set start_version = row['start_version'] %}
  {% set end_version = row['end_version'] %}
  {% if flags.WHICH in ('run', 'build') %}
    {% set pin_sql %}
      update {{ source('control', 'watermark_control') }}
      set current_commit_version = {{ end_version if end_version is not none else 'null' }},
          transaction_status = 'InProgress',
          updated_timestamp = cast(current_timestamp as timestamp(6))
      where watermark_name = '{{ watermark_name }}'
    {% endset %}
    {% do run_query(pin_sql) %}
  {% endif %}
  {{ return({'start_version': start_version, 'end_version': end_version}) }}
{% endmacro %}

{#- post_hook: promote the pinned current_commit_version into last_commit_version, mark SUCCEEDED. -#}
{% macro watermark_version_finish(watermark_name) %}
  {% if not execute %}{{ return("select 1") }}{% endif %}
  {{ return(
     "update " ~ source('control', 'watermark_control') ~
     " set last_commit_version = current_commit_version," ~
     " transaction_status = 'SUCCEEDED'," ~
     " updated_timestamp = cast(current_timestamp as timestamp(6))" ~
     " where watermark_name = '" ~ watermark_name ~ "'") }}
{% endmacro %}


{# ============================ TIMESTAMP-based (MERGE/delete) ========================= #}

{#- Body: returns {start_ts = last processed end_timestamp}. Read source rows with
    updated_timestamp > start_ts (and <= the end you capture for this run). -#}
{% macro watermark_ts_begin(watermark_name) %}
  {% if not execute %}{{ return({'start_ts': none}) }}{% endif %}
  {% set read_sql %}
    select end_timestamp as start_ts
    from {{ source('control', 'watermark_control') }}
    where watermark_name = '{{ watermark_name }}'
  {% endset %}
  {% set rows = run_query(read_sql).rows %}
  {{ return({'start_ts': (rows[0]['start_ts'] if rows else none)}) }}
{% endmacro %}

{#- post_hook: advance the window to [prev end, new end]. Pass the run's end timestamp in. -#}
{% macro watermark_ts_finish(watermark_name, prev_end_ts, new_end_ts) %}
  {% if not execute %}{{ return("select 1") }}{% endif %}
  {{ return(
     "update " ~ source('control', 'watermark_control') ~
     " set start_timestamp = timestamp '" ~ prev_end_ts ~ "'," ~
     " end_timestamp = timestamp '" ~ new_end_ts ~ "'," ~
     " transaction_status = 'SUCCEEDED'," ~
     " updated_timestamp = cast(current_timestamp as timestamp(6))" ~
     " where watermark_name = '" ~ watermark_name ~ "'") }}
{% endmacro %}
