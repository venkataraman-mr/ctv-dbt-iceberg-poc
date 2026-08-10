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
          updated_timestamp = cast(current_timestamp as timestamp(6) with time zone)
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
     " updated_timestamp = cast(current_timestamp as timestamp(6) with time zone)" ~
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
     " set start_timestamp = cast(timestamp '" ~ prev_end_ts ~ "' as timestamp(6) with time zone)," ~
     " end_timestamp = cast(timestamp '" ~ new_end_ts ~ "' as timestamp(6) with time zone)," ~
     " transaction_status = 'SUCCEEDED'," ~
     " updated_timestamp = cast(current_timestamp as timestamp(6) with time zone)" ~
     " where watermark_name = '" ~ watermark_name ~ "'") }}
{% endmacro %}

{#- RUN-TIME timestamp-watermark finish for the Piece-4 sync pattern. Pass this as a run-time template
    string in post_hook (config/post_hook is captured at PARSE, so the advance value cannot be baked in
    at parse — that was the "watermark never advanced" bug). At run it reads max(ts_col) from the
    already-built candidate relation `rel` and advances the window: start := old end, end := that max.
    No rows in the candidate -> no-op (watermark unchanged). rel/ts_col are embedded as literals by the
    caller; the macro re-renders at run (execute=True) and returns the UPDATE. -#}
{% macro watermark_ts_finish_from_relation(watermark_name, rel, ts_col='updated_timestamp') %}
  {% if not execute %}{{ return("select 1") }}{% endif %}
  {% set q %}select cast(max({{ ts_col }}) as varchar) as m from {{ rel }}{% endset %}
  {% set m = run_query(q).rows[0]['m'] %}
  {% if m is none %}{{ return("select 1") }}{% endif %}
  {{ return(
     "update " ~ source('control', 'watermark_control') ~
     " set start_timestamp = end_timestamp," ~
     " end_timestamp = cast(timestamp '" ~ m ~ "' as timestamp(6) with time zone)," ~
     " transaction_status = 'SUCCEEDED'," ~
     " updated_timestamp = cast(current_timestamp as timestamp(6) with time zone)" ~
     " where watermark_name = '" ~ watermark_name ~ "'") }}
{% endmacro %}

{#- Reusable timestamp -> snapshot resolver for TIMESTAMP-watermarked table_changes reads on an
    Iceberg source. Generic (any process that keeps a timestamp watermark on an append-only Iceberg
    table can use it); pairs with watermark_ts_begin/finish above.

    A stored timestamp watermark isn't directly usable by Trino system.table_changes, which reads a
    snapshot-id range (unlike Delta's timestamp-native table_changes). This maps the watermark:
      start_ts is EXCLUSIVE -> start_snapshot = newest snapshot committed AT/BEFORE start_ts;
      end_snapshot = current newest snapshot; and end_committed_at = that snapshot's exact commit
      time, which the caller stores as the new end_timestamp (via watermark_ts_finish) so the next
      run's `committed_at <= start_ts` lands exactly on end_snap with no wall-clock drift.
    First run (start_ts is none) -> {start_snap: none}: caller does a one-time full read
    `for version as of end_snap`.

    HARDENING (no watermark-schema change): deterministic tie-break `order by committed_at DESC,
    snapshot_id DESC` everywhere (snapshots sharing a millisecond never resolve arbitrarily), and the
    watermark end stores end_snap's committed_at (not a run wall-clock time).

    Returns {start_snap, end_snap, end_committed_at}. -#}
{% macro snapshot_range_since_ts(source_relation, start_ts) %}
  {% if not execute %}{{ return({'start_snap': none, 'end_snap': none, 'end_committed_at': none}) }}{% endif %}
  {% set snaps = snapshots_table(source_relation) %}
  {% if start_ts is none %}
    {% set q %}
      select snapshot_id as end_snap, committed_at as end_committed_at
      from {{ snaps }}
      order by committed_at desc, snapshot_id desc
      limit 1
    {% endset %}
    {% set row = run_query(q).rows[0] %}
    {{ return({'start_snap': none, 'end_snap': row['end_snap'], 'end_committed_at': row['end_committed_at']}) }}
  {% else %}
    {% set q %}
      select
        (select snapshot_id from {{ snaps }}
          where committed_at <= timestamp '{{ start_ts }}'
          order by committed_at desc, snapshot_id desc limit 1)   as start_snap,
        (select snapshot_id from {{ snaps }}
          order by committed_at desc, snapshot_id desc limit 1)   as end_snap,
        (select committed_at from {{ snaps }}
          order by committed_at desc, snapshot_id desc limit 1)   as end_committed_at
    {% endset %}
    {% set row = run_query(q).rows[0] %}
    {{ return({'start_snap': row['start_snap'], 'end_snap': row['end_snap'], 'end_committed_at': row['end_committed_at']}) }}
  {% endif %}
{% endmacro %}
