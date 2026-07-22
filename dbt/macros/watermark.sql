{#
  Version-based watermark framework (Iceberg + Trino).
  Use ONLY on append-only sources (e.g. bronze) — table_changes cannot read snapshots that
  contain delete files. For MERGE-written sources, use a timestamp watermark instead.
  Control table: iceberg.silver.watermark_control (one row per named process).
#}

{% macro snapshots_table(source_relation) %}
  {{ return(source_relation.database ~ '.' ~ source_relation.schema ~ '."' ~ source_relation.identifier ~ '$snapshots"') }}
{% endmacro %}

{#- Body step: pins the current source snapshot, marks InProgress, returns {start, end}. -#}
{% macro watermark_begin(watermark_name, source_relation) %}
  {% if not execute %}{{ return({'start_version': none, 'end_version': 0}) }}{% endif %}
  {% set read_sql %}
    select cast(last_commit_version as varchar) as start_version,
           cast((select snapshot_id from {{ snapshots_table(source_relation) }}
                 order by committed_at desc limit 1) as varchar) as end_version
    from {{ source('control', 'watermark_control') }}
    where watermark_name = '{{ watermark_name }}'
  {% endset %}
  {% set row = run_query(read_sql).rows[0] %}
  {% set start_version = row['start_version'] %}
  {% set end_version = row['end_version'] %}
  {% if flags.WHICH in ('run', 'build') %}
    {% set mark_sql %}
      update {{ source('control', 'watermark_control') }}
      set current_commit_version = {{ end_version if end_version is not none else 'null' }},
          transaction_status = 'InProgress',
          updated_timestamp = cast(current_timestamp as timestamp(6))
      where watermark_name = '{{ watermark_name }}'
    {% endset %}
    {% do run_query(mark_sql) %}
  {% endif %}
  {{ return({'start_version': start_version, 'end_version': end_version}) }}
{% endmacro %}

{#- post_hook: promotes the pinned version into last_commit_version on success (idempotent MERGE). -#}
{% macro watermark_finish(watermark_name) %}
  {% if not execute %}{{ return("select 1") }}{% endif %}
  {{ return("update " ~ source('control', 'watermark_control') ~
            " set transaction_status='Success', last_commit_version=current_commit_version," ~
            " updated_timestamp=cast(current_timestamp as timestamp(6))" ~
            " where watermark_name='" ~ watermark_name ~ "'") }}
{% endmacro %}
