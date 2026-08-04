{#
  Piece 3 shared macros — creative push (Trino/dbt-native, no Python).

  Two helpers used by Job A (DIGITAL_RAW_OCC_TO_CRTV_STAGING) and Job B
  (DIGITAL_RAW_OCC_TO_CRTV_FIRST_SEEN_UPDATE):

    * pg_call(inner_sql)          -> a Trino statement that tunnels `inner_sql` to Postgres via the
                                     PostgreSQL connector's system.execute passthrough (for CALLs /
                                     DML that return no rows). Used in post_hooks.
    * reserve_creative_ids(n)     -> reserves a contiguous block of n ids from the Postgres sequence
                                     tempwork.creative_id_seq_ctv_poc and returns the block START.
                                     Read through the connector's system.query table function.

  (The generic timestamp->snapshot resolver `snapshot_range_since_ts`, used by Job B's timestamp
  watermark, is reusable across any timestamp-based table_changes read and lives with the watermark
  framework in macros/watermark.sql.)

  Both are only meaningful at run time (execute=true); they no-op safely during parse.
  VERIFY ON VM: postgres.system.execute / postgres.system.query availability + behavior on the wired
  `postgres` catalog (Trino 476), and that system.query tolerates the side-effecting nextval.
#}

{#- Tunnel a no-result statement (CALL / INSERT / UPDATE) to Postgres. Doubles single quotes so the
    inner SQL survives being wrapped in the system.execute string literal. -#}
{% macro pg_call(inner_sql) %}
  {%- set doubled = inner_sql | replace("'", "''") -%}
  {{ return("call postgres.system.execute(query => '" ~ doubled ~ "')") }}
{% endmacro %}

{#- Reserve n contiguous creative ids; return the block START (min) — the analog of the Universal
    Creative ID API returning a [min, max] block.

    Two Trino calls, because Trino can't both write and return in one: (1) RESERVE via system.execute
    (writable) -> the Postgres procedure sp_reserve_creative_ids_ctv_poc(n) atomically pops n values
    off the sequence and records the block into tempwork.creative_id_block_ctv_poc; (2) READ the newest
    block row via system.query (read-only). Job A is single-threaded (every 20 min), so "newest block"
    is unambiguously this run's. The final model assigns creative_id = block_start + row_number()-1
    over the new creatives (block_end is recorded for audit / a sanity check that end = start+n-1). -#}
{% macro reserve_creative_ids(n) %}
  {% if not execute %}{{ return(0) }}{% endif %}
  {% set cnt = (n | int) if n is not none else 0 %}
  {% if cnt <= 0 %}{{ return(0) }}{% endif %}
  {% set do_reserve %}
    call postgres.system.execute(query => 'call tempwork.sp_reserve_creative_ids_ctv_poc({{ cnt }})')
  {% endset %}
  {% do run_query(do_reserve) %}
  {% set read_q %}
    select block_start
    from table(postgres.system.query(query =>
      'select block_start from tempwork.creative_id_block_ctv_poc order by block_id desc limit 1'))
  {% endset %}
  {% set base = run_query(read_q).rows[0]['block_start'] | int %}
  {{ return(base) }}
{% endmacro %}
