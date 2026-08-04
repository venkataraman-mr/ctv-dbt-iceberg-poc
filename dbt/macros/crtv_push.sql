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

{#- Reserve n contiguous creative ids; return the block start (min). nextval() runs in Postgres via
    the system.query passthrough; increment 1 + cache 1 makes the block contiguous, and Job A is
    single-threaded (every 20 min) so there is no concurrent reservation. -#}
{% macro reserve_creative_ids(n) %}
  {% if not execute %}{{ return(0) }}{% endif %}
  {% set cnt = (n | int) if n is not none else 0 %}
  {% if cnt <= 0 %}{{ return(0) }}{% endif %}
  {% set q %}
    select min(id) as base
    from table(postgres.system.query(query =>
      'select nextval(''tempwork.creative_id_seq_ctv_poc'')::bigint as id from generate_series(1, {{ cnt }})'))
  {% endset %}
  {% set base = run_query(q).rows[0]['base'] %}
  {{ return(base) }}
{% endmacro %}
