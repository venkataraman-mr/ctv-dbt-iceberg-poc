{#
  Piece 5 (gold occurrence) shared macros.

  reserve_occurrence_ids(n) — reserve a contiguous block of n occurrence_ids from the Postgres sequence
  tempwork.occurrence_id_seq_ctv_poc (START 75,000,000,000) and return the block START. Exact analog of
  reserve_creative_ids (crtv_push.sql): (1) RESERVE via the writable postgres.system.execute ->
  sp_reserve_occurrence_ids_ctv_poc(n) pops n values + records the block in tempwork.occurrence_id_block_ctv_poc;
  (2) READ the newest block row via the read-only system.query. The Half-A writer assigns
  occurrence_id = block_start + row_number()-1 over the Not-Hold occurrences. Runs at COMPILE (execute=True).
#}
{% macro reserve_occurrence_ids(n) %}
  {% if not execute %}{{ return(0) }}{% endif %}
  {% set cnt = (n | int) if n is not none else 0 %}
  {% if cnt <= 0 %}{{ return(0) }}{% endif %}
  {% set do_reserve %}
    call postgres.system.execute(query => 'call tempwork.sp_reserve_occurrence_ids_ctv_poc({{ cnt }})')
  {% endset %}
  {% do run_query(do_reserve) %}
  {% set read_q %}
    select block_start
    from table(postgres.system.query(query =>
      'select block_start from tempwork.occurrence_id_block_ctv_poc order by block_id desc limit 1'))
  {% endset %}
  {% set base = run_query(read_q).rows[0]['block_start'] | int %}
  {{ return(base) }}
{% endmacro %}
