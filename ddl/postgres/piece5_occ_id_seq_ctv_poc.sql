-- Piece 5 (gold occurrence) — occurrence_id block reservation, mirroring the Piece-3 creative_id pattern.
-- gold.digital_gold_occurrence.occurrence_id is an IDENTITY column in prod (not in the gold INSERT list);
-- Iceberg has no identity, so we reserve ids from a Postgres sequence. Per Venkat: START AT 75,000,000,000
-- (guarantees no collision with existing prod occurrence_id). Run once on prod Postgres (needs tempwork_admin_role).

CREATE SEQUENCE IF NOT EXISTS tempwork.occurrence_id_seq_ctv_poc
    AS bigint
    START WITH 75000000000
    INCREMENT BY 1
    MINVALUE 75000000000
    NO MAXVALUE
    CACHE 1
    NO CYCLE;

-- Block table = the "API response log": Trino's writable system.execute can't return rows and its
-- row-returning system.query is read-only, so the reservation proc records the block and dbt reads it back.
CREATE TABLE IF NOT EXISTS tempwork.occurrence_id_block_ctv_poc (
    block_id     bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reserved_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    n            integer     NOT NULL,
    block_start  bigint      NOT NULL,
    block_end    bigint      NOT NULL
);

-- sp_reserve_occurrence_ids_ctv_poc(n): atomically pops n contiguous values off the sequence and records the block.
CREATE OR REPLACE PROCEDURE tempwork.sp_reserve_occurrence_ids_ctv_poc(IN p_n integer)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_start bigint;
    v_end   bigint;
BEGIN
    IF p_n IS NULL OR p_n <= 0 THEN
        RETURN;
    END IF;
    SELECT min(v), max(v) INTO v_start, v_end
    FROM (SELECT nextval('tempwork.occurrence_id_seq_ctv_poc') AS v
          FROM generate_series(1, p_n)) s;
    INSERT INTO tempwork.occurrence_id_block_ctv_poc (n, block_start, block_end)
    VALUES (p_n, v_start, v_end);
END;
$procedure$
;
