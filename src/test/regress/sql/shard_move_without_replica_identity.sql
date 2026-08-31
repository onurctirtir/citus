--
-- SHARD_MOVE_WITHOUT_REPLICA_IDENTITY
--
-- Tests non-blocking (logical replication) shard moves for tables that do NOT
-- have a replica identity (no primary key; REPLICA IDENTITY DEFAULT with no PK,
-- or REPLICA IDENTITY NOTHING).
--
-- The behavior is controlled by three independent, default-off GUCs. With all of
-- them off the behavior is identical to upstream Citus:
--
--  * citus.set_replica_identity_full_for_logical_replication
--      When on, Citus temporarily sets REPLICA IDENTITY FULL on the SOURCE shards
--      of a no-replica-identity table for the duration of the logical replication
--      (so the publisher does not reject the user's UPDATE/DELETE statements) and
--      restores the original replica identity afterwards. This is what makes a
--      no-replica-identity table publishable, so it is the sole switch that lets
--      "auto" mode admit such a table for a non-blocking move.
--
--  * citus.create_existing_indexes_early_for_logical_replication
--      When on, one of the table's existing usable btree indexes is built early on
--      the destination shard (before catch-up) so the subscriber can use an index
--      scan, and that index is excluded from the late CREATE INDEX phase so it is
--      created exactly once. Speed only; never affects admission.
--
--  * citus.create_temporary_indexes_for_logical_replication
--      When on, a throwaway helper btree index is built on the destination shard
--      for a table that has no usable index, and dropped when the move finishes.
--      Speed only; never affects admission.
--

CREATE SCHEMA move_no_ri;
SET search_path TO move_no_ri;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 8980000;

--
-- SECTION 1: all GUCs off => upstream behavior is preserved.
--

--
-- 1a) no replica identity + a usable secondary btree index, "auto":
--     with the admission GUC off this still errors, exactly like upstream.
--
CREATE TABLE t_usable_idx (a int, b text);
CREATE INDEX t_usable_idx_a ON t_usable_idx (a);
SELECT create_distributed_table('t_usable_idx', 'a', colocate_with:='none');
INSERT INTO t_usable_idx SELECT g, 'v' || g FROM generate_series(1, 100) g;

SELECT min(shardid) AS shardid_usable FROM pg_dist_shard WHERE logicalrelid='t_usable_idx'::regclass \gset
SELECT nodeport AS src_usable,
       CASE WHEN nodeport = :worker_1_port THEN :worker_2_port ELSE :worker_1_port END AS tgt_usable
FROM pg_dist_shard_placement WHERE shardid = :shardid_usable \gset

-- errors (admission GUC off)
SELECT citus_move_shard_placement(:shardid_usable, 'localhost', :src_usable, 'localhost', :tgt_usable, shard_transfer_mode:='auto');

--
-- 1b) no replica identity + NO index at all, "auto": errors, exactly like upstream.
--
CREATE TABLE t_no_idx (a int, b text);
SELECT create_distributed_table('t_no_idx', 'a', colocate_with:='none');
INSERT INTO t_no_idx SELECT g, 'v' || g FROM generate_series(1, 100) g;

SELECT min(shardid) AS shardid_no_idx FROM pg_dist_shard WHERE logicalrelid='t_no_idx'::regclass \gset
SELECT nodeport AS src_no_idx,
       CASE WHEN nodeport = :worker_1_port THEN :worker_2_port ELSE :worker_1_port END AS tgt_no_idx
FROM pg_dist_shard_placement WHERE shardid = :shardid_no_idx \gset

-- errors (admission GUC off)
SELECT citus_move_shard_placement(:shardid_no_idx, 'localhost', :src_no_idx, 'localhost', :tgt_no_idx, shard_transfer_mode:='auto');

--
-- 1c) force_logical always skips the admission check. With the admission GUC off
--     Citus does NOT touch the replica identity, so this behaves exactly like
--     upstream: the move itself succeeds (there are no concurrent writes here) and
--     the source replica identity is left as the original default 'd'.
--
SELECT citus_move_shard_placement(:shardid_no_idx, 'localhost', :src_no_idx, 'localhost', :tgt_no_idx, shard_transfer_mode:='force_logical');
SELECT public.wait_for_resource_cleanup();
SELECT count(*) FROM t_no_idx;
SELECT DISTINCT result FROM run_command_on_placements('t_no_idx', 'SELECT relreplident FROM pg_class WHERE oid = ''%s''::regclass');

--
-- SECTION 2: citus.set_replica_identity_full_for_logical_replication
--            admits a no-replica-identity table for a non-blocking move and
--            faithfully restores its original replica identity afterwards.
--

SET citus.set_replica_identity_full_for_logical_replication TO on;

--
-- 2a) no replica identity + NO index, "auto": now admitted (the source is set to
--     REPLICA IDENTITY FULL for the move). Move succeeds, data is preserved, and the
--     original default replica identity 'd' is restored on the surviving placement.
--
SELECT min(shardid) AS shardid_no_idx2 FROM pg_dist_shard WHERE logicalrelid='t_no_idx'::regclass \gset
SELECT nodeport AS src_no_idx2,
       CASE WHEN nodeport = :worker_1_port THEN :worker_2_port ELSE :worker_1_port END AS tgt_no_idx2
FROM pg_dist_shard_placement WHERE shardid = :shardid_no_idx2 \gset

SELECT citus_move_shard_placement(:shardid_no_idx2, 'localhost', :src_no_idx2, 'localhost', :tgt_no_idx2, shard_transfer_mode:='auto');
SELECT public.wait_for_resource_cleanup();
SELECT count(*) FROM t_no_idx;
SELECT DISTINCT result FROM run_command_on_placements('t_no_idx', 'SELECT relreplident FROM pg_class WHERE oid = ''%s''::regclass');

--
-- 2b) no replica identity + a usable btree index, "auto": also admitted; the
--     original default replica identity 'd' is restored.
--
SELECT min(shardid) AS shardid_usable2 FROM pg_dist_shard WHERE logicalrelid='t_usable_idx'::regclass \gset
SELECT nodeport AS src_usable2,
       CASE WHEN nodeport = :worker_1_port THEN :worker_2_port ELSE :worker_1_port END AS tgt_usable2
FROM pg_dist_shard_placement WHERE shardid = :shardid_usable2 \gset

SELECT citus_move_shard_placement(:shardid_usable2, 'localhost', :src_usable2, 'localhost', :tgt_usable2, shard_transfer_mode:='auto');
SELECT public.wait_for_resource_cleanup();
SELECT count(*) FROM t_usable_idx;
SELECT DISTINCT result FROM run_command_on_placements('t_usable_idx', 'SELECT relreplident FROM pg_class WHERE oid = ''%s''::regclass');

--
-- 2c) REPLICA IDENTITY NOTHING + a usable index, "auto": admitted, and the original
--     NOTHING identity is faithfully restored ('n', not left as FULL 'f').
--
CREATE TABLE t_nothing_idx (a int, b text);
CREATE INDEX t_nothing_idx_a ON t_nothing_idx (a);
ALTER TABLE t_nothing_idx REPLICA IDENTITY NOTHING;
SELECT create_distributed_table('t_nothing_idx', 'a', colocate_with:='none');
INSERT INTO t_nothing_idx SELECT g, 'v' || g FROM generate_series(1, 100) g;

SELECT min(shardid) AS shardid_nothing FROM pg_dist_shard WHERE logicalrelid='t_nothing_idx'::regclass \gset
SELECT nodeport AS src_nothing,
       CASE WHEN nodeport = :worker_1_port THEN :worker_2_port ELSE :worker_1_port END AS tgt_nothing
FROM pg_dist_shard_placement WHERE shardid = :shardid_nothing \gset

SELECT citus_move_shard_placement(:shardid_nothing, 'localhost', :src_nothing, 'localhost', :tgt_nothing, shard_transfer_mode:='auto');
SELECT public.wait_for_resource_cleanup();
SELECT count(*) FROM t_nothing_idx;
SELECT DISTINCT result FROM run_command_on_placements('t_nothing_idx', 'SELECT relreplident FROM pg_class WHERE oid = ''%s''::regclass');

RESET citus.set_replica_identity_full_for_logical_replication;

--
-- SECTION 3: citus.create_existing_indexes_early_for_logical_replication builds one
--            existing usable index early on the destination shard and excludes it
--            from the late CREATE INDEX phase, so it is created exactly once. If the
--            exclusion were wrong the move would fail with a duplicate-index error,
--            so a successful move with preserved data is the proof it works.
--
--            (The admission GUC is on as well, because these are no-replica-identity
--            tables moved in "auto".)
--

SET citus.set_replica_identity_full_for_logical_replication TO on;
SET citus.create_existing_indexes_early_for_logical_replication TO on;

--
-- 3a) no replica identity + a usable btree index, "auto": the existing index is
--     built early on the destination shard (not a second time), the move succeeds,
--     data is preserved, and the original replica identity 'd' is restored.
--
CREATE TABLE t_early_idx (a int, b text);
CREATE INDEX t_early_idx_a ON t_early_idx (a);
SELECT create_distributed_table('t_early_idx', 'a', colocate_with:='none');
INSERT INTO t_early_idx SELECT g, 'v' || g FROM generate_series(1, 100) g;

SELECT min(shardid) AS shardid_early FROM pg_dist_shard WHERE logicalrelid='t_early_idx'::regclass \gset
SELECT nodeport AS src_early,
       CASE WHEN nodeport = :worker_1_port THEN :worker_2_port ELSE :worker_1_port END AS tgt_early
FROM pg_dist_shard_placement WHERE shardid = :shardid_early \gset

SELECT citus_move_shard_placement(:shardid_early, 'localhost', :src_early, 'localhost', :tgt_early, shard_transfer_mode:='auto');
SELECT public.wait_for_resource_cleanup();
SELECT count(*) FROM t_early_idx;
-- every placement still has exactly one copy of the index (built once)
SELECT DISTINCT result FROM run_command_on_placements('t_early_idx', 'SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indrelid = ''%s''::regclass');
SELECT DISTINCT result FROM run_command_on_placements('t_early_idx', 'SELECT relreplident FROM pg_class WHERE oid = ''%s''::regclass');

--
-- 3b) no replica identity + only a PARTIAL btree index: it is not eligible to be
--     built early, so nothing is built early and the late phase creates it normally.
--     The move still succeeds and data is preserved.
--
CREATE TABLE t_early_partial (a int, b text);
CREATE INDEX t_early_partial_a ON t_early_partial (a) WHERE a > 0;
SELECT create_distributed_table('t_early_partial', 'a', colocate_with:='none');
INSERT INTO t_early_partial SELECT g, 'v' || g FROM generate_series(1, 100) g;

SELECT min(shardid) AS shardid_epartial FROM pg_dist_shard WHERE logicalrelid='t_early_partial'::regclass \gset
SELECT nodeport AS src_epartial,
       CASE WHEN nodeport = :worker_1_port THEN :worker_2_port ELSE :worker_1_port END AS tgt_epartial
FROM pg_dist_shard_placement WHERE shardid = :shardid_epartial \gset

SELECT citus_move_shard_placement(:shardid_epartial, 'localhost', :src_epartial, 'localhost', :tgt_epartial, shard_transfer_mode:='auto');
SELECT public.wait_for_resource_cleanup();
SELECT count(*) FROM t_early_partial;
SELECT DISTINCT result FROM run_command_on_placements('t_early_partial', 'SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indrelid = ''%s''::regclass');

RESET citus.create_existing_indexes_early_for_logical_replication;
RESET citus.set_replica_identity_full_for_logical_replication;

--
-- SECTION 4: citus.create_temporary_indexes_for_logical_replication builds a
--            throwaway helper btree index on the destination shard of a table that
--            has no usable index, and drops it when the move finishes. Uses
--            force_logical so the admission GUC is not required.
--

--
-- 4a) no replica identity + no index, force_logical, temp GUC on: the helper index
--     is built and dropped, the move succeeds, data is preserved, and no helper
--     index (citus_ri_helper_%) is left behind. force_logical is used so no
--     admission GUC is needed; there are no concurrent writes, so the move is safe
--     even though Citus does not set REPLICA IDENTITY FULL here (that is exercised
--     separately in section 2). The source replica identity stays at the default 'd'.
--
CREATE TABLE t_temp (a int, b text, c int);
SELECT create_distributed_table('t_temp', 'a', colocate_with:='none');
INSERT INTO t_temp SELECT g % 10, 'v' || g, g FROM generate_series(1, 200) g;

SELECT min(shardid) AS shardid_temp FROM pg_dist_shard WHERE logicalrelid='t_temp'::regclass \gset
SELECT nodeport AS src_temp,
       CASE WHEN nodeport = :worker_1_port THEN :worker_2_port ELSE :worker_1_port END AS tgt_temp
FROM pg_dist_shard_placement WHERE shardid = :shardid_temp \gset

SET citus.create_temporary_indexes_for_logical_replication TO on;
SELECT citus_move_shard_placement(:shardid_temp, 'localhost', :src_temp, 'localhost', :tgt_temp, shard_transfer_mode:='force_logical');
RESET citus.create_temporary_indexes_for_logical_replication;
SELECT public.wait_for_resource_cleanup();
-- data preserved
SELECT count(*) FROM t_temp;
-- no leftover temporary helper index on any placement
SELECT bool_or(result::int > 0) AS any_leftover_helper_index
FROM run_command_on_placements('t_temp', 'SELECT count(*) FROM pg_class WHERE relkind = ''i'' AND relname LIKE ''citus_ri_helper_%%''');
-- source replica identity unchanged (default 'd')
SELECT DISTINCT result FROM run_command_on_placements('t_temp', 'SELECT relreplident FROM pg_class WHERE oid = ''%s''::regclass');

--
-- 4b) The temp helper also covers a table the user explicitly set to REPLICA
--     IDENTITY FULL that has no index. Such a table can already publish all its
--     modifications, so Citus never touches its replica identity, but the subscriber
--     would still sequential-scan the destination shard during catch-up. With the
--     temp GUC on a helper index is built (and dropped afterwards) for it too, and
--     the user's REPLICA IDENTITY FULL is left untouched ('f').
--
CREATE TABLE t_temp_full (a int, b text, c int);
SELECT create_distributed_table('t_temp_full', 'a', colocate_with:='none');
ALTER TABLE t_temp_full REPLICA IDENTITY FULL;
INSERT INTO t_temp_full SELECT g % 10, 'v' || g, g FROM generate_series(1, 200) g;

SELECT min(shardid) AS shardid_tempfull FROM pg_dist_shard WHERE logicalrelid='t_temp_full'::regclass \gset
SELECT nodeport AS src_tempfull,
       CASE WHEN nodeport = :worker_1_port THEN :worker_2_port ELSE :worker_1_port END AS tgt_tempfull
FROM pg_dist_shard_placement WHERE shardid = :shardid_tempfull \gset

SET citus.create_temporary_indexes_for_logical_replication TO on;
SELECT citus_move_shard_placement(:shardid_tempfull, 'localhost', :src_tempfull, 'localhost', :tgt_tempfull, shard_transfer_mode:='force_logical');
RESET citus.create_temporary_indexes_for_logical_replication;
SELECT public.wait_for_resource_cleanup();
-- data preserved
SELECT count(*) FROM t_temp_full;
-- no leftover temporary helper index on any placement
SELECT bool_or(result::int > 0) AS any_leftover_helper_index
FROM run_command_on_placements('t_temp_full', 'SELECT count(*) FROM pg_class WHERE relkind = ''i'' AND relname LIKE ''citus_ri_helper_%%''');
-- the user's REPLICA IDENTITY FULL is preserved (Citus never touched it): 'f'
SELECT DISTINCT result FROM run_command_on_placements('t_temp_full', 'SELECT relreplident FROM pg_class WHERE oid = ''%s''::regclass');
-- drop this intentionally-FULL table so it does not trip the strong "no leftover
-- FULL shards" check below
DROP TABLE t_temp_full;

-- no leftover cleanup records after all of the moves above
SELECT count(*) AS leftover_cleanup_records FROM pg_dist_cleanup;

-- strong check: no shard of this schema on any worker is left with REPLICA
-- IDENTITY FULL ('f'). All table creation is already done, so it is safe to
-- switch connections (which resets session GUCs) from here on.
\c - - - :worker_1_port
SET citus.override_table_visibility TO off;
SELECT count(*) AS full_ri_shards_on_worker_1
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'move_no_ri' AND c.relkind = 'r' AND c.relreplident = 'f';
\c - - - :worker_2_port
SET citus.override_table_visibility TO off;
SELECT count(*) AS full_ri_shards_on_worker_2
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'move_no_ri' AND c.relkind = 'r' AND c.relreplident = 'f';
\c - - - :master_port

SET client_min_messages TO WARNING;
DROP SCHEMA move_no_ri CASCADE;
