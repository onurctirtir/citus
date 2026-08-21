--
-- METADATA_SYNC_IDEMPOTENCY
--
-- Regression test for idempotent shell-table object creation during metadata
-- sync / node activation. A previously failed or interrupted metadata sync can
-- leave an orphan shell table on a worker -- a physical table with no
-- pg_dist_partition row -- whose index or extended-statistics name collides with
-- an object that a later sync (re)creates for a different distributed table.
-- Re-activating the node must still succeed instead of failing with
-- 'relation "..." already exists' / 'statistics object "..." already exists'.
--
CREATE SCHEMA metadata_sync_idempotency;
SET search_path TO metadata_sync_idempotency;
SET citus.shard_count TO 2;
SET citus.shard_replication_factor TO 1;
SET client_min_messages TO WARNING;

CREATE TABLE dist_tbl (id bigint, payload text);
SELECT create_distributed_table('dist_tbl', 'id');
CREATE INDEX dist_tbl_payload_idx ON dist_tbl (payload);

-- Drop the synced shell tables from worker_1 so the index name is free there,
-- then plant an orphan shell table whose index name collides with dist_tbl's
-- index. The orphan is created directly on the worker with DDL propagation
-- disabled, so the coordinator holds no metadata for it (no pg_dist_partition
-- row); neither node activation nor stop_metadata_sync(clear_metadata=>true) can
-- find and drop such an orphan, because both enumerate shell tables via the
-- worker's pg_dist_partition.
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port, clear_metadata => true);

\c - - - :worker_1_port
SET citus.enable_ddl_propagation TO off;
SET client_min_messages TO WARNING;
CREATE SCHEMA IF NOT EXISTS metadata_sync_idempotency;
SET search_path TO metadata_sync_idempotency;
CREATE TABLE orphan_shell (id bigint, payload text);
CREATE INDEX dist_tbl_payload_idx ON orphan_shell (payload);

\c - - - :master_port
SET search_path TO metadata_sync_idempotency;
SET client_min_messages TO WARNING;

-- Re-activating the node must succeed despite the colliding orphan index; the
-- shell-table sync now emits DROP INDEX IF EXISTS before CREATE INDEX. Without
-- the fix this fails with 'relation "dist_tbl_payload_idx" already exists'.
SELECT citus_activate_node('localhost', :worker_1_port) IS NOT NULL AS activated;

-- The real distributed table must own its index on the worker after activation.
\c - - - :worker_1_port
SET search_path TO metadata_sync_idempotency;
SELECT indexrelid::regclass AS index_on_dist_tbl
FROM pg_index
WHERE indrelid = 'metadata_sync_idempotency.dist_tbl'::regclass
  AND indexrelid = 'metadata_sync_idempotency.dist_tbl_payload_idx'::regclass;

\c - - - :master_port
SET search_path TO metadata_sync_idempotency;
SET client_min_messages TO WARNING;

-- Extended-statistics variant of the same orphan-name-collision. Extended
-- statistics names live in a per-schema namespace shared across tables, so an
-- orphan statistics object left on a worker can collide with the CREATE
-- STATISTICS a later sync emits for a different distributed table. The
-- shell-table sync now emits DROP STATISTICS IF EXISTS before CREATE STATISTICS.
CREATE TABLE dist_stat_tbl (a int, b int);
SELECT create_distributed_table('dist_stat_tbl', 'a');
CREATE STATISTICS dist_stat ON a, b FROM dist_stat_tbl;

SELECT stop_metadata_sync_to_node('localhost', :worker_1_port, clear_metadata => true);

\c - - - :worker_1_port
SET citus.enable_ddl_propagation TO off;
SET client_min_messages TO WARNING;
SET search_path TO metadata_sync_idempotency;
CREATE TABLE orphan_stat_owner (a int, b int);
CREATE STATISTICS dist_stat ON a, b FROM orphan_stat_owner;

\c - - - :master_port
SET search_path TO metadata_sync_idempotency;
SET client_min_messages TO WARNING;

-- Re-activating the node must succeed despite the colliding orphan statistics.
-- Without the fix this fails with 'statistics object "dist_stat" already exists'.
SELECT citus_activate_node('localhost', :worker_1_port) IS NOT NULL AS activated;

-- The statistics object must now belong to the real distributed shell table.
\c - - - :worker_1_port
SET search_path TO metadata_sync_idempotency;
SELECT stxrelid::regclass AS stat_on_dist_stat_tbl
FROM pg_statistic_ext
WHERE stxname = 'dist_stat'
  AND stxnamespace = 'metadata_sync_idempotency'::regnamespace;

\c - - - :master_port
SET client_min_messages TO WARNING;
DROP SCHEMA metadata_sync_idempotency CASCADE;
