-- citus--14.0-1--15.0-1
-- bump version to 15.0-1

#include "udfs/citus_internal_get_next_colocation_id/15.0-1.sql"

-- drop the legacy version that we kept for backward compatibility at Citus 13 and 14
DROP FUNCTION IF EXISTS pg_catalog.worker_adjust_identity_column_seq_ranges(regclass);
#include "udfs/citus_internal_adjust_identity_column_seq_settings/15.0-1.sql"

-- drop the legacy version that we kept for backward compatibility at Citus 13 and 14
DROP FUNCTION IF EXISTS pg_catalog.worker_apply_sequence_command(text, regtype);
#include "udfs/worker_apply_sequence_command/15.0-1.sql"

#include "udfs/citus_internal_lock_colocation_id/15.0-1.sql"

#include "udfs/citus_internal_acquire_placement_colocation_lock/15.0-1.sql"

-- cluster changes block UDFs
#include "udfs/citus_cluster_changes_block/15.0-1.sql"
#include "udfs/citus_cluster_changes_unblock/15.0-1.sql"
#include "udfs/citus_cluster_changes_block_status/15.0-1.sql"

-- add the force_advanced_logical shard transfer mode, which enables the extra
-- logical-replication capabilities for tables that lack a usable replica identity
ALTER TYPE citus.shard_transfer_mode ADD VALUE IF NOT EXISTS 'force_advanced_logical';
