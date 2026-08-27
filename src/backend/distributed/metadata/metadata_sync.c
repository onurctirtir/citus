/*-------------------------------------------------------------------------
 *
 * metadata_sync.c
 *
 * Routines for synchronizing metadata to all workers.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_am.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/async.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "parser/parse_type.h"
#include "portability/instr_time.h"
#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "distributed/adaptive_executor.h"
#include "distributed/argutils.h"
#include "distributed/backend_data.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/deparser.h"
#include "distributed/distribution_column.h"
#include "distributed/listutils.h"
#include "distributed/maintenanced.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata/pg_dist_object.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/pg_dist_colocation.h"
#include "distributed/pg_dist_node.h"
#include "distributed/pg_dist_schema.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/tenant_schema_metadata.h"
#include "distributed/utils/array_type.h"
#include "distributed/utils/function.h"
#include "distributed/version_compat.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_transaction.h"


/* managed via a GUC */
char *EnableManualMetadataChangesForUser = "";
int MetadataSyncTransMode = METADATA_SYNC_TRANSACTIONAL;

/*
 * MetadataSyncCacheFlushInterval is the number of distributed objects we
 * process between cache flushes while syncing metadata to a node. See
 * FlushMetadataSyncCachesIfNeeded(), OrderObjectAddressListInDependencyOrder()
 * and FilterObjectAddressListByPredicate().
 * Set via citus.metadata_sync_cache_flush_interval; 0 disables the flushing.
 */
int MetadataSyncCacheFlushInterval = 1000;

/*
 * MetadataSyncBatchSize is the number of distributed objects whose
 * metadata-sync commands we accumulate and send to the activated nodes in a single
 * round-trip while syncing metadata to a node.
 *
 * Set via citus.metadata_sync_batch_size; 1 disables batching and restores the
 * one-object-per-round-trip behavior.
 */
int MetadataSyncBatchSize = 1000;

/*
 * MetadataSyncUsePool controls whether the shell table creation step of
 * metadata sync uses a pool of parallel connections to the activated node
 * (driven by the adaptive executor) instead of the serial single-connection
 * path.
 *
 * Set via citus.metadata_sync_use_pool; default false (serial path, no behavior
 * change). Only takes effect in nontransactional mode, where the parallel
 * connections cannot share one distributed transaction anyway.
 */
bool MetadataSyncUsePool = false;


static void EnsureObjectMetadataIsSane(int distributionArgumentIndex,
									   int colocationId);
static List * GetFunctionDependenciesForObjects(ObjectAddress *objectAddress);
static char * SchemaOwnerName(Oid objectId);
static bool HasMetadataWorkers(void);
static void CreateShellTableOnWorkers(Oid relationId);
static void CreateTableMetadataOnWorkers(Oid relationId);
static void CreateDependingViewsOnWorkers(Oid relationId);
static void AddTableToPublications(Oid relationId);
static NodeMetadataSyncResult SyncNodeMetadataToNodesOptional(void);
static bool MetadataSyncShellTablePoolEnabled(MetadataSyncContext *context);
static bool IsCitusShellTableDependency(const ObjectAddress *dependency);
static bool IsDependentOnShellTableObject(const ObjectAddress *dependency);
static void SendDeferredDependentCreationCommands(MetadataSyncContext *context);
typedef List * (*NodeTargetedPoolDeparseFn)(HeapTuple tuple, TupleDesc tupleDesc,
											 MetadataSyncContext *context);
static void RunNodeTargetedPoolPhase(MetadataSyncContext *context,
									 WorkerNode *workerNode, Oid scanRelationId,
									 NodeTargetedPoolDeparseFn deparseFn,
									 const char *objectLabel);
static List * ShellTablePoolDeparse(HeapTuple tuple, TupleDesc tupleDesc,
									MetadataSyncContext *context);
static List * SequencePoolDeparse(HeapTuple tuple, TupleDesc tupleDesc,
								  MetadataSyncContext *context);
static List * DistTableMetadataPoolDeparse(HeapTuple tuple, TupleDesc tupleDesc,
										   MetadataSyncContext *context);
static List * DistObjectMarkPoolDeparse(HeapTuple tuple, TupleDesc tupleDesc,
										MetadataSyncContext *context);
static List * CreateNodeTargetedPoolTaskList(List *commandListPerTask,
											 WorkerNode *workerNode);
static bool ShouldSyncTableMetadataInternal(bool hashDistributed,
											bool citusTableWithNoDistKey);
static bool SyncNodeMetadataSnapshotToNode(WorkerNode *workerNode, bool raiseOnError);
static void FlushMetadataSyncCachesIfNeeded(MetadataSyncContext *context,
											int64 processedCount);
static bool MetadataSyncBatchingEnabled(MetadataSyncContext *context);
static void InitializeActivatedNodeCommandBatch(MetadataSyncContext *context,
												MemoryContext commandsContext);
static void SendOrBatchCommandListToActivatedNodes(MetadataSyncContext *context,
												   List *commandList);
static void FlushBatchedCommandsToActivatedNodes(MetadataSyncContext *context);
static void DropMetadataSnapshotOnNode(WorkerNode *workerNode);
static char * CreateSequenceDependencyCommand(Oid relationId, Oid sequenceId,
											  char *columnName);
static GrantStmt * GenerateGrantStmtForRights(ObjectType objectType,
											  Oid roleOid,
											  Oid objectId,
											  char *permission,
											  bool withGrantOption);
static List * GetObjectsForGrantStmt(ObjectType objectType, Oid objectId);
static AccessPriv * GetAccessPrivObjectForGrantStmt(char *permission);
static List * GenerateGrantOnSchemaQueriesFromAclItem(Oid schemaOid,
													  AclItem *aclItem);
static List * GenerateGrantOnFunctionQueriesFromAclItem(Oid schemaOid,
														AclItem *aclItem);
static List * GrantOnSequenceDDLCommands(Oid sequenceOid);
static List * GenerateGrantOnSequenceQueriesFromAclItem(Oid sequenceOid,
														AclItem *aclItem);
static char * GenerateSetRoleQuery(Oid roleOid);
static void MetadataSyncSigTermHandler(SIGNAL_ARGS);
static void MetadataSyncSigAlrmHandler(SIGNAL_ARGS);


static bool ShouldSkipMetadataChecks(void);
static void EnsurePartitionMetadataIsSane(Oid relationId, char distributionMethod,
										  int colocationId, char replicationModel,
										  Var *distributionKey);
static void EnsureCoordinatorInitiatedOperation(void);
static void EnsureShardMetadataIsSane(Oid relationId, int64 shardId, char storageType,
									  text *shardMinValue,
									  text *shardMaxValue);
static void EnsureShardPlacementMetadataIsSane(Oid relationId, int64 shardId,
											   int64 placementId,
											   int64 shardLength, int32 groupId);
static char * ColocationGroupCreateCommand(uint32 colocationId, int shardCount,
										   int replicationFactor,
										   Oid distributionColumnType,
										   Oid distributionColumnCollation);
static char * ColocationGroupDeleteCommand(uint32 colocationId);
static char * RemoteSchemaIdExpressionById(Oid schemaId);
static char * RemoteSchemaIdExpressionByName(char *schemaName);
static char * RemoteTypeIdExpression(Oid typeId);
static char * RemoteCollationIdExpression(Oid colocationId);
static char * RemoteTableIdExpression(Oid relationId);


PG_FUNCTION_INFO_V1(start_metadata_sync_to_all_nodes);
PG_FUNCTION_INFO_V1(start_metadata_sync_to_node);
PG_FUNCTION_INFO_V1(stop_metadata_sync_to_node);
PG_FUNCTION_INFO_V1(worker_record_sequence_dependency);


/*
 * Functions to modify metadata. Normally modifying metadata requires
 * superuser. However, these functions can be called with superusers
 * or regular users as long as the regular user owns the input object.
 */
PG_FUNCTION_INFO_V1(citus_internal_add_partition_metadata);
PG_FUNCTION_INFO_V1(citus_internal_delete_partition_metadata);
PG_FUNCTION_INFO_V1(citus_internal_add_shard_metadata);
PG_FUNCTION_INFO_V1(citus_internal_add_placement_metadata);
PG_FUNCTION_INFO_V1(citus_internal_delete_placement_metadata);
PG_FUNCTION_INFO_V1(citus_internal_add_placement_metadata_legacy);
PG_FUNCTION_INFO_V1(citus_internal_update_placement_metadata);
PG_FUNCTION_INFO_V1(citus_internal_delete_shard_metadata);
PG_FUNCTION_INFO_V1(citus_internal_update_relation_colocation);
PG_FUNCTION_INFO_V1(citus_internal_add_object_metadata);
PG_FUNCTION_INFO_V1(citus_internal_add_colocation_metadata);
PG_FUNCTION_INFO_V1(citus_internal_delete_colocation_metadata);
PG_FUNCTION_INFO_V1(citus_internal_add_tenant_schema);
PG_FUNCTION_INFO_V1(citus_internal_delete_tenant_schema);
PG_FUNCTION_INFO_V1(citus_internal_update_none_dist_table_metadata);


static bool got_SIGTERM = false;
static bool got_SIGALRM = false;

#define METADATA_SYNC_APP_NAME "Citus Metadata Sync Daemon"


/*
 * start_metadata_sync_to_node function sets hasmetadata column of the given
 * node to true, and then activate node without replicating reference tables.
 */
Datum
start_metadata_sync_to_node(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);

	EnsureSuperUser();
	EnsureCoordinator();

	char *nodeNameString = text_to_cstring(nodeName);
	WorkerNode *workerNode = ModifiableWorkerNode(nodeNameString, nodePort);

	/*
	 * Create MetadataSyncContext which is used throughout nodes' activation.
	 * It contains activated nodes, bare connections if the mode is nontransactional,
	 * and a memory context for allocation.
	 */
	bool collectCommands = false;
	bool nodesAddedInSameTransaction = false;
	MetadataSyncContext *context = CreateMetadataSyncContext(list_make1(workerNode),
															 collectCommands,
															 nodesAddedInSameTransaction);

	ActivateNodeList(context);
	TransactionModifiedNodeMetadata = true;

	PG_RETURN_VOID();
}


/*
 * start_metadata_sync_to_all_nodes function sets hasmetadata column of
 * all the primary worker nodes to true, and then activate nodes without
 * replicating reference tables.
 */
Datum
start_metadata_sync_to_all_nodes(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	EnsureSuperUser();
	EnsureCoordinator();

	List *nodeList = ActivePrimaryNonCoordinatorNodeList(RowShareLock);

	/*
	 * Create MetadataSyncContext which is used throughout nodes' activation.
	 * It contains activated nodes, bare connections if the mode is nontransactional,
	 * and a memory context for allocation.
	 */
	bool collectCommands = false;
	bool nodesAddedInSameTransaction = false;
	MetadataSyncContext *context = CreateMetadataSyncContext(nodeList,
															 collectCommands,
															 nodesAddedInSameTransaction);

	ActivateNodeList(context);
	TransactionModifiedNodeMetadata = true;

	PG_RETURN_BOOL(true);
}


/*
 * SyncCitusTableMetadata syncs citus table metadata to worker nodes with metadata.
 * Our definition of metadata includes the shell table and its inter relations with
 * other shell tables, corresponding pg_dist_object, pg_dist_partiton, pg_dist_shard
 * and pg_dist_shard placement entries. This function also propagates the views that
 * depend on the given relation, to the metadata workers, and adds the relation to
 * the appropriate publications.
 */
void
SyncCitusTableMetadata(Oid relationId)
{
	CreateShellTableOnWorkers(relationId);
	CreateTableMetadataOnWorkers(relationId);
	CreateInterTableRelationshipOfRelationOnWorkers(relationId);

	if (!IsTableOwnedByExtension(relationId))
	{
		ObjectAddress relationAddress = { 0 };
		ObjectAddressSet(relationAddress, RelationRelationId, relationId);
		MarkObjectDistributed(&relationAddress);
	}

	CreateDependingViewsOnWorkers(relationId);
	AddTableToPublications(relationId);
}


/*
 * CreateDependingViewsOnWorkers takes a relationId and creates the views that depend on
 * that relation on workers with metadata. Propagated views are marked as distributed.
 */
static void
CreateDependingViewsOnWorkers(Oid relationId)
{
	List *views = GetDependingViews(relationId);

	if (list_length(views) < 1)
	{
		/* no view to propagate */
		return;
	}

	SendCommandToWorkersWithMetadata(DISABLE_DDL_PROPAGATION);

	Oid viewOid = InvalidOid;
	foreach_oid(viewOid, views)
	{
		if (!ShouldMarkRelationDistributed(viewOid))
		{
			continue;
		}

		ObjectAddress *viewAddress = palloc0(sizeof(ObjectAddress));
		ObjectAddressSet(*viewAddress, RelationRelationId, viewOid);
		EnsureAllObjectDependenciesExistOnAllNodes(list_make1(viewAddress));

		char *createViewCommand = CreateViewDDLCommand(viewOid);
		char *alterViewOwnerCommand = AlterViewOwnerCommand(viewOid);

		SendCommandToWorkersWithMetadata(createViewCommand);
		SendCommandToWorkersWithMetadata(alterViewOwnerCommand);

		MarkObjectDistributed(viewAddress);
	}

	SendCommandToWorkersWithMetadata(ENABLE_DDL_PROPAGATION);
}


/*
 * AddTableToPublications adds the table to a publication on workers with metadata.
 */
static void
AddTableToPublications(Oid relationId)
{
	List *publicationIds = GetRelationPublications(relationId);
	if (publicationIds == NIL)
	{
		return;
	}

	Oid publicationId = InvalidOid;

	SendCommandToWorkersWithMetadata(DISABLE_DDL_PROPAGATION);

	foreach_oid(publicationId, publicationIds)
	{
		ObjectAddress *publicationAddress = palloc0(sizeof(ObjectAddress));
		ObjectAddressSet(*publicationAddress, PublicationRelationId, publicationId);
		List *addresses = list_make1(publicationAddress);

		if (!ShouldPropagateAnyObject(addresses))
		{
			/* skip non-distributed publications */
			continue;
		}

		/* ensure schemas exist */
		EnsureAllObjectDependenciesExistOnAllNodes(addresses);

		bool isAdd = true;
		char *alterPublicationCommand =
			GetAlterPublicationTableDDLCommand(publicationId, relationId, isAdd);

		/* send ALTER PUBLICATION .. ADD to workers with metadata */
		SendCommandToWorkersWithMetadata(alterPublicationCommand);
	}

	SendCommandToWorkersWithMetadata(ENABLE_DDL_PROPAGATION);
}


/*
 * EnsureSequentialModeMetadataOperations makes sure that the current transaction is
 * already in sequential mode, or can still safely be put in sequential mode,
 * it errors if that is not possible. The error contains information for the user to
 * retry the transaction with sequential mode set from the beginning.
 *
 * Metadata objects (e.g., distributed table on the workers) exists only 1 instance of
 * the type used by potentially multiple other shards/connections. To make sure all
 * shards/connections in the transaction can interact with the metadata needs to be
 * visible on all connections used by the transaction, meaning we can only use 1
 * connection per node.
 */
void
EnsureSequentialModeMetadataOperations(void)
{
	if (!IsTransactionBlock())
	{
		/* we do not need to switch to sequential mode if we are not in a transaction */
		return;
	}

	if (ParallelQueryExecutedInTransaction())
	{
		ereport(ERROR, (errmsg(
							"cannot execute metadata syncing operation because there was a "
							"parallel operation on a distributed table in the "
							"transaction"),
						errdetail("When modifying metadata, Citus needs to "
								  "perform all operations over a single connection per "
								  "node to ensure consistency."),
						errhint("Try re-running the transaction with "
								"\"SET LOCAL citus.multi_shard_modify_mode TO "
								"\'sequential\';\"")));
	}

	ereport(DEBUG1, (errmsg("switching to sequential query execution mode"),
					 errdetail("Metadata synced or stopped syncing. To make "
							   "sure subsequent commands see the metadata correctly "
							   "we need to make sure to use only one connection for "
							   "all future commands")));
	SetLocalMultiShardModifyModeToSequential();
}


/*
 * stop_metadata_sync_to_node function sets the hasmetadata column of the specified node
 * to false in pg_dist_node table, thus indicating that the specified worker node does not
 * receive DDL changes anymore and cannot be used for issuing queries.
 */
Datum
stop_metadata_sync_to_node(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();
	EnsureSuperUser();

	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	bool clearMetadata = PG_GETARG_BOOL(2);
	char *nodeNameString = text_to_cstring(nodeName);

	LockRelationOid(DistNodeRelationId(), ExclusiveLock);

	WorkerNode *workerNode = FindWorkerNodeAnyCluster(nodeNameString, nodePort);
	if (workerNode == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("node (%s,%d) does not exist", nodeNameString, nodePort)));
	}

	if (NodeIsCoordinator(workerNode))
	{
		ereport(NOTICE, (errmsg("node (%s,%d) is the coordinator and should have "
								"metadata, skipping stopping the metadata sync",
								nodeNameString, nodePort)));
		PG_RETURN_VOID();
	}

	if (clearMetadata)
	{
		if (NodeIsPrimary(workerNode))
		{
			ereport(NOTICE, (errmsg("dropping metadata on the node (%s,%d)",
									nodeNameString, nodePort)));
			DropMetadataSnapshotOnNode(workerNode);
		}
		else
		{
			/*
			 * If this is a secondary node we can't actually clear metadata from it,
			 * we assume the primary node is cleared.
			 */
			ereport(NOTICE, (errmsg("(%s,%d) is a secondary node: to clear the metadata,"
									" you should clear metadata from the primary node",
									nodeNameString, nodePort)));
		}
	}

	workerNode = SetWorkerColumn(workerNode, Anum_pg_dist_node_hasmetadata, BoolGetDatum(
									 false));
	workerNode = SetWorkerColumn(workerNode, Anum_pg_dist_node_metadatasynced,
								 BoolGetDatum(false));

	TransactionModifiedNodeMetadata = true;

	PG_RETURN_VOID();
}


/*
 * ClusterHasKnownMetadataWorkers returns true if the node executing the function
 * knows at least one worker with metadata. We do it
 * (a) by checking the node that executes the function is a worker with metadata
 * (b) the coordinator knows at least one worker with metadata.
 */
bool
ClusterHasKnownMetadataWorkers()
{
	bool workerWithMetadata = false;

	if (!IsCoordinator())
	{
		workerWithMetadata = true;
	}

	if (workerWithMetadata || HasMetadataWorkers())
	{
		return true;
	}

	return false;
}


/*
 * ShouldSyncUserCommandForObject checks if the user command should be synced to the
 * worker nodes for the given object.
 */
bool
ShouldSyncUserCommandForObject(ObjectAddress objectAddress)
{
	if (objectAddress.classId == RelationRelationId)
	{
		Oid relOid = objectAddress.objectId;
		return ShouldSyncTableMetadata(relOid) ||
			   ShouldSyncSequenceMetadata(relOid) ||
			   get_rel_relkind(relOid) == RELKIND_VIEW;
	}

	return false;
}


/*
 * ShouldSyncTableMetadata checks if the metadata of a distributed table should be
 * propagated to metadata workers, i.e. the table is a hash distributed table or
 * a Citus table that doesn't have shard key.
 */
bool
ShouldSyncTableMetadata(Oid relationId)
{
	if (!EnableMetadataSync ||
		!OidIsValid(relationId) || !IsCitusTable(relationId))
	{
		return false;
	}

	CitusTableCacheEntry *tableEntry = GetCitusTableCacheEntry(relationId);

	bool hashDistributed = IsCitusTableTypeCacheEntry(tableEntry, HASH_DISTRIBUTED);
	bool citusTableWithNoDistKey =
		!HasDistributionKeyCacheEntry(tableEntry);

	return ShouldSyncTableMetadataInternal(hashDistributed, citusTableWithNoDistKey);
}


/*
 * ShouldSyncTableMetadataViaCatalog checks if the metadata of a Citus table should
 * be propagated to metadata workers, i.e. the table is an MX table or Citus table
 * that doesn't have shard key.
 * Tables with streaming replication model (which means RF=1) and hash distribution are
 * considered as MX tables.
 *
 * ShouldSyncTableMetadataViaCatalog does not use the CitusTableCache and instead reads
 * from catalog tables directly.
 */
bool
ShouldSyncTableMetadataViaCatalog(Oid relationId)
{
	if (!OidIsValid(relationId) || !IsCitusTableViaCatalog(relationId))
	{
		return false;
	}

	char partitionMethod = PartitionMethodViaCatalog(relationId);
	bool hashDistributed = partitionMethod == DISTRIBUTE_BY_HASH;
	bool citusTableWithNoDistKey = partitionMethod == DISTRIBUTE_BY_NONE;

	return ShouldSyncTableMetadataInternal(hashDistributed, citusTableWithNoDistKey);
}


/*
 * FetchRelationIdFromPgPartitionHeapTuple returns relation id from given heap tuple.
 */
Oid
FetchRelationIdFromPgPartitionHeapTuple(HeapTuple heapTuple, TupleDesc tupleDesc)
{
	Assert(heapTuple->t_tableOid == DistPartitionRelationId());

	bool isNullArray[Natts_pg_dist_partition];
	Datum datumArray[Natts_pg_dist_partition];
	heap_deform_tuple(heapTuple, tupleDesc, datumArray, isNullArray);

	Datum relationIdDatum = datumArray[Anum_pg_dist_partition_logicalrelid - 1];
	Oid relationId = DatumGetObjectId(relationIdDatum);

	return relationId;
}


/*
 * ShouldSyncTableMetadataInternal decides whether we should sync the metadata for a table
 * based on whether it is a hash distributed table, or a citus table with no distribution
 * key.
 *
 * This function is here to make sure that ShouldSyncTableMetadata and
 * ShouldSyncTableMetadataViaCatalog behaves the same way.
 */
static bool
ShouldSyncTableMetadataInternal(bool hashDistributed, bool citusTableWithNoDistKey)
{
	return hashDistributed || citusTableWithNoDistKey;
}


/*
 * ShouldSyncSequenceMetadata checks if the metadata of a sequence should be
 * propagated to metadata workers, i.e. the sequence is marked as distributed
 */
bool
ShouldSyncSequenceMetadata(Oid relationId)
{
	if (!OidIsValid(relationId) || !(get_rel_relkind(relationId) == RELKIND_SEQUENCE))
	{
		return false;
	}

	ObjectAddress *sequenceAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*sequenceAddress, RelationRelationId, relationId);

	return IsAnyObjectDistributed(list_make1(sequenceAddress));
}


/*
 * MetadataSyncCacheFlushIntervalReached returns true if the number of processed
 * distributed relations has reached the configured flush interval.
 * See citus.metadata_sync_cache_flush_interval GUC.
 */
bool
MetadataSyncCacheFlushIntervalReached(int64 processedCount)
{
	return MetadataSyncCacheFlushInterval > 0 &&
		   processedCount % MetadataSyncCacheFlushInterval == 0;
}


/*
 * FlushCachesForMetadataSync flushes the Citus distributed-table and distributed-object
 * caches, as well as the postgres relcache/catcache entries.
 */
void
FlushCachesForMetadataSync(void)
{
	/* free the Citus distributed-table and distributed-object caches built so far */
	FlushDistTableCache();
	FlushDistObjectCache();

	/* free the PostgreSQL relcache/catcache entries built so far */
	InvalidateSystemCaches();
}


/*
 * SyncMetadataSnapshotToNode does the following:
 * SyncNodeMetadataSnapshotToNode does the following:
 *  1. Sets the localGroupId on the worker so the worker knows which tuple in
 *     pg_dist_node represents itself.
 *  2. Recreates the node metadata on the given worker.
 * If raiseOnError is true, it errors out if synchronization fails.
 */
static bool
SyncNodeMetadataSnapshotToNode(WorkerNode *workerNode, bool raiseOnError)
{
	char *currentUser = CurrentUserName();

	/* generate and add the local group id's update query */
	char *localGroupIdUpdateCommand = LocalGroupIdUpdateCommand(workerNode->groupId);

	/* generate the queries which drop the node metadata */
	List *dropMetadataCommandList = NodeMetadataDropCommands();

	/* generate the queries which create the node metadata from scratch */
	List *createMetadataCommandList = NodeMetadataCreateCommands();

	List *recreateMetadataSnapshotCommandList = list_make1(localGroupIdUpdateCommand);
	recreateMetadataSnapshotCommandList = list_concat(recreateMetadataSnapshotCommandList,
													  dropMetadataCommandList);
	recreateMetadataSnapshotCommandList = list_concat(recreateMetadataSnapshotCommandList,
													  createMetadataCommandList);

	/*
	 * Send the snapshot recreation commands in a single remote transaction and
	 * if requested, error out in any kind of failure. Note that it is not
	 * required to send createMetadataSnapshotCommandList in the same transaction
	 * that we send nodeDeleteCommand and nodeInsertCommand commands below.
	 */
	if (raiseOnError)
	{
		SendMetadataCommandListToWorkerListInCoordinatedTransaction(list_make1(
																		workerNode),
																	currentUser,
																	recreateMetadataSnapshotCommandList);
		return true;
	}
	else
	{
		bool success =
			SendOptionalMetadataCommandListToWorkerInCoordinatedTransaction(
				workerNode->workerName, workerNode->workerPort,
				currentUser, recreateMetadataSnapshotCommandList);

		return success;
	}
}


/*
 * DropMetadataSnapshotOnNode creates the queries which drop the metadata and sends them
 * to the worker given as parameter.
 */
static void
DropMetadataSnapshotOnNode(WorkerNode *workerNode)
{
	EnsureSequentialModeMetadataOperations();

	char *userName = CurrentUserName();

	/*
	 * Detach partitions, break dependencies between sequences and table then
	 * remove shell tables first.
	 */
	bool singleTransaction = true;
	List *dropMetadataCommandList = DetachPartitionCommandList();
	dropMetadataCommandList = lappend(dropMetadataCommandList,
									  BREAK_ALL_CITUS_TABLE_SEQUENCE_DEPENDENCY_COMMAND);
	dropMetadataCommandList = lappend(dropMetadataCommandList,
									  WorkerDropAllShellTablesCommand(singleTransaction));
	dropMetadataCommandList = list_concat(dropMetadataCommandList,
										  NodeMetadataDropCommands());
	dropMetadataCommandList = lappend(dropMetadataCommandList,
									  LocalGroupIdUpdateCommand(0));

	/* remove all dist table and object/table related metadata afterwards */
	dropMetadataCommandList = lappend(dropMetadataCommandList, DELETE_ALL_PARTITIONS);
	dropMetadataCommandList = lappend(dropMetadataCommandList, DELETE_ALL_SHARDS);
	dropMetadataCommandList = lappend(dropMetadataCommandList, DELETE_ALL_PLACEMENTS);
	dropMetadataCommandList = lappend(dropMetadataCommandList,
									  DELETE_ALL_DISTRIBUTED_OBJECTS);
	dropMetadataCommandList = lappend(dropMetadataCommandList, DELETE_ALL_COLOCATION);

	Assert(superuser());
	SendOptionalMetadataCommandListToWorkerInCoordinatedTransaction(
		workerNode->workerName,
		workerNode->workerPort,
		userName,
		dropMetadataCommandList);
}


/*
 * NodeMetadataCreateCommands returns list of queries that are
 * required to create the current metadata snapshot of the node that the
 * function is called. The metadata snapshot commands includes the
 * following queries:
 *
 * (i)   Query that populates pg_dist_node table
 */
List *
NodeMetadataCreateCommands(void)
{
	List *metadataSnapshotCommandList = NIL;
	bool includeNodesFromOtherClusters = true;
	List *workerNodeList = ReadDistNode(includeNodesFromOtherClusters);

	/* make sure we have deterministic output for our tests */
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	/* generate insert command for pg_dist_node table */
	char *nodeListInsertCommand = NodeListInsertCommand(workerNodeList);
	metadataSnapshotCommandList = lappend(metadataSnapshotCommandList,
										  nodeListInsertCommand);

	return metadataSnapshotCommandList;
}


/*
 * CitusTableMetadataCreateCommandList returns the set of commands necessary to
 * create the given distributed table metadata on a worker.
 */
List *
CitusTableMetadataCreateCommandList(Oid relationId)
{
	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);

	List *commandList = NIL;

	/* command to insert pg_dist_partition entry */
	char *metadataCommand = DistributionCreateCommand(cacheEntry);
	commandList = lappend(commandList, metadataCommand);

	/* commands to insert pg_dist_shard & pg_dist_placement entries */
	List *shardIntervalList = LoadShardIntervalList(relationId);
	List *shardMetadataInsertCommandList = ShardListInsertCommand(shardIntervalList);
	commandList = list_concat(commandList, shardMetadataInsertCommandList);

	return commandList;
}


/*
 * NodeMetadataDropCommands returns list of queries that are required to
 * drop all the metadata of the node that are not related to clustered tables.
 * The drop metadata snapshot commands includes the following queries:
 *
 * (i) Queries that delete all the rows from pg_dist_node table
 */
List *
NodeMetadataDropCommands(void)
{
	List *dropSnapshotCommandList = NIL;

	dropSnapshotCommandList = lappend(dropSnapshotCommandList, DELETE_ALL_NODES);

	return dropSnapshotCommandList;
}


/*
 * NodeListInsertCommand generates a single multi-row INSERT command that can be
 * executed to insert the nodes that are in workerNodeList to pg_dist_node table.
 */
char *
NodeListInsertCommand(List *workerNodeList)
{
	StringInfo nodeListInsertCommand = makeStringInfo();
	int workerCount = list_length(workerNodeList);
	int processedWorkerNodeCount = 0;
	Oid primaryRole = PrimaryNodeRoleId();

	/* if there are no workers, return NULL */
	if (workerCount == 0)
	{
		return nodeListInsertCommand->data;
	}

	if (primaryRole == InvalidOid)
	{
		ereport(ERROR, (errmsg("bad metadata, noderole does not exist"),
						errdetail("you should never see this, please submit "
								  "a bug report"),
						errhint("run ALTER EXTENSION citus UPDATE and try again")));
	}

	/* generate the query without any values yet */
	appendStringInfo(nodeListInsertCommand,
					 "INSERT INTO pg_dist_node (nodeid, groupid, nodename, nodeport, "
					 "noderack, hasmetadata, metadatasynced, isactive, noderole, "
					 "nodecluster, shouldhaveshards) VALUES ");

	/* iterate over the worker nodes, add the values */
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		char *hasMetadataString = workerNode->hasMetadata ? "TRUE" : "FALSE";
		char *metadataSyncedString = workerNode->metadataSynced ? "TRUE" : "FALSE";
		char *isActiveString = workerNode->isActive ? "TRUE" : "FALSE";
		char *shouldHaveShards = workerNode->shouldHaveShards ? "TRUE" : "FALSE";

		Datum nodeRoleOidDatum = ObjectIdGetDatum(workerNode->nodeRole);
		Datum nodeRoleStringDatum = DirectFunctionCall1(enum_out, nodeRoleOidDatum);
		char *nodeRoleString = DatumGetCString(nodeRoleStringDatum);

		appendStringInfo(nodeListInsertCommand,
						 "(%d, %d, %s, %d, %s, %s, %s, %s, '%s'::noderole, %s, %s)",
						 workerNode->nodeId,
						 workerNode->groupId,
						 quote_literal_cstr(workerNode->workerName),
						 workerNode->workerPort,
						 quote_literal_cstr(workerNode->workerRack),
						 hasMetadataString,
						 metadataSyncedString,
						 isActiveString,
						 nodeRoleString,
						 quote_literal_cstr(workerNode->nodeCluster),
						 shouldHaveShards);

		processedWorkerNodeCount++;
		if (processedWorkerNodeCount != workerCount)
		{
			appendStringInfo(nodeListInsertCommand, ",");
		}
	}

	return nodeListInsertCommand->data;
}


/*
 * NodeListIdempotentInsertCommand generates an idempotent multi-row INSERT command that
 * can be executed to insert the nodes that are in workerNodeList to pg_dist_node table.
 * It would insert new nodes or replace current nodes with new nodes if nodename-nodeport
 * pairs already exist.
 */
char *
NodeListIdempotentInsertCommand(List *workerNodeList)
{
	StringInfo nodeInsertIdempotentCommand = makeStringInfo();
	char *nodeInsertStr = NodeListInsertCommand(workerNodeList);
	appendStringInfoString(nodeInsertIdempotentCommand, nodeInsertStr);
	char *onConflictStr = " ON CONFLICT ON CONSTRAINT pg_dist_node_nodename_nodeport_key "
						  "DO UPDATE SET nodeid = EXCLUDED.nodeid, "
						  "groupid = EXCLUDED.groupid, "
						  "nodename = EXCLUDED.nodename, "
						  "nodeport = EXCLUDED.nodeport, "
						  "noderack = EXCLUDED.noderack, "
						  "hasmetadata = EXCLUDED.hasmetadata, "
						  "isactive = EXCLUDED.isactive, "
						  "noderole = EXCLUDED.noderole, "
						  "nodecluster = EXCLUDED.nodecluster ,"
						  "metadatasynced = EXCLUDED.metadatasynced, "
						  "shouldhaveshards = EXCLUDED.shouldhaveshards";
	appendStringInfoString(nodeInsertIdempotentCommand, onConflictStr);
	return nodeInsertIdempotentCommand->data;
}


/*
 * MarkObjectsDistributedCreateCommand generates a command that can be executed to
 * insert or update the provided objects into pg_dist_object on a worker node.
 */
char *
MarkObjectsDistributedCreateCommand(List *addresses,
									List *distributionArgumentIndexes,
									List *colocationIds,
									List *forceDelegations)
{
	StringInfo insertDistributedObjectsCommand = makeStringInfo();

	Assert(list_length(addresses) == list_length(distributionArgumentIndexes));
	Assert(list_length(distributionArgumentIndexes) == list_length(colocationIds));

	appendStringInfo(insertDistributedObjectsCommand,
					 "WITH distributed_object_data(typetext, objnames, "
					 "objargs, distargumentindex, colocationid, force_delegation)  AS (VALUES ");

	bool isFirstObject = true;
	for (int currentObjectCounter = 0; currentObjectCounter < list_length(addresses);
		 currentObjectCounter++)
	{
		ObjectAddress *address = list_nth(addresses, currentObjectCounter);
		int distributionArgumentIndex = list_nth_int(distributionArgumentIndexes,
													 currentObjectCounter);
		int colocationId = list_nth_int(colocationIds, currentObjectCounter);
		int forceDelegation = list_nth_int(forceDelegations, currentObjectCounter);
		List *names = NIL;
		List *args = NIL;

		char *objectType = getObjectTypeDescription(address, false);
		getObjectIdentityParts(address, &names, &args, false);

		if (!isFirstObject)
		{
			appendStringInfo(insertDistributedObjectsCommand, ", ");
		}
		isFirstObject = false;

		appendStringInfo(insertDistributedObjectsCommand,
						 "(%s, ARRAY[",
						 quote_literal_cstr(objectType));

		char *name = NULL;
		bool firstInNameLoop = true;
		foreach_ptr(name, names)
		{
			if (!firstInNameLoop)
			{
				appendStringInfo(insertDistributedObjectsCommand, ", ");
			}
			firstInNameLoop = false;
			appendStringInfoString(insertDistributedObjectsCommand,
								   quote_literal_cstr(name));
		}

		appendStringInfo(insertDistributedObjectsCommand, "]::text[], ARRAY[");

		char *arg;
		bool firstInArgLoop = true;
		foreach_ptr(arg, args)
		{
			if (!firstInArgLoop)
			{
				appendStringInfo(insertDistributedObjectsCommand, ", ");
			}
			firstInArgLoop = false;
			appendStringInfoString(insertDistributedObjectsCommand,
								   quote_literal_cstr(arg));
		}

		appendStringInfo(insertDistributedObjectsCommand, "]::text[], ");

		appendStringInfo(insertDistributedObjectsCommand, "%d, ",
						 distributionArgumentIndex);

		appendStringInfo(insertDistributedObjectsCommand, "%d, ",
						 colocationId);

		appendStringInfo(insertDistributedObjectsCommand, "%s)",
						 forceDelegation ? "true" : "false");
	}

	appendStringInfo(insertDistributedObjectsCommand, ") ");

	appendStringInfo(insertDistributedObjectsCommand,
					 "SELECT citus_internal_add_object_metadata("
					 "typetext, objnames, objargs, distargumentindex::int, colocationid::int, force_delegation::bool) "
					 "FROM distributed_object_data;");

	return insertDistributedObjectsCommand->data;
}


/*
 * citus_internal_add_object_metadata is an internal UDF to
 * add a row to pg_dist_object.
 */
Datum
citus_internal_add_object_metadata(PG_FUNCTION_ARGS)
{
	char *textType = TextDatumGetCString(PG_GETARG_DATUM(0));
	ArrayType *nameArray = PG_GETARG_ARRAYTYPE_P(1);
	ArrayType *argsArray = PG_GETARG_ARRAYTYPE_P(2);
	int distributionArgumentIndex = PG_GETARG_INT32(3);
	int colocationId = PG_GETARG_INT32(4);
	bool forceDelegation = PG_GETARG_INT32(5);

	if (!ShouldSkipMetadataChecks())
	{
		/* this UDF is not allowed for executing as a separate command */
		EnsureCoordinatorInitiatedOperation();

		/*
		 * Ensure given distributionArgumentIndex and colocationId values are
		 * sane. Since we check sanity of object related parameters within
		 * PgGetObjectAddress below, we are not checking them here.
		 */
		EnsureObjectMetadataIsSane(distributionArgumentIndex, colocationId);
	}

	/*
	 * We check the acl/ownership while getting the object address. That
	 * funtion also checks the sanity of given textType, nameArray and
	 * argsArray parameters
	 */
	ObjectAddress objectAddress = PgGetObjectAddress(textType, nameArray,
													 argsArray);

	/* First, disable propagation off to not to cause infinite propagation */
	bool prevDependencyCreationValue = EnableMetadataSync;
	SetLocalEnableMetadataSync(false);

	MarkObjectDistributed(&objectAddress);

	if (distributionArgumentIndex != INVALID_DISTRIBUTION_ARGUMENT_INDEX ||
		colocationId != INVALID_COLOCATION_ID)
	{
		int *distributionArgumentIndexAddress =
			distributionArgumentIndex == INVALID_DISTRIBUTION_ARGUMENT_INDEX ?
			NULL :
			&distributionArgumentIndex;

		int *colocationIdAddress =
			colocationId == INVALID_COLOCATION_ID ?
			NULL :
			&colocationId;

		bool *forceDelegationAddress =
			forceDelegation == false ?
			NULL :
			&forceDelegation;
		UpdateFunctionDistributionInfo(&objectAddress,
									   distributionArgumentIndexAddress,
									   colocationIdAddress,
									   forceDelegationAddress);
	}

	SetLocalEnableMetadataSync(prevDependencyCreationValue);

	PG_RETURN_VOID();
}


/*
 * EnsureObjectMetadataIsSane checks whether the distribution argument index and
 * colocation id metadata params for distributed object is sane. You can look
 * PgGetObjectAddress to find checks related to object sanity.
 */
static void
EnsureObjectMetadataIsSane(int distributionArgumentIndex, int colocationId)
{
	if (distributionArgumentIndex != INVALID_DISTRIBUTION_ARGUMENT_INDEX)
	{
		if (distributionArgumentIndex < 0 ||
			distributionArgumentIndex > FUNC_MAX_ARGS)
		{
			ereport(ERROR, errmsg("distribution_argument_index must be between"
								  " 0 and %d", FUNC_MAX_ARGS));
		}
	}

	if (colocationId != INVALID_COLOCATION_ID)
	{
		if (colocationId < 0)
		{
			ereport(ERROR, errmsg("colocationId must be a positive number"));
		}
	}
}


/*
 * DistributionCreateCommands generates a commands that can be
 * executed to replicate the metadata for a Citus table.
 */
char *
DistributionCreateCommand(CitusTableCacheEntry *cacheEntry)
{
	StringInfo insertDistributionCommand = makeStringInfo();
	Oid relationId = cacheEntry->relationId;
	char distributionMethod = cacheEntry->partitionMethod;
	char *qualifiedRelationName =
		generate_qualified_relation_name(relationId);
	uint32 colocationId = cacheEntry->colocationId;
	char replicationModel = cacheEntry->replicationModel;
	StringInfo tablePartitionKeyNameString = makeStringInfo();

	if (!HasDistributionKeyCacheEntry(cacheEntry))
	{
		appendStringInfo(tablePartitionKeyNameString, "NULL");
	}
	else
	{
		char *partitionKeyColumnName =
			ColumnToColumnName(relationId, (Node *) cacheEntry->partitionColumn);
		appendStringInfo(tablePartitionKeyNameString, "%s",
						 quote_literal_cstr(partitionKeyColumnName));
	}

	appendStringInfo(insertDistributionCommand,
					 "SELECT citus_internal_add_partition_metadata "
					 "(%s::regclass, '%c', %s, %d, '%c')",
					 quote_literal_cstr(qualifiedRelationName),
					 distributionMethod,
					 tablePartitionKeyNameString->data,
					 colocationId,
					 replicationModel);

	return insertDistributionCommand->data;
}


/*
 * DistributionDeleteCommand generates a command that can be executed
 * to drop a distributed table and its metadata on a remote node.
 */
char *
DistributionDeleteCommand(const char *schemaName, const char *tableName)
{
	StringInfo deleteDistributionCommand = makeStringInfo();

	char *distributedRelationName = quote_qualified_identifier(schemaName, tableName);

	appendStringInfo(deleteDistributionCommand,
					 "SELECT worker_drop_distributed_table(%s)",
					 quote_literal_cstr(distributedRelationName));

	return deleteDistributionCommand->data;
}


/*
 * DistributionDeleteMetadataCommand returns a query to delete pg_dist_partition
 * metadata from a worker node for a given table.
 */
char *
DistributionDeleteMetadataCommand(Oid relationId)
{
	StringInfo deleteCommand = makeStringInfo();
	char *qualifiedRelationName = generate_qualified_relation_name(relationId);

	appendStringInfo(deleteCommand,
					 "SELECT pg_catalog.citus_internal_delete_partition_metadata(%s)",
					 quote_literal_cstr(qualifiedRelationName));

	return deleteCommand->data;
}


/*
 * TableOwnerResetCommand generates a commands that can be executed
 * to reset the table owner.
 */
char *
TableOwnerResetCommand(Oid relationId)
{
	StringInfo ownerResetCommand = makeStringInfo();
	char *qualifiedRelationName = generate_qualified_relation_name(relationId);
	char *tableOwnerName = TableOwner(relationId);

	appendStringInfo(ownerResetCommand,
					 "ALTER TABLE %s OWNER TO %s",
					 qualifiedRelationName,
					 quote_identifier(tableOwnerName));

	return ownerResetCommand->data;
}


/*
 * ShardListInsertCommand generates a single command that can be
 * executed to replicate shard and shard placement metadata for the
 * given shard intervals. The function assumes that each shard has a
 * single placement, and asserts this information.
 */
List *
ShardListInsertCommand(List *shardIntervalList)
{
	List *commandList = NIL;
	int shardCount = list_length(shardIntervalList);

	/* if there are no shards, return empty list */
	if (shardCount == 0)
	{
		return commandList;
	}

	/* add placements to insertPlacementCommand */
	StringInfo insertPlacementCommand = makeStringInfo();
	appendStringInfo(insertPlacementCommand,
					 "WITH placement_data(shardid, "
					 "shardlength, groupid, placementid)  AS (VALUES ");

	ShardInterval *shardInterval = NULL;
	bool firstPlacementProcessed = false;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		uint64 shardId = shardInterval->shardId;
		List *shardPlacementList = ActiveShardPlacementList(shardId);

		ShardPlacement *placement = NULL;
		foreach_ptr(placement, shardPlacementList)
		{
			if (firstPlacementProcessed)
			{
				/*
				 * As long as this is not the first placement of the first shard,
				 * append the comma.
				 */
				appendStringInfo(insertPlacementCommand, ", ");
			}
			firstPlacementProcessed = true;

			appendStringInfo(insertPlacementCommand,
							 "(%ld, %ld, %d, %ld)",
							 shardId,
							 placement->shardLength,
							 placement->groupId,
							 placement->placementId);
		}
	}

	appendStringInfo(insertPlacementCommand, ") ");

	appendStringInfo(insertPlacementCommand,
					 "SELECT citus_internal_add_placement_metadata("
					 "shardid, shardlength, groupid, placementid) "
					 "FROM placement_data;");

	/* now add shards to insertShardCommand */
	StringInfo insertShardCommand = makeStringInfo();
	appendStringInfo(insertShardCommand,
					 "WITH shard_data(relationname, shardid, storagetype, "
					 "shardminvalue, shardmaxvalue)  AS (VALUES ");

	foreach_ptr(shardInterval, shardIntervalList)
	{
		uint64 shardId = shardInterval->shardId;
		Oid distributedRelationId = shardInterval->relationId;
		char *qualifiedRelationName = generate_qualified_relation_name(
			distributedRelationId);
		StringInfo minHashToken = makeStringInfo();
		StringInfo maxHashToken = makeStringInfo();

		if (shardInterval->minValueExists)
		{
			appendStringInfo(minHashToken, "'%d'", DatumGetInt32(
								 shardInterval->minValue));
		}
		else
		{
			appendStringInfo(minHashToken, "NULL");
		}

		if (shardInterval->maxValueExists)
		{
			appendStringInfo(maxHashToken, "'%d'", DatumGetInt32(
								 shardInterval->maxValue));
		}
		else
		{
			appendStringInfo(maxHashToken, "NULL");
		}

		appendStringInfo(insertShardCommand,
						 "(%s::regclass, %ld, '%c'::\"char\", %s, %s)",
						 quote_literal_cstr(qualifiedRelationName),
						 shardId,
						 shardInterval->storageType,
						 minHashToken->data,
						 maxHashToken->data);

		if (llast(shardIntervalList) != shardInterval)
		{
			appendStringInfo(insertShardCommand, ", ");
		}
	}

	appendStringInfo(insertShardCommand, ") ");

	appendStringInfo(insertShardCommand,
					 "SELECT citus_internal_add_shard_metadata(relationname, shardid, "
					 "storagetype, shardminvalue, shardmaxvalue) "
					 "FROM shard_data;");

	/*
	 * There are no active placements for the table, so do not create the
	 * command as it'd lead to syntax error.
	 *
	 * This is normally not an expected situation, however the current
	 * implementation of citus_disable_node allows to disable nodes with
	 * the only active placements. So, for example a single shard/placement
	 * distributed table on a disabled node might trigger zero placement
	 * case.
	 *
	 * TODO: remove this check once citus_disable_node errors out for
	 * the above scenario.
	 */
	if (firstPlacementProcessed)
	{
		/* first insert shards, than the placements */
		commandList = lappend(commandList, insertShardCommand->data);
		commandList = lappend(commandList, insertPlacementCommand->data);
	}

	return commandList;
}


/*
 * ShardListDeleteCommand generates a command list that can be executed to delete
 * shard and shard placement metadata for the given shard.
 */
List *
ShardDeleteCommandList(ShardInterval *shardInterval)
{
	uint64 shardId = shardInterval->shardId;

	StringInfo deleteShardCommand = makeStringInfo();
	appendStringInfo(deleteShardCommand,
					 "SELECT citus_internal_delete_shard_metadata(%ld);", shardId);

	return list_make1(deleteShardCommand->data);
}


/*
 * NodeDeleteCommand generate a command that can be
 * executed to delete the metadata for a worker node.
 */
char *
NodeDeleteCommand(uint32 nodeId)
{
	StringInfo nodeDeleteCommand = makeStringInfo();

	appendStringInfo(nodeDeleteCommand,
					 "DELETE FROM pg_dist_node "
					 "WHERE nodeid = %u", nodeId);

	return nodeDeleteCommand->data;
}


/*
 * NodeStateUpdateCommand generates a command that can be executed to update
 * isactive column of a node in pg_dist_node table.
 */
char *
NodeStateUpdateCommand(uint32 nodeId, bool isActive)
{
	StringInfo nodeStateUpdateCommand = makeStringInfo();
	char *isActiveString = isActive ? "TRUE" : "FALSE";

	appendStringInfo(nodeStateUpdateCommand,
					 "UPDATE pg_dist_node SET isactive = %s "
					 "WHERE nodeid = %u", isActiveString, nodeId);

	return nodeStateUpdateCommand->data;
}


/*
 * ShouldHaveShardsUpdateCommand generates a command that can be executed to
 * update the shouldhaveshards column of a node in pg_dist_node table.
 */
char *
ShouldHaveShardsUpdateCommand(uint32 nodeId, bool shouldHaveShards)
{
	StringInfo nodeStateUpdateCommand = makeStringInfo();
	char *shouldHaveShardsString = shouldHaveShards ? "TRUE" : "FALSE";

	appendStringInfo(nodeStateUpdateCommand,
					 "UPDATE pg_catalog.pg_dist_node SET shouldhaveshards = %s "
					 "WHERE nodeid = %u", shouldHaveShardsString, nodeId);

	return nodeStateUpdateCommand->data;
}


/*
 * ColocationIdUpdateCommand creates the SQL command to change the colocationId
 * of the table with the given name to the given colocationId in pg_dist_partition
 * table.
 */
char *
ColocationIdUpdateCommand(Oid relationId, uint32 colocationId)
{
	StringInfo command = makeStringInfo();
	char *qualifiedRelationName = generate_qualified_relation_name(relationId);
	appendStringInfo(command,
					 "SELECT citus_internal_update_relation_colocation(%s::regclass, %d)",
					 quote_literal_cstr(qualifiedRelationName), colocationId);

	return command->data;
}


/*
 * PlacementUpsertCommand creates a SQL command for upserting a pg_dist_placment
 * entry with the given properties. In the case of a conflict on placementId, the command
 * updates all properties (excluding the placementId) with the given ones.
 */
char *
PlacementUpsertCommand(uint64 shardId, uint64 placementId,
					   uint64 shardLength, int32 groupId)
{
	StringInfo command = makeStringInfo();

	appendStringInfo(command, UPSERT_PLACEMENT, shardId, shardLength,
					 groupId, placementId);

	return command->data;
}


/*
 * LocalGroupIdUpdateCommand creates the SQL command required to set the local group id
 * of a worker and returns the command in a string.
 */
char *
LocalGroupIdUpdateCommand(int32 groupId)
{
	StringInfo updateCommand = makeStringInfo();

	appendStringInfo(updateCommand, "UPDATE pg_dist_local_group SET groupid = %d",
					 groupId);

	return updateCommand->data;
}


/*
 * DDLCommandsForSequence returns the DDL commands needs to be run to create the
 * sequence and alter the owner to the given owner name.
 */
List *
DDLCommandsForSequence(Oid sequenceOid, char *ownerName)
{
	List *sequenceDDLList = NIL;
	char *sequenceDef = pg_get_sequencedef_string(sequenceOid);
	char *escapedSequenceDef = quote_literal_cstr(sequenceDef);
	StringInfo wrappedSequenceDef = makeStringInfo();
	StringInfo sequenceGrantStmt = makeStringInfo();
	char *sequenceName = generate_qualified_relation_name(sequenceOid);
	Form_pg_sequence sequenceData = pg_get_sequencedef(sequenceOid);
	Oid sequenceTypeOid = sequenceData->seqtypid;
	char *typeName = format_type_be(sequenceTypeOid);

	/* create schema if needed */
	appendStringInfo(wrappedSequenceDef,
					 WORKER_APPLY_SEQUENCE_COMMAND,
					 escapedSequenceDef,
					 quote_literal_cstr(typeName));

	appendStringInfo(sequenceGrantStmt,
					 "ALTER SEQUENCE %s OWNER TO %s", sequenceName,
					 quote_identifier(ownerName));

	sequenceDDLList = lappend(sequenceDDLList, wrappedSequenceDef->data);
	sequenceDDLList = lappend(sequenceDDLList, sequenceGrantStmt->data);
	sequenceDDLList = list_concat(sequenceDDLList, GrantOnSequenceDDLCommands(
									  sequenceOid));

	return sequenceDDLList;
}


/*
 * GetAttributeTypeOid returns the OID of the type of the attribute of
 * provided relationId that has the provided attnum
 */
Oid
GetAttributeTypeOid(Oid relationId, AttrNumber attnum)
{
	Oid resultOid = InvalidOid;

	ScanKeyData key[2];

	/* Grab an appropriate lock on the pg_attribute relation */
	Relation attrel = table_open(AttributeRelationId, AccessShareLock);

	/* Use the index to scan only system attributes of the target relation */
	ScanKeyInit(&key[0],
				Anum_pg_attribute_attrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationId));
	ScanKeyInit(&key[1],
				Anum_pg_attribute_attnum,
				BTLessEqualStrategyNumber, F_INT2LE,
				Int16GetDatum(attnum));

	SysScanDesc scan = systable_beginscan(attrel, AttributeRelidNumIndexId, true, NULL, 2,
										  key);

	HeapTuple attributeTuple;
	while (HeapTupleIsValid(attributeTuple = systable_getnext(scan)))
	{
		Form_pg_attribute att = (Form_pg_attribute) GETSTRUCT(attributeTuple);
		resultOid = att->atttypid;
	}

	systable_endscan(scan);
	table_close(attrel, AccessShareLock);

	return resultOid;
}


/*
 * GetDependentSequencesWithRelation appends the attnum and id of sequences that
 * have direct (owned sequences) or indirect dependency with the given relationId,
 * to the lists passed as NIL initially.
 * For both cases, we use the intermediate AttrDefault object from pg_depend.
 * If attnum is specified, we only return the sequences related to that
 * attribute of the relationId.
 * See DependencyType for the possible values of depType.
 * We use DEPENDENCY_INTERNAL for sequences created by identity column.
 * DEPENDENCY_AUTO for regular sequences.
 */
void
GetDependentSequencesWithRelation(Oid relationId, List **seqInfoList,
								  AttrNumber attnum, char depType)
{
	Assert(*seqInfoList == NIL);

	List *attrdefResult = NIL;
	List *attrdefAttnumResult = NIL;
	ScanKeyData key[3];
	HeapTuple tup;

	Relation depRel = table_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationId));
	if (attnum)
	{
		ScanKeyInit(&key[2],
					Anum_pg_depend_refobjsubid,
					BTEqualStrategyNumber, F_INT4EQ,
					Int32GetDatum(attnum));
	}

	SysScanDesc scan = systable_beginscan(depRel, DependReferenceIndexId, true,
										  NULL, attnum ? 3 : 2, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend deprec = (Form_pg_depend) GETSTRUCT(tup);

		if (deprec->classid == AttrDefaultRelationId &&
			deprec->objsubid == 0 &&
			deprec->refobjsubid != 0 &&
			deprec->deptype == depType)
		{
			/*
			 * We are going to generate corresponding SequenceInfo
			 * in the following loop.
			 */
			attrdefResult = lappend_oid(attrdefResult, deprec->objid);
			attrdefAttnumResult = lappend_int(attrdefAttnumResult, deprec->refobjsubid);
		}
		else if (deprec->deptype == depType &&
				 deprec->refobjsubid != 0 &&
				 deprec->classid == RelationRelationId &&
				 get_rel_relkind(deprec->objid) == RELKIND_SEQUENCE)
		{
			SequenceInfo *seqInfo = (SequenceInfo *) palloc(sizeof(SequenceInfo));

			seqInfo->sequenceOid = deprec->objid;
			seqInfo->attributeNumber = deprec->refobjsubid;
			seqInfo->isNextValDefault = false;

			*seqInfoList = lappend(*seqInfoList, seqInfo);
		}
	}

	systable_endscan(scan);

	table_close(depRel, AccessShareLock);

	AttrNumber attrdefAttnum = InvalidAttrNumber;
	Oid attrdefOid = InvalidOid;
	forboth_int_oid(attrdefAttnum, attrdefAttnumResult, attrdefOid, attrdefResult)
	{
		List *sequencesFromAttrDef = GetSequencesFromAttrDef(attrdefOid);

		/* to simplify and eliminate cases like "DEFAULT nextval('..') - nextval('..')" */
		if (list_length(sequencesFromAttrDef) > 1)
		{
			ereport(ERROR, (errmsg(
								"More than one sequence in a column default"
								" is not supported for distribution "
								"or for adding local tables to metadata")));
		}

		if (list_length(sequencesFromAttrDef) == 1)
		{
			SequenceInfo *seqInfo = (SequenceInfo *) palloc(sizeof(SequenceInfo));

			seqInfo->sequenceOid = linitial_oid(sequencesFromAttrDef);
			seqInfo->attributeNumber = attrdefAttnum;
			seqInfo->isNextValDefault = true;

			*seqInfoList = lappend(*seqInfoList, seqInfo);
		}
	}
}


/*
 * GetDependentDependentRelationsWithSequence returns a list of oids of
 * relations that have have a dependency on the given sequence.
 * There are three types of dependencies:
 * 1. direct auto (owned sequences), created using SERIAL or BIGSERIAL
 * 2. indirect auto (through an AttrDef), created using DEFAULT nextval('..')
 * 3. internal, created using GENERATED ALWAYS AS IDENTITY
 *
 * Depending on the passed deptype, we return the relations that have the
 * given type(s):
 * - DEPENDENCY_AUTO returns both 1 and 2
 * - DEPENDENCY_INTERNAL returns 3
 *
 * The returned list can contain duplicates, as the same relation can have
 * multiple dependencies on the sequence.
 */
List *
GetDependentRelationsWithSequence(Oid sequenceOid, char depType)
{
	List *relations = NIL;
	ScanKeyData key[2];
	HeapTuple tup;

	Relation depRel = table_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(sequenceOid));
	SysScanDesc scan = systable_beginscan(depRel, DependDependerIndexId, true,
										  NULL, lengthof(key), key);
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend deprec = (Form_pg_depend) GETSTRUCT(tup);

		if (
			deprec->refclassid == RelationRelationId &&
			deprec->refobjsubid != 0 &&
			deprec->deptype == depType)
		{
			relations = lappend_oid(relations, deprec->refobjid);
		}
	}

	systable_endscan(scan);

	table_close(depRel, AccessShareLock);

	if (depType == DEPENDENCY_AUTO)
	{
		Oid attrDefOid;
		List *attrDefOids = GetAttrDefsFromSequence(sequenceOid);

		foreach_oid(attrDefOid, attrDefOids)
		{
			ObjectAddress columnAddress = GetAttrDefaultColumnAddress(attrDefOid);
			relations = lappend_oid(relations, columnAddress.objectId);
		}
	}

	return relations;
}


/*
 * GetSequencesFromAttrDef returns a list of sequence OIDs that have
 * dependency with the given attrdefOid in pg_depend
 */
List *
GetSequencesFromAttrDef(Oid attrdefOid)
{
	List *sequencesResult = NIL;
	ScanKeyData key[2];
	HeapTuple tup;

	Relation depRel = table_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(AttrDefaultRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(attrdefOid));

	SysScanDesc scan = systable_beginscan(depRel, DependDependerIndexId, true,
										  NULL, 2, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend deprec = (Form_pg_depend) GETSTRUCT(tup);

		if (deprec->refclassid == RelationRelationId &&
			deprec->deptype == DEPENDENCY_NORMAL &&
			get_rel_relkind(deprec->refobjid) == RELKIND_SEQUENCE)
		{
			sequencesResult = lappend_oid(sequencesResult, deprec->refobjid);
		}
	}

	systable_endscan(scan);

	table_close(depRel, AccessShareLock);

	return sequencesResult;
}


#if PG_VERSION_NUM < PG_VERSION_15

/*
 * Given a pg_attrdef OID, return the relation OID and column number of
 * the owning column (represented as an ObjectAddress for convenience).
 *
 * Returns InvalidObjectAddress if there is no such pg_attrdef entry.
 */
ObjectAddress
GetAttrDefaultColumnAddress(Oid attrdefoid)
{
	ObjectAddress result = InvalidObjectAddress;
	ScanKeyData skey[1];
	HeapTuple tup;

	Relation attrdef = table_open(AttrDefaultRelationId, AccessShareLock);
	ScanKeyInit(&skey[0],
				Anum_pg_attrdef_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(attrdefoid));
	SysScanDesc scan = systable_beginscan(attrdef, AttrDefaultOidIndexId, true,
										  NULL, 1, skey);

	if (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_attrdef atdform = (Form_pg_attrdef) GETSTRUCT(tup);

		result.classId = RelationRelationId;
		result.objectId = atdform->adrelid;
		result.objectSubId = atdform->adnum;
	}

	systable_endscan(scan);
	table_close(attrdef, AccessShareLock);

	return result;
}


#endif


/*
 * GetAttrDefsFromSequence returns a list of attrdef OIDs that have
 * a dependency on the given sequence
 */
List *
GetAttrDefsFromSequence(Oid seqOid)
{
	List *attrDefsResult = NIL;
	ScanKeyData key[2];
	HeapTuple tup;

	Relation depRel = table_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(seqOid));
	SysScanDesc scan = systable_beginscan(depRel, DependReferenceIndexId, true,
										  NULL, lengthof(key), key);
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend deprec = (Form_pg_depend) GETSTRUCT(tup);

		if (deprec->classid == AttrDefaultRelationId &&
			deprec->deptype == DEPENDENCY_NORMAL)
		{
			attrDefsResult = lappend_oid(attrDefsResult, deprec->objid);
		}
	}

	systable_endscan(scan);

	table_close(depRel, AccessShareLock);

	return attrDefsResult;
}


/*
 * GetDependentFunctionsWithRelation returns the dependent functions for the
 * given relation id.
 */
List *
GetDependentFunctionsWithRelation(Oid relationId)
{
	List *referencingObjects = NIL;
	List *functionOids = NIL;
	ScanKeyData key[2];
	HeapTuple tup;

	Relation depRel = table_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationId));

	SysScanDesc scan = systable_beginscan(depRel, DependReferenceIndexId, true,
										  NULL, 2, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend deprec = (Form_pg_depend) GETSTRUCT(tup);

		/*
		 * objsubid is nonzero only for table columns and zero for anything else.
		 * Since we are trying to find a dependency from the column of a table to
		 * function we've added deprec->refobjsubid != 0 check.
		 *
		 * We are following DEPENDENCY_AUTO for dependencies via column and
		 * DEPENDENCY_NORMAL anything else. Since only procedure dependencies
		 * for those dependencies will be obtained in GetFunctionDependenciesForObjects
		 * following both dependency types are not harmful.
		 */
		if ((deprec->refobjsubid != 0 && deprec->deptype == DEPENDENCY_AUTO) ||
			deprec->deptype == DEPENDENCY_NORMAL)
		{
			ObjectAddress *refAddress = palloc(sizeof(ObjectAddress));
			ObjectAddressSubSet(*refAddress, deprec->classid,
								deprec->objid,
								deprec->objsubid);
			referencingObjects = lappend(referencingObjects, refAddress);
		}
	}

	systable_endscan(scan);

	table_close(depRel, AccessShareLock);

	ObjectAddress *referencingObject = NULL;
	foreach_ptr(referencingObject, referencingObjects)
	{
		functionOids = list_concat(functionOids,
								   GetFunctionDependenciesForObjects(referencingObject));
	}

	return functionOids;
}


/*
 * GetFunctionDependenciesForObjects returns a list of function OIDs that have
 * dependency with the given object
 */
static List *
GetFunctionDependenciesForObjects(ObjectAddress *objectAddress)
{
	List *functionOids = NIL;
	ScanKeyData key[3];
	HeapTuple tup;

	Relation depRel = table_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(objectAddress->classId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(objectAddress->objectId));
	ScanKeyInit(&key[2],
				Anum_pg_depend_objsubid,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(objectAddress->objectSubId));

	SysScanDesc scan = systable_beginscan(depRel, DependDependerIndexId, true,
										  NULL, 3, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend deprec = (Form_pg_depend) GETSTRUCT(tup);

		if (deprec->refclassid == ProcedureRelationId)
		{
			functionOids = lappend_oid(functionOids, deprec->refobjid);
		}
	}

	systable_endscan(scan);

	table_close(depRel, AccessShareLock);

	return functionOids;
}


/*
 * SequenceDependencyCommandList generates commands to record the dependency
 * of sequences on tables on the worker. This dependency does not exist by
 * default since the sequences and table are created separately, but it is
 * necessary to ensure that the sequence is dropped when the table is
 * dropped.
 */
List *
SequenceDependencyCommandList(Oid relationId)
{
	List *sequenceCommandList = NIL;
	List *columnNameList = NIL;
	List *sequenceIdList = NIL;

	ExtractDefaultColumnsAndOwnedSequences(relationId, &columnNameList, &sequenceIdList);

	char *columnName = NULL;
	Oid sequenceId = InvalidOid;
	forboth_ptr_oid(columnName, columnNameList, sequenceId, sequenceIdList)
	{
		if (!OidIsValid(sequenceId))
		{
			/*
			 * ExtractDefaultColumnsAndOwnedSequences returns entries for all columns,
			 * but with 0 sequence ID unless there is default nextval(..).
			 */
			continue;
		}

		char *sequenceDependencyCommand =
			CreateSequenceDependencyCommand(relationId, sequenceId, columnName);

		sequenceCommandList = lappend(sequenceCommandList,
									  makeTableDDLCommandString(
										  sequenceDependencyCommand));
	}

	return sequenceCommandList;
}


/*
 * IdentitySequenceDependencyCommandList generate a command to execute
 * a UDF (WORKER_ADJUST_IDENTITY_COLUMN_SEQ_RANGES) on workers to modify the identity
 * columns min/max values to produce unique values on workers.
 */
List *
IdentitySequenceDependencyCommandList(Oid targetRelationId)
{
	List *commandList = NIL;

	Relation relation = relation_open(targetRelationId, AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(relation);

	bool tableHasIdentityColumn = false;
	for (int attributeIndex = 0; attributeIndex < tupleDescriptor->natts;
		 attributeIndex++)
	{
		Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, attributeIndex);

		if (attributeForm->attidentity)
		{
			tableHasIdentityColumn = true;
			break;
		}
	}

	relation_close(relation, NoLock);

	if (tableHasIdentityColumn)
	{
		StringInfo stringInfo = makeStringInfo();
		char *tableName = generate_qualified_relation_name(targetRelationId);

		appendStringInfo(stringInfo,
						 WORKER_ADJUST_IDENTITY_COLUMN_SEQ_RANGES,
						 quote_literal_cstr(tableName));


		commandList = lappend(commandList,
							  makeTableDDLCommandString(
								  stringInfo->data));
	}

	return commandList;
}


/*
 * CreateSequenceDependencyCommand generates a query string for calling
 * worker_record_sequence_dependency on the worker to recreate a sequence->table
 * dependency.
 */
static char *
CreateSequenceDependencyCommand(Oid relationId, Oid sequenceId, char *columnName)
{
	char *relationName = generate_qualified_relation_name(relationId);
	char *sequenceName = generate_qualified_relation_name(sequenceId);

	StringInfo sequenceDependencyCommand = makeStringInfo();

	appendStringInfo(sequenceDependencyCommand,
					 "SELECT pg_catalog.worker_record_sequence_dependency"
					 "(%s::regclass,%s::regclass,%s)",
					 quote_literal_cstr(sequenceName),
					 quote_literal_cstr(relationName),
					 quote_literal_cstr(columnName));

	return sequenceDependencyCommand->data;
}


/*
 * worker_record_sequence_dependency records the fact that the sequence depends on
 * the table in pg_depend, such that it will be automatically dropped.
 */
Datum
worker_record_sequence_dependency(PG_FUNCTION_ARGS)
{
	Oid sequenceOid = PG_GETARG_OID(0);
	Oid relationOid = PG_GETARG_OID(1);
	Name columnName = PG_GETARG_NAME(2);
	const char *columnNameStr = NameStr(*columnName);

	/* lookup column definition */
	HeapTuple columnTuple = SearchSysCacheAttName(relationOid, columnNameStr);
	if (!HeapTupleIsValid(columnTuple))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
						errmsg("column \"%s\" does not exist",
							   columnNameStr)));
	}

	Form_pg_attribute columnForm = (Form_pg_attribute) GETSTRUCT(columnTuple);
	if (columnForm->attnum <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot create dependency on system column \"%s\"",
							   columnNameStr)));
	}

	ObjectAddress sequenceAddr = {
		.classId = RelationRelationId,
		.objectId = sequenceOid,
		.objectSubId = 0
	};
	ObjectAddress relationAddr = {
		.classId = RelationRelationId,
		.objectId = relationOid,
		.objectSubId = columnForm->attnum
	};


	EnsureTableOwner(sequenceOid);
	EnsureTableOwner(relationOid);

	/* dependency from sequence to table */
	recordDependencyOn(&sequenceAddr, &relationAddr, DEPENDENCY_AUTO);

	ReleaseSysCache(columnTuple);

	PG_RETURN_VOID();
}


/*
 * CreateSchemaDDLCommand returns a "CREATE SCHEMA..." SQL string for creating the given
 * schema if not exists and with proper authorization.
 */
char *
CreateSchemaDDLCommand(Oid schemaId)
{
	char *schemaName = get_namespace_name(schemaId);

	StringInfo schemaNameDef = makeStringInfo();
	const char *quotedSchemaName = quote_identifier(schemaName);
	const char *ownerName = quote_identifier(SchemaOwnerName(schemaId));
	appendStringInfo(schemaNameDef, CREATE_SCHEMA_COMMAND, quotedSchemaName, ownerName);

	return schemaNameDef->data;
}


/*
 * GrantOnSchemaDDLCommands creates a list of ddl command for replicating the permissions
 * of roles on schemas.
 */
List *
GrantOnSchemaDDLCommands(Oid schemaOid)
{
	HeapTuple schemaTuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(schemaOid));
	bool isNull = true;
	Datum aclDatum = SysCacheGetAttr(NAMESPACEOID, schemaTuple, Anum_pg_namespace_nspacl,
									 &isNull);
	if (isNull)
	{
		ReleaseSysCache(schemaTuple);
		return NIL;
	}
	Acl *acl = DatumGetAclPCopy(aclDatum);
	AclItem *aclDat = ACL_DAT(acl);
	int aclNum = ACL_NUM(acl);
	List *commands = NIL;

	ReleaseSysCache(schemaTuple);

	for (int i = 0; i < aclNum; i++)
	{
		commands = list_concat(commands,
							   GenerateGrantOnSchemaQueriesFromAclItem(
								   schemaOid,
								   &aclDat[i]));
	}

	return commands;
}


/*
 * GenerateGrantOnSchemaQueryFromACLItem generates a query string for replicating a users permissions
 * on a schema.
 */
List *
GenerateGrantOnSchemaQueriesFromAclItem(Oid schemaOid, AclItem *aclItem)
{
	AclMode permissions = ACLITEM_GET_PRIVS(*aclItem) & ACL_ALL_RIGHTS_SCHEMA;
	AclMode grants = ACLITEM_GET_GOPTIONS(*aclItem) & ACL_ALL_RIGHTS_SCHEMA;

	/*
	 * seems unlikely but we check if there is a grant option in the list without the actual permission
	 */
	Assert(!(grants & ACL_USAGE) || (permissions & ACL_USAGE));
	Assert(!(grants & ACL_CREATE) || (permissions & ACL_CREATE));
	Oid granteeOid = aclItem->ai_grantee;
	List *queries = NIL;

	queries = lappend(queries, GenerateSetRoleQuery(aclItem->ai_grantor));

	if (permissions & ACL_USAGE)
	{
		char *query = DeparseTreeNode((Node *) GenerateGrantStmtForRights(
										  OBJECT_SCHEMA, granteeOid, schemaOid, "USAGE",
										  grants & ACL_USAGE));
		queries = lappend(queries, query);
	}
	if (permissions & ACL_CREATE)
	{
		char *query = DeparseTreeNode((Node *) GenerateGrantStmtForRights(
										  OBJECT_SCHEMA, granteeOid, schemaOid, "CREATE",
										  grants & ACL_CREATE));
		queries = lappend(queries, query);
	}

	queries = lappend(queries, "RESET ROLE");

	return queries;
}


/*
 * GenerateGrantStmtForRights is the function for creating GrantStmt's for all
 * types of objects that are supported. It takes parameters to fill a GrantStmt's
 * fields and returns the GrantStmt.
 * The field `objects` of GrantStmt doesn't have a common structure for all types.
 * Make sure you have added your object type to GetObjectsForGrantStmt.
 */
static GrantStmt *
GenerateGrantStmtForRights(ObjectType objectType,
						   Oid roleOid,
						   Oid objectId,
						   char *permission,
						   bool withGrantOption)
{
	GrantStmt *stmt = makeNode(GrantStmt);
	stmt->is_grant = true;
	stmt->targtype = ACL_TARGET_OBJECT;
	stmt->objtype = objectType;
	stmt->objects = GetObjectsForGrantStmt(objectType, objectId);
	stmt->privileges = list_make1(GetAccessPrivObjectForGrantStmt(permission));
	stmt->grantees = list_make1(GetRoleSpecObjectForUser(roleOid));
	stmt->grant_option = withGrantOption;

	return stmt;
}


/*
 * GetObjectsForGrantStmt takes an object type and object id and returns the 'objects'
 * field to be used when creating GrantStmt. We have only one object here (the one with
 * the oid = objectId) but we pass it into the GrantStmt as a list with one element,
 * as GrantStmt->objects field is actually a list.
 */
static List *
GetObjectsForGrantStmt(ObjectType objectType, Oid objectId)
{
	switch (objectType)
	{
		/* supported object types */
		case OBJECT_SCHEMA:
		{
			return list_make1(makeString(get_namespace_name(objectId)));
		}

		/* enterprise supported object types */
		case OBJECT_FUNCTION:
		case OBJECT_AGGREGATE:
		case OBJECT_PROCEDURE:
		{
			ObjectWithArgs *owa = ObjectWithArgsFromOid(objectId);
			return list_make1(owa);
		}

		case OBJECT_FDW:
		{
			ForeignDataWrapper *fdw = GetForeignDataWrapper(objectId);
			return list_make1(makeString(fdw->fdwname));
		}

		case OBJECT_FOREIGN_SERVER:
		{
			ForeignServer *server = GetForeignServer(objectId);
			return list_make1(makeString(server->servername));
		}

		case OBJECT_SEQUENCE:
		{
			Oid namespaceOid = get_rel_namespace(objectId);
			RangeVar *sequence = makeRangeVar(get_namespace_name(namespaceOid),
											  get_rel_name(objectId), -1);
			return list_make1(sequence);
		}

		default:
		{
			elog(ERROR, "unsupported object type for GRANT");
		}
	}

	return NIL;
}


/*
 * GrantOnFunctionDDLCommands creates a list of ddl command for replicating the permissions
 * of roles on distributed functions.
 */
List *
GrantOnFunctionDDLCommands(Oid functionOid)
{
	HeapTuple proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionOid));

	bool isNull = true;
	Datum aclDatum = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_proacl,
									 &isNull);
	if (isNull)
	{
		ReleaseSysCache(proctup);
		return NIL;
	}

	Acl *acl = DatumGetAclPCopy(aclDatum);
	AclItem *aclDat = ACL_DAT(acl);
	int aclNum = ACL_NUM(acl);
	List *commands = NIL;

	ReleaseSysCache(proctup);

	for (int i = 0; i < aclNum; i++)
	{
		commands = list_concat(commands,
							   GenerateGrantOnFunctionQueriesFromAclItem(
								   functionOid,
								   &aclDat[i]));
	}

	return commands;
}


/*
 * GrantOnForeignServerDDLCommands creates a list of ddl command for replicating the
 * permissions of roles on distributed foreign servers.
 */
List *
GrantOnForeignServerDDLCommands(Oid serverId)
{
	HeapTuple servertup = SearchSysCache1(FOREIGNSERVEROID, ObjectIdGetDatum(serverId));

	bool isNull = true;
	Datum aclDatum = SysCacheGetAttr(FOREIGNSERVEROID, servertup,
									 Anum_pg_foreign_server_srvacl, &isNull);
	if (isNull)
	{
		ReleaseSysCache(servertup);
		return NIL;
	}

	Acl *aclEntry = DatumGetAclPCopy(aclDatum);
	AclItem *privileges = ACL_DAT(aclEntry);
	int numberOfPrivsGranted = ACL_NUM(aclEntry);
	List *commands = NIL;

	ReleaseSysCache(servertup);

	for (int i = 0; i < numberOfPrivsGranted; i++)
	{
		commands = list_concat(commands,
							   GenerateGrantOnForeignServerQueriesFromAclItem(
								   serverId,
								   &privileges[i]));
	}

	return commands;
}


/*
 * GenerateGrantOnForeignServerQueriesFromAclItem generates a query string for
 * replicating a users permissions on a foreign server.
 */
List *
GenerateGrantOnForeignServerQueriesFromAclItem(Oid serverId, AclItem *aclItem)
{
	/* privileges to be granted */
	AclMode permissions = ACLITEM_GET_PRIVS(*aclItem) & ACL_ALL_RIGHTS_FOREIGN_SERVER;

	/* WITH GRANT OPTION clause */
	AclMode grants = ACLITEM_GET_GOPTIONS(*aclItem) & ACL_ALL_RIGHTS_FOREIGN_SERVER;

	/*
	 * seems unlikely but we check if there is a grant option in the list without the actual permission
	 */
	Assert(!(grants & ACL_USAGE) || (permissions & ACL_USAGE));

	Oid granteeOid = aclItem->ai_grantee;
	List *queries = NIL;

	/* switch to the role which had granted acl */
	queries = lappend(queries, GenerateSetRoleQuery(aclItem->ai_grantor));

	/* generate the GRANT stmt that will be executed by the grantor role */
	if (permissions & ACL_USAGE)
	{
		char *query = DeparseTreeNode((Node *) GenerateGrantStmtForRights(
										  OBJECT_FOREIGN_SERVER, granteeOid, serverId,
										  "USAGE", grants & ACL_USAGE));
		queries = lappend(queries, query);
	}

	/* reset the role back */
	queries = lappend(queries, "RESET ROLE");

	return queries;
}


/*
 * GenerateGrantOnFunctionQueryFromACLItem generates a query string for replicating a users permissions
 * on a distributed function.
 */
List *
GenerateGrantOnFunctionQueriesFromAclItem(Oid functionOid, AclItem *aclItem)
{
	AclMode permissions = ACLITEM_GET_PRIVS(*aclItem) & ACL_ALL_RIGHTS_FUNCTION;
	AclMode grants = ACLITEM_GET_GOPTIONS(*aclItem) & ACL_ALL_RIGHTS_FUNCTION;

	/*
	 * seems unlikely but we check if there is a grant option in the list without the actual permission
	 */
	Assert(!(grants & ACL_EXECUTE) || (permissions & ACL_EXECUTE));
	Oid granteeOid = aclItem->ai_grantee;
	List *queries = NIL;

	queries = lappend(queries, GenerateSetRoleQuery(aclItem->ai_grantor));

	if (permissions & ACL_EXECUTE)
	{
		char prokind = get_func_prokind(functionOid);
		ObjectType objectType;

		if (prokind == PROKIND_FUNCTION)
		{
			objectType = OBJECT_FUNCTION;
		}
		else if (prokind == PROKIND_PROCEDURE)
		{
			objectType = OBJECT_PROCEDURE;
		}
		else if (prokind == PROKIND_AGGREGATE)
		{
			objectType = OBJECT_AGGREGATE;
		}
		else
		{
			ereport(ERROR, (errmsg("unsupported prokind"),
							errdetail("GRANT commands on procedures are propagated only "
									  "for procedures, functions, and aggregates.")));
		}

		char *query = DeparseTreeNode((Node *) GenerateGrantStmtForRights(
										  objectType, granteeOid, functionOid, "EXECUTE",
										  grants & ACL_EXECUTE));
		queries = lappend(queries, query);
	}

	queries = lappend(queries, "RESET ROLE");

	return queries;
}


/*
 * GenerateGrantOnFDWQueriesFromAclItem generates a query string for
 * replicating a users permissions on a foreign data wrapper.
 */
List *
GenerateGrantOnFDWQueriesFromAclItem(Oid FDWId, AclItem *aclItem)
{
	/* privileges to be granted */
	AclMode permissions = ACLITEM_GET_PRIVS(*aclItem) & ACL_ALL_RIGHTS_FDW;

	/* WITH GRANT OPTION clause */
	AclMode grants = ACLITEM_GET_GOPTIONS(*aclItem) & ACL_ALL_RIGHTS_FDW;

	/*
	 * seems unlikely but we check if there is a grant option in the list without the actual permission
	 */
	Assert(!(grants & ACL_USAGE) || (permissions & ACL_USAGE));

	Oid granteeOid = aclItem->ai_grantee;
	List *queries = NIL;

	/* switch to the role which had granted acl */
	queries = lappend(queries, GenerateSetRoleQuery(aclItem->ai_grantor));

	/* generate the GRANT stmt that will be executed by the grantor role */
	if (permissions & ACL_USAGE)
	{
		char *query = DeparseTreeNode((Node *) GenerateGrantStmtForRights(
										  OBJECT_FDW, granteeOid, FDWId, "USAGE",
										  grants & ACL_USAGE));
		queries = lappend(queries, query);
	}

	/* reset the role back */
	queries = lappend(queries, "RESET ROLE");

	return queries;
}


/*
 * GetAccessPrivObjectForGrantStmt creates an AccessPriv object for the given permission.
 * It will be used when creating GrantStmt objects.
 */
static AccessPriv *
GetAccessPrivObjectForGrantStmt(char *permission)
{
	AccessPriv *accessPriv = makeNode(AccessPriv);
	accessPriv->priv_name = pstrdup(permission);
	accessPriv->cols = NULL;

	return accessPriv;
}


/*
 * GrantOnSequenceDDLCommands creates a list of ddl command for replicating the permissions
 * of roles on distributed sequences.
 */
static List *
GrantOnSequenceDDLCommands(Oid sequenceOid)
{
	HeapTuple seqtup = SearchSysCache1(RELOID, ObjectIdGetDatum(sequenceOid));
	bool isNull = false;
	Datum aclDatum = SysCacheGetAttr(RELOID, seqtup, Anum_pg_class_relacl,
									 &isNull);
	if (isNull)
	{
		ReleaseSysCache(seqtup);
		return NIL;
	}

	Acl *acl = DatumGetAclPCopy(aclDatum);
	AclItem *aclDat = ACL_DAT(acl);
	int aclNum = ACL_NUM(acl);
	List *commands = NIL;

	ReleaseSysCache(seqtup);

	for (int i = 0; i < aclNum; i++)
	{
		commands = list_concat(commands,
							   GenerateGrantOnSequenceQueriesFromAclItem(
								   sequenceOid,
								   &aclDat[i]));
	}

	return commands;
}


/*
 * GenerateGrantOnSequenceQueriesFromAclItem generates a query string for replicating a users permissions
 * on a distributed sequence.
 */
static List *
GenerateGrantOnSequenceQueriesFromAclItem(Oid sequenceOid, AclItem *aclItem)
{
	AclMode permissions = ACLITEM_GET_PRIVS(*aclItem) & ACL_ALL_RIGHTS_SEQUENCE;
	AclMode grants = ACLITEM_GET_GOPTIONS(*aclItem) & ACL_ALL_RIGHTS_SEQUENCE;

	/*
	 * seems unlikely but we check if there is a grant option in the list without the actual permission
	 */
	Assert(!(grants & ACL_USAGE) || (permissions & ACL_USAGE));
	Assert(!(grants & ACL_SELECT) || (permissions & ACL_SELECT));
	Assert(!(grants & ACL_UPDATE) || (permissions & ACL_UPDATE));

	Oid granteeOid = aclItem->ai_grantee;
	List *queries = NIL;
	queries = lappend(queries, GenerateSetRoleQuery(aclItem->ai_grantor));

	if (permissions & ACL_USAGE)
	{
		char *query = DeparseTreeNode((Node *) GenerateGrantStmtForRights(
										  OBJECT_SEQUENCE, granteeOid, sequenceOid,
										  "USAGE", grants & ACL_USAGE));
		queries = lappend(queries, query);
	}

	if (permissions & ACL_SELECT)
	{
		char *query = DeparseTreeNode((Node *) GenerateGrantStmtForRights(
										  OBJECT_SEQUENCE, granteeOid, sequenceOid,
										  "SELECT", grants & ACL_SELECT));
		queries = lappend(queries, query);
	}

	if (permissions & ACL_UPDATE)
	{
		char *query = DeparseTreeNode((Node *) GenerateGrantStmtForRights(
										  OBJECT_SEQUENCE, granteeOid, sequenceOid,
										  "UPDATE", grants & ACL_UPDATE));
		queries = lappend(queries, query);
	}

	queries = lappend(queries, "RESET ROLE");

	return queries;
}


/*
 * SetLocalEnableMetadataSync sets the enable_metadata_sync locally
 */
void
SetLocalEnableMetadataSync(bool state)
{
	set_config_option("citus.enable_metadata_sync", state == true ? "on" : "off",
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);
}


static char *
GenerateSetRoleQuery(Oid roleOid)
{
	StringInfo buf = makeStringInfo();
	appendStringInfo(buf, "SET ROLE %s", quote_identifier(GetUserNameFromId(roleOid,
																			false)));
	return buf->data;
}


/*
 * TruncateTriggerCreateCommand creates a SQL query calling worker_create_truncate_trigger
 * function, which creates the truncate trigger on the worker.
 */
TableDDLCommand *
TruncateTriggerCreateCommand(Oid relationId)
{
	StringInfo triggerCreateCommand = makeStringInfo();
	char *tableName = generate_qualified_relation_name(relationId);

	appendStringInfo(triggerCreateCommand,
					 "SELECT worker_create_truncate_trigger(%s)",
					 quote_literal_cstr(tableName));

	TableDDLCommand *triggerDDLCommand = makeTableDDLCommandString(
		triggerCreateCommand->data);

	return triggerDDLCommand;
}


/*
 * SchemaOwnerName returns the name of the owner of the specified schema.
 */
static char *
SchemaOwnerName(Oid objectId)
{
	Oid ownerId = InvalidOid;

	HeapTuple tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(objectId));
	if (HeapTupleIsValid(tuple))
	{
		ownerId = ((Form_pg_namespace) GETSTRUCT(tuple))->nspowner;
	}
	else
	{
		ownerId = GetUserId();
	}

	char *ownerName = GetUserNameFromId(ownerId, false);

	ReleaseSysCache(tuple);

	return ownerName;
}


/*
 * HasMetadataWorkers returns true if any of the workers in the cluster has its
 * hasmetadata column set to true, which happens when start_metadata_sync_to_node
 * command is run.
 */
static bool
HasMetadataWorkers(void)
{
	List *workerNodeList = ActiveReadableNonCoordinatorNodeList();

	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		if (workerNode->hasMetadata)
		{
			return true;
		}
	}

	return false;
}


/*
 * CreateInterTableRelationshipOfRelationOnWorkers create inter table relationship
 * for the the given relation id on each worker node with metadata.
 */
void
CreateInterTableRelationshipOfRelationOnWorkers(Oid relationId)
{
	/* if the table is owned by an extension we don't create */
	bool tableOwnedByExtension = IsTableOwnedByExtension(relationId);
	if (tableOwnedByExtension)
	{
		return;
	}

	List *commandList =
		InterTableRelationshipOfRelationCommandList(relationId);

	/* prevent recursive propagation */
	SendCommandToWorkersWithMetadata(DISABLE_DDL_PROPAGATION);

	const char *command = NULL;
	foreach_ptr(command, commandList)
	{
		SendCommandToWorkersWithMetadata(command);
	}
}


/*
 * InterTableRelationshipOfRelationCommandList returns the command list to create
 * inter table relationship for the given relation.
 */
List *
InterTableRelationshipOfRelationCommandList(Oid relationId)
{
	/* commands to create foreign key constraints */
	List *commandList = GetReferencingForeignConstaintCommands(relationId);

	/* commands to create partitioning hierarchy */
	if (PartitionTable(relationId))
	{
		char *alterTableAttachPartitionCommands =
			GenerateAlterTableAttachPartitionCommand(relationId);
		commandList = lappend(commandList, alterTableAttachPartitionCommands);
	}

	return commandList;
}


/*
 * CreateShellTableOnWorkers creates the shell table on each worker node with metadata
 * including sequence dependency and truncate triggers.
 */
static void
CreateShellTableOnWorkers(Oid relationId)
{
	if (IsTableOwnedByExtension(relationId))
	{
		return;
	}

	List *commandList = list_make1(DISABLE_DDL_PROPAGATION);

	IncludeSequenceDefaults includeSequenceDefaults = WORKER_NEXTVAL_SEQUENCE_DEFAULTS;
	IncludeIdentities includeIdentityDefaults = INCLUDE_IDENTITY;

	bool creatingShellTableOnRemoteNode = true;
	List *tableDDLCommands = GetFullTableCreationCommands(relationId,
														  includeSequenceDefaults,
														  includeIdentityDefaults,
														  creatingShellTableOnRemoteNode);

	TableDDLCommand *tableDDLCommand = NULL;
	foreach_ptr(tableDDLCommand, tableDDLCommands)
	{
		Assert(CitusIsA(tableDDLCommand, TableDDLCommand));
		commandList = lappend(commandList, GetTableDDLCommand(tableDDLCommand));
	}

	const char *command = NULL;
	foreach_ptr(command, commandList)
	{
		SendCommandToWorkersWithMetadata(command);
	}
}


/*
 * CreateTableMetadataOnWorkers creates the list of commands needed to create the
 * metadata of the given distributed table and sends these commands to all metadata
 * workers i.e. workers with hasmetadata=true. Before sending the commands, in order
 * to prevent recursive propagation, DDL propagation on workers are disabled with a
 * `SET citus.enable_ddl_propagation TO off;` command.
 */
static void
CreateTableMetadataOnWorkers(Oid relationId)
{
	List *commandList = CitusTableMetadataCreateCommandList(relationId);

	/* prevent recursive propagation */
	SendCommandToWorkersWithMetadata(DISABLE_DDL_PROPAGATION);

	/* send the commands one by one */
	const char *command = NULL;
	foreach_ptr(command, commandList)
	{
		SendCommandToWorkersWithMetadata(command);
	}
}


/*
 * DetachPartitionCommandList returns list of DETACH commands to detach partitions
 * of all distributed tables. This function is used for detaching partitions in MX
 * workers before DROPping distributed partitioned tables in them. Thus, we are
 * disabling DDL propagation to the beginning of the commands (we are also enabling
 * DDL propagation at the end of command list to swtich back to original state). As
 * an extra step, if there are no partitions to DETACH, this function simply returns
 * empty list to not disable/enable DDL propagation for nothing.
 */
List *
DetachPartitionCommandList(void)
{
	List *detachPartitionCommandList = NIL;
	List *distributedTableList = CitusTableList();

	/* we iterate over all distributed partitioned tables and DETACH their partitions */
	CitusTableCacheEntry *cacheEntry = NULL;
	foreach_ptr(cacheEntry, distributedTableList)
	{
		if (!PartitionedTable(cacheEntry->relationId))
		{
			continue;
		}

		List *partitionList = PartitionList(cacheEntry->relationId);
		List *detachCommands =
			GenerateDetachPartitionCommandRelationIdList(partitionList);
		detachPartitionCommandList = list_concat(detachPartitionCommandList,
												 detachCommands);
	}

	if (list_length(detachPartitionCommandList) == 0)
	{
		return NIL;
	}

	detachPartitionCommandList =
		lcons(DISABLE_DDL_PROPAGATION, detachPartitionCommandList);

	/*
	 * We probably do not need this but as an extra precaution, we are enabling
	 * DDL propagation to switch back to original state.
	 */
	detachPartitionCommandList = lappend(detachPartitionCommandList,
										 ENABLE_DDL_PROPAGATION);

	return detachPartitionCommandList;
}


/*
 * SyncNodeMetadataToNodesOptional tries recreating the metadata
 * snapshot in the metadata workers that are out of sync.
 * Returns the result of synchronization.
 *
 * This function must be called within coordinated transaction
 * since updates on the pg_dist_node metadata must be rollbacked if anything
 * goes wrong.
 */
static NodeMetadataSyncResult
SyncNodeMetadataToNodesOptional(void)
{
	NodeMetadataSyncResult result = NODE_METADATA_SYNC_SUCCESS;
	if (!IsCoordinator())
	{
		return NODE_METADATA_SYNC_SUCCESS;
	}

	/*
	 * Request a RowExclusiveLock so we don't run concurrently with other
	 * functions updating pg_dist_node, but allow concurrency with functions
	 * which are just reading from pg_dist_node.
	 */
	if (!ConditionalLockRelationOid(DistNodeRelationId(), RowExclusiveLock))
	{
		return NODE_METADATA_SYNC_FAILED_LOCK;
	}

	List *syncedWorkerList = NIL;
	List *workerList = ActivePrimaryNonCoordinatorNodeList(NoLock);
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerList)
	{
		if (workerNode->hasMetadata && !workerNode->metadataSynced)
		{
			bool raiseInterrupts = false;
			if (!SyncNodeMetadataSnapshotToNode(workerNode, raiseInterrupts))
			{
				ereport(WARNING, (errmsg("failed to sync metadata to %s:%d",
										 workerNode->workerName,
										 workerNode->workerPort)));
				result = NODE_METADATA_SYNC_FAILED_SYNC;
			}
			else
			{
				/* we add successfully synced nodes to set metadatasynced column later */
				syncedWorkerList = lappend(syncedWorkerList, workerNode);
			}
		}
	}

	foreach_ptr(workerNode, syncedWorkerList)
	{
		SetWorkerColumnOptional(workerNode, Anum_pg_dist_node_metadatasynced,
								BoolGetDatum(true));

		/* we fetch the same node again to check if it's synced or not */
		WorkerNode *nodeUpdated = FindWorkerNode(workerNode->workerName,
												 workerNode->workerPort);
		if (!nodeUpdated->metadataSynced)
		{
			/* set the result to FAILED to trigger the sync again */
			result = NODE_METADATA_SYNC_FAILED_SYNC;
		}
	}

	return result;
}


/*
 * SyncNodeMetadataToNodes recreates the node metadata snapshot in all the
 * metadata workers.
 *
 * This function runs within a coordinated transaction since updates on
 * the pg_dist_node metadata must be rollbacked if anything
 * goes wrong.
 */
void
SyncNodeMetadataToNodes(void)
{
	EnsureCoordinator();

	/*
	 * Request a RowExclusiveLock so we don't run concurrently with other
	 * functions updating pg_dist_node, but allow concurrency with functions
	 * which are just reading from pg_dist_node.
	 */
	if (!ConditionalLockRelationOid(DistNodeRelationId(), RowExclusiveLock))
	{
		ereport(ERROR, (errmsg("cannot sync metadata because a concurrent "
							   "metadata syncing operation is in progress")));
	}

	List *workerList = ActivePrimaryNonCoordinatorNodeList(NoLock);
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerList)
	{
		if (workerNode->hasMetadata)
		{
			SetWorkerColumnLocalOnly(workerNode, Anum_pg_dist_node_metadatasynced,
									 BoolGetDatum(true));

			bool raiseOnError = true;
			SyncNodeMetadataSnapshotToNode(workerNode, raiseOnError);
		}
	}
}


/*
 * SyncNodeMetadataToNodesMain is the main function for syncing node metadata to
 * MX nodes. It retries until success and then exits.
 */
void
SyncNodeMetadataToNodesMain(Datum main_arg)
{
	Oid databaseOid = DatumGetObjectId(main_arg);

	/* extension owner is passed via bgw_extra */
	Oid extensionOwner = InvalidOid;
	memcpy_s(&extensionOwner, sizeof(extensionOwner),
			 MyBgworkerEntry->bgw_extra, sizeof(Oid));

	pqsignal(SIGTERM, MetadataSyncSigTermHandler);
	pqsignal(SIGALRM, MetadataSyncSigAlrmHandler);
	BackgroundWorkerUnblockSignals();

	/* connect to database, after that we can actually access catalogs */
	BackgroundWorkerInitializeConnectionByOid(databaseOid, extensionOwner, 0);

	/* make worker recognizable in pg_stat_activity */
	pgstat_report_appname(METADATA_SYNC_APP_NAME);

	bool syncedAllNodes = false;

	while (!syncedAllNodes)
	{
		InvalidateMetadataSystemCache();
		StartTransactionCommand();

		/*
		 * Some functions in ruleutils.c, which we use to get the DDL for
		 * metadata propagation, require an active snapshot.
		 */
		PushActiveSnapshot(GetTransactionSnapshot());

		if (!LockCitusExtension())
		{
			ereport(DEBUG1, (errmsg("could not lock the citus extension, "
									"skipping metadata sync")));
		}
		else if (CheckCitusVersion(DEBUG1) && CitusHasBeenLoaded())
		{
			UseCoordinatedTransaction();

			NodeMetadataSyncResult result = SyncNodeMetadataToNodesOptional();
			syncedAllNodes = (result == NODE_METADATA_SYNC_SUCCESS);

			/* we use LISTEN/NOTIFY to wait for metadata syncing in tests */
			if (result != NODE_METADATA_SYNC_FAILED_LOCK)
			{
				Async_Notify(METADATA_SYNC_CHANNEL, NULL);
			}
		}

		PopActiveSnapshot();
		CommitTransactionCommand();
		ProcessCompletedNotifies();

		if (syncedAllNodes)
		{
			break;
		}

		/*
		 * If backend is cancelled (e.g. bacause of distributed deadlock),
		 * CHECK_FOR_INTERRUPTS() will raise a cancellation error which will
		 * result in exit(1).
		 */
		CHECK_FOR_INTERRUPTS();

		/*
		 * SIGTERM is used for when maintenance daemon tries to clean-up
		 * metadata sync daemons spawned by terminated maintenance daemons.
		 */
		if (got_SIGTERM)
		{
			exit(0);
		}

		/*
		 * SIGALRM is used for testing purposes and it simulates an error in metadata
		 * sync daemon.
		 */
		if (got_SIGALRM)
		{
			elog(ERROR, "Error in metadata sync daemon");
		}

		pg_usleep(MetadataSyncRetryInterval * 1000);
	}
}


/*
 * MetadataSyncSigTermHandler set a flag to request termination of metadata
 * sync daemon.
 */
static void
MetadataSyncSigTermHandler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGTERM = true;
	if (MyProc != NULL)
	{
		SetLatch(&MyProc->procLatch);
	}

	errno = save_errno;
}


/*
 * MetadataSyncSigAlrmHandler set a flag to request error at metadata
 * sync daemon. This is used for testing purposes.
 */
static void
MetadataSyncSigAlrmHandler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGALRM = true;
	if (MyProc != NULL)
	{
		SetLatch(&MyProc->procLatch);
	}

	errno = save_errno;
}


/*
 * SpawnSyncNodeMetadataToNodes starts a background worker which runs node metadata
 * sync. On success it returns workers' handle. Otherwise it returns NULL.
 */
BackgroundWorkerHandle *
SpawnSyncNodeMetadataToNodes(Oid database, Oid extensionOwner)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle = NULL;

	/* Configure a worker. */
	memset(&worker, 0, sizeof(worker));
	SafeSnprintf(worker.bgw_name, BGW_MAXLEN,
				 "Citus Metadata Sync: %u/%u",
				 database, extensionOwner);
	worker.bgw_flags =
		BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;

	/* don't restart, we manage restarts from maintenance daemon */
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	strcpy_s(worker.bgw_library_name, sizeof(worker.bgw_library_name), "citus");
	strcpy_s(worker.bgw_function_name, sizeof(worker.bgw_library_name),
			 "SyncNodeMetadataToNodesMain");
	worker.bgw_main_arg = ObjectIdGetDatum(MyDatabaseId);
	memcpy_s(worker.bgw_extra, sizeof(worker.bgw_extra), &extensionOwner,
			 sizeof(Oid));
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		return NULL;
	}

	pid_t pid;
	WaitForBackgroundWorkerStartup(handle, &pid);

	return handle;
}


/*
 * SignalMetadataSyncDaemon signals metadata sync daemons belonging to
 * the given database.
 */
void
SignalMetadataSyncDaemon(Oid database, int sig)
{
	int backendCount = pgstat_fetch_stat_numbackends();
	for (int backend = 1; backend <= backendCount; backend++)
	{
		LocalPgBackendStatus *localBeEntry = pgstat_fetch_stat_local_beentry(backend);
		if (!localBeEntry)
		{
			continue;
		}

		PgBackendStatus *beStatus = &localBeEntry->backendStatus;
		if (beStatus->st_databaseid == database &&
			strncmp(beStatus->st_appname, METADATA_SYNC_APP_NAME, BGW_MAXLEN) == 0)
		{
			kill(beStatus->st_procpid, sig);
		}
	}
}


/*
 * ShouldInitiateMetadataSync returns if metadata sync daemon should be initiated.
 * It sets lockFailure to true if pg_dist_node lock couldn't be acquired for the
 * check.
 */
bool
ShouldInitiateMetadataSync(bool *lockFailure)
{
	if (!IsCoordinator())
	{
		*lockFailure = false;
		return false;
	}

	Oid distNodeOid = DistNodeRelationId();
	if (!ConditionalLockRelationOid(distNodeOid, AccessShareLock))
	{
		*lockFailure = true;
		return false;
	}

	bool shouldSyncMetadata = false;

	List *workerList = ActivePrimaryNonCoordinatorNodeList(NoLock);
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerList)
	{
		if (workerNode->hasMetadata && !workerNode->metadataSynced)
		{
			shouldSyncMetadata = true;
			break;
		}
	}

	UnlockRelationOid(distNodeOid, AccessShareLock);

	*lockFailure = false;
	return shouldSyncMetadata;
}


/*
 * citus_internal_add_partition_metadata is an internal UDF to
 * add a row to pg_dist_partition.
 */
Datum
citus_internal_add_partition_metadata(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	PG_ENSURE_ARGNOTNULL(0, "relation");
	Oid relationId = PG_GETARG_OID(0);

	PG_ENSURE_ARGNOTNULL(1, "distribution method");
	char distributionMethod = PG_GETARG_CHAR(1);

	PG_ENSURE_ARGNOTNULL(3, "Colocation ID");
	int colocationId = PG_GETARG_INT32(3);

	PG_ENSURE_ARGNOTNULL(4, "replication model");
	char replicationModel = PG_GETARG_CHAR(4);

	text *distributionColumnText = NULL;
	char *distributionColumnString = NULL;
	Var *distributionColumnVar = NULL;

	/* this flag is only valid for citus local tables, so set it to false */
	bool autoConverted = false;

	/* only owner of the table (or superuser) is allowed to add the Citus metadata */
	EnsureTableOwner(relationId);

	/* we want to serialize all the metadata changes to this table */
	LockRelationOid(relationId, ShareUpdateExclusiveLock);

	if (!PG_ARGISNULL(2))
	{
		distributionColumnText = PG_GETARG_TEXT_P(2);
		distributionColumnString = text_to_cstring(distributionColumnText);

		distributionColumnVar =
			BuildDistributionKeyFromColumnName(relationId, distributionColumnString,
											   AccessShareLock);
		Assert(distributionColumnVar != NULL);
	}

	if (!ShouldSkipMetadataChecks())
	{
		/* this UDF is not allowed allowed for executing as a separate command */
		EnsureCoordinatorInitiatedOperation();

		if (distributionMethod == DISTRIBUTE_BY_NONE && distributionColumnVar != NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("Reference or local tables cannot have "
								   "distribution columns")));
		}
		else if (distributionMethod != DISTRIBUTE_BY_NONE &&
				 distributionColumnVar == NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("Distribution column cannot be NULL for "
								   "relation \"%s\"", get_rel_name(relationId))));
		}

		/*
		 * Even if the table owner is a malicious user and the partition
		 * metadata is not sane, the user can only affect its own tables.
		 * Given that the user is owner of the table, we should allow.
		 */
		EnsurePartitionMetadataIsSane(relationId, distributionMethod, colocationId,
									  replicationModel, distributionColumnVar);
	}

	InsertIntoPgDistPartition(relationId, distributionMethod, distributionColumnVar,
							  colocationId, replicationModel, autoConverted);

	PG_RETURN_VOID();
}


/*
 * EnsurePartitionMetadataIsSane ensures that the input values are safe
 * for inserting into pg_dist_partition metadata.
 */
static void
EnsurePartitionMetadataIsSane(Oid relationId, char distributionMethod, int colocationId,
							  char replicationModel, Var *distributionColumnVar)
{
	if (!(distributionMethod == DISTRIBUTE_BY_HASH ||
		  distributionMethod == DISTRIBUTE_BY_NONE))
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Metadata syncing is only allowed for hash, reference "
							   "and local tables:%c", distributionMethod)));
	}

	if (colocationId < INVALID_COLOCATION_ID)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Metadata syncing is only allowed for valid "
							   "colocation id values.")));
	}
	else if (colocationId != INVALID_COLOCATION_ID &&
			 distributionMethod == DISTRIBUTE_BY_HASH)
	{
		int count = 1;
		List *targetColocatedTableList =
			ColocationGroupTableList(colocationId, count);

		/*
		 * If we have any colocated hash tables, ensure if they share the
		 * same distribution key properties.
		 */
		if (list_length(targetColocatedTableList) >= 1)
		{
			Oid targetRelationId = linitial_oid(targetColocatedTableList);

			EnsureColumnTypeEquality(relationId, targetRelationId, distributionColumnVar,
									 DistPartitionKeyOrError(targetRelationId));
		}
	}


	if (!(replicationModel == REPLICATION_MODEL_2PC ||
		  replicationModel == REPLICATION_MODEL_STREAMING ||
		  replicationModel == REPLICATION_MODEL_COORDINATOR))
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Metadata syncing is only allowed for "
							   "known replication models.")));
	}

	if (distributionMethod == DISTRIBUTE_BY_NONE &&
		!(replicationModel == REPLICATION_MODEL_STREAMING ||
		  replicationModel == REPLICATION_MODEL_2PC))
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Local or references tables can only have '%c' or '%c' "
							   "as the replication model.",
							   REPLICATION_MODEL_STREAMING, REPLICATION_MODEL_2PC)));
	}
}


/*
 * citus_internal_delete_partition_metadata is an internal UDF to
 * delete a row in pg_dist_partition.
 */
Datum
citus_internal_delete_partition_metadata(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	PG_ENSURE_ARGNOTNULL(0, "relation");
	Oid relationId = PG_GETARG_OID(0);

	/* only owner of the table (or superuser) is allowed to add the Citus metadata */
	EnsureTableOwner(relationId);

	/* we want to serialize all the metadata changes to this table */
	LockRelationOid(relationId, ShareUpdateExclusiveLock);

	if (!ShouldSkipMetadataChecks())
	{
		EnsureCoordinatorInitiatedOperation();
	}

	DeletePartitionRow(relationId);

	PG_RETURN_VOID();
}


/*
 * citus_internal_add_shard_metadata is an internal UDF to
 * add a row to pg_dist_shard.
 */
Datum
citus_internal_add_shard_metadata(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	PG_ENSURE_ARGNOTNULL(0, "relation");
	Oid relationId = PG_GETARG_OID(0);

	PG_ENSURE_ARGNOTNULL(1, "shard id");
	int64 shardId = PG_GETARG_INT64(1);

	PG_ENSURE_ARGNOTNULL(2, "storage type");
	char storageType = PG_GETARG_CHAR(2);

	text *shardMinValue = NULL;
	if (!PG_ARGISNULL(3))
	{
		shardMinValue = PG_GETARG_TEXT_P(3);
	}

	text *shardMaxValue = NULL;
	if (!PG_ARGISNULL(4))
	{
		shardMaxValue = PG_GETARG_TEXT_P(4);
	}

	/* only owner of the table (or superuser) is allowed to add the Citus metadata */
	EnsureTableOwner(relationId);

	/* we want to serialize all the metadata changes to this table */
	LockRelationOid(relationId, ShareUpdateExclusiveLock);

	if (!ShouldSkipMetadataChecks())
	{
		/* this UDF is not allowed allowed for executing as a separate command */
		EnsureCoordinatorInitiatedOperation();

		/*
		 * Even if the table owner is a malicious user and the shard metadata is
		 * not sane, the user can only affect its own tables. Given that the
		 * user is owner of the table, we should allow.
		 */
		EnsureShardMetadataIsSane(relationId, shardId, storageType, shardMinValue,
								  shardMaxValue);
	}

	InsertShardRow(relationId, shardId, storageType, shardMinValue, shardMaxValue);

	PG_RETURN_VOID();
}


/*
 * EnsureCoordinatorInitiatedOperation is a helper function which ensures that
 * the execution is initiated by the coordinator on a worker node.
 */
static void
EnsureCoordinatorInitiatedOperation(void)
{
	/*
	 * We are restricting the operation to only MX workers with the local group id
	 * check. The other two checks are to ensure that the operation is initiated
	 * by the coordinator.
	 */
	if (!(IsCitusInternalBackend() || IsRebalancerInternalBackend()) ||
		GetLocalGroupId() == COORDINATOR_GROUP_ID)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("This is an internal Citus function can only be "
							   "used in a distributed transaction")));
	}
}


/*
 * EnsureShardMetadataIsSane ensures that the input values are safe
 * for inserting into pg_dist_shard metadata.
 */
static void
EnsureShardMetadataIsSane(Oid relationId, int64 shardId, char storageType,
						  text *shardMinValue, text *shardMaxValue)
{
	if (shardId <= INVALID_SHARD_ID)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Invalid shard id: %ld", shardId)));
	}

	if (!(storageType == SHARD_STORAGE_TABLE ||
		  storageType == SHARD_STORAGE_FOREIGN))
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Invalid shard storage type: %c", storageType)));
	}

	char partitionMethod = PartitionMethodViaCatalog(relationId);
	if (partitionMethod == DISTRIBUTE_BY_INVALID)
	{
		/* connection from the coordinator operating on a shard */
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("The relation \"%s\" does not have a valid "
							   "entry in pg_dist_partition.",
							   get_rel_name(relationId))));
	}
	else if (!(partitionMethod == DISTRIBUTE_BY_HASH ||
			   partitionMethod == DISTRIBUTE_BY_NONE))
	{
		/* connection from the coordinator operating on a shard */
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Metadata syncing is only allowed for hash, "
							   "reference and local tables: %c", partitionMethod)));
	}

	List *distShardTupleList = LookupDistShardTuples(relationId);
	if (partitionMethod == DISTRIBUTE_BY_NONE)
	{
		if (shardMinValue != NULL || shardMaxValue != NULL)
		{
			char *relationName = get_rel_name(relationId);
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("Shards of reference or local table \"%s\" should "
								   "have NULL shard ranges", relationName)));
		}
		else if (list_length(distShardTupleList) != 0)
		{
			char *relationName = get_rel_name(relationId);
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("relation \"%s\" has already at least one shard, "
								   "adding more is not allowed", relationName)));
		}
	}
	else if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		if (shardMinValue == NULL || shardMaxValue == NULL)
		{
			char *relationName = get_rel_name(relationId);
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("Shards of has distributed table  \"%s\" "
								   "cannot have NULL shard ranges", relationName)));
		}

		char *shardMinValueString = text_to_cstring(shardMinValue);
		char *shardMaxValueString = text_to_cstring(shardMaxValue);

		/* pg_strtoint32 does the syntax and out of bound checks for us */
		int32 shardMinValueInt = pg_strtoint32(shardMinValueString);
		int32 shardMaxValueInt = pg_strtoint32(shardMaxValueString);

		if (shardMinValueInt > shardMaxValueInt)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("shardMinValue=%d is greater than "
								   "shardMaxValue=%d for table \"%s\", which is "
								   "not allowed", shardMinValueInt,
								   shardMaxValueInt, get_rel_name(relationId))));
		}

		/*
		 * We are only dealing with hash distributed tables, that's why we
		 * can hard code data type and typemod.
		 */
		const int intervalTypeId = INT4OID;
		const int intervalTypeMod = -1;

		Relation distShardRelation = table_open(DistShardRelationId(), AccessShareLock);
		TupleDesc distShardTupleDesc = RelationGetDescr(distShardRelation);

		FmgrInfo *shardIntervalCompareFunction =
			GetFunctionInfo(intervalTypeId, BTREE_AM_OID, BTORDER_PROC);

		HeapTuple shardTuple = NULL;
		foreach_ptr(shardTuple, distShardTupleList)
		{
			ShardInterval *shardInterval =
				TupleToShardInterval(shardTuple, distShardTupleDesc,
									 intervalTypeId, intervalTypeMod);

			Datum firstMin = Int32GetDatum(shardMinValueInt);
			Datum firstMax = Int32GetDatum(shardMaxValueInt);
			Datum secondMin = shardInterval->minValue;
			Datum secondMax = shardInterval->maxValue;
			Oid collationId = InvalidOid;

			/*
			 * This is an unexpected case as we are reading the metadata, which has
			 * already been verified for being not NULL. Still, lets be extra
			 * cautious to avoid any crashes.
			 */
			if (!shardInterval->minValueExists || !shardInterval->maxValueExists)
			{
				char *relationName = get_rel_name(relationId);
				ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
								errmsg("Shards of has distributed table  \"%s\" "
									   "cannot have NULL shard ranges", relationName)));
			}

			if (ShardIntervalsOverlapWithParams(firstMin, firstMax, secondMin, secondMax,
												shardIntervalCompareFunction,
												collationId))
			{
				ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
								errmsg("Shard intervals overlap for table \"%s\": "
									   "%ld and %ld", get_rel_name(relationId),
									   shardId, shardInterval->shardId)));
			}
		}

		table_close(distShardRelation, NoLock);
	}
}


/*
 * citus_internal_add_placement_metadata is an internal UDF to
 * add a row to pg_dist_placement.
 */
Datum
citus_internal_add_placement_metadata(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	int64 shardId = PG_GETARG_INT64(0);
	int64 shardLength = PG_GETARG_INT64(1);
	int32 groupId = PG_GETARG_INT32(2);
	int64 placementId = PG_GETARG_INT64(3);

	citus_internal_add_placement_metadata_internal(shardId, shardLength,
												   groupId, placementId);

	PG_RETURN_VOID();
}


/*
 * citus_internal_add_placement_metadata is an internal UDF to
 * delete a row from pg_dist_placement.
 */
Datum
citus_internal_delete_placement_metadata(PG_FUNCTION_ARGS)
{
	PG_ENSURE_ARGNOTNULL(0, "placement_id");
	int64 placementId = PG_GETARG_INT64(0);

	if (!ShouldSkipMetadataChecks())
	{
		/* this UDF is not allowed allowed for executing as a separate command */
		EnsureCoordinatorInitiatedOperation();
	}

	DeleteShardPlacementRow(placementId);

	PG_RETURN_VOID();
}


/*
 * citus_internal_add_placement_metadata_legacy is the old function that will be dropped.
 */
Datum
citus_internal_add_placement_metadata_legacy(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	int64 shardId = PG_GETARG_INT64(0);
	int64 shardLength = PG_GETARG_INT64(2);
	int32 groupId = PG_GETARG_INT32(3);
	int64 placementId = PG_GETARG_INT64(4);

	citus_internal_add_placement_metadata_internal(shardId, shardLength,
												   groupId, placementId);
	PG_RETURN_VOID();
}


/*
 * citus_internal_add_placement_metadata_internal is the internal function
 * too insert a row into pg_dist_placement
 */
void
citus_internal_add_placement_metadata_internal(int64 shardId, int64 shardLength,
											   int32 groupId, int64 placementId)
{
	bool missingOk = false;
	Oid relationId = LookupShardRelationFromCatalog(shardId, missingOk);

	/* only owner of the table is allowed to modify the metadata */
	EnsureTableOwner(relationId);

	/* we want to serialize all the metadata changes to this table */
	LockRelationOid(relationId, ShareUpdateExclusiveLock);

	if (!ShouldSkipMetadataChecks())
	{
		/* this UDF is not allowed allowed for executing as a separate command */
		EnsureCoordinatorInitiatedOperation();

		/*
		 * Even if the table owner is a malicious user, as long as the shard placements
		 * fit into basic requirements of Citus metadata, the user can only affect its
		 * own tables. Given that the user is owner of the table, we should allow.
		 */
		EnsureShardPlacementMetadataIsSane(relationId, shardId, placementId,
										   shardLength, groupId);
	}

	InsertShardPlacementRow(shardId, placementId, shardLength, groupId);
}


/*
 * EnsureShardPlacementMetadataIsSane ensures if the input parameters for
 * the shard placement metadata is sane.
 */
static void
EnsureShardPlacementMetadataIsSane(Oid relationId, int64 shardId, int64 placementId,
								   int64 shardLength, int32 groupId)
{
	/* we have just read the metadata, so we are sure that the shard exists */
	Assert(ShardExists(shardId));

	if (placementId <= INVALID_PLACEMENT_ID)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Shard placement has invalid placement id "
							   "(%ld) for shard(%ld)", placementId, shardId)));
	}

	bool nodeIsInMetadata = false;
	WorkerNode *workerNode =
		PrimaryNodeForGroup(groupId, &nodeIsInMetadata);
	if (!workerNode)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Node with group id %d for shard placement "
							   "%ld does not exist", groupId, shardId)));
	}
}


/*
 * ShouldSkipMetadataChecks returns true if the current user is allowed to
 * make any
 */
static bool
ShouldSkipMetadataChecks(void)
{
	if (strcmp(EnableManualMetadataChangesForUser, "") != 0)
	{
		/*
		 * EnableManualMetadataChangesForUser is a GUC which
		 * can be changed by a super user. We use this GUC as
		 * a safety belt in case the current metadata checks are
		 * too restrictive and the operator can allow users to skip
		 * the checks.
		 */

		/*
		 * Make sure that the user exists, and print it to prevent any
		 * optimization skipping the get_role_oid call.
		 */
		bool missingOK = false;
		Oid allowedUserId = get_role_oid(EnableManualMetadataChangesForUser, missingOK);
		if (allowedUserId == GetUserId())
		{
			return true;
		}
	}

	return false;
}


/*
 * citus_internal_update_placement_metadata is an internal UDF to
 * update a row in pg_dist_placement.
 */
Datum
citus_internal_update_placement_metadata(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	int64 shardId = PG_GETARG_INT64(0);
	int32 sourceGroupId = PG_GETARG_INT32(1);
	int32 targetGroupId = PG_GETARG_INT32(2);

	ShardPlacement *placement = NULL;
	if (!ShouldSkipMetadataChecks())
	{
		/* this UDF is not allowed allowed for executing as a separate command */
		EnsureCoordinatorInitiatedOperation();

		if (!ShardExists(shardId))
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("Shard id does not exists: %ld", shardId)));
		}

		bool missingOk = false;
		EnsureShardOwner(shardId, missingOk);

		/*
		 * This function ensures that the source group exists hence we
		 * call it from this code-block.
		 */
		placement = ActiveShardPlacementOnGroup(sourceGroupId, shardId);

		bool nodeIsInMetadata = false;
		WorkerNode *workerNode =
			PrimaryNodeForGroup(targetGroupId, &nodeIsInMetadata);
		if (!workerNode)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("Node with group id %d for shard placement "
								   "%ld does not exist", targetGroupId, shardId)));
		}
	}
	else
	{
		placement = ActiveShardPlacementOnGroup(sourceGroupId, shardId);
	}

	/*
	 * Updating pg_dist_placement ensures that the node with targetGroupId
	 * exists and this is the only placement on that group.
	 */
	if (placement == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Active placement for shard %ld is not "
							   "found on group:%d", shardId, targetGroupId)));
	}

	UpdatePlacementGroupId(placement->placementId, targetGroupId);

	PG_RETURN_VOID();
}


/*
 * citus_internal_delete_shard_metadata is an internal UDF to
 * delete a row in pg_dist_shard and corresponding placement rows
 * from pg_dist_shard_placement.
 */
Datum
citus_internal_delete_shard_metadata(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	int64 shardId = PG_GETARG_INT64(0);

	if (!ShouldSkipMetadataChecks())
	{
		/* this UDF is not allowed allowed for executing as a separate command */
		EnsureCoordinatorInitiatedOperation();

		if (!ShardExists(shardId))
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("Shard id does not exists: %ld", shardId)));
		}

		bool missingOk = false;
		EnsureShardOwner(shardId, missingOk);
	}

	List *shardPlacementList = ShardPlacementList(shardId);
	ShardPlacement *shardPlacement = NULL;
	foreach_ptr(shardPlacement, shardPlacementList)
	{
		DeleteShardPlacementRow(shardPlacement->placementId);
	}

	DeleteShardRow(shardId);

	PG_RETURN_VOID();
}


/*
 * citus_internal_update_relation_colocation is an internal UDF to
 * delete a row in pg_dist_shard and corresponding placement rows
 * from pg_dist_shard_placement.
 */
Datum
citus_internal_update_relation_colocation(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid relationId = PG_GETARG_OID(0);
	uint32 targetColocationId = PG_GETARG_UINT32(1);

	EnsureTableOwner(relationId);

	if (!ShouldSkipMetadataChecks())
	{
		/* this UDF is not allowed allowed for executing as a separate command */
		EnsureCoordinatorInitiatedOperation();

		/* ensure that the table is in pg_dist_partition */
		char partitionMethod = PartitionMethodViaCatalog(relationId);
		if (partitionMethod == DISTRIBUTE_BY_INVALID)
		{
			/* connection from the coordinator operating on a shard */
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("The relation \"%s\" does not have a valid "
								   "entry in pg_dist_partition.",
								   get_rel_name(relationId))));
		}
		else if (!IsCitusTableType(relationId, HASH_DISTRIBUTED) &&
				 !IsCitusTableType(relationId, SINGLE_SHARD_DISTRIBUTED))
		{
			/* connection from the coordinator operating on a shard */
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("Updating colocation ids are only allowed for hash "
								   "and single shard distributed tables: %c",
								   partitionMethod)));
		}

		int count = 1;
		List *targetColocatedTableList =
			ColocationGroupTableList(targetColocationId, count);

		if (list_length(targetColocatedTableList) == 0)
		{
			/* the table is colocated with none, so nothing to check */
		}
		else
		{
			Oid targetRelationId = linitial_oid(targetColocatedTableList);

			ErrorIfShardPlacementsNotColocated(relationId, targetRelationId);
			CheckReplicationModel(relationId, targetRelationId);
			CheckDistributionColumnType(relationId, targetRelationId);
		}
	}

	bool localOnly = true;
	UpdateRelationColocationGroup(relationId, targetColocationId, localOnly);

	PG_RETURN_VOID();
}


/*
 * citus_internal_add_colocation_metadata is an internal UDF to
 * add a row to pg_dist_colocation.
 */
Datum
citus_internal_add_colocation_metadata(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureSuperUser();

	int colocationId = PG_GETARG_INT32(0);
	int shardCount = PG_GETARG_INT32(1);
	int replicationFactor = PG_GETARG_INT32(2);
	Oid distributionColumnType = PG_GETARG_INT32(3);
	Oid distributionColumnCollation = PG_GETARG_INT32(4);

	if (!ShouldSkipMetadataChecks())
	{
		/* this UDF is not allowed allowed for executing as a separate command */
		EnsureCoordinatorInitiatedOperation();
	}

	InsertColocationGroupLocally(colocationId, shardCount, replicationFactor,
								 distributionColumnType, distributionColumnCollation);

	PG_RETURN_VOID();
}


/*
 * citus_internal_delete_colocation_metadata is an internal UDF to
 * delte row from pg_dist_colocation.
 */
Datum
citus_internal_delete_colocation_metadata(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureSuperUser();

	int colocationId = PG_GETARG_INT32(0);

	if (!ShouldSkipMetadataChecks())
	{
		/* this UDF is not allowed allowed for executing as a separate command */
		EnsureCoordinatorInitiatedOperation();
	}

	DeleteColocationGroupLocally(colocationId);

	PG_RETURN_VOID();
}


/*
 * citus_internal_add_tenant_schema is an internal UDF to
 * call InsertTenantSchemaLocally on a remote node.
 *
 * None of the parameters are allowed to be NULL. To set the colocation
 * id to NULL in metadata, use INVALID_COLOCATION_ID.
 */
Datum
citus_internal_add_tenant_schema(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	PG_ENSURE_ARGNOTNULL(0, "schema_id");
	Oid schemaId = PG_GETARG_OID(0);

	PG_ENSURE_ARGNOTNULL(1, "colocation_id");
	uint32 colocationId = PG_GETARG_INT32(1);

	InsertTenantSchemaLocally(schemaId, colocationId);

	PG_RETURN_VOID();
}


/*
 * citus_internal_delete_tenant_schema is an internal UDF to
 * call DeleteTenantSchemaLocally on a remote node.
 *
 * The schemaId parameter is not allowed to be NULL. Morever, input schema is
 * expected to be dropped already because this function is called from Citus
 * drop hook and only used to clean up metadata after the schema is dropped.
 */
Datum
citus_internal_delete_tenant_schema(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	PG_ENSURE_ARGNOTNULL(0, "schema_id");
	Oid schemaId = PG_GETARG_OID(0);

	DeleteTenantSchemaLocally(schemaId);

	PG_RETURN_VOID();
}


/*
 * citus_internal_update_none_dist_table_metadata is an internal UDF to
 * update a row in pg_dist_partition that belongs to given none-distributed
 * table.
 */
Datum
citus_internal_update_none_dist_table_metadata(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	PG_ENSURE_ARGNOTNULL(0, "relation_id");
	Oid relationId = PG_GETARG_OID(0);

	PG_ENSURE_ARGNOTNULL(1, "replication_model");
	char replicationModel = PG_GETARG_CHAR(1);

	PG_ENSURE_ARGNOTNULL(2, "colocation_id");
	uint32 colocationId = PG_GETARG_INT32(2);

	PG_ENSURE_ARGNOTNULL(3, "auto_converted");
	bool autoConverted = PG_GETARG_BOOL(3);

	if (!ShouldSkipMetadataChecks())
	{
		EnsureCoordinatorInitiatedOperation();
	}

	UpdateNoneDistTableMetadata(relationId, replicationModel,
								colocationId, autoConverted);

	PG_RETURN_VOID();
}


/*
 * SyncNewColocationGroup synchronizes a new pg_dist_colocation entry to a worker.
 */
void
SyncNewColocationGroupToNodes(uint32 colocationId, int shardCount, int replicationFactor,
							  Oid distributionColumnType, Oid distributionColumnCollation)
{
	char *command = ColocationGroupCreateCommand(colocationId, shardCount,
												 replicationFactor,
												 distributionColumnType,
												 distributionColumnCollation);

	/*
	 * We require superuser for all pg_dist_colocation operations because we have
	 * no reasonable way of restricting access.
	 */
	SendCommandToWorkersWithMetadataViaSuperUser(command);
}


/*
 * ColocationGroupCreateCommand returns a command for creating a colocation group.
 */
static char *
ColocationGroupCreateCommand(uint32 colocationId, int shardCount, int replicationFactor,
							 Oid distributionColumnType, Oid distributionColumnCollation)
{
	StringInfo insertColocationCommand = makeStringInfo();

	appendStringInfo(insertColocationCommand,
					 "SELECT pg_catalog.citus_internal_add_colocation_metadata("
					 "%d, %d, %d, %s, %s)",
					 colocationId,
					 shardCount,
					 replicationFactor,
					 RemoteTypeIdExpression(distributionColumnType),
					 RemoteCollationIdExpression(distributionColumnCollation));

	return insertColocationCommand->data;
}


/*
 * RemoteTypeIdExpression returns an expression in text form that can
 * be used to obtain the OID of a type on a different node when included
 * in a query string.
 */
static char *
RemoteTypeIdExpression(Oid typeId)
{
	/* by default, use 0 (InvalidOid) */
	char *expression = "0";

	/* we also have pg_dist_colocation entries for reference tables */
	if (typeId != InvalidOid)
	{
		char *typeName = format_type_extended(typeId, -1,
											  FORMAT_TYPE_FORCE_QUALIFY |
											  FORMAT_TYPE_ALLOW_INVALID);

		/* format_type_extended returns ??? in case of an unknown type */
		if (strcmp(typeName, "???") != 0)
		{
			StringInfo regtypeExpression = makeStringInfo();

			appendStringInfo(regtypeExpression,
							 "%s::regtype",
							 quote_literal_cstr(typeName));

			expression = regtypeExpression->data;
		}
	}

	return expression;
}


/*
 * RemoteCollationIdExpression returns an expression in text form that can
 * be used to obtain the OID of a collation on a different node when included
 * in a query string.
 */
static char *
RemoteCollationIdExpression(Oid colocationId)
{
	/* by default, use 0 (InvalidOid) */
	char *expression = "0";

	if (colocationId != InvalidOid)
	{
		Datum collationIdDatum = ObjectIdGetDatum(colocationId);
		HeapTuple collationTuple = SearchSysCache1(COLLOID, collationIdDatum);

		if (HeapTupleIsValid(collationTuple))
		{
			Form_pg_collation collationform =
				(Form_pg_collation) GETSTRUCT(collationTuple);
			char *collationName = NameStr(collationform->collname);
			char *collationSchemaName = get_namespace_name(collationform->collnamespace);
			char *qualifiedCollationName = quote_qualified_identifier(collationSchemaName,
																	  collationName);

			StringInfo regcollationExpression = makeStringInfo();
			appendStringInfo(regcollationExpression,
							 "%s::regcollation",
							 quote_literal_cstr(qualifiedCollationName));

			expression = regcollationExpression->data;
		}

		ReleaseSysCache(collationTuple);
	}

	return expression;
}


/*
 * SyncDeleteColocationGroupToNodes deletes a pg_dist_colocation record from workers.
 */
void
SyncDeleteColocationGroupToNodes(uint32 colocationId)
{
	char *command = ColocationGroupDeleteCommand(colocationId);

	/*
	 * We require superuser for all pg_dist_colocation operations because we have
	 * no reasonable way of restricting access.
	 */
	SendCommandToWorkersWithMetadataViaSuperUser(command);
}


/*
 * ColocationGroupDeleteCommand returns a command for deleting a colocation group.
 */
static char *
ColocationGroupDeleteCommand(uint32 colocationId)
{
	StringInfo deleteColocationCommand = makeStringInfo();

	appendStringInfo(deleteColocationCommand,
					 "SELECT pg_catalog.citus_internal_delete_colocation_metadata(%d)",
					 colocationId);

	return deleteColocationCommand->data;
}


/*
 * TenantSchemaInsertCommand returns a command to call
 * citus_internal_add_tenant_schema().
 */
char *
TenantSchemaInsertCommand(Oid schemaId, uint32 colocationId)
{
	StringInfo command = makeStringInfo();
	appendStringInfo(command,
					 "SELECT pg_catalog.citus_internal_add_tenant_schema(%s, %u)",
					 RemoteSchemaIdExpressionById(schemaId), colocationId);

	return command->data;
}


/*
 * TenantSchemaDeleteCommand returns a command to call
 * citus_internal_delete_tenant_schema().
 */
char *
TenantSchemaDeleteCommand(char *schemaName)
{
	StringInfo command = makeStringInfo();
	appendStringInfo(command,
					 "SELECT pg_catalog.citus_internal_delete_tenant_schema(%s)",
					 RemoteSchemaIdExpressionByName(schemaName));

	return command->data;
}


/*
 * UpdateNoneDistTableMetadataCommand returns a command to call
 * citus_internal_update_none_dist_table_metadata().
 */
char *
UpdateNoneDistTableMetadataCommand(Oid relationId, char replicationModel,
								   uint32 colocationId, bool autoConverted)
{
	StringInfo command = makeStringInfo();
	appendStringInfo(command,
					 "SELECT pg_catalog.citus_internal_update_none_dist_table_metadata(%s, '%c', %u, %s)",
					 RemoteTableIdExpression(relationId), replicationModel, colocationId,
					 autoConverted ? "true" : "false");

	return command->data;
}


/*
 * AddPlacementMetadataCommand returns a command to call
 * citus_internal_add_placement_metadata().
 */
char *
AddPlacementMetadataCommand(uint64 shardId, uint64 placementId,
							uint64 shardLength, int32 groupId)
{
	StringInfo command = makeStringInfo();
	appendStringInfo(command,
					 "SELECT citus_internal_add_placement_metadata(%ld, %ld, %d, %ld)",
					 shardId, shardLength, groupId, placementId);
	return command->data;
}


/*
 * DeletePlacementMetadataCommand returns a command to call
 * citus_internal_delete_placement_metadata().
 */
char *
DeletePlacementMetadataCommand(uint64 placementId)
{
	StringInfo command = makeStringInfo();
	appendStringInfo(command,
					 "SELECT pg_catalog.citus_internal_delete_placement_metadata(%ld)",
					 placementId);
	return command->data;
}


/*
 * RemoteSchemaIdExpressionById returns an expression in text form that
 * can be used to obtain the OID of the schema with given schema id on a
 * different node when included in a query string.
 */
static char *
RemoteSchemaIdExpressionById(Oid schemaId)
{
	char *schemaName = get_namespace_name(schemaId);
	if (schemaName == NULL)
	{
		ereport(ERROR, (errmsg("schema with OID %u does not exist", schemaId)));
	}

	return RemoteSchemaIdExpressionByName(schemaName);
}


/*
 * RemoteSchemaIdExpressionByName returns an expression in text form that
 * can be used to obtain the OID of the schema with given schema name on a
 * different node when included in a query string.
 */
static char *
RemoteSchemaIdExpressionByName(char *schemaName)
{
	StringInfo regnamespaceExpr = makeStringInfo();
	appendStringInfo(regnamespaceExpr, "%s::regnamespace",
					 quote_literal_cstr(quote_identifier(schemaName)));

	return regnamespaceExpr->data;
}


/*
 * RemoteTableIdExpression returns an expression in text form that
 * can be used to obtain the OID of given table on a different node
 * when included in a query string.
 */
static char *
RemoteTableIdExpression(Oid relationId)
{
	StringInfo regclassExpr = makeStringInfo();
	appendStringInfo(regclassExpr, "%s::regclass",
					 quote_literal_cstr(generate_qualified_relation_name(relationId)));

	return regclassExpr->data;
}


/*
 * SetMetadataSyncNodesFromNodeList sets list of nodes that needs to be metadata
 * synced among given node list into metadataSyncContext.
 */
void
SetMetadataSyncNodesFromNodeList(MetadataSyncContext *context, List *nodeList)
{
	/* sync is disabled, then no nodes to sync */
	if (!EnableMetadataSync)
	{
		return;
	}

	List *activatedWorkerNodeList = NIL;

	WorkerNode *node = NULL;
	foreach_ptr(node, nodeList)
	{
		if (NodeIsPrimary(node))
		{
			/* warn if we have coordinator in nodelist */
			if (NodeIsCoordinator(node))
			{
				ereport(NOTICE, (errmsg("%s:%d is the coordinator and already contains "
										"metadata, skipping syncing the metadata",
										node->workerName, node->workerPort)));
				continue;
			}

			activatedWorkerNodeList = lappend(activatedWorkerNodeList, node);
		}
	}

	context->activatedWorkerNodeList = activatedWorkerNodeList;
}


/*
 * EstablishAndSetMetadataSyncBareConnections establishes and sets
 * connections used throughout nontransactional metadata sync.
 */
void
EstablishAndSetMetadataSyncBareConnections(MetadataSyncContext *context)
{
	Assert(MetadataSyncTransMode == METADATA_SYNC_NON_TRANSACTIONAL);

	int connectionFlags = REQUIRE_METADATA_CONNECTION;

	/* establish bare connections to activated worker nodes */
	List *bareConnectionList = NIL;
	WorkerNode *node = NULL;
	foreach_ptr(node, context->activatedWorkerNodeList)
	{
		MultiConnection *connection = GetNodeUserDatabaseConnection(connectionFlags,
																	node->workerName,
																	node->workerPort,
																	CurrentUserName(),
																	NULL);

		Assert(connection != NULL);
		ForceConnectionCloseAtTransactionEnd(connection);
		bareConnectionList = lappend(bareConnectionList, connection);
	}

	context->activatedWorkerBareConnections = bareConnectionList;
}


/*
 * CreateMetadataSyncContext creates a context which contains worker connections
 * and a MemoryContext to be used throughout the metadata sync.
 *
 * If we collect commands, connections will not be established as caller's intent
 * is to collect sync commands.
 *
 * If the nodes are newly added before activation, we would not try to unset
 * metadatasynced in separate transaction during nontransactional metadatasync.
 */
MetadataSyncContext *
CreateMetadataSyncContext(List *nodeList, bool collectCommands,
						  bool nodesAddedInSameTransaction)
{
	/* should be alive during local transaction during the sync */
	MemoryContext context = AllocSetContextCreate(TopTransactionContext,
												  "metadata_sync_context",
												  ALLOCSET_DEFAULT_SIZES);

	MetadataSyncContext *metadataSyncContext = (MetadataSyncContext *) palloc0(
		sizeof(MetadataSyncContext));

	metadataSyncContext->context = context;
	metadataSyncContext->transactionMode = MetadataSyncTransMode;
	metadataSyncContext->collectCommands = collectCommands;
	metadataSyncContext->collectedCommands = NIL;
	metadataSyncContext->nodesAddedInSameTransaction = nodesAddedInSameTransaction;

	/* filter the nodes that needs to be activated from given node list */
	SetMetadataSyncNodesFromNodeList(metadataSyncContext, nodeList);

	/*
	 * establish connections only for nontransactional mode to prevent connection
	 * open-close for each command
	 */
	if (!collectCommands && MetadataSyncTransMode == METADATA_SYNC_NON_TRANSACTIONAL)
	{
		EstablishAndSetMetadataSyncBareConnections(metadataSyncContext);
	}

	/* use 2PC coordinated transactions if we operate in transactional mode */
	if (MetadataSyncTransMode == METADATA_SYNC_TRANSACTIONAL)
	{
		Use2PCForCoordinatedTransaction();
	}

	return metadataSyncContext;
}


/*
 * ResetMetadataSyncMemoryContext resets memory context inside metadataSyncContext, if
 * we are not collecting commands.
 */
void
ResetMetadataSyncMemoryContext(MetadataSyncContext *context)
{
	if (!MetadataSyncCollectsCommands(context))
	{
		MemoryContextReset(context->context);
	}
}


/*
 * MetadataSyncCollectsCommands returns whether context is used for collecting
 * commands instead of sending them to workers.
 */
bool
MetadataSyncCollectsCommands(MetadataSyncContext *context)
{
	return context->collectCommands;
}


/*
 * SendOrCollectCommandListToActivatedNodes sends the commands to the activated nodes with
 * bare connections inside metadatacontext or via coordinated connections.
 * Note that when context only collects commands, we add commands into the context
 * without sending the commands.
 */
void
SendOrCollectCommandListToActivatedNodes(MetadataSyncContext *context, List *commands)
{
	/* do nothing if no commands */
	if (commands == NIL)
	{
		return;
	}

	/*
	 * do not send any command to workers if we collect commands.
	 * Collect commands into metadataSyncContext's collected command
	 * list.
	 */
	if (MetadataSyncCollectsCommands(context))
	{
		context->collectedCommands = list_concat(context->collectedCommands, commands);
		return;
	}

	/* send commands to new workers, the current user should be a superuser */
	Assert(superuser());

	if (context->transactionMode == METADATA_SYNC_TRANSACTIONAL)
	{
		List *workerNodes = context->activatedWorkerNodeList;
		SendMetadataCommandListToWorkerListInCoordinatedTransaction(workerNodes,
																	CurrentUserName(),
																	commands);
	}
	else if (context->transactionMode == METADATA_SYNC_NON_TRANSACTIONAL)
	{
		List *workerConnections = context->activatedWorkerBareConnections;
		if (MetadataSyncBatchingEnabled(context))
		{
			ExecuteRemoteCommandsInConnectionsInPipelineMode(workerConnections,
															 commands);
		}
		else
		{
			SendCommandListToWorkerListWithBareConnections(workerConnections,
														   commands);
		}
	}
	else
	{
		pg_unreachable();
	}
}


/*
 * SendOrCollectCommandListToMetadataNodes sends the commands to the metadata nodes with
 * bare connections inside metadatacontext or via coordinated connections.
 * Note that when context only collects commands, we add commands into the context
 * without sending the commands.
 */
void
SendOrCollectCommandListToMetadataNodes(MetadataSyncContext *context, List *commands)
{
	/*
	 * do not send any command to workers if we collcet commands.
	 * Collect commands into metadataSyncContext's collected command
	 * list.
	 */
	if (MetadataSyncCollectsCommands(context))
	{
		context->collectedCommands = list_concat(context->collectedCommands, commands);
		return;
	}

	/* send commands to new workers, the current user should be a superuser */
	Assert(superuser());

	if (context->transactionMode == METADATA_SYNC_TRANSACTIONAL)
	{
		List *metadataNodes = TargetWorkerSetNodeList(NON_COORDINATOR_METADATA_NODES,
													  RowShareLock);
		SendMetadataCommandListToWorkerListInCoordinatedTransaction(metadataNodes,
																	CurrentUserName(),
																	commands);
	}
	else if (context->transactionMode == METADATA_SYNC_NON_TRANSACTIONAL)
	{
		SendBareCommandListToMetadataWorkers(commands);
	}
	else
	{
		pg_unreachable();
	}
}


/*
 * SendOrCollectCommandListToSingleNode sends the commands to the specific worker
 * indexed by nodeIdx with bare connection inside metadatacontext or via coordinated
 * connection. Note that when context only collects commands, we add commands into
 * the context without sending the commands.
 */
void
SendOrCollectCommandListToSingleNode(MetadataSyncContext *context, List *commands,
									 int nodeIdx)
{
	/*
	 * Do not send any command to workers if we collect commands.
	 * Collect commands into metadataSyncContext's collected command
	 * list.
	 */
	if (MetadataSyncCollectsCommands(context))
	{
		context->collectedCommands = list_concat(context->collectedCommands, commands);
		return;
	}

	/* send commands to new workers, the current user should be a superuser */
	Assert(superuser());

	if (context->transactionMode == METADATA_SYNC_TRANSACTIONAL)
	{
		List *workerNodes = context->activatedWorkerNodeList;
		Assert(nodeIdx < list_length(workerNodes));

		WorkerNode *node = list_nth(workerNodes, nodeIdx);
		SendMetadataCommandListToWorkerListInCoordinatedTransaction(list_make1(node),
																	CurrentUserName(),
																	commands);
	}
	else if (context->transactionMode == METADATA_SYNC_NON_TRANSACTIONAL)
	{
		List *workerConnections = context->activatedWorkerBareConnections;
		Assert(nodeIdx < list_length(workerConnections));

		MultiConnection *workerConnection = list_nth(workerConnections, nodeIdx);
		List *connectionList = list_make1(workerConnection);
		SendCommandListToWorkerListWithBareConnections(connectionList, commands);
	}
	else
	{
		pg_unreachable();
	}
}


/*
 * WorkerDropAllShellTablesCommand returns command required to drop shell tables
 * from workers. When singleTransaction is false, we create transaction per shell
 * table. Otherwise, we drop all shell tables within single transaction.
 */
char *
WorkerDropAllShellTablesCommand(bool singleTransaction)
{
	char *singleTransactionString = (singleTransaction) ? "true" : "false";
	StringInfo removeAllShellTablesCommand = makeStringInfo();
	appendStringInfo(removeAllShellTablesCommand, WORKER_DROP_ALL_SHELL_TABLES,
					 singleTransactionString);
	return removeAllShellTablesCommand->data;
}


/*
 * WorkerDropSequenceDependencyCommand returns command to drop sequence dependencies for
 * given table.
 */
char *
WorkerDropSequenceDependencyCommand(Oid relationId)
{
	char *qualifiedTableName = generate_qualified_relation_name(relationId);
	StringInfo breakSequenceDepCommand = makeStringInfo();
	appendStringInfo(breakSequenceDepCommand,
					 BREAK_CITUS_TABLE_SEQUENCE_DEPENDENCY_COMMAND,
					 quote_literal_cstr(qualifiedTableName));
	return breakSequenceDepCommand->data;
}


/*
 * PropagateNodeWideObjectsCommandList is called during node activation to
 * propagate any object that should be propagated for every node. These are
 * generally not linked to any distributed object but change system wide behaviour.
 */
static List *
PropagateNodeWideObjectsCommandList(void)
{
	/* collect all commands */
	List *ddlCommands = NIL;

	if (EnableAlterRoleSetPropagation)
	{
		/*
		 * Get commands for database and postgres wide settings. Since these settings are not
		 * linked to any role that can be distributed we need to distribute them seperately
		 */
		List *alterRoleSetCommands = GenerateAlterRoleSetCommandForRole(InvalidOid);
		ddlCommands = list_concat(ddlCommands, alterRoleSetCommands);
	}

	return ddlCommands;
}


/*
 * SyncDistributedObjects sync the distributed objects to the nodes in metadataSyncContext
 * with transactional or nontransactional mode according to transactionMode inside
 * metadataSyncContext.
 *
 * Transactions should be ordered like below:
 * - Nodewide objects (only roles for now),
 * - Deletion of sequence and shell tables and metadata entries
 * - All dependencies (e.g., types, schemas, sequences) and all shell distributed
 *   table and their pg_dist_xx metadata entries
 * - Inter relation between those shell tables
 *
 * Note that we do not create the distributed dependencies on the coordinator
 * since all the dependencies should be present in the coordinator already.
 */
void
SyncDistributedObjects(MetadataSyncContext *context)
{
	if (context->activatedWorkerNodeList == NIL)
	{
		return;
	}

	EnsureSequentialModeMetadataOperations();

	Assert(ShouldPropagate());

	/* Send systemwide objects, only roles for now */
	SendNodeWideObjectsSyncCommands(context);

	/*
	 * Break dependencies between sequences-shell tables, then remove shell tables,
	 * and metadata tables respectively.
	 * We should delete shell tables before metadata entries as we look inside
	 * pg_dist_partition to figure out shell tables.
	 */
	SendShellTableDeletionCommands(context);
	SendMetadataDeletionCommands(context);

	/*
	 * Commands to insert pg_dist_colocation entries.
	 * Replicating dist objects and their metadata depends on this step.
	 */
	SendColocationMetadataCommands(context);

	/*
	 * Replicate all objects of the pg_dist_object to the remote node and
	 * create metadata entries for Citus tables (pg_dist_shard, pg_dist_shard_placement,
	 * pg_dist_partition, pg_dist_object).
	 */
	SendDependencyCreationCommands(context);

	/*
	 * When the pool path is enabled, SendDependencyCreationCommands above also
	 * skips the distributed sequences; (re)create them here in parallel over a
	 * pool of connections, before the shell tables so each shell table's column
	 * defaults can be re-associated with an already-created sequence. Sequences
	 * are an embarrassingly parallel leaf class (no intra-class edges) whose
	 * upward dependencies (roles, schemas) were just created above, so the phase
	 * barrier holds.
	 */
	if (MetadataSyncShellTablePoolEnabled(context))
	{
		SendSequenceCreationCommandsViaPool(context);
	}

	/*
	 * When the pool path is enabled, SendDependencyCreationCommands above skips
	 * the Citus shell tables and we (re)create them here in parallel over a pool
	 * of connections to each activated node. Shell tables have no dependencies
	 * on each other, so they are embarrassingly parallel; all of their upward
	 * dependencies (roles, schemas, types, functions, sequences, ...) were just
	 * created by SendDependencyCreationCommands and the sequence pool phase, so
	 * the phase barrier holds.
	 */
	if (MetadataSyncShellTablePoolEnabled(context))
	{
		SendShellTableCreationCommandsViaPool(context);
	}

	/*
	 * (Re)create the objects that depend on shell tables (views, materialized
	 * views, publications) which SendDependencyCreationCommands deferred because
	 * their target shell tables were just created above in the pool phase. This
	 * matches the stock dependency order (these objects come after their tables
	 * and before the per-table metadata below). In the serial path the deferred
	 * list is empty, so this is a no-op.
	 */
	SendDeferredDependentCreationCommands(context);

	/*
	 * The per-table shard/partition metadata (pg_dist_shard, pg_dist_shard_
	 * placement, pg_dist_partition) and the pg_dist_object marks are independent
	 * across objects, so in principle they can be (re)created over the connection
	 * pool as well (SendDist*CommandsViaPool()). In practice, however, these two
	 * layers are *cheap per object* (a few small catalog inserts each), and the
	 * serial path already streams them over a single connection with libpq
	 * pipelining AND is already a bounded-memory batched catalog scan (it never
	 * materializes a whole-cluster list). Measurement on a 44k-table cluster
	 * showed the pool path *regresses* both layers by ~2.4x versus the serial
	 * pipelined path (pipelining hides ~5x of per-statement round-trips that the
	 * executor pool cannot pipeline through), while the pool's only advantage --
	 * K-way worker concurrency -- buys little because each object is too cheap to
	 * saturate a worker backend. So we keep these two layers on the serial
	 * pipelined path even when the pool is enabled; the pool is reserved for the
	 * shell-table (and sequence) layers, where per-object worker CPU dominates
	 * and K-way concurrency wins. The *ViaPool variants are retained (declared in
	 * the header) for A/B measurement.
	 */
	SendDistTableMetadataCommands(context);
	SendDistObjectCommands(context);

	/*
	 * Commands to insert pg_dist_schema entries.
	 *
	 * Need to be done after syncing distributed objects because the schemas
	 * need to exist on the worker.
	 */
	SendTenantSchemaMetadataCommands(context);

	/*
	 * After creating each table, handle the inter table relationship between
	 * those tables.
	 */
	SendInterTableRelationshipCommands(context);
}


/*
 * SendNodeWideObjectsSyncCommands sends systemwide objects to workers with
 * transactional or nontransactional mode according to transactionMode inside
 * metadataSyncContext.
 */
void
SendNodeWideObjectsSyncCommands(MetadataSyncContext *context)
{
	/* propagate node wide objects. It includes only roles for now. */
	List *commandList = PropagateNodeWideObjectsCommandList();

	if (commandList == NIL)
	{
		return;
	}

	commandList = lcons(DISABLE_DDL_PROPAGATION, commandList);
	commandList = lappend(commandList, ENABLE_DDL_PROPAGATION);
	SendOrCollectCommandListToActivatedNodes(context, commandList);
}


/*
 * SendShellTableDeletionCommands sends sequence, and shell table deletion
 * commands to workers with transactional or nontransactional mode according to
 * transactionMode inside metadataSyncContext.
 */
void
SendShellTableDeletionCommands(MetadataSyncContext *context)
{
	/* break all sequence deps for citus tables */
	char *breakSeqDepsCommand = BREAK_ALL_CITUS_TABLE_SEQUENCE_DEPENDENCY_COMMAND;
	SendOrCollectCommandListToActivatedNodes(context, list_make1(breakSeqDepsCommand));

	/* remove shell tables */
	bool singleTransaction = (context->transactionMode == METADATA_SYNC_TRANSACTIONAL);
	char *dropShellTablesCommand = WorkerDropAllShellTablesCommand(singleTransaction);
	SendOrCollectCommandListToActivatedNodes(context, list_make1(dropShellTablesCommand));
}


/*
 * SendMetadataDeletionCommands sends metadata entry deletion commands to workers
 * with transactional or nontransactional mode according to transactionMode inside
 * metadataSyncContext.
 */
void
SendMetadataDeletionCommands(MetadataSyncContext *context)
{
	/* remove pg_dist_partition entries */
	SendOrCollectCommandListToActivatedNodes(context, list_make1(DELETE_ALL_PARTITIONS));

	/* remove pg_dist_shard entries */
	SendOrCollectCommandListToActivatedNodes(context, list_make1(DELETE_ALL_SHARDS));

	/* remove pg_dist_placement entries */
	SendOrCollectCommandListToActivatedNodes(context, list_make1(DELETE_ALL_PLACEMENTS));

	/* remove pg_dist_object entries */
	SendOrCollectCommandListToActivatedNodes(context,
											 list_make1(DELETE_ALL_DISTRIBUTED_OBJECTS));

	/* remove pg_dist_colocation entries */
	SendOrCollectCommandListToActivatedNodes(context, list_make1(DELETE_ALL_COLOCATION));

	/* remove pg_dist_schema entries */
	SendOrCollectCommandListToActivatedNodes(context,
											 list_make1(DELETE_ALL_TENANT_SCHEMAS));
}


/*
 * SendColocationMetadataCommands sends colocation metadata with transactional or
 * nontransactional mode according to transactionMode inside metadataSyncContext.
 */
void
SendColocationMetadataCommands(MetadataSyncContext *context)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 0;

	Relation relation = table_open(DistColocationRelationId(), AccessShareLock);
	SysScanDesc scanDesc = systable_beginscan(relation, InvalidOid, false, NULL,
											  scanKeyCount, scanKey);

	MemoryContext oldContext = MemoryContextSwitchTo(context->context);
	HeapTuple nextTuple = NULL;
	while (true)
	{
		ResetMetadataSyncMemoryContext(context);

		nextTuple = systable_getnext(scanDesc);
		if (!HeapTupleIsValid(nextTuple))
		{
			break;
		}

		StringInfo colocationGroupCreateCommand = makeStringInfo();
		appendStringInfo(colocationGroupCreateCommand,
						 "WITH colocation_group_data (colocationid, shardcount, "
						 "replicationfactor, distributioncolumntype, "
						 "distributioncolumncollationname, "
						 "distributioncolumncollationschema)  AS (VALUES ");

		Form_pg_dist_colocation colocationForm =
			(Form_pg_dist_colocation) GETSTRUCT(nextTuple);

		appendStringInfo(colocationGroupCreateCommand,
						 "(%d, %d, %d, %s, ",
						 colocationForm->colocationid,
						 colocationForm->shardcount,
						 colocationForm->replicationfactor,
						 RemoteTypeIdExpression(colocationForm->distributioncolumntype));

		/*
		 * For collations, include the names in the VALUES section and then
		 * join with pg_collation.
		 */
		Oid distributionColumCollation = colocationForm->distributioncolumncollation;
		if (distributionColumCollation != InvalidOid)
		{
			Datum collationIdDatum = ObjectIdGetDatum(distributionColumCollation);
			HeapTuple collationTuple = SearchSysCache1(COLLOID, collationIdDatum);
			if (HeapTupleIsValid(collationTuple))
			{
				Form_pg_collation collationform =
					(Form_pg_collation) GETSTRUCT(collationTuple);
				char *collationName = NameStr(collationform->collname);
				char *collationSchemaName =
					get_namespace_name(collationform->collnamespace);
				appendStringInfo(colocationGroupCreateCommand,
								 "%s, %s)",
								 quote_literal_cstr(collationName),
								 quote_literal_cstr(collationSchemaName));
				ReleaseSysCache(collationTuple);
			}
			else
			{
				appendStringInfo(colocationGroupCreateCommand,
								 "NULL, NULL)");
			}
		}
		else
		{
			appendStringInfo(colocationGroupCreateCommand,
							 "NULL, NULL)");
		}

		appendStringInfo(colocationGroupCreateCommand,
						 ") SELECT pg_catalog.citus_internal_add_colocation_metadata("
						 "colocationid, shardcount, replicationfactor, "
						 "distributioncolumntype, coalesce(c.oid, 0)) "
						 "FROM colocation_group_data d LEFT JOIN pg_collation c "
						 "ON (d.distributioncolumncollationname = c.collname "
						 "AND d.distributioncolumncollationschema::regnamespace"
						 " = c.collnamespace)");

		List *commandList = list_make1(colocationGroupCreateCommand->data);
		SendOrCollectCommandListToActivatedNodes(context, commandList);
	}
	MemoryContextSwitchTo(oldContext);

	systable_endscan(scanDesc);
	table_close(relation, AccessShareLock);
}


/*
 * SendTenantSchemaMetadataCommands sends tenant schema metadata entries with
 * transactional or nontransactional mode according to transactionMode inside
 * metadataSyncContext.
 */
void
SendTenantSchemaMetadataCommands(MetadataSyncContext *context)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 0;

	Relation pgDistTenantSchema = table_open(DistTenantSchemaRelationId(),
											 AccessShareLock);
	SysScanDesc scanDesc = systable_beginscan(pgDistTenantSchema, InvalidOid, false, NULL,
											  scanKeyCount, scanKey);

	MemoryContext oldContext = MemoryContextSwitchTo(context->context);
	HeapTuple heapTuple = NULL;
	while (true)
	{
		ResetMetadataSyncMemoryContext(context);

		heapTuple = systable_getnext(scanDesc);
		if (!HeapTupleIsValid(heapTuple))
		{
			break;
		}

		Form_pg_dist_schema tenantSchemaForm =
			(Form_pg_dist_schema) GETSTRUCT(heapTuple);

		StringInfo insertTenantSchemaCommand = makeStringInfo();
		appendStringInfo(insertTenantSchemaCommand,
						 "SELECT pg_catalog.citus_internal_add_tenant_schema(%s, %u)",
						 RemoteSchemaIdExpressionById(tenantSchemaForm->schemaid),
						 tenantSchemaForm->colocationid);

		List *commandList = list_make1(insertTenantSchemaCommand->data);
		SendOrCollectCommandListToActivatedNodes(context, commandList);
	}
	MemoryContextSwitchTo(oldContext);

	systable_endscan(scanDesc);
	table_close(pgDistTenantSchema, AccessShareLock);
}


/*
 * SendDependencyCreationCommands sends dependency creation commands to workers
 * with transactional or nontransactional mode according to transactionMode
 * inside metadataSyncContext.
 */
void
SendDependencyCreationCommands(MetadataSyncContext *context)
{
	/* disable ddl propagation */
	SendOrCollectCommandListToActivatedNodes(context,
											 list_make1(DISABLE_DDL_PROPAGATION));

	MemoryContext oldContext = MemoryContextSwitchTo(context->context);

	/*
	 * When the pool path is enabled, Citus shell tables AND distributed
	 * sequences are created later over pools of parallel connections
	 * (SendSequenceCreationCommandsViaPool() then
	 * SendShellTableCreationCommandsViaPool()). Exclude both from the dependency
	 * list *at scan time* so the millions of shell tables and sequences on a
	 * large cluster are never materialized here, never filtered, and --
	 * crucially -- never fed into the per-object pg_depend traversal in
	 * OrderObjectAddressListInDependencyOrder() below, whose cost and peak
	 * memory would otherwise be proportional to the number of distributed
	 * tables/sequences and dominate sync wall time on the single coordinator
	 * backend. Both classes are embarrassingly parallel (no intra-class edges),
	 * so no dependency ordering is lost by removing them from this phase.
	 */
	bool poolShellTables = MetadataSyncShellTablePoolEnabled(context);

	/* collect all dependencies in creation order and get their ddl commands */
	List *dependencies = poolShellTables ?
						 GetDistributedObjectAddressListWithoutShellTablesAndSequences() :
						 GetDistributedObjectAddressList();

	/*
	 * Depending on changes in the environment, such as the enable_metadata_sync guc
	 * there might be objects in the distributed object address list that should currently
	 * not be propagated by citus as they are 'not supported'.
	 */
	dependencies = FilterObjectAddressListByPredicate(dependencies,
													  &SupportedDependencyByCitus,
													  true);

	dependencies = OrderObjectAddressListInDependencyOrder(dependencies, true);

	/*
	 * commandsContext holds the batched command strings until they are flushed.
	 * It lives under context->context so the dependency objects gathered above
	 * (which stay in context->context) are preserved across the per-flush resets
	 * of this batch context.
	 */
	MemoryContext commandsContext = AllocSetContextCreate(context->context,
														  "dependency commands context",
														  ALLOCSET_DEFAULT_SIZES);

	/*
	 * Build each dependency's ddl commands in a per-object context that we reset
	 * every iteration, so the deparse and catalog scratch does not accumulate in
	 * the batch context until the batch is flushed.
	 */
	MemoryContext perObjectContext = AllocSetContextCreate(oldContext,
														   "dependency commands per object context",
														   ALLOCSET_DEFAULT_SIZES);
	ObjectAddress *dependency = NULL;
	int64 processedCount = 0;
	InitializeActivatedNodeCommandBatch(context, commandsContext);
	foreach_ptr(dependency, dependencies)
	{
		MemoryContextReset(perObjectContext);
		MemoryContextSwitchTo(perObjectContext);

		/*
		 * When the pool path is enabled, Citus shell tables are created later
		 * in SendShellTableCreationCommandsViaPool() over a pool of parallel
		 * connections, so skip them here. They have no intra-class dependencies
		 * (only upward edges into the prerequisite objects created in this
		 * step), so deferring them to a later phase preserves creation order.
		 */
		if (poolShellTables && IsCitusShellTableDependency(dependency))
		{
			FlushMetadataSyncCachesIfNeeded(context, ++processedCount);
			continue;
		}

		/*
		 * When the pool path is enabled, objects that DEPEND ON a Citus shell
		 * table (views/materialized views over a distributed table, and
		 * publications created FOR TABLE a distributed table) cannot be created
		 * here: their target shell tables are deferred to the later parallel
		 * phase, so emitting e.g. "CREATE VIEW v AS SELECT ... FROM t" or
		 * "CREATE PUBLICATION p FOR TABLE t" now would fail with "relation t
		 * does not exist". Stash them (in dependency order) and (re)create them
		 * in SendDeferredDependentCreationCommands() once the shell tables exist.
		 */
		if (poolShellTables && IsDependentOnShellTableObject(dependency))
		{
			MemoryContext stashContext = MemoryContextSwitchTo(TopTransactionContext);
			ObjectAddress *deferredAddress = palloc0(sizeof(ObjectAddress));
			*deferredAddress = *dependency;
			context->deferredDependentObjectAddresses =
				lappend(context->deferredDependentObjectAddresses, deferredAddress);
			MemoryContextSwitchTo(stashContext);

			FlushMetadataSyncCachesIfNeeded(context, ++processedCount);
			continue;
		}

		/*
		 * We expect extension-owned objects to be created as a result
		 * of the extension being created.
		 */
		if (!IsAnyObjectAddressOwnedByExtension(list_make1(dependency), NULL))
		{
			/* dependency creation commands */
			List *ddlCommands = GetAllDependencyCreateDDLCommands(list_make1(dependency));
			SendOrBatchCommandListToActivatedNodes(context, ddlCommands);
		}

		/*
		 * We flush the caches even when we skip the dependency creation commands
		 * because we still opened catalog entries to reach this decision, so
		 * advance the cache-flush counter and flush if needed on this skip path
		 * too.
		 */
		FlushMetadataSyncCachesIfNeeded(context, ++processedCount);
	}

	MemoryContextSwitchTo(context->context);

	/* send whatever remains in the final partial batch */
	FlushBatchedCommandsToActivatedNodes(context);

	MemoryContextSwitchTo(oldContext);

	MemoryContextDelete(perObjectContext);

	if (!MetadataSyncCollectsCommands(context))
	{
		MemoryContextDelete(commandsContext);
	}
	ResetMetadataSyncMemoryContext(context);

	/* enable ddl propagation */
	SendOrCollectCommandListToActivatedNodes(context, list_make1(ENABLE_DDL_PROPAGATION));
}


/*
 * NodeTargetedPoolDeparseFn builds, for a single catalog tuple encountered during
 * a pool phase scan, the list of command strings that (re)create the corresponding
 * object on the activated node -- or returns NIL to skip this tuple. It runs in a
 * per-object memory context that the driver resets after each tuple, so callbacks
 * may allocate freely. tupleDesc describes the scanned relation's tuples.
 */
typedef List * (*NodeTargetedPoolDeparseFn)(HeapTuple tuple, TupleDesc tupleDesc,
											 MetadataSyncContext *context);


/*
 * RunNodeTargetedPoolPhase drives one parallel metadata-sync phase over a pool of
 * connections to a single activated node, using the adaptive executor.
 *
 * It scans scanRelationId (pg_dist_partition or pg_dist_object) streaming-style
 * and, for every tuple, calls deparseFn to obtain that object's (re)creation
 * command list (or NIL to skip). The commands are chunked into waves to bound
 * memory: batch_size objects' commands are bundled into one executor task,
 * pool_size tasks are gathered into one wave, the wave is executed (blocking)
 * over pool_size parallel connections, and its memory is reset before the next
 * wave. Peak coordinator memory is therefore ~one wave, never the whole cluster,
 * and the phase never materializes a dependency-ordered list of all objects --
 * every class driven this way is a leaf class with no intra-class edges.
 *
 * This is the shared engine behind all metadata-sync pool phases (shell tables,
 * sequences, per-table shard/partition metadata, pg_dist_object marks); each
 * phase differs only in which relation it scans and in its deparseFn.
 */
static void
RunNodeTargetedPoolPhase(MetadataSyncContext *context, WorkerNode *workerNode,
						 Oid scanRelationId, NodeTargetedPoolDeparseFn deparseFn,
						 const char *objectLabel)
{
	int poolSize = MaxAdaptiveExecutorPoolSize;
	if (poolSize < 1)
	{
		poolSize = 1;
	}

	/* number of objects whose commands are bundled into one executor task */
	int objectsPerTask = MetadataSyncBatchSize;

	/* number of tasks dispatched together in one blocking executor call (a wave) */
	int tasksPerWave = poolSize;

	Relation relation = table_open(scanRelationId, AccessShareLock);
	TupleDesc tupleDesc = RelationGetDescr(relation);
	SysScanDesc scanDesc = systable_beginscan(relation, InvalidOid, false, NULL, 0, NULL);

	MemoryContext oldContext = CurrentMemoryContext;

	/*
	 * perObjectContext holds the transient deparse/catalog scratch for a single
	 * object's command bundle; it is reset after each object. waveContext holds
	 * the current wave's task query strings and Task structs; it is reset after
	 * the wave executes. Peak memory is bounded to one wave.
	 */
	MemoryContext perObjectContext =
		AllocSetContextCreate(oldContext, "node targeted pool per object context",
							  ALLOCSET_DEFAULT_SIZES);
	MemoryContext waveContext =
		AllocSetContextCreate(oldContext, "node targeted pool wave context",
							  ALLOCSET_DEFAULT_SIZES);

	/*
	 * Track the coordinator-side deparse cost (building each wave's command
	 * strings, single-threaded) separately from the worker-side execution cost
	 * (running the wave over the connection pool), and report both in the
	 * completion LOG line below. ExecuteTaskListOutsideTransaction blocks per
	 * wave so the two phases do not overlap; surfacing the split makes it
	 * observable whether a given pool phase is worker-bound (parallelism helps)
	 * or coordinator-deparse-bound (parallelism is capped).
	 */
	instr_time deparseTime;
	instr_time executeTime;
	INSTR_TIME_SET_ZERO(deparseTime);
	INSTR_TIME_SET_ZERO(executeTime);

	int64 objectCount = 0;
	int64 waveCount = 0;
	int64 processedCount = 0;

	/* list of per-task command lists making up the current wave, in waveContext */
	List *waveTaskCommandLists = NIL;
	/* current task's command list (individual statements), in waveContext */
	List *taskCommandList = NIL;
	int objectsInTask = 0;

	instr_time deparseStart;
	INSTR_TIME_SET_CURRENT(deparseStart);

	while (true)
	{
		HeapTuple heapTuple = systable_getnext(scanDesc);
		bool scanDone = !HeapTupleIsValid(heapTuple);

		if (!scanDone)
		{
			MemoryContextSwitchTo(perObjectContext);

			List *objectCommands = deparseFn(heapTuple, tupleDesc, context);

			if (objectCommands != NIL)
			{
				MemoryContextSwitchTo(waveContext);

				if (taskCommandList == NIL)
				{
					/*
					 * Each task runs as its own implicit transaction on a worker
					 * backend; disable DDL propagation there so the DDL is not
					 * re-propagated to the other nodes. This SET is a session GUC
					 * that persists for the rest of the task's statements on that
					 * connection.
					 *
					 * The commands are attached to the task as a query string
					 * list (one statement per element) rather than a single
					 * concatenated string: the adaptive executor sends and
					 * accounts for results one query at a time, so bundling
					 * several row-returning statements (e.g. the SELECT
					 * worker_*()/citus_internal_*() calls used here) into one
					 * string would desynchronize its per-query result bookkeeping
					 * (task->queryCount).
					 */
					taskCommandList =
						list_make1(pstrdup(DISABLE_DDL_PROPAGATION));
				}

				char *command = NULL;
				foreach_ptr(command, objectCommands)
				{
					taskCommandList = lappend(taskCommandList, pstrdup(command));
				}

				objectsInTask++;
				objectCount++;
			}

			MemoryContextSwitchTo(waveContext);
			MemoryContextReset(perObjectContext);

			/*
			 * The deparseFn opened catalog entries to reach its decision, so
			 * advance the cache-flush counter like the other per-object scan
			 * loops do (bounds coordinator relcache growth on clusters with
			 * millions of objects).
			 */
			FlushMetadataSyncCachesIfNeeded(context, ++processedCount);
		}

		MemoryContextSwitchTo(waveContext);

		/* close off the current task when it is full or the scan is done */
		bool taskFull = (objectsInTask >= objectsPerTask);
		if (taskCommandList != NIL && (taskFull || scanDone))
		{
			waveTaskCommandLists = lappend(waveTaskCommandLists, taskCommandList);
			taskCommandList = NIL;
			objectsInTask = 0;
		}

		/* dispatch the wave when it is full or the scan is done */
		bool waveFull = (list_length(waveTaskCommandLists) >= tasksPerWave);
		if (waveTaskCommandLists != NIL && (waveFull || scanDone))
		{
			List *taskList = CreateNodeTargetedPoolTaskList(waveTaskCommandLists,
															workerNode);

			instr_time deparseEnd;
			INSTR_TIME_SET_CURRENT(deparseEnd);
			INSTR_TIME_ACCUM_DIFF(deparseTime, deparseEnd, deparseStart);

			instr_time executeStart;
			INSTR_TIME_SET_CURRENT(executeStart);
			ExecuteTaskListOutsideTransaction(ROW_MODIFY_NONE, taskList, poolSize, NIL);
			instr_time executeEnd;
			INSTR_TIME_SET_CURRENT(executeEnd);
			INSTR_TIME_ACCUM_DIFF(executeTime, executeEnd, executeStart);

			waveCount++;

			/* free the wave's task strings and Task structs before the next wave */
			MemoryContextReset(waveContext);
			waveTaskCommandLists = NIL;
			taskCommandList = NIL;
			objectsInTask = 0;

			/* resume the deparse timer for the next wave */
			INSTR_TIME_SET_CURRENT(deparseStart);
		}

		if (scanDone)
		{
			break;
		}
	}

	MemoryContextSwitchTo(oldContext);

	systable_endscan(scanDesc);
	table_close(relation, AccessShareLock);

	MemoryContextDelete(perObjectContext);
	MemoryContextDelete(waveContext);

	ereport(LOG, (errmsg("parallel %s on node %s:%d completed: "
						 "%ld objects in %ld waves (pool size %d, batch size %d); "
						 "coordinator deparse %.0f ms, worker execute %.0f ms",
						 objectLabel,
						 workerNode->workerName, workerNode->workerPort,
						 (long) objectCount, (long) waveCount, poolSize,
						 objectsPerTask,
						 INSTR_TIME_GET_MILLISEC(deparseTime),
						 INSTR_TIME_GET_MILLISEC(executeTime))));
}


/*
 * ShellTablePoolDeparse (NodeTargetedPoolDeparseFn) returns the shell table
 * creation command bundle for the distributed table described by a
 * pg_dist_partition tuple, or NIL for extension-owned tables (which are created
 * with their extension, exactly as the serial dependency path skips them).
 */
static List *
ShellTablePoolDeparse(HeapTuple tuple, TupleDesc tupleDesc,
					  MetadataSyncContext *context)
{
	Oid relationId = FetchRelationIdFromPgPartitionHeapTuple(tuple, tupleDesc);

	ObjectAddress tableAddress = { 0 };
	ObjectAddressSet(tableAddress, RelationRelationId, relationId);
	if (IsAnyObjectAddressOwnedByExtension(list_make1(&tableAddress), NULL))
	{
		return NIL;
	}

	return ShellTableCreationCommandList(relationId);
}


/*
 * SequencePoolDeparse (NodeTargetedPoolDeparseFn) returns the CREATE bundle for
 * the distributed sequence described by a pg_dist_object tuple, or NIL when the
 * tuple is not a distributed sequence or the sequence is extension-owned.
 *
 * It uses the same predicate (IsPgDistObjectRowDistributedSequence) that excludes
 * distributed sequences from the serial dependency ordering phase, keeping the
 * "excluded there == created here" invariant in one place.
 */
static List *
SequencePoolDeparse(HeapTuple tuple, TupleDesc tupleDesc,
					MetadataSyncContext *context)
{
	Form_pg_dist_object distObjectForm = (Form_pg_dist_object) GETSTRUCT(tuple);
	Oid classId = distObjectForm->classid;
	Oid objId = distObjectForm->objid;

	if (!IsPgDistObjectRowDistributedSequence(classId, objId))
	{
		return NIL;
	}

	ObjectAddress seqAddress = { 0 };
	ObjectAddressSet(seqAddress, RelationRelationId, objId);
	if (IsAnyObjectAddressOwnedByExtension(list_make1(&seqAddress), NULL))
	{
		return NIL;
	}

	return DDLCommandsForSequence(objId, TableOwner(objId));
}


/*
 * DistTableMetadataPoolDeparse (NodeTargetedPoolDeparseFn) returns the Citus
 * table metadata command list (pg_dist_shard, pg_dist_shard_placement,
 * pg_dist_partition entries) for the distributed table described by a
 * pg_dist_partition tuple, or NIL for tables whose metadata should not be
 * synced. It mirrors the per-tuple body of the serial
 * SendDistTableMetadataCommands().
 */
static List *
DistTableMetadataPoolDeparse(HeapTuple tuple, TupleDesc tupleDesc,
							 MetadataSyncContext *context)
{
	Oid relationId = FetchRelationIdFromPgPartitionHeapTuple(tuple, tupleDesc);

	if (!ShouldSyncTableMetadata(relationId))
	{
		return NIL;
	}

	return CitusTableMetadataCreateCommandList(relationId);
}


/*
 * DistObjectMarkPoolDeparse (NodeTargetedPoolDeparseFn) returns the single
 * command that (re)creates the pg_dist_object mark for the object described by a
 * pg_dist_object tuple. It mirrors the per-tuple body of the serial
 * SendDistObjectCommands(): every pg_dist_object row is marked, so it never
 * skips.
 */
static List *
DistObjectMarkPoolDeparse(HeapTuple tuple, TupleDesc tupleDesc,
						  MetadataSyncContext *context)
{
	Form_pg_dist_object distObjectForm = (Form_pg_dist_object) GETSTRUCT(tuple);

	ObjectAddress *address = palloc(sizeof(ObjectAddress));
	ObjectAddressSubSet(*address, distObjectForm->classid, distObjectForm->objid,
						distObjectForm->objsubid);

	bool distributionArgumentIndexIsNull = false;
	Datum distributionArgumentIndexDatum =
		heap_getattr(tuple, Anum_pg_dist_object_distribution_argument_index,
					 tupleDesc, &distributionArgumentIndexIsNull);
	int32 distributionArgumentIndex = DatumGetInt32(distributionArgumentIndexDatum);

	bool colocationIdIsNull = false;
	Datum colocationIdDatum =
		heap_getattr(tuple, Anum_pg_dist_object_colocationid, tupleDesc,
					 &colocationIdIsNull);
	int32 colocationId = DatumGetInt32(colocationIdDatum);

	bool forceDelegationIsNull = false;
	Datum forceDelegationDatum =
		heap_getattr(tuple, Anum_pg_dist_object_force_delegation, tupleDesc,
					 &forceDelegationIsNull);
	bool forceDelegation = DatumGetBool(forceDelegationDatum);

	if (distributionArgumentIndexIsNull)
	{
		distributionArgumentIndex = INVALID_DISTRIBUTION_ARGUMENT_INDEX;
	}

	if (colocationIdIsNull)
	{
		colocationId = INVALID_COLOCATION_ID;
	}

	if (forceDelegationIsNull)
	{
		forceDelegation = NO_FORCE_PUSHDOWN;
	}

	char *command =
		MarkObjectsDistributedCreateCommand(list_make1(address),
											list_make1_int(distributionArgumentIndex),
											list_make1_int(colocationId),
											list_make1_int(forceDelegation));

	return list_make1(command);
}


/*
 * SendShellTableCreationCommandsViaPool creates the shell tables of all Citus
 * tables on the activated nodes using a pool of parallel connections per node,
 * driven by the adaptive executor.
 *
 * This is the parallel counterpart of the shell table creation that
 * SendDependencyCreationCommands() performs serially over the single metadata
 * connection. It is only reached in nontransactional mode with
 * citus.metadata_sync_use_pool on (see MetadataSyncShellTablePoolEnabled()).
 * The prerequisite objects the shell tables depend on (roles, schemas, types,
 * functions, sequences, ...) were already created before this runs, so the
 * barrier ordering is preserved.
 */
void
SendShellTableCreationCommandsViaPool(MetadataSyncContext *context)
{
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, context->activatedWorkerNodeList)
	{
		RunNodeTargetedPoolPhase(context, workerNode, DistPartitionRelationId(),
								 ShellTablePoolDeparse, "shell table creation");
	}
}


/*
 * SendSequenceCreationCommandsViaPool creates the distributed sequences on all
 * activated nodes using a pool of parallel connections.
 *
 * It runs AFTER SendDependencyCreationCommands() (so the roles/schemas the
 * sequences depend on already exist) and BEFORE
 * SendShellTableCreationCommandsViaPool() (so each shell table can re-associate
 * its column defaults with the already-created sequences). Distributed sequences
 * form an embarrassingly parallel leaf class -- no intra-class edges, only upward
 * dependencies on the small prerequisite set -- so, like shell tables, they are
 * excluded from the serial dependency ordering phase and (re)created here in
 * parallel instead.
 */
void
SendSequenceCreationCommandsViaPool(MetadataSyncContext *context)
{
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, context->activatedWorkerNodeList)
	{
		RunNodeTargetedPoolPhase(context, workerNode, DistObjectRelationId(),
								 SequencePoolDeparse, "sequence creation");
	}
}


/*
 * SendDistTableMetadataCommandsViaPool creates the Citus table metadata entries
 * (pg_dist_shard, pg_dist_shard_placement, pg_dist_partition) for all distributed
 * tables on the activated nodes using a pool of parallel connections.
 *
 * This is the parallel counterpart of SendDistTableMetadataCommands(). The rows
 * for different tables are mutually independent (each references only its own
 * table plus already-synced pg_dist_colocation entries), so the phase is
 * embarrassingly parallel. It runs after the shell tables exist, exactly like
 * the serial per-table metadata loop it replaces.
 */
void
SendDistTableMetadataCommandsViaPool(MetadataSyncContext *context)
{
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, context->activatedWorkerNodeList)
	{
		RunNodeTargetedPoolPhase(context, workerNode, DistPartitionRelationId(),
								 DistTableMetadataPoolDeparse,
								 "dist table metadata creation");
	}
}


/*
 * SendDistObjectCommandsViaPool creates the pg_dist_object marks for all
 * distributed objects on the activated nodes using a pool of parallel
 * connections.
 *
 * This is the parallel counterpart of SendDistObjectCommands(). Each mark is a
 * self-contained metadata insert for one already-existing object, independent of
 * every other mark, so the phase is embarrassingly parallel. It runs after all
 * the objects it marks (prerequisites, sequences, shell tables) exist on the
 * worker, exactly like the serial loop it replaces.
 */
void
SendDistObjectCommandsViaPool(MetadataSyncContext *context)
{
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, context->activatedWorkerNodeList)
	{
		RunNodeTargetedPoolPhase(context, workerNode, DistObjectRelationId(),
								 DistObjectMarkPoolDeparse,
								 "dist object metadata creation");
	}
}


/*
 * CreateNodeTargetedPoolTaskList builds one DDL_TASK per per-task command list
 * in commandListPerTask, each targeting the single (shardless) placement on
 * workerNode. This mirrors ConvertNonExistingPlacementDDLCommandsToTasks() but
 * takes the WorkerNode directly, since the caller already has it, and attaches
 * the commands as a query string list so the adaptive executor runs (and
 * accounts for) them one statement at a time.
 *
 * It is shared by the metadata sync pool phases (shell tables and sequences):
 * both dispatch batches of independent, node-targeted DDL over the connection
 * pool in exactly this shape.
 */
static List *
CreateNodeTargetedPoolTaskList(List *commandListPerTask, WorkerNode *workerNode)
{
	List *taskList = NIL;
	int taskId = 1;
	List *taskCommandList = NIL;
	foreach_ptr(taskCommandList, commandListPerTask)
	{
		Task *task = CreateBasicTask(INVALID_JOB_ID, taskId, DDL_TASK, NULL);
		SetTaskQueryStringList(task, taskCommandList);

		/* node-targeted task with a synthetic placement and no real shard */
		ShardPlacement *taskPlacement = CitusMakeNode(ShardPlacement);
		SetPlacementNodeMetadata(taskPlacement, workerNode);
		task->taskPlacementList = list_make1(taskPlacement);

		taskList = lappend(taskList, task);
		taskId++;
	}

	return taskList;
}


/*
 * MetadataSyncShellTablePoolEnabled returns whether the shell table creation
 * step should use the parallel connection pool path instead of the serial
 * single-connection path.
 *
 * The pool path requires:
 *   - citus.metadata_sync_use_pool is on;
 *   - nontransactional mode, because the parallel connections each auto-commit
 *     their own work and so cannot participate in the single distributed
 *     transaction used by transactional mode;
 *   - that we are actually sending commands, not collecting them (the command
 *     collection path has no target connections to execute over).
 */
static bool
MetadataSyncShellTablePoolEnabled(MetadataSyncContext *context)
{
	return MetadataSyncUsePool &&
		   context->transactionMode == METADATA_SYNC_NON_TRANSACTIONAL &&
		   !MetadataSyncCollectsCommands(context);
}


/*
 * IsCitusShellTableDependency returns true if the given distributed object is a
 * Citus table relation whose shell table is (re)created during metadata sync.
 * This matches exactly the branch in GetDependencyCreateDDLCommands() that emits
 * the shell table bundle (see ShellTableCreationCommandList()).
 */
static bool
IsCitusShellTableDependency(const ObjectAddress *dependency)
{
	if (getObjectClass(dependency) != OCLASS_CLASS)
	{
		return false;
	}

	char relKind = get_rel_relkind(dependency->objectId);
	if (relKind != RELKIND_RELATION && relKind != RELKIND_PARTITIONED_TABLE &&
		relKind != RELKIND_FOREIGN_TABLE)
	{
		return false;
	}

	return IsCitusTable(dependency->objectId);
}


/*
 * IsDependentOnShellTableObject returns true if the given distributed object's
 * creation DDL references a Citus shell table, and therefore must be created
 * AFTER the shell table phase when the shell table pool path is enabled.
 *
 * These are:
 *  - views and materialized views (their definition selects from the table), and
 *  - publications (CREATE PUBLICATION ... FOR TABLE names the specific table).
 *
 * In the normal (serial) path these are emitted in dependency order after their
 * target tables, so no special handling is needed. In the pool path the shell
 * tables are deferred out of the dependency order, so these objects would be
 * emitted before their target tables exist; SendDependencyCreationCommands()
 * stashes them and SendDeferredDependentCreationCommands() (re)creates them once
 * the shell tables exist.
 */
static bool
IsDependentOnShellTableObject(const ObjectAddress *dependency)
{
	if (dependency->classId == PublicationRelationId)
	{
		return true;
	}

	if (getObjectClass(dependency) == OCLASS_CLASS)
	{
		char relKind = get_rel_relkind(dependency->objectId);
		if (relKind == RELKIND_VIEW || relKind == RELKIND_MATVIEW)
		{
			return true;
		}
	}

	return false;
}


/*
 * SendDeferredDependentCreationCommands (re)creates the objects that depend on
 * Citus shell tables (views, materialized views, publications) which
 * SendDependencyCreationCommands() deferred because their target shell tables
 * were created later in the parallel pool phase.
 *
 * It must be called AFTER SendShellTableCreationCommandsViaPool() so the shell
 * tables the deferred objects reference already exist, and BEFORE the per-table
 * metadata commands, matching the position these objects would have in the
 * stock dependency order. The deferred addresses were collected in dependency
 * order, so recreating them in list order preserves any relative ordering among
 * them (e.g. a view built on another view).
 *
 * In the serial path the deferred list is empty (nothing was stashed), so this
 * is a no-op and safe to call unconditionally.
 */
static void
SendDeferredDependentCreationCommands(MetadataSyncContext *context)
{
	List *deferredObjectAddresses = context->deferredDependentObjectAddresses;
	if (deferredObjectAddresses == NIL)
	{
		return;
	}

	/* disable ddl propagation */
	SendOrCollectCommandListToActivatedNodes(context,
											 list_make1(DISABLE_DDL_PROPAGATION));

	MemoryContext oldContext = MemoryContextSwitchTo(context->context);

	/*
	 * commandsContext holds the batched command strings until they are flushed;
	 * perObjectContext holds each object's deparse scratch and is reset every
	 * iteration so it does not accumulate.
	 */
	MemoryContext commandsContext = AllocSetContextCreate(context->context,
														  "deferred dependent commands context",
														  ALLOCSET_DEFAULT_SIZES);
	MemoryContext perObjectContext = AllocSetContextCreate(oldContext,
														   "deferred dependent per object context",
														   ALLOCSET_DEFAULT_SIZES);

	ObjectAddress *dependency = NULL;
	InitializeActivatedNodeCommandBatch(context, commandsContext);
	foreach_ptr(dependency, deferredObjectAddresses)
	{
		MemoryContextReset(perObjectContext);
		MemoryContextSwitchTo(perObjectContext);

		/*
		 * We expect extension-owned objects to be created as a result
		 * of the extension being created.
		 */
		if (!IsAnyObjectAddressOwnedByExtension(list_make1(dependency), NULL))
		{
			List *ddlCommands = GetAllDependencyCreateDDLCommands(list_make1(dependency));
			SendOrBatchCommandListToActivatedNodes(context, ddlCommands);
		}
	}

	MemoryContextSwitchTo(context->context);

	/* send whatever remains in the final partial batch */
	FlushBatchedCommandsToActivatedNodes(context);

	MemoryContextSwitchTo(oldContext);

	MemoryContextDelete(perObjectContext);

	if (!MetadataSyncCollectsCommands(context))
	{
		MemoryContextDelete(commandsContext);
	}
	ResetMetadataSyncMemoryContext(context);

	/*
	 * The deferred addresses live in TopTransactionContext; drop our reference
	 * so a subsequent activation in the same transaction starts clean. The
	 * memory itself is reclaimed when the local sync transaction ends.
	 */
	context->deferredDependentObjectAddresses = NIL;

	/* enable ddl propagation */
	SendOrCollectCommandListToActivatedNodes(context, list_make1(ENABLE_DDL_PROPAGATION));
}


/*
 * SendDistTableMetadataCommands sends commands related to pg_dist_shard and,
 * pg_dist_shard_placement entries to workers with transactional or nontransactional
 * mode according to transactionMode inside metadataSyncContext.
 */
void
SendDistTableMetadataCommands(MetadataSyncContext *context)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 0;

	Relation relation = table_open(DistPartitionRelationId(), AccessShareLock);
	TupleDesc tupleDesc = RelationGetDescr(relation);

	SysScanDesc scanDesc = systable_beginscan(relation, InvalidOid, false, NULL,
											  scanKeyCount, scanKey);

	MemoryContext oldContext = MemoryContextSwitchTo(context->context);

	/*
	 * Build each object's commands in a per-object context that we reset every
	 * iteration, so the per-object deparse and catalog scratch does not pile up
	 * in the batch context until the batch is flushed.
	 */
	MemoryContext perObjectContext = AllocSetContextCreate(oldContext,
														   "dist table commands per object context",
														   ALLOCSET_DEFAULT_SIZES);
	HeapTuple nextTuple = NULL;
	int64 processedCount = 0;
	InitializeActivatedNodeCommandBatch(context, context->context);
	while (true)
	{
		MemoryContextReset(perObjectContext);
		MemoryContextSwitchTo(perObjectContext);

		nextTuple = systable_getnext(scanDesc);
		if (!HeapTupleIsValid(nextTuple))
		{
			break;
		}

		/*
		 * Create Citus table metadata commands (pg_dist_shard, pg_dist_shard_placement,
		 * pg_dist_partition). Only Citus tables have shard metadata.
		 */
		Oid relationId = FetchRelationIdFromPgPartitionHeapTuple(nextTuple, tupleDesc);
		if (ShouldSyncTableMetadata(relationId))
		{
			List *commandList = CitusTableMetadataCreateCommandList(relationId);
			SendOrBatchCommandListToActivatedNodes(context, commandList);
		}

		/*
		 * We flush the caches even when we skip the dependency creation commands
		 * because ShouldSyncTableMetadata still opened relation to reach this
		 * decision, so advance the cache-flush counter and flush if needed on this
		 * skip path too.
		 */
		FlushMetadataSyncCachesIfNeeded(context, ++processedCount);
	}

	MemoryContextSwitchTo(context->context);

	/* send whatever remains in the final partial batch */
	FlushBatchedCommandsToActivatedNodes(context);

	MemoryContextSwitchTo(oldContext);

	MemoryContextDelete(perObjectContext);

	systable_endscan(scanDesc);
	table_close(relation, AccessShareLock);
}


/*
 * SendDistObjectCommands sends commands related to pg_dist_object entries to
 * workers with transactional or nontransactional mode according to transactionMode
 * inside metadataSyncContext.
 */
void
SendDistObjectCommands(MetadataSyncContext *context)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 0;

	Relation relation = table_open(DistObjectRelationId(), AccessShareLock);
	TupleDesc tupleDesc = RelationGetDescr(relation);

	SysScanDesc scanDesc = systable_beginscan(relation, InvalidOid, false, NULL,
											  scanKeyCount, scanKey);

	MemoryContext oldContext = MemoryContextSwitchTo(context->context);

	/*
	 * Build each object's commands in a per-object context that we reset every
	 * iteration, so the per-object deparse and catalog scratch does not pile up
	 * in the batch context until the batch is flushed.
	 */
	MemoryContext perObjectContext = AllocSetContextCreate(oldContext,
														   "dist object commands per object context",
														   ALLOCSET_DEFAULT_SIZES);
	HeapTuple nextTuple = NULL;
	int64 processedCount = 0;
	InitializeActivatedNodeCommandBatch(context, context->context);
	while (true)
	{
		MemoryContextReset(perObjectContext);
		MemoryContextSwitchTo(perObjectContext);

		nextTuple = systable_getnext(scanDesc);
		if (!HeapTupleIsValid(nextTuple))
		{
			break;
		}

		Form_pg_dist_object pg_dist_object = (Form_pg_dist_object) GETSTRUCT(nextTuple);

		ObjectAddress *address = palloc(sizeof(ObjectAddress));

		ObjectAddressSubSet(*address, pg_dist_object->classid, pg_dist_object->objid,
							pg_dist_object->objsubid);

		bool distributionArgumentIndexIsNull = false;
		Datum distributionArgumentIndexDatum =
			heap_getattr(nextTuple,
						 Anum_pg_dist_object_distribution_argument_index,
						 tupleDesc,
						 &distributionArgumentIndexIsNull);
		int32 distributionArgumentIndex = DatumGetInt32(distributionArgumentIndexDatum);

		bool colocationIdIsNull = false;
		Datum colocationIdDatum =
			heap_getattr(nextTuple,
						 Anum_pg_dist_object_colocationid,
						 tupleDesc,
						 &colocationIdIsNull);
		int32 colocationId = DatumGetInt32(colocationIdDatum);

		bool forceDelegationIsNull = false;
		Datum forceDelegationDatum =
			heap_getattr(nextTuple,
						 Anum_pg_dist_object_force_delegation,
						 tupleDesc,
						 &forceDelegationIsNull);
		bool forceDelegation = DatumGetBool(forceDelegationDatum);

		if (distributionArgumentIndexIsNull)
		{
			distributionArgumentIndex = INVALID_DISTRIBUTION_ARGUMENT_INDEX;
		}

		if (colocationIdIsNull)
		{
			colocationId = INVALID_COLOCATION_ID;
		}

		if (forceDelegationIsNull)
		{
			forceDelegation = NO_FORCE_PUSHDOWN;
		}

		char *workerMetadataUpdateCommand =
			MarkObjectsDistributedCreateCommand(list_make1(address),
												list_make1_int(distributionArgumentIndex),
												list_make1_int(colocationId),
												list_make1_int(forceDelegation));

		SendOrBatchCommandListToActivatedNodes(context,
											   list_make1(workerMetadataUpdateCommand));

		FlushMetadataSyncCachesIfNeeded(context, ++processedCount);
	}

	MemoryContextSwitchTo(context->context);

	/* send whatever remains in the final partial batch */
	FlushBatchedCommandsToActivatedNodes(context);

	MemoryContextSwitchTo(oldContext);

	MemoryContextDelete(perObjectContext);

	systable_endscan(scanDesc);
	relation_close(relation, NoLock);
}


/*
 * SendInterTableRelationshipCommands sends inter-table relationship commands
 * (e.g. constraints, attach partitions) to workers with transactional or
 * nontransactional mode per inter table relationship according to transactionMode
 * inside metadataSyncContext.
 */
void
SendInterTableRelationshipCommands(MetadataSyncContext *context)
{
	/* disable ddl propagation */
	SendOrCollectCommandListToActivatedNodes(context,
											 list_make1(DISABLE_DDL_PROPAGATION));

	ScanKeyData scanKey[1];
	int scanKeyCount = 0;

	Relation relation = table_open(DistPartitionRelationId(), AccessShareLock);
	TupleDesc tupleDesc = RelationGetDescr(relation);

	SysScanDesc scanDesc = systable_beginscan(relation, InvalidOid, false, NULL,
											  scanKeyCount, scanKey);

	MemoryContext oldContext = MemoryContextSwitchTo(context->context);

	/*
	 * Build each object's commands in a per-object context that we reset every
	 * iteration, so the per-object deparse and catalog scratch does not pile up
	 * in the batch context until the batch is flushed.
	 */
	MemoryContext perObjectContext = AllocSetContextCreate(oldContext,
														   "inter-table commands per object context",
														   ALLOCSET_DEFAULT_SIZES);
	HeapTuple nextTuple = NULL;
	int64 processedCount = 0;
	InitializeActivatedNodeCommandBatch(context, context->context);
	while (true)
	{
		MemoryContextReset(perObjectContext);
		MemoryContextSwitchTo(perObjectContext);

		nextTuple = systable_getnext(scanDesc);
		if (!HeapTupleIsValid(nextTuple))
		{
			break;
		}

		/*
		 * Skip foreign key and partition creation when the Citus table is
		 * owned by an extension or when the table doesn't need to be synced.
		 */
		Oid relationId = FetchRelationIdFromPgPartitionHeapTuple(nextTuple, tupleDesc);
		if (ShouldSyncTableMetadata(relationId) && !IsTableOwnedByExtension(relationId))
		{
			List *commandList = InterTableRelationshipOfRelationCommandList(relationId);
			SendOrBatchCommandListToActivatedNodes(context, commandList);
		}

		/*
		 * We flush the caches even when we skip the dependency creation commands
		 * because we still opened catalog entries to reach this decision, so advance
		 * the cache-flush counter and flush if needed on this skip path too.
		 */
		FlushMetadataSyncCachesIfNeeded(context, ++processedCount);
	}

	MemoryContextSwitchTo(context->context);

	/* send whatever remains in the final partial batch */
	FlushBatchedCommandsToActivatedNodes(context);

	MemoryContextSwitchTo(oldContext);

	MemoryContextDelete(perObjectContext);

	systable_endscan(scanDesc);
	table_close(relation, AccessShareLock);

	/* enable ddl propagation */
	SendOrCollectCommandListToActivatedNodes(context, list_make1(ENABLE_DDL_PROPAGATION));
}


/*
 * FlushMetadataSyncCachesIfNeeded drops the distributed-table cache, the
 * distributed-object cache, and the postgres relation/catalog caches that
 * accumulate while metadata sync opens each Citus table to build its
 * DDL and metadata commands if needed.
 *
 * We flush caches when actually sending commands to workers, not while merely
 * collecting.
 *
 * The caches are transparently rebuilt on the next access.
 */
static void
FlushMetadataSyncCachesIfNeeded(MetadataSyncContext *context, int64 processedCount)
{
	if (MetadataSyncCollectsCommands(context))
	{
		return;
	}

	if (!MetadataSyncCacheFlushIntervalReached(processedCount))
	{
		return;
	}

	FlushCachesForMetadataSync();
}


/*
 * MetadataSyncBatchingEnabled returns whether metadata sync should accumulate
 * the commands of multiple distributed objects and send them to the activated
 * nodes in batches.
 *
 * Note that we don't batch while merely collecting commands.
 */
static bool
MetadataSyncBatchingEnabled(MetadataSyncContext *context)
{
	return !MetadataSyncCollectsCommands(context) && MetadataSyncBatchSize > 1;
}


/*
 * InitializeActivatedNodeCommandBatch initializes the batch state that
 * SendOrBatchCommandListToActivatedNodes() accumulates into. Callers invoke it
 * once before the loop that produces the per-object command lists and pass the
 * memory context that holds the batched command strings, which is reset after
 * each flush.
 */
static void
InitializeActivatedNodeCommandBatch(MetadataSyncContext *context,
									MemoryContext commandsContext)
{
	context->batchedCommands = NIL;
	context->batchedCount = 0;
	context->batchedCommandsContext = commandsContext;
}


/*
 * SendOrBatchCommandListToActivatedNodes accumulates commandList into the
 * current batch and flushes it to the activated nodes once the batch reaches
 * citus.metadata_sync_batch_size distributed objects.
 *
 * When batching is disabled (including while merely collecting commands) it uses
 * an effective batch size of 1, i.e. it flushes after every distributed object.
 *
 * The finished command strings are copied into the batch memory context passed
 * to InitializeActivatedNodeCommandBatch(), so the caller should use a short-living
 * memory context that it resets after every object while generating commandList to
 * keep peak memory bounded.
 *
 * After the loop, the caller must call FlushBatchedCommandsToActivatedNodes() to
 * send any remaining partial batch.
 */
static void
SendOrBatchCommandListToActivatedNodes(MetadataSyncContext *context, List *commandList)
{
	MemoryContext oldContext = MemoryContextSwitchTo(context->batchedCommandsContext);

	char *command = NULL;
	foreach_ptr(command, commandList)
	{
		context->batchedCommands = lappend(context->batchedCommands, pstrdup(command));
	}

	MemoryContextSwitchTo(oldContext);

	context->batchedCount++;

	int batchSize = MetadataSyncBatchingEnabled(context) ? MetadataSyncBatchSize : 1;
	if (context->batchedCount >= batchSize)
	{
		FlushBatchedCommandsToActivatedNodes(context);
	}
}


/*
 * FlushBatchedCommandsToActivatedNodes sends the commands accumulated by
 * SendOrBatchCommandListToActivatedNodes() to the activated nodes and then
 * resets the batch state and the memory context that held the commands.
 */
static void
FlushBatchedCommandsToActivatedNodes(MetadataSyncContext *context)
{
	if (context->batchedCommands != NIL)
	{
		SendOrCollectCommandListToActivatedNodes(context, context->batchedCommands);
	}

	context->batchedCommands = NIL;
	context->batchedCount = 0;

	if (!MetadataSyncCollectsCommands(context))
	{
		MemoryContextReset(context->batchedCommandsContext);
	}
}
