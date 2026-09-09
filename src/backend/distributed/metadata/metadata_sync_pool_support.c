/*-------------------------------------------------------------------------
 *
 * metadata_sync_pool_support.c
 *
 * Support functions for the metadata sync connection pool, for edge-gated
 * and streaming-leaf task sources.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "catalog/pg_class.h"

#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata/pg_dist_object.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata_sync_pool.h"
#include "distributed/metadata_utility.h"

/* edge-gated source - support functions */
static void EdgeGatedSeed(MetadataSyncPool *pool);
static MetadataSyncPoolTask * EdgeGatedPullReady(MetadataSyncPool *pool);
static List * EdgeGatedDeparse(MetadataSyncPool *pool, MetadataSyncPoolTask *task);
static void EdgeGatedOnComplete(MetadataSyncPool *pool, MetadataSyncPoolTask *task);
static void EdgeGatedOnDrained(MetadataSyncPool *pool);

/* streaming-leaf source - support functions */
static MetadataSyncPoolTask * StreamingLeafPullReady(MetadataSyncPool *pool);
static List * StreamingLeafDeparse(MetadataSyncPool *pool, MetadataSyncPoolTask *task);
static void StreamingLeafOnComplete(MetadataSyncPool *pool, MetadataSyncPoolTask *task);
static void StreamingLeafClose(MetadataSyncPool *pool);

/* edge-gated source - support functions */
const MetadataSyncTaskSourceOps EdgeGatedSourceOps = {
	.seed = EdgeGatedSeed,
	.pullReady = EdgeGatedPullReady,
	.deparse = EdgeGatedDeparse,
	.onComplete = EdgeGatedOnComplete,
	.onDrained = EdgeGatedOnDrained,
	.close = NULL,
};

/* streaming-leaf source - support functions */
const MetadataSyncTaskSourceOps StreamingLeafSourceOps = {
	.seed = NULL,
	.pullReady = StreamingLeafPullReady,
	.deparse = StreamingLeafDeparse,
	.onComplete = StreamingLeafOnComplete,
	.onDrained = NULL,
	.close = StreamingLeafClose,
};


/*
 * EdgeGatedSeed seeds the edge-gated ready queue with every in-degree-0 task, so
 * the drain can begin dispatching them to idle connections.
 */
static void
EdgeGatedSeed(MetadataSyncPool *pool)
{
	MemoryContext oldContext = MemoryContextSwitchTo(pool->poolContext);
	MetadataSyncPoolTask *task = NULL;
	foreach_ptr(task, pool->taskList)
	{
		if (task->inDegree == 0)
		{
			pool->readyQueue = lappend(pool->readyQueue, task);
		}
	}
	MemoryContextSwitchTo(oldContext);
}


/*
 * EdgeGatedPullReady pops the next in-degree-0 task off the edge-gated ready
 * queue, or returns NULL if the queue is currently empty.
 */
static MetadataSyncPoolTask *
EdgeGatedPullReady(MetadataSyncPool *pool)
{
	if (pool->readyQueue == NIL)
	{
		return NULL;
	}

	MetadataSyncPoolTask *task = linitial(pool->readyQueue);

	MemoryContext queueContext = MemoryContextSwitchTo(pool->poolContext);
	pool->readyQueue = list_delete_first(pool->readyQueue);
	MemoryContextSwitchTo(queueContext);

	return task;
}


/*
 * EdgeGatedDeparse deparses an edge-gated task's create commands from the
 * dependency machinery.
 */
static List *
EdgeGatedDeparse(MetadataSyncPool *pool, MetadataSyncPoolTask *task)
{
	bool bundlePartitionMetadata = true;
	return GetAllDependencyCreateDDLCommands(list_make1(&task->objectAddress),
											 bundlePartitionMetadata);
}


/*
 * EdgeGatedOnComplete records an edge-gated task as done, advances the progress
 * and cache-flush bookkeeping, and decrements each successor's in-degree,
 * enqueuing any successor that becomes ready (in-degree 0). Used both when a
 * task's DDL was executed on a connection and when an object with no commands is
 * completed inline.
 */
static void
EdgeGatedOnComplete(MetadataSyncPool *pool, MetadataSyncPoolTask *task)
{
	task->done = true;
	pool->remainingTasks--;
	pool->completedTasks++;

	FlushMetadataSyncCachesIfNeeded(pool->context, pool->completedTasks);
	LogMetadataSyncProgress(pool->objectLabel, pool->completedTasks - 1,
							pool->completedTasks, pool->totalTasks);

	MetadataSyncPoolTask *successor = NULL;
	foreach_ptr(successor, task->successors)
	{
		successor->inDegree--;

		if (successor->inDegree == 0 && !successor->dispatched && !successor->done)
		{
			MemoryContext oldContext = MemoryContextSwitchTo(pool->poolContext);
			pool->readyQueue = lappend(pool->readyQueue, successor);
			MemoryContextSwitchTo(oldContext);
		}
	}
}


/*
 * EdgeGatedOnDrained is called when the edge-gated pool goes idle with nothing in
 * flight. Unreaped tasks with an empty ready queue mean a dependency cycle, so it
 * raises rather than letting the drain end silently.
 */
static void
EdgeGatedOnDrained(MetadataSyncPool *pool)
{
	if (pool->remainingTasks > 0)
	{
		ereport(ERROR, (errmsg("metadata sync pool stalled with "
							   "%ld object(s) remaining", pool->remainingTasks),
						errdetail("This indicates a dependency cycle among the "
								  "objects being synced.")));
	}
}


/*
 * StreamingLeafPullReady advances the pool's catalog scan cursor to the next
 * object it should create, allocating a fresh in-degree-0 task in the pool
 * context, or returns NULL once the scan is exhausted. Tuples whose extractOid
 * yields InvalidOid (rows that do not correspond to an object this pool creates,
 * e.g. non-sequence pg_dist_object rows) are skipped.
 *
 * systable_getnext runs in the parent transaction's resource owner here (no
 * per-object subtransaction is active at pull time), so the scan's buffer pins
 * survive the per-object deparse subtransaction that StreamingLeafDeparse rolls
 * back -- the same interleaving SendInterTableRelationshipCommands relies on.
 */
static MetadataSyncPoolTask *
StreamingLeafPullReady(MetadataSyncPool *pool)
{
	while (!pool->streamDone)
	{
		HeapTuple tuple = systable_getnext(pool->streamScan);
		if (!HeapTupleIsValid(tuple))
		{
			pool->streamDone = true;
			return NULL;
		}

		Oid objectId = pool->streamExtractOid(tuple, pool->streamTupleDesc);
		if (!OidIsValid(objectId))
		{
			/* not an object this pool creates; skip it */
			continue;
		}

		MemoryContext oldContext = MemoryContextSwitchTo(pool->poolContext);
		MetadataSyncPoolTask *task = palloc0(sizeof(MetadataSyncPoolTask));
		ObjectAddressSet(task->objectAddress, RelationRelationId, objectId);
		task->inDegree = 0;
		task->successors = NIL;
		task->dispatched = false;
		task->done = false;
		MemoryContextSwitchTo(oldContext);

		return task;
	}

	return NULL;
}


/*
 * StreamingLeafDeparse builds a streaming-leaf task's DDL command list.
 */
static List *
StreamingLeafDeparse(MetadataSyncPool *pool, MetadataSyncPoolTask *task)
{
	return pool->streamBuilder(task->objectAddress.objectId);
}


/*
 * StreamingLeafOnComplete records a streaming-leaf task as done. The task has no
 * successors, so it just counts it, fires the periodic cache flush, logs
 * progress, and frees the task (bounding the number of live streaming-task
 * allocations to at most one per connection).
 */
static void
StreamingLeafOnComplete(MetadataSyncPool *pool, MetadataSyncPoolTask *task)
{
	task->done = true;
	pool->completedTasks++;

	FlushMetadataSyncCachesIfNeeded(pool->context, pool->completedTasks);
	LogMetadataSyncProgress(pool->objectLabel, pool->completedTasks - 1,
							pool->completedTasks, 0);

	pfree(task);
}


/*
 * StreamingLeafClose ends the pool's catalog scan and releases the scanned
 * relation before the pool's connections are closed.
 */
static void
StreamingLeafClose(MetadataSyncPool *pool)
{
	if (pool->streamScan != NULL)
	{
		systable_endscan(pool->streamScan);
		table_close(pool->streamRelation, AccessShareLock);
		pool->streamScan = NULL;
		pool->streamRelation = NULL;
	}
}


/*
 * ShellTableStreamExtractOid maps a pg_dist_partition tuple to the OID of the
 * distributed table whose shell table should be created on the target node. It is
 * the STREAMING_LEAF source extractor for the shell-table pool phase.
 */
Oid
ShellTableStreamExtractOid(HeapTuple tuple, TupleDesc tupleDesc)
{
	return FetchRelationIdFromPgPartitionHeapTuple(tuple, tupleDesc);
}


/*
 * ShellTableStreamBuilder returns the shell table creation command bundle for a
 * distributed table, or NIL for extension-owned tables (which are created with
 * their extension, exactly as the serial dependency path skips them). It is the
 * STREAMING_LEAF source builder for the shell-table pool phase.
 */
List *
ShellTableStreamBuilder(Oid relationId)
{
	ObjectAddress tableAddress = { 0 };
	ObjectAddressSet(tableAddress, RelationRelationId, relationId);
	if (IsAnyObjectAddressOwnedByExtension(list_make1(&tableAddress), NULL))
	{
		return NIL;
	}

	bool bundlePartitionMetadata = true;
	return ShellTableCreationCommandList(relationId, bundlePartitionMetadata);
}


/*
 * SequenceStreamExtractOid maps a pg_dist_object tuple to the OID of the
 * distributed sequence to create, or InvalidOid when the tuple is not a
 * distributed sequence. It is the STREAMING_LEAF source extractor for the
 * sequence pool phase.
 *
 * It uses the same predicate (ObjectIsSequence) that excludes
 * distributed sequences from the serial dependency ordering phase, keeping the
 * "excluded there == created here" invariant in one place.
 */
Oid
SequenceStreamExtractOid(HeapTuple tuple, TupleDesc tupleDesc)
{
	Form_pg_dist_object distObjectForm = (Form_pg_dist_object) GETSTRUCT(tuple);
	Oid classId = distObjectForm->classid;
	Oid objId = distObjectForm->objid;

	if (!ObjectIsSequence(classId, objId))
	{
		return InvalidOid;
	}

	return objId;
}


/*
 * SequenceStreamBuilder returns the CREATE bundle for a distributed sequence, or
 * NIL when the sequence is extension-owned. It is the STREAMING_LEAF source
 * builder for the sequence pool phase.
 */
List *
SequenceStreamBuilder(Oid sequenceId)
{
	ObjectAddress seqAddress = { 0 };
	ObjectAddressSet(seqAddress, RelationRelationId, sequenceId);
	if (IsAnyObjectAddressOwnedByExtension(list_make1(&seqAddress), NULL))
	{
		return NIL;
	}

	return DDLCommandsForSequence(sequenceId, TableOwner(sequenceId));
}
