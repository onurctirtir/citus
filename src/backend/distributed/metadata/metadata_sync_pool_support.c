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
#include "distributed/metadata_sync.h"
#include "distributed/metadata_sync_pool.h"

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
 * the drain can begin dispatching them to idle connections. pool->readyQueue is
 * a plain int32 FIFO array sized totalTasks (see its declaration): each task id
 * is pushed at most once in its lifetime (once here if it starts in-degree 0,
 * or once later in EdgeGatedOnComplete() when its in-degree first reaches 0),
 * so this initial push can never overflow the array even though later pushes
 * also land in it.
 */
static void
EdgeGatedSeed(MetadataSyncPool *pool)
{
	for (int64 taskId = 0; taskId < pool->totalTasks; taskId++)
	{
		if (pool->taskById[taskId]->inDegree == 0)
		{
			pool->readyQueue[pool->readyTail++] = (int32) taskId;
		}
	}
}


/*
 * EdgeGatedPullReady pops the next in-degree-0 task id off the edge-gated ready
 * queue, or returns NULL if the queue is currently empty. Popping is a plain
 * cursor advance (readyHead++), never a shift of the remaining entries, because
 * the queue is a flat array rather than a List.
 */
static MetadataSyncPoolTask *
EdgeGatedPullReady(MetadataSyncPool *pool)
{
	if (pool->readyHead == pool->readyTail)
	{
		return NULL;
	}

	int32 taskId = pool->readyQueue[pool->readyHead++];
	return pool->taskById[taskId];
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
 * completed inline. Successors are walked via the task's CSR range
 * (successorStart/successorCount indexing into pool->successorIds), not a List.
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

	for (int32 successorIndex = 0; successorIndex < task->successorCount;
		successorIndex++)
	{
		int32 successorId = pool->successorIds[task->successorStart + successorIndex];
		MetadataSyncPoolTask *successor = pool->taskById[successorId];

		successor->inDegree--;

		if (successor->inDegree == 0 && !successor->dispatched && !successor->done)
		{
			pool->readyQueue[pool->readyTail++] = successorId;
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
		task->successorStart = 0;
		task->successorCount = 0;
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
