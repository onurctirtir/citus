/*-------------------------------------------------------------------------
 *
 * metadata_sync_pool.h
 *    Generic wave-less connection-pool executor for metadata sync.
 *
 * A MetadataSyncPool drives K persistent async libpq connections to a single
 * activated node and runs a make(1)-style ready queue over them: whenever a
 * connection is idle it pulls the next ready task from a pluggable task source,
 * deparses that object's DDL on dispatch, sends it, and reaps the result. Object
 * command strings are NEVER materialized up front -- only object identities are
 * held, and each object's DDL is deparsed on dispatch and freed on reap, so
 * coordinator memory is bounded by the number of in-flight objects (<= K), not by
 * the object count.
 *
 * The dispatch -> wait -> reap connection core in metadata_sync_pool_core.c is source
 * agnostic: it never touches a ready queue, a scan cursor, or a dependency edge
 * directly. All source-specific behavior is provided through a
 * MetadataSyncTaskSourceOps vtable set on the pool. metadata_sync_pool_core.c
 * implements one source:
 *
 *   edge-gated source (dependency-creation phase)
 *       Tasks are materialized up front from a dependency-ordered ObjectAddress
 *       tuplestore plus an edge tuplestore. The ready queue is seeded with the
 *       in-degree-0 tasks and drained with in-degree gating: completing a task
 *       advances its successors. Every distributed object -- tables, sequences,
 *       shell tables, views, publications, and anything else pg_dist_object
 *       tracks -- is scanned and ordered uniformly and flows through this single
 *       source; there is no special-cased leaf phase for any object kind, since
 *       an object with no dependency edges is simply an in-degree-0 task like any
 *       other.
 *
 * A separate streaming-leaf source (MetadataSyncTaskSourceOps StreamingLeafSourceOps,
 * driven via OpenStreamingLeafSource()) also exists in metadata_sync_pool_core.c: it
 * produces tasks on demand by scanning a catalog relation, without any HTAB, edge
 * set, or successor bookkeeping. Metadata sync's dependency-creation phase does not
 * use it; it is kept for other pool-driven phases (e.g. shelldrop's drop path) that
 * still want a bounded-memory leaf scan.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_METADATA_SYNC_POOL_H
#define CITUS_METADATA_SYNC_POOL_H

#include "postgres.h"

#include "access/genam.h"
#include "access/htup.h"
#include "access/tupdesc.h"
#include "catalog/objectaddress.h"
#include "utils/relcache.h"
#include "utils/tuplestore.h"

#include "distributed/metadata_sync.h"

/*
 * MetadataSyncPoolTask is one object to (re)create on the target node. For an
 * edge-gated source it also carries the in-degree (count of not-yet-created
 * prerequisites that are also tasks) and its CSR successor range (successorStart/
 * successorCount index into the owning pool's flat successorIds array -- see
 * that field's comment); for a streaming-leaf source those stay at their
 * zero-initialized values (in-degree 0, no successors).
 */
typedef struct MetadataSyncPoolTask
{
	ObjectAddress objectAddress;
	int inDegree;
	int32 successorStart;      /* index into pool->successorIds where this task's successors begin */
	int32 successorCount;      /* number of successors, starting at successorStart */
	bool dispatched;
	bool done;
} MetadataSyncPoolTask;

struct MetadataSyncPoolConnection;
struct MetadataSyncPool;

/*
 * MetadataSyncTaskSourceOps is the vtable that specializes the generic pool
 * executor for a particular task source. The dispatch -> wait -> reap core calls
 * only these ops; it never inspects a ready queue, scan cursor, or edge set
 * itself.
 *
 *   seed        (optional) called once before the drain loop. The edge-gated
 *               source seeds the ready queue with its in-degree-0 tasks; the
 *               streaming source has nothing to seed (NULL).
 *   pullReady   (required) returns the next task an idle connection should run, or
 *               NULL if none is available right now (ready queue empty / scan
 *               drained).
 *   deparse     (required) produces the DDL command list for a task (NIL if there
 *               is nothing to send, in which case the task is completed inline).
 *   onComplete  (required) records a task as done. The edge-gated source advances
 *               successors' in-degrees; the streaming source counts, flushes
 *               caches, logs progress, and frees the task.
 *   onDrained   (optional) called once when the pool goes idle with no task in
 *               flight. The edge-gated source raises on remaining tasks (a
 *               dependency cycle); the streaming source is done (NULL).
 *   close       (optional) tears down source-owned resources before the
 *               connections are closed. The streaming source ends its catalog
 *               scan; the edge-gated source owns nothing extra (NULL).
 */
typedef struct MetadataSyncTaskSourceOps
{
	void (*seed)(struct MetadataSyncPool *pool);
	MetadataSyncPoolTask *(*pullReady)(struct MetadataSyncPool *pool);
	List *(*deparse)(struct MetadataSyncPool *pool, MetadataSyncPoolTask *task);
	void (*onComplete)(struct MetadataSyncPool *pool, MetadataSyncPoolTask *task);
	void (*onDrained)(struct MetadataSyncPool *pool);
	void (*close)(struct MetadataSyncPool *pool);
} MetadataSyncTaskSourceOps;

typedef struct MetadataSyncPool
{
	MetadataSyncContext *context;
	WorkerNode *workerNode;
	int connectionCount;
	struct MetadataSyncPoolConnection *connections;    /* array [connectionCount] */
	const MetadataSyncTaskSourceOps *ops;

	/* progress/log label, e.g. "dependency objects", "shell tables" */
	const char *objectLabel;

	/*
	 * edge-gated source: tasks materialized up front (one per row of the
	 * dependency-ordered address tuplestore, in read order) and drained with
	 * in-degree gating. Every structure below is a flat array sized once
	 * totalTasks is known (MetadataSyncPoolRegisterTasks uses
	 * tuplestore_tuple_count() to presize them exactly) -- there is no
	 * ObjectAddress-keyed HTAB in the pool at all, because
	 * OrderObjectAddressTuplestoreInDependencyOrderWithEdges() already assigns
	 * each task its dense id and resolves every edge to an id pair upstream
	 * (see dependency.c); MetadataSyncPoolApplyEdges() only ever indexes taskById
	 * by that id.
	 */
	MetadataSyncPoolTask **taskById; /* array[totalTasks], indexed by dense task id */
	int32 *successorIds;             /* flat CSR successor array; see MetadataSyncPoolTask.successorStart/successorCount */

	/*
	 * readyQueue is a plain int32 FIFO array of task ids, sized totalTasks: each
	 * task id is pushed onto it at most once (when it first becomes in-degree 0)
	 * and popped at most once, so readyHead/readyTail are both monotonically
	 * increasing and never wrap around -- unlike a List-based queue, popping
	 * never has to shift any remaining elements.
	 */
	int32 *readyQueue;
	int64 readyHead;   /* next slot to pop (readyQueue[readyHead]); readyHead == readyTail means empty */
	int64 readyTail;   /* next slot to push (readyQueue[readyTail++] = id) */
	int64 remainingTasks;

	/* streaming-leaf source: on-demand catalog scan, at most K live tasks */
	Relation streamRelation;
	SysScanDesc streamScan;
	TupleDesc streamTupleDesc;
	bool streamDone;
	Oid (*streamExtractOid)(HeapTuple tuple, TupleDesc tupleDesc);
	List *(*streamBuilder)(Oid objectId);

	int64 completedTasks;
	int64 totalTasks;
	MemoryContext poolContext;      /* long-lived across the phase */
	MemoryContext perObjectContext; /* reset per deparse */
} MetadataSyncPool;

/* public pool drivers */
extern MetadataSyncPool * OpenMetadataSyncPool(MetadataSyncContext *context,
											   WorkerNode *workerNode,
											   int connectionCount,
											   const MetadataSyncTaskSourceOps *ops,
											   const char *objectLabel);
extern void OpenStreamingLeafSource(MetadataSyncPool *pool, Oid scanRelationId,
									Oid (*extractOid)(HeapTuple, TupleDesc),
									List *(*builder)(Oid));
extern void MetadataSyncPoolRegisterTasks(MetadataSyncPool *pool,
										  Tuplestorestate *taskAddressStore,
										  TupleDesc taskAddressTupleDesc);
extern void MetadataSyncPoolApplyEdges(MetadataSyncPool *pool,
									   Tuplestorestate *edgeStore,
									   TupleDesc edgeTupleDesc);
extern void RunMetadataSyncPool(MetadataSyncPool *pool);
extern void CloseMetadataSyncPool(MetadataSyncPool *pool);

/* public support functions */
extern const MetadataSyncTaskSourceOps EdgeGatedSourceOps;
extern const MetadataSyncTaskSourceOps StreamingLeafSourceOps;

#endif   /* CITUS_METADATA_SYNC_POOL_H */
