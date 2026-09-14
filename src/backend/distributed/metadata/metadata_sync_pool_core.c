/*-------------------------------------------------------------------------
 *
 * metadata_sync_pool_core.c
 *    Connection-pool executor for metadata sync.
 *
 * This file owns the source-agnostic core of the metadata sync connection pool:
 * opening K persistent async connections to a single activated node, draining a
 * ready queue over them (dispatch ready tasks onto idle connections, wait for at
 * least one to become readable, reap the completed ones), and closing the pool.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/genam.h"
#include "access/table.h"
#include "storage/latch.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

#include "distributed/connection_management.h"
#include "distributed/listutils.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata_sync_pool.h"
#include "distributed/remote_commands.h"

typedef struct MetadataSyncPoolConnection
{
	MultiConnection *connection;
	MetadataSyncPoolTask *inFlightTask;
} MetadataSyncPoolConnection;

/* static helpers */
static void DispatchReadyPoolTasks(MetadataSyncPool *pool);
static void WaitForAnyReadyPoolConnection(MetadataSyncPool *pool);
static void ReapReadyPoolConnections(MetadataSyncPool *pool);
static void CompletePoolTask(MetadataSyncPool *pool,
							 MetadataSyncPoolConnection *poolConnection);


/*
 * OpenMetadataSyncPool opens connectionCount parallel connections to a
 * single activated node and returns an initialized pool bound to the task source
 * described by ops. Each connection is a fresh, exclusively-claimed connection
 * (so the connectionCount connections are distinct sockets driven concurrently)
 * that is closed at transaction end, and has DDL propagation disabled once up
 * front (each connection is its own worker session, so the SET must be repeated
 * per connection). Source-specific state (task hash, ready queue, catalog scan)
 * starts empty and is set up by the source before RunMetadataSyncPool.
 */
MetadataSyncPool *
OpenMetadataSyncPool(MetadataSyncContext *context, WorkerNode *workerNode,
					 int connectionCount, const MetadataSyncTaskSourceOps *ops,
					 const char *objectLabel)
{
	MemoryContext poolContext =
		AllocSetContextCreate(context->context,
							  "metadata sync pool context",
							  ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(poolContext);

	MetadataSyncPool *pool = palloc0(sizeof(MetadataSyncPool));
	pool->context = context;
	pool->workerNode = workerNode;
	pool->connectionCount = connectionCount;
	pool->ops = ops;
	pool->objectLabel = objectLabel;
	pool->connections =
		palloc0(connectionCount * sizeof(MetadataSyncPoolConnection));
	pool->poolContext = poolContext;
	pool->perObjectContext =
		AllocSetContextCreate(poolContext,
							  "metadata sync pool per object context",
							  ALLOCSET_DEFAULT_SIZES);

	int connectionFlags = FORCE_NEW_CONNECTION | OUTSIDE_TRANSACTION;
	List *connectionList = NIL;
	for (int connectionIndex = 0; connectionIndex < connectionCount; connectionIndex++)
	{
		MultiConnection *connection =
			GetNodeUserDatabaseConnection(connectionFlags, workerNode->workerName,
										  workerNode->workerPort, CurrentUserName(),
										  NULL);

		ClaimConnectionExclusively(connection);
		ForceConnectionCloseAtTransactionEnd(connection);

		pool->connections[connectionIndex].connection = connection;
		pool->connections[connectionIndex].inFlightTask = NULL;
		connectionList = lappend(connectionList, connection);
	}

	/* establish all connections in parallel */
	FinishConnectionListEstablishment(connectionList);

	/* verify each connection and disable DDL propagation on its worker session */
	for (int connectionIndex = 0; connectionIndex < connectionCount; connectionIndex++)
	{
		MultiConnection *connection = pool->connections[connectionIndex].connection;
		if (PQstatus(connection->pgConn) != CONNECTION_OK)
		{
			ReportConnectionError(connection, ERROR);
		}

		ExecuteCriticalRemoteCommand(connection, DISABLE_DDL_PROPAGATION);
	}

	MemoryContextSwitchTo(oldContext);

	return pool;
}


/*
 * OpenStreamingLeafSource turns a pool into a STREAMING_LEAF source over the
 * catalog relation scanRelationId. It opens the relation with an AccessShareLock
 * and starts a full sequential systable scan whose cursor lives in the pool's
 * (parent-transaction-owned) resource owner.
 *
 * extractOid maps a scanned tuple to the OID of the object to create (or
 * InvalidOid to skip the tuple, e.g. a pg_dist_object row that is not a
 * distributed sequence); builder maps that OID to the object's DDL command list.
 * The scan is closed by CloseMetadataSyncPool.
 */
void
OpenStreamingLeafSource(MetadataSyncPool *pool, Oid scanRelationId,
						Oid (*extractOid)(HeapTuple, TupleDesc),
						List *(*builder)(Oid))
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 0;

	pool->streamRelation = table_open(scanRelationId, AccessShareLock);
	pool->streamTupleDesc = RelationGetDescr(pool->streamRelation);
	pool->streamScan = systable_beginscan(pool->streamRelation, InvalidOid, false,
										  NULL, scanKeyCount, scanKey);
	pool->streamDone = false;
	pool->streamExtractOid = extractOid;
	pool->streamBuilder = builder;
}


/*
 * MetadataSyncPoolRegisterTasks creates one MetadataSyncPoolTask per row of
 * taskAddressStore (produced by OrderObjectAddressTuplestoreInDependencyOrderWithEdges(),
 * which already excludes extension-owned objects -- see that function's
 * comment) and stores it at pool->taskById[id], where id is simply the row's
 * 0-based forward-read position: OrderObjectAddressTuplestoreInDependencyOrderWithEdges()
 * hands out ids in that same order, so no ObjectAddress -> id lookup is needed
 * here at all. All tasks start with in-degree 0; edges are layered on
 * afterwards by MetadataSyncPoolApplyEdges, which is what actually raises
 * in-degrees for tasks that have a prerequisite.
 *
 * taskAddressStore is rewound (tuplestore_rescan) before being read, so the
 * caller does not need to reset the read cursor itself; the caller retains
 * ownership of the tuplestore and may reuse it for the next worker node.
 */
void
MetadataSyncPoolRegisterTasks(MetadataSyncPool *pool,
							  Tuplestorestate *taskAddressStore,
							  TupleDesc taskAddressTupleDesc)
{
	MemoryContext oldContext = MemoryContextSwitchTo(pool->poolContext);

	int64 totalTasks = tuplestore_tuple_count(taskAddressStore);
	pool->taskById = palloc0(totalTasks * sizeof(MetadataSyncPoolTask *));
	pool->readyQueue = palloc(totalTasks * sizeof(int32));
	pool->readyHead = 0;
	pool->readyTail = 0;

	TupleTableSlot *slot = CreateObjectAddressTupleSlot(taskAddressTupleDesc);

	tuplestore_rescan(taskAddressStore);

	ObjectAddress address;
	int64 processedCount = 0;
	int32 nextId = 0;
	while (GetNextObjectAddressFromTuplestore(taskAddressStore, slot, &address))
	{
		MetadataSyncPoolTask *task = palloc0(sizeof(MetadataSyncPoolTask));
		task->objectAddress = address;
		task->inDegree = 0;
		task->successorStart = 0;
		task->successorCount = 0;
		task->dispatched = false;
		task->done = false;

		pool->taskById[nextId++] = task;

		FlushMetadataSyncCachesIfNeeded(pool->context, ++processedCount);
	}

	Assert(nextId == totalTasks);

	ExecDropSingleTupleTableSlot(slot);

	pool->totalTasks = totalTasks;
	pool->remainingTasks = pool->totalTasks;

	MemoryContextSwitchTo(oldContext);
}


/*
 * MetadataSyncPoolApplyEdges layers the pre-resolved dependency edges read from
 * edgeStore onto the registered tasks: edgeStore's rows are (prereqTaskId,
 * dependentTaskId) int32 pairs, already resolved to dense task ids (both
 * extension-collapse and self-edge dropping already applied) by
 * OrderObjectAddressTuplestoreInDependencyOrderWithEdges() -- see that
 * function's comment -- so this function does no ObjectAddress lookups at all;
 * it only builds the CSR (compressed sparse row) successor array from the id
 * pairs, in two passes over edgeStore:
 *
 *   Pass 1 counts each prereq task's outgoing edges into its own
 *   successorStart field (temporarily holding a raw count, not yet an offset)
 *   and each dependent task's in-degree. A prefix-sum turns those raw counts
 *   into true CSR start offsets in successorStart, and pool->successorIds is
 *   allocated to the resulting total edge count.
 *
 *   Pass 2 walks edgeStore again, and for each pair writes dependentTaskId into
 *   pool->successorIds at prereqTask->successorStart + prereqTask->successorCount,
 *   then increments prereqTask->successorCount -- reusing successorCount both as
 *   a fill cursor during this pass and, once the pass completes, as the task's
 *   final successor count (the two coincide because every task's successors are
 *   filled contiguously starting from its own successorStart).
 *
 * This two-pass shape means the CSR array is built with no extra O(edge count)
 * scratch array and no hash table at all.
 *
 * edgeStore is rewound (tuplestore_rescan) before each pass; the caller retains
 * ownership of it and may reuse it for the next worker node.
 */
void
MetadataSyncPoolApplyEdges(MetadataSyncPool *pool, Tuplestorestate *edgeStore,
						  TupleDesc edgeTupleDesc)
{
	MemoryContext oldContext = MemoryContextSwitchTo(pool->poolContext);

	TupleTableSlot *slot = CreateObjectDependencyEdgeIdTupleSlot(edgeTupleDesc);

	/* pass 1: raw successor counts (in successorStart) and in-degrees */
	tuplestore_rescan(edgeStore);

	int32 prereqTaskId;
	int32 dependentTaskId;
	int64 totalEdges = 0;
	while (GetNextObjectDependencyEdgeIdsFromTuplestore(edgeStore, slot,
														&prereqTaskId,
														&dependentTaskId))
	{
		pool->taskById[prereqTaskId]->successorStart++;
		pool->taskById[dependentTaskId]->inDegree++;
		totalEdges++;
	}

	/* prefix sum: convert raw per-task counts into CSR start offsets */
	int32 offset = 0;
	for (int64 taskId = 0; taskId < pool->totalTasks; taskId++)
	{
		MetadataSyncPoolTask *task = pool->taskById[taskId];
		int32 rawCount = task->successorStart;
		task->successorStart = offset;
		task->successorCount = 0;
		offset += rawCount;
	}
	Assert(offset == totalEdges);

	pool->successorIds = palloc(totalEdges * sizeof(int32));

	/* pass 2: fill pool->successorIds, using successorCount as a fill cursor */
	tuplestore_rescan(edgeStore);

	while (GetNextObjectDependencyEdgeIdsFromTuplestore(edgeStore, slot,
														&prereqTaskId,
														&dependentTaskId))
	{
		MetadataSyncPoolTask *prereqTask = pool->taskById[prereqTaskId];
		int32 slotIndex = prereqTask->successorStart + prereqTask->successorCount;
		pool->successorIds[slotIndex] = dependentTaskId;
		prereqTask->successorCount++;
	}

	ExecDropSingleTupleTableSlot(slot);

	MemoryContextSwitchTo(oldContext);
}


/*
 * RunMetadataSyncPool drains the pool: it repeatedly dispatches ready tasks onto
 * idle connections, waits for at least one in-flight connection to become
 * readable, and reaps the completed connections. It returns once the task source
 * is exhausted and no connection is still in flight.
 *
 * The task source is given a chance to seed itself (e.g. an edge-gated source
 * seeds its ready queue with all in-degree-0 tasks) before the drain loop. When
 * the pool goes idle with nothing in flight, the source's onDrained hook runs
 * (e.g. an edge-gated source raises on remaining tasks, which means a dependency
 * cycle) and the drain ends.
 */
void
RunMetadataSyncPool(MetadataSyncPool *pool)
{
	if (pool->ops->seed != NULL)
	{
		pool->ops->seed(pool);
	}

	while (true)
	{
		DispatchReadyPoolTasks(pool);

		bool anyInFlight = false;
		for (int connectionIndex = 0; connectionIndex < pool->connectionCount;
			 connectionIndex++)
		{
			if (pool->connections[connectionIndex].inFlightTask != NULL)
			{
				anyInFlight = true;
				break;
			}
		}

		if (!anyInFlight)
		{
			/*
			 * Nothing is executing and nothing could be dispatched. Give the
			 * source a chance to complain (an edge-gated source raises here if
			 * tasks remain, which indicates a dependency cycle); otherwise the
			 * drain is done.
			 */
			if (pool->ops->onDrained != NULL)
			{
				pool->ops->onDrained(pool);
			}

			break;
		}

		WaitForAnyReadyPoolConnection(pool);
		ReapReadyPoolConnections(pool);
	}

	ereport(DEBUG1, (errmsg("parallel %s on node %s:%d completed: %ld object(s) "
							"(pool size %d)",
							pool->objectLabel, pool->workerNode->workerName,
							pool->workerNode->workerPort, pool->completedTasks,
							pool->connectionCount)));
}


/*
 * CloseMetadataSyncPool tears down any source-owned resources, closes the pool's
 * connections (releasing the worker backends before the next phase), and frees
 * the pool's memory context.
 */
void
CloseMetadataSyncPool(MetadataSyncPool *pool)
{
	if (pool->ops->close != NULL)
	{
		pool->ops->close(pool);
	}

	for (int connectionIndex = 0; connectionIndex < pool->connectionCount;
		 connectionIndex++)
	{
		MultiConnection *connection = pool->connections[connectionIndex].connection;
		if (connection != NULL)
		{
			CloseConnection(connection);
		}
	}

	MemoryContextDelete(pool->poolContext);
}


/*
 * DispatchReadyPoolTasks fills every idle connection with a ready task. For each
 * idle connection it pulls the next available task from the source until it
 * either sends one object's DDL (the connection becomes busy) or the source runs
 * dry. An object whose deparse yields no commands is completed inline (via the
 * source's onComplete hook) without occupying a connection.
 *
 * Each object's command list is sent as ONE multi-statement string, which the
 * worker runs as a single implicit transaction, giving per-object atomicity. The
 * command string is built in the pool's per-object context, which is reset before
 * each deparse; SendRemoteCommand copies the string into libpq's send buffer, so
 * it is safe to reset that context on the next deparse.
 */
static void
DispatchReadyPoolTasks(MetadataSyncPool *pool)
{
	for (int connectionIndex = 0; connectionIndex < pool->connectionCount;
		 connectionIndex++)
	{
		MetadataSyncPoolConnection *poolConnection = &pool->connections[connectionIndex];
		if (poolConnection->inFlightTask != NULL)
		{
			/* connection is busy */
			continue;
		}

		while (true)
		{
			MetadataSyncPoolTask *task = pool->ops->pullReady(pool);
			if (task == NULL)
			{
				/* nothing available for this connection right now */
				break;
			}

			task->dispatched = true;

			/* deparse this object's DDL in the reset per-object context */
			MemoryContextReset(pool->perObjectContext);
			MemoryContext deparseContext =
				MemoryContextSwitchTo(pool->perObjectContext);

			List *commandList = pool->ops->deparse(pool, task);

			if (commandList == NIL)
			{
				/* nothing to send; complete inline and try the next task */
				MemoryContextSwitchTo(deparseContext);
				pool->ops->onComplete(pool, task);
				continue;
			}

			StringInfo commandString = makeStringInfo();
			char *command = NULL;
			foreach_ptr(command, commandList)
			{
				appendStringInfo(commandString, "%s;", command);
			}

			int querySent =
				SendRemoteCommand(poolConnection->connection, commandString->data);

			MemoryContextSwitchTo(deparseContext);

			if (querySent == 0)
			{
				ReportConnectionError(poolConnection->connection, ERROR);
			}

			poolConnection->inFlightTask = task;

			/* this connection is now busy; move on to the next connection */
			break;
		}
	}
}


/*
 * WaitForAnyReadyPoolConnection blocks until at least one in-flight connection is
 * ready for IO (readable, or writeable while output is still pending), or an
 * interrupt/postmaster-death event fires. It mirrors the WaitEventSet usage in the
 * adaptive executor's connection multiplexer: it waits for readability on every
 * in-flight connection's socket, and for writeability on any connection whose
 * command is not fully flushed yet -- draining that output here so a command larger
 * than the socket send buffer cannot deadlock against the worker -- plus the process
 * latch (for interrupt handling) and postmaster death.
 */
static void
WaitForAnyReadyPoolConnection(MetadataSyncPool *pool)
{
	int inFlightCount = 0;
	for (int connectionIndex = 0; connectionIndex < pool->connectionCount;
		 connectionIndex++)
	{
		if (pool->connections[connectionIndex].inFlightTask != NULL)
		{
			inFlightCount++;
		}
	}

	if (inFlightCount == 0)
	{
		return;
	}

	/* room for the in-flight sockets plus the latch and postmaster death */
	WaitEventSet *waitEventSet =
		CreateWaitEventSet(CurrentMemoryContext, inFlightCount + 2);

	for (int connectionIndex = 0; connectionIndex < pool->connectionCount;
		 connectionIndex++)
	{
		MetadataSyncPoolConnection *poolConnection = &pool->connections[connectionIndex];
		if (poolConnection->inFlightTask == NULL)
		{
			continue;
		}

		/*
		 * Flush any output still buffered for this connection. SendRemoteCommand()
		 * only attempts a single flush, so a command larger than the socket send
		 * buffer can leave bytes unsent. If we then waited for readability only, the
		 * worker would block waiting for the rest of the command while we block
		 * waiting for a result that can never arrive. So wait for writeability too
		 * whenever output is still pending, and push it out below.
		 */
		int waitFlags = WL_SOCKET_READABLE;
		int flushStatus = PQflush(poolConnection->connection->pgConn);
		if (flushStatus == -1)
		{
			ReportConnectionError(poolConnection->connection, ERROR);
		}
		else if (flushStatus == 1)
		{
			waitFlags |= WL_SOCKET_WRITEABLE;
		}

		int sock = PQsocket(poolConnection->connection->pgConn);
		int waitEventSetIndex =
			CitusAddWaitEventSetToSet(waitEventSet, waitFlags, sock, NULL,
									  (void *) poolConnection);
		if (waitEventSetIndex == WAIT_EVENT_SET_INDEX_FAILED)
		{
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
							errmsg("connection for metadata sync to node %s:%d failed",
								   poolConnection->connection->hostname,
								   poolConnection->connection->port)));
		}
	}

	AddWaitEventToSet(waitEventSet, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL, NULL);
	AddWaitEventToSet(waitEventSet, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);

	WaitEvent *events = palloc0((inFlightCount + 2) * sizeof(WaitEvent));

	int eventCount = WaitEventSetWait(waitEventSet, -1, events, inFlightCount + 2,
									  WAIT_EVENT_CLIENT_READ);

	for (int eventIndex = 0; eventIndex < eventCount; eventIndex++)
	{
		WaitEvent *event = &events[eventIndex];

		if (event->events & WL_POSTMASTER_DEATH)
		{
			ereport(ERROR, (errmsg("postmaster was shut down, exiting")));
		}

		if (event->events & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}

		/*
		 * A connection became writeable with output still pending: push more of the
		 * buffered command out. Reading of any ready result is handled by
		 * ReapReadyPoolConnections() after this returns; servicing both directions
		 * keeps the multi-statement send/receive from deadlocking.
		 */
		if (event->events & WL_SOCKET_WRITEABLE)
		{
			MetadataSyncPoolConnection *poolConnection =
				(MetadataSyncPoolConnection *) event->user_data;
			if (PQflush(poolConnection->connection->pgConn) == -1)
			{
				ReportConnectionError(poolConnection->connection, ERROR);
			}
		}
	}

	FreeWaitEventSet(waitEventSet);
	pfree(events);
}


/*
 * ReapReadyPoolConnections consumes input on every in-flight connection and, for
 * each one whose result is ready, drains all of that object's results (raising an
 * error on any failure) and completes the task. Draining loops
 * GetRemoteCommandResult() until NULL so that every statement of the object's
 * multi-statement command (including row-returning worker_* helper calls) is
 * consumed.
 */
static void
ReapReadyPoolConnections(MetadataSyncPool *pool)
{
	for (int connectionIndex = 0; connectionIndex < pool->connectionCount;
		 connectionIndex++)
	{
		MetadataSyncPoolConnection *poolConnection = &pool->connections[connectionIndex];
		if (poolConnection->inFlightTask == NULL)
		{
			continue;
		}

		MultiConnection *connection = poolConnection->connection;

		if (PQconsumeInput(connection->pgConn) == 0)
		{
			ReportConnectionError(connection, ERROR);
		}

		if (PQisBusy(connection->pgConn))
		{
			/* results not ready yet */
			continue;
		}

		PGresult *result = NULL;
		bool raiseInterrupts = true;
		while ((result = GetRemoteCommandResult(connection, raiseInterrupts)) != NULL)
		{
			if (!IsResponseOK(result))
			{
				ReportResultError(connection, result, ERROR);
			}

			PQclear(result);
		}

		CompletePoolTask(pool, poolConnection);
	}
}


/*
 * CompletePoolTask marks the connection's in-flight task complete and frees the
 * connection for the next dispatch. The source's onComplete hook does the
 * source-specific bookkeeping (advancing dependency successors, or
 * counting/flushing/freeing a streaming leaf task).
 */
static void
CompletePoolTask(MetadataSyncPool *pool,
				 MetadataSyncPoolConnection *poolConnection)
{
	MetadataSyncPoolTask *task = poolConnection->inFlightTask;
	poolConnection->inFlightTask = NULL;

	pool->ops->onComplete(pool, task);
}
