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

/* HTAB entry mapping an ObjectAddress to its task (HASH_BLOBS over ObjectAddress) */
typedef struct MetadataSyncPoolTaskEntry
{
	ObjectAddress key;
	MetadataSyncPoolTask *task;
} MetadataSyncPoolTaskEntry;

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
 * MetadataSyncPoolRegisterTasks creates one MetadataSyncPoolTask per
 * distinct object address in taskAddresses and registers it in the pool's
 * address -> task hash. All tasks start with in-degree 0; edges are layered on
 * afterwards by MetadataSyncPoolApplyEdges.
 */
void
MetadataSyncPoolRegisterTasks(MetadataSyncPool *pool,
							  List *taskAddresses)
{
	MemoryContext oldContext = MemoryContextSwitchTo(pool->poolContext);

	if (pool->taskByAddress == NULL)
	{
		HASHCTL info;
		memset(&info, 0, sizeof(info));
		info.keysize = sizeof(ObjectAddress);
		info.entrysize = sizeof(MetadataSyncPoolTaskEntry);
		info.hcxt = pool->poolContext;
		int hashFlags = (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		pool->taskByAddress = hash_create("metadata sync pool tasks",
										  32, &info, hashFlags);
	}

	ObjectAddress *address = NULL;
	foreach_ptr(address, taskAddresses)
	{
		bool found = false;
		MetadataSyncPoolTaskEntry *entry =
			hash_search(pool->taskByAddress, address, HASH_ENTER, &found);
		if (found)
		{
			/* duplicate address in the ordered list; one task is enough */
			continue;
		}

		MetadataSyncPoolTask *task = palloc0(sizeof(MetadataSyncPoolTask));
		task->objectAddress = *address;
		task->inDegree = 0;
		task->successors = NIL;
		task->dispatched = false;
		task->done = false;

		entry->task = task;
		pool->taskList = lappend(pool->taskList, task);
	}

	pool->totalTasks = list_length(pool->taskList);
	pool->remainingTasks = pool->totalTasks;

	MemoryContextSwitchTo(oldContext);
}


/*
 * MetadataSyncPoolApplyEdges layers the dependency edges onto the registered tasks:
 * for every prereq -> dependent edge it appends the dependent to the prerequisite's
 * successor list and increments the dependent's in-degree, but only after resolving
 * BOTH endpoints to the task that actually creates them.
 *
 * An address that is itself a task resolves to that task. An address that is not a
 * task but is owned by an extension resolves to the OWNING EXTENSION's task, because
 * extension-owned objects are created as a side effect of CREATE EXTENSION rather than
 * as their own pool task -- so the real creation-order constraint runs through the
 * extension. Resolving both endpoints matters in two ways:
 *
 *   - a prerequisite reached only THROUGH an extension-owned intermediary (a
 *     distributed function whose signature uses an extension-owned type) collapses the
 *     edge onto that extension, so the dependent waits for CREATE EXTENSION; and
 *   - an edge whose DEPENDENT is extension-owned (an object the extension itself
 *     creates that depends on another scheduled object) collapses onto the extension,
 *     so CREATE EXTENSION still runs after that prerequisite.
 *
 * An endpoint that resolves to neither a task nor an extension task is created out of
 * band before this phase and its edge is dropped, exactly as the serial path assumes.
 * Self-edges, including ones that become self-edges after the extension collapse, are
 * ignored so a task can never block on itself.
 */
void
MetadataSyncPoolApplyEdges(MetadataSyncPool *pool, List *edgeList)
{
	ObjectDependencyEdge *edge = NULL;
	foreach_ptr(edge, edgeList)
	{
		if (memcmp(&edge->prereq, &edge->dependent, sizeof(ObjectAddress)) == 0)
		{
			/* self-edge, ignore */
			continue;
		}

		/*
		 * Resolve each endpoint to a scheduled task, following an extension-owned
		 * object to its owning extension task (see the function comment).
		 */
		MetadataSyncPoolTask *endpointTask[2] = { NULL, NULL };
		ObjectAddress *endpointAddress[2] = { &edge->prereq, &edge->dependent };
		for (int endpoint = 0; endpoint < 2; endpoint++)
		{
			ObjectAddress *address = endpointAddress[endpoint];

			bool found = false;
			MetadataSyncPoolTaskEntry *entry =
				hash_search(pool->taskByAddress, address, HASH_FIND, &found);
			if (!found)
			{
				ObjectAddress extensionAddress = { 0 };
				if (IsAnyObjectAddressOwnedByExtension(list_make1(address),
													   &extensionAddress))
				{
					entry = hash_search(pool->taskByAddress, &extensionAddress,
										HASH_FIND, &found);
				}
			}

			endpointTask[endpoint] = found ? entry->task : NULL;
		}

		MetadataSyncPoolTask *prereqTask = endpointTask[0];
		MetadataSyncPoolTask *dependentTask = endpointTask[1];

		if (prereqTask == NULL || dependentTask == NULL)
		{
			/* an endpoint is created out of band; drop the edge */
			continue;
		}

		if (prereqTask == dependentTask)
		{
			/* both endpoints resolve to the same task (e.g. after the extension
			 * collapse); ignore this self-edge */
			continue;
		}

		MemoryContext oldContext = MemoryContextSwitchTo(pool->poolContext);
		prereqTask->successors = lappend(prereqTask->successors, dependentTask);
		MemoryContextSwitchTo(oldContext);

		dependentTask->inDegree++;
	}
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
