/*-------------------------------------------------------------------------
 *
 * dependency.c
 *    Functions to follow and record dependencies for objects to be
 *    created in the right order.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_DEPENDENCY_H
#define CITUS_DEPENDENCY_H

#include "postgres.h"

#include "catalog/objectaddress.h"
#include "catalog/pg_depend.h"
#include "nodes/pg_list.h"
#include "utils/tuplestore.h"

#include "distributed/errormessage.h"

typedef bool (*AddressPredicate)(const ObjectAddress *);

/*
 * ObjectDependencyEdge is a single directed edge of the object dependency graph:
 * prereq must be (re)created before dependent.
 */
typedef struct ObjectDependencyEdge
{
	ObjectAddress prereq;
	ObjectAddress dependent;
} ObjectDependencyEdge;

extern List * GetUniqueDependenciesList(List *objectAddressesList);
extern List * GetDependenciesForObject(const ObjectAddress *target);
extern List * GetAllSupportedDependenciesForObject(const ObjectAddress *target);
extern List * GetAllDependenciesForObject(const ObjectAddress *target);
extern bool ErrorOrWarnIfAnyObjectHasUnsupportedDependency(List *objectAddresses);
extern DeferredErrorMessage * DeferErrorIfAnyObjectHasUnsupportedDependency(const List *
																			objectAddresses);
extern List * GetAllCitusDependedDependenciesForObject(const ObjectAddress *target);
extern List * OrderObjectAddressListInDependencyOrder(List *objectAddressList,
													  bool flushCaches);
extern List * OrderObjectAddressListInDependencyOrderWithEdges(List *objectAddressList,
															   bool flushCaches,
															   List **edgeList);
extern bool SupportedDependencyByCitus(const ObjectAddress *address);
extern List * GetPgDependTuplesForDependingObjects(Oid targetObjectClassId,
												   Oid targetObjectId);
extern List * GetDependingViews(Oid relationId);
extern Oid GetDependingView(Form_pg_depend pg_depend);
extern List * FilterObjectAddressListByPredicate(List *objectAddressList,
												 AddressPredicate predicate,
												 bool flushCaches);

/*
 * Tuplestore-backed counterparts of the List-based APIs above. They exist so a
 * very large distributed object set (e.g. millions of Citus shell tables) can be
 * scanned, filtered, and ordered without ever materializing the whole set as an
 * in-memory List -- Tuplestorestate transparently spills to a temp file once its
 * content exceeds work_mem. They are additive: none of the List-based callers
 * above are changed or removed by their existence.
 */
extern TupleDesc CreateObjectAddressTupleDesc(void);
extern TupleDesc CreateObjectDependencyEdgeTupleDesc(void);
extern TupleTableSlot * CreateObjectAddressTupleSlot(TupleDesc tupleDesc);
extern TupleTableSlot * CreateObjectDependencyEdgeTupleSlot(TupleDesc tupleDesc);
extern void PutObjectAddressToTuplestore(Tuplestorestate *tupleStore,
										 TupleDesc tupleDesc,
										 const ObjectAddress *address);
extern bool GetNextObjectAddressFromTuplestore(Tuplestorestate *tupleStore,
											   TupleTableSlot *slot,
											   ObjectAddress *addressOut);
extern void PutObjectDependencyEdgeToTuplestore(Tuplestorestate *tupleStore,
												TupleDesc tupleDesc,
												const ObjectDependencyEdge *edge);
extern bool GetNextObjectDependencyEdgeFromTuplestore(Tuplestorestate *tupleStore,
													  TupleTableSlot *slot,
													  ObjectDependencyEdge *edgeOut);

/*
 * The dense-id counterpart of the ObjectDependencyEdge tuplestore API above:
 * OrderObjectAddressTuplestoreInDependencyOrderWithEdges() resolves every edge's
 * two ObjectAddress endpoints to the dense int32 task id assigned to the
 * corresponding row of *orderedAddressStore (row N gets id N; see that
 * function's comment) and returns the edges in this smaller 2-column format, so
 * callers never need their own ObjectAddress -> task hash lookup to consume the
 * dependency graph.
 */
extern TupleDesc CreateObjectDependencyEdgeIdTupleDesc(void);
extern TupleTableSlot * CreateObjectDependencyEdgeIdTupleSlot(TupleDesc tupleDesc);
extern void PutObjectDependencyEdgeIdsToTuplestore(Tuplestorestate *tupleStore,
												   TupleDesc tupleDesc,
												   int32 prereqTaskId,
												   int32 dependentTaskId);
extern bool GetNextObjectDependencyEdgeIdsFromTuplestore(Tuplestorestate *tupleStore,
														 TupleTableSlot *slot,
														 int32 *prereqTaskId,
														 int32 *dependentTaskId);

extern void OrderObjectAddressTuplestoreInDependencyOrderWithEdges(
	Tuplestorestate *objectAddressStore, TupleDesc objectAddressTupleDesc,
	bool flushCaches, Tuplestorestate **orderedAddressStore,
	TupleDesc *orderedAddressTupleDesc, Tuplestorestate **edgeStore,
	TupleDesc *edgeTupleDesc);
extern void FilterObjectAddressTuplestoreByPredicate(Tuplestorestate *objectAddressStore,
													 TupleDesc objectAddressTupleDesc,
													 AddressPredicate predicate,
													 bool flushCaches,
													 Tuplestorestate **filteredStore,
													 TupleDesc *filteredTupleDesc);

#endif /* CITUS_DEPENDENCY_H */
