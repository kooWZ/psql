/*-------------------------------------------------------------------------
 *
 * nodeBlockNestloop.h
 *
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeBlockNestloop.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEBLOCKNESTLOOP_H
#define NODEBLOCKNESTLOOP_H

#include "nodes/execnodes.h"

extern NestLoopState *ExecInitBlockNestLoop(NestLoop *node, EState *estate, int eflags);
extern TupleTableSlot *ExecBlockNestLoop(NestLoopState *node);
extern void ExecEndBlockNestLoop(NestLoopState *node);
extern void ExecReScanBlockNestLoop(NestLoopState *node);

#endif   /* NODENESTLOOP_H */
