/*-------------------------------------------------------------------------
 *
 * 魔改后的nodeNestloop.c
 *	  support block-nest-loop joins
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeNestloop.c
 *
 *-------------------------------------------------------------------------
 */
/*
 *	 INTERFACE ROUTINES
 *		ExecNestLoop	 - BNLJ的主程序
 *		ExecInitNestLoop - 初始化
 *		ExecEndNestLoop  - 析构
 */

#include "postgres.h"

#include "executor/execdebug.h"
#include "executor/nodeNestloop.h"
#include "utils/memutils.h"
#include "miscadmin.h"

int BNLJ_block_size = 1;
/* ----------------------------------------------------------------
 *		ExecNestLoop(node)
 *
 * old comments
 *		Returns the tuple joined from inner and outer tuples which
 *		satisfies the qualification clause.
 *
 *		It scans the inner relation to join with current outer tuple.
 *
 *		If none is found, next tuple from the outer relation is retrieved
 *		and the inner relation is scanned from the beginning again to join
 *		with the outer tuple.
 *
 *		NULL is returned if all the remaining outer tuples are tried and
 *		all fail to join with the inner tuples.
 *
 *		NULL is also returned if there is no tuple from inner relation.
 *
 *		Conditions:
 *		  -- outerTuple contains current tuple from outer relation and
 *			 the right son(inner relation) maintains "cursor" at the tuple
 *			 returned previously.
 *				This is achieved by maintaining a scan position on the outer
 *				relation.
 *
 *		Initial States:
 *		  -- the outer child and the inner child
 *			   are prepared to return the first tuple.
 * ----------------------------------------------------------------
 */



/*
 * 从outer表中获取一块Block
 */
static Tuplestorestate*
fetchNextBlock(NestLoopState *node)
{
    /*
     * 初始化TupleStoreState
     */
    Tuplestorestate *newBlock;
    newBlock = tuplestore_begin_heap(false, false, work_mem);
    tuplestore_set_eflags(newBlock, EXEC_FLAG_REWIND);
    tuplestore_alloc_read_pointer(newBlock, EXEC_FLAG_REWIND);
    tuplestore_select_read_pointer(newBlock, 0);

    list_free(node->nl_ExcludedOuter);
    node->nl_ExcludedOuter = NULL;
    list_free(node->nl_MatchedOuterIndexes);
    node->nl_MatchedOuterIndexes = NULL;

    TupleTableSlot* newSlot;
    int fetched = 0;
    for(;;)
    {
        newSlot = ExecProcNode(outerPlanState(node));
        if (TupIsNull(newSlot))
        {
            node->nl_reachedBlockEnd = true;
            break;
        }
        tuplestore_puttupleslot(newBlock, newSlot);
        fetched++;
        if (fetched == BNLJ_block_size)
            break;
    }

    /*
     * 把读指针放回开头位置
     */
    tuplestore_rescan(newBlock);
    return newBlock;
}

/*
 * 从当前的TupleStoreSlot中读一个Slot
 */
static TupleTableSlot*
fetchNextSlot(NestLoopState *node)
{
    Tuplestorestate *currentBlock;

    currentBlock = node->block;

    /*
     * 初始化Slot，让他拥有合适的TupleDesc(即Slot属性)
     */
    TupleTableSlot *newSlot;
    newSlot = MakeTupleTableSlot();
    ExecSetSlotDescriptor(newSlot,ExecGetResultType(outerPlanState(node)));

    /*
     * 以不复制的方式获得一个Slot(直接传指针)
     */
    do
    {
        tuplestore_gettupleslot(currentBlock, true, false, newSlot);
    }
    while(list_member_int(node->nl_ExcludedOuter, get_readptr_index(currentBlock)-1));

    return newSlot;
}

TupleTableSlot *
ExecNestLoop(NestLoopState *node)
{
    NestLoop   *nl;
    PlanState  *innerPlan;
    PlanState  *outerPlan;
    TupleTableSlot *outerTupleSlot;
    TupleTableSlot *innerTupleSlot;
    List	   *joinqual;
    List	   *otherqual;
    ExprContext *econtext;
    ListCell   *lc;
    Tuplestorestate *block; //added

    /*
     * get information from the node
     */
    ENL1_printf("getting info from node");

    nl = (NestLoop *) node->js.ps.plan;
    joinqual = node->js.joinqual;
    otherqual = node->js.ps.qual;
    outerPlan = outerPlanState(node);
    innerPlan = innerPlanState(node);
    econtext = node->js.ps.ps_ExprContext;
    block = node->block; //added

    /*
     * Check to see if we're still projecting out tuples from a previous join
     * tuple (because there is a function-returning-set in the projection
     * expressions).  If so, try to project another one.
     */
    if (node->js.ps.ps_TupFromTlist)
    {
        TupleTableSlot *result;
        ExprDoneCond isDone;

        result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);
        if (isDone == ExprMultipleResult)
            return result;
        /* Done with that source tuple... */
        node->js.ps.ps_TupFromTlist = false;
    }

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.  Note this can't happen
     * until we're done projecting out tuples from a join tuple.
     */
    ResetExprContext(econtext);

    /*
     * 如果第一次进行loop，先fetch一个block
     */
    if (block == NULL)
    {
        block = fetchNextBlock(node);
        node->block = block;
        node->nl_NeedNewBlock = false;
    }

    ENL1_printf("entering main loop");

    for (;;)
    {
        /*
         * 先获取内表
         * 如果需要新的Inner,就调用子节点获取一个
         * 如果不需要，就用econtext中保存的上次的
         */
        if (node->nl_NeedNewInner)
        {
            ENL1_printf("getting new inner tuple");

            innerTupleSlot = ExecProcNode(innerPlan);
            econtext->ecxt_innertuple = innerTupleSlot;

            node->nl_NeedNewInner = false;
            tuplestore_rescan(block);
        } else {
            innerTupleSlot = econtext->ecxt_innertuple;
        }

        /*
         * 如果inner为空，说明对于这个block，内表已经扫描过一遍了
         * 我们需要一个新的block
         */
        if (TupIsNull(innerTupleSlot) || innerTupleSlot == node->nl_NullInnerTupleSlot)
        {
            ENL1_printf("no inner tuple, need new block");

            if (node->js.jointype == JOIN_LEFT || node->js.jointype == JOIN_ANTI)
            {
                tuplestore_select_read_pointer(block, 1);
                int len = get_tuplestore_len(block);

                int index = get_readptr_index(block)-1;
                for (;index+1<len;)
                {
                    econtext->ecxt_outertuple = fetchNextSlot(node);
                    index = get_readptr_index(block)-1;
                    if (!list_member_int(node->nl_MatchedOuterIndexes, index))
                    {
                        /*
                         * We are doing an outer join and there were no join matches
                         * for this outer tuple.  Generate a fake join tuple with
                         * nulls for the inner tuple, and return it if it passes the
                         * non-join quals.
                         */
                        econtext->ecxt_innertuple = node->nl_NullInnerTupleSlot;

                        ENL1_printf("testing qualification for outer-join tuple");

                        if (otherqual == NIL || ExecQual(otherqual, econtext, false))
                        {
                            /*
                             * qualification was satisfied so we project and return
                             * the slot containing the result tuple using
                             * ExecProject().
                             */
                            TupleTableSlot *result;
                            ExprDoneCond isDone;

                            ENL1_printf("qualification succeeded, projecting tuple");

                            result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);

                            if (isDone != ExprEndResult)
                            {
                                node->js.ps.ps_TupFromTlist =
                                        (isDone == ExprMultipleResult);
                                return result;
                            }
                        }
                    }
                }
                tuplestore_rescan(block);
                tuplestore_select_read_pointer(block, 0);
            }

            if (node->nl_reachedBlockEnd)
                return NULL;

            node->nl_NeedNewInner = true;
            node->nl_NeedNewOuter = true;
            node->nl_NeedNewBlock = true;


            /*
             * inner从头开始
             */
            ENL1_printf("rescanning inner plan");
            ExecReScan(innerPlan);

            /*
             * Otherwise just return to top of loop for a new tuple.
             */
            continue;
        }
        /*
         * 两种情况：
         * 1. 外表数正好是block_size的倍数，那么fetch新表时会返回一个长度为0的表，下面的get_tuplestore_len(block) == 0可以捕捉到
         * 2. 不是倍数，那么最后一个block不满，比较完最后一个block之后，不应该fetch下一个block
         */
        /*
         * 更新外表Block
         */

        if (node->nl_NeedNewBlock)
        {
            /*
             * 释放旧的
             */
            tuplestore_end(block);

            block = fetchNextBlock(node);

            /*
             * 如果一整个Block都空，那么说明外表已经循环了一遍了，BLNJ结束
             */
            if (get_tuplestore_len(block) == 0)
                return NULL;

            node->block = block;
            node->nl_NeedNewBlock = false;
        }

        /*
         * 开始block内循环
         */
        for(;;)
        {
            ENL1_printf("getting new outer tuple");
            outerTupleSlot = fetchNextSlot(node);

            /*
             * 如果outer为空
             * 说明当前的inner已经扫描过这个block了，需要下一个inner
             */
            if (TupIsNull(outerTupleSlot))
            {
                node->nl_NeedNewInner = true;
                break;
            }

            ENL1_printf("saving new outer tuple information");
            econtext->ecxt_outertuple = outerTupleSlot;

            node->nl_NeedNewOuter = false;
            node->nl_MatchedOuter = false;

            /*
             * fetch the values of any outer Vars that must be passed to the
             * inner scan, and store them in the appropriate PARAM_EXEC slots.
             */
            foreach(lc, nl->nestParams)
            {
                NestLoopParam *nlp = (NestLoopParam *) lfirst(lc);
                int			paramno = nlp->paramno;
                ParamExecData *prm;

                prm = &(econtext->ecxt_param_exec_vals[paramno]);
                /* Param value should be an OUTER var */
                Assert(IsA(nlp->paramval, Var));
                Assert(nlp->paramval->varno == OUTER);
                Assert(nlp->paramval->varattno > 0);
                prm->value = slot_getattr(outerTupleSlot,
                                          nlp->paramval->varattno,
                                          &(prm->isnull));
                /* Flag parameter value as changed */
                innerPlan->chgParam = bms_add_member(innerPlan->chgParam,
                                                     paramno);
            }

            /*
             * 检查这一组inner和outer是否符合join条件
             *
             * Only the joinquals determine MatchedOuter status, but all quals
             * must pass to actually return the tuple.
             */

            ENL1_printf("testing qualification");

            if (ExecQual(joinqual, econtext, false))
            {
                node->nl_MatchedOuter = true;

                /* In an antijoin, we never return a matched tuple */
                if (node->js.jointype == JOIN_ANTI) //fixed, TODO test
                {
                    node->nl_ExcludedOuter = lappend_int(node->nl_ExcludedOuter, get_readptr_index(block)-1);
                    node->nl_NeedNewOuter = true;
                    /* return to top of loop */
                    continue;
                }

                /*
                 * 如果是左连接，我们需要储存当前block中已经匹配上的index
                 * 代替了node->nl_MatchedOuter的作用
                 */
                if (node->js.jointype == JOIN_LEFT)
                    node->nl_MatchedOuterIndexes = lappend_int(node->nl_MatchedOuterIndexes, get_readptr_index(block)-1);

                /*
                 * In a semijoin, we'll consider returning the first match, but
                 * after that we're done with this outer tuple.
                 */
                if (node->js.jointype == JOIN_SEMI) //TODO test
                {
                    node->nl_ExcludedOuter = lappend_int(node->nl_ExcludedOuter, get_readptr_index(block)-1);
                    node->nl_NeedNewOuter = true;
                }

                if (otherqual == NIL || ExecQual(otherqual, econtext, false))
                {
                    /*
                     * qualification was satisfied so we project and return the
                     * slot containing the result tuple using ExecProject().
                     */
                    TupleTableSlot *result;
                    ExprDoneCond isDone;

                    ENL1_printf("qualification succeeded, projecting tuple");

                    result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);

                    if (isDone != ExprEndResult)
                    {
                        node->js.ps.ps_TupFromTlist =
                                (isDone == ExprMultipleResult);
                        return result;
                    }
                }
            }

            /*
             * Tuple fails qual, so free per-tuple memory and try again.
             */
            ResetExprContext(econtext); //不会清空econtext->ecxt_innertuple

            ENL1_printf("qualification failed, looping");
        }
    }
}

/* ----------------------------------------------------------------
 *		ExecInitNestLoop 初始化
 * ----------------------------------------------------------------
 */
NestLoopState *
ExecInitNestLoop(NestLoop *node, EState *estate, int eflags)
{
    NestLoopState *nlstate;

    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    NL1_printf("ExecInitBlockNestLoop: %s\n",
               "initializing node");

    /*
     * create state structure
     */
    nlstate = makeNode(NestLoopState);
    nlstate->js.ps.plan = (Plan *) node;
    nlstate->js.ps.state = estate;
    nlstate->block = NULL; //added
    nlstate->nl_ExcludedOuter = NULL;
    nlstate->nl_MatchedOuterIndexes = NULL;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &nlstate->js.ps);

    /*
     * initialize child expressions
     */
    nlstate->js.ps.targetlist = (List *)
            ExecInitExpr((Expr *) node->join.plan.targetlist,
                         (PlanState *) nlstate);
    nlstate->js.ps.qual = (List *)
            ExecInitExpr((Expr *) node->join.plan.qual,
                         (PlanState *) nlstate);
    nlstate->js.jointype = node->join.jointype;
    nlstate->js.joinqual = (List *)
            ExecInitExpr((Expr *) node->join.joinqual,
                         (PlanState *) nlstate);

    /*
     * initialize child nodes
     *
     * If we have no parameters to pass into the inner rel from the outer,
     * tell the inner child that cheap rescans would be good.  If we do have
     * such parameters, then there is no point in REWIND support at all in the
     * inner child, because it will always be rescanned with fresh parameter
     * values.
     */
    outerPlanState(nlstate) = ExecInitNode(outerPlan(node), estate, eflags);
    if (node->nestParams == NIL)
        eflags |= EXEC_FLAG_REWIND;
    else
        eflags &= ~EXEC_FLAG_REWIND;
    innerPlanState(nlstate) = ExecInitNode(innerPlan(node), estate, eflags);

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &nlstate->js.ps);

    switch (node->join.jointype)
    {
        case JOIN_INNER:
        case JOIN_SEMI:
            break;
        case JOIN_LEFT:
        case JOIN_ANTI:
            nlstate->nl_NullInnerTupleSlot =
                    ExecInitNullTupleSlot(estate,
                                          ExecGetResultType(innerPlanState(nlstate)));
            break;
        default:
            elog(ERROR, "unrecognized join type: %d",
                 (int) node->join.jointype);
    }

    /*
     * initialize tuple type and projection info
     */
    ExecAssignResultTypeFromTL(&nlstate->js.ps);
    ExecAssignProjectionInfo(&nlstate->js.ps, NULL);

    /*
     * finally, wipe the current outer tuple clean.
     */
    nlstate->js.ps.ps_TupFromTlist = false;
    nlstate->nl_NeedNewOuter = true;
    nlstate->nl_MatchedOuter = false;
    nlstate->nl_NeedNewInner= true; //added
    nlstate->nl_NeedNewBlock = true; //added
    nlstate->nl_reachedBlockEnd = false;

    NL1_printf("ExecInitNestLoop: %s\n",
               "node initialized");

    return nlstate;
}

/* ----------------------------------------------------------------
 *		ExecEndNestLoop
 *
 *		closes down scans and frees allocated storage
 * ----------------------------------------------------------------
 */
void
ExecEndNestLoop(NestLoopState *node)
{
    NL1_printf("ExecEndNestLoop: %s\n",
               "ending node processing");

    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->js.ps);

    /*
     * 释放TupleStore
     */
    if (node->block)
        tuplestore_end(node->block);
    node->block = NULL;
    list_free(node->nl_ExcludedOuter);
    list_free(node->nl_MatchedOuterIndexes);

    /*
     * clean out the tuple table
     */
    ExecClearTuple(node->js.ps.ps_ResultTupleSlot);

    /*
     * close down subplans
     */
    ExecEndNode(outerPlanState(node));
    ExecEndNode(innerPlanState(node));

    NL1_printf("ExecEndNestLoop: %s\n",
               "node processing ended");
}

/* ----------------------------------------------------------------
 *		ExecReScanNestLoop
 * ----------------------------------------------------------------
 */
void
ExecReScanNestLoop(NestLoopState *node)
{
    PlanState  *outerPlan = outerPlanState(node);

    /*
     * If outerPlan->chgParam is not null then plan will be automatically
     * re-scanned by first ExecProcNode.
     */
    if (outerPlan->chgParam == NULL)
        ExecReScan(outerPlan);

    /*
     * innerPlan is re-scanned for each new outer tuple and MUST NOT be
     * re-scanned from here or you'll get troubles from inner index scans when
     * outer Vars are used as run-time keys...
     */

    node->js.ps.ps_TupFromTlist = false;
    node->nl_NeedNewOuter = true;
    node->nl_MatchedOuter = false;
    node->nl_NeedNewInner = true;
    node->nl_reachedBlockEnd = false;
    if (node->block)
        tuplestore_end(node->block);
    node->block = NULL;
    node->nl_NeedNewBlock = true;

    list_free(node->nl_ExcludedOuter);
    list_free(node->nl_MatchedOuterIndexes);
    node->nl_ExcludedOuter = NULL;
    node->nl_MatchedOuterIndexes = NULL;
}