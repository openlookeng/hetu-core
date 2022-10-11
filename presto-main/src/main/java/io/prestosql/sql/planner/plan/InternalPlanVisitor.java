/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.sql.planner.plan;

import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanVisitor;

public abstract class InternalPlanVisitor<R, C>
        extends PlanVisitor<R, C>
{
    public R visitRemoteSource(RemoteSourceNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitOutput(OutputNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitOffset(OffsetNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitCreateIndex(CreateIndexNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitUpdateIndex(UpdateIndexNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitDistinctLimit(DistinctLimitNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitSample(SampleNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitExplainAnalyze(ExplainAnalyzeNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitIndexSource(IndexSourceNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitSemiJoin(SemiJoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitSpatialJoin(SpatialJoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitIndexJoin(IndexJoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitSort(SortNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableWriter(TableWriterNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitDelete(DeleteNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitUpdate(UpdateNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableUpdate(TableUpdateNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitVacuumTable(VacuumTableNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableDelete(TableDeleteNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableExecute(TableExecuteNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableFinish(TableFinishNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitCubeFinish(CubeFinishNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitStatisticsWriterNode(StatisticsWriterNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitUnnest(UnnestNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitRowNumber(RowNumberNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTopNRankingNumber(TopNRankingNumberNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitExchange(ExchangeNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitEnforceSingleRow(EnforceSingleRowNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitApply(ApplyNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitAssignUniqueId(AssignUniqueId node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitLateralJoin(LateralJoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    @Override
    public R visitCTEScan(CTEScanNode node, C context)
    {
        return visitPlan(node, context);
    }

    @Override
    public R visitTableExecute(PlanNode node, C context)
    {
        return visitPlan(node, context);
    }
}
