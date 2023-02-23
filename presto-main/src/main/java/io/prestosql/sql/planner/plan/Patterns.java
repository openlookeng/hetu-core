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

import io.prestosql.matching.Pattern;
import io.prestosql.matching.Property;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.ExceptNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.GroupIdNode;
import io.prestosql.spi.plan.IntersectNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.JoinOnAggregationNode;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.MarkDistinctNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.TopNNode;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.tree.Expression;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.matching.Pattern.typeOf;
import static io.prestosql.matching.Property.optionalProperty;
import static io.prestosql.matching.Property.property;

public class Patterns
{
    private Patterns() {}

    public static Pattern<AssignUniqueId> assignUniqueId()
    {
        return typeOf(AssignUniqueId.class);
    }

    public static Pattern<AggregationNode> aggregation()
    {
        return typeOf(AggregationNode.class);
    }

    public static Pattern<ApplyNode> applyNode()
    {
        return typeOf(ApplyNode.class);
    }

    public static Pattern<DeleteNode> delete()
    {
        return typeOf(DeleteNode.class);
    }

    public static Pattern<UpdateNode> update()
    {
        return typeOf(UpdateNode.class);
    }

    public static Pattern<TableExecuteNode> tableExecute()
    {
        return typeOf(TableExecuteNode.class);
    }

    public static Pattern<ExchangeNode> exchange()
    {
        return typeOf(ExchangeNode.class);
    }

    public static Pattern<ExplainAnalyzeNode> explainAnalyze()
    {
        return typeOf(ExplainAnalyzeNode.class);
    }

    public static Pattern<EnforceSingleRowNode> enforceSingleRow()
    {
        return typeOf(EnforceSingleRowNode.class);
    }

    public static Pattern<IndexJoinNode> indexJoin()
    {
        return typeOf(IndexJoinNode.class);
    }

    public static Pattern<FilterNode> filter()
    {
        return typeOf(FilterNode.class);
    }

    public static Pattern<IndexSourceNode> indexSource()
    {
        return typeOf(IndexSourceNode.class);
    }

    public static Pattern<JoinNode> join()
    {
        return typeOf(JoinNode.class);
    }

    public static Pattern<JoinOnAggregationNode> joinOnAggregation()
    {
        return typeOf(JoinOnAggregationNode.class);
    }

    public static Pattern<SpatialJoinNode> spatialJoin()
    {
        return typeOf(SpatialJoinNode.class);
    }

    public static Pattern<LateralJoinNode> lateralJoin()
    {
        return typeOf(LateralJoinNode.class);
    }

    public static Pattern<OffsetNode> offset()
    {
        return typeOf(OffsetNode.class);
    }

    public static Pattern<LimitNode> limit()
    {
        return typeOf(LimitNode.class);
    }

    public static Pattern<MarkDistinctNode> markDistinct()
    {
        return typeOf(MarkDistinctNode.class);
    }

    public static Pattern<OutputNode> output()
    {
        return typeOf(OutputNode.class);
    }

    public static Pattern<ProjectNode> project()
    {
        return typeOf(ProjectNode.class);
    }

    public static Pattern<SampleNode> sample()
    {
        return typeOf(SampleNode.class);
    }

    public static Pattern<SemiJoinNode> semiJoin()
    {
        return typeOf(SemiJoinNode.class);
    }

    public static Pattern<SortNode> sort()
    {
        return typeOf(SortNode.class);
    }

    public static Pattern<TableFinishNode> tableFinish()
    {
        return typeOf(TableFinishNode.class);
    }

    public static Pattern<TableScanNode> tableScan()
    {
        return typeOf(TableScanNode.class);
    }

    public static Pattern<TableWriterNode> tableWriterNode()
    {
        return typeOf(TableWriterNode.class);
    }

    public static Pattern<VacuumTableNode> vacuumTableNode()
    {
        return typeOf(VacuumTableNode.class);
    }

    public static Pattern<TableDeleteNode> tableDeleteNode()
    {
        return typeOf(TableDeleteNode.class);
    }

    public static Pattern<TopNNode> topN()
    {
        return typeOf(TopNNode.class);
    }

    public static Pattern<UnionNode> union()
    {
        return typeOf(UnionNode.class);
    }

    public static Pattern<ValuesNode> values()
    {
        return typeOf(ValuesNode.class);
    }

    public static Pattern<WindowNode> window()
    {
        return typeOf(WindowNode.class);
    }

    public static Pattern<RowNumberNode> rowNumber()
    {
        return typeOf(RowNumberNode.class);
    }

    public static Pattern<TopNRankingNumberNode> topNRankingNumber()
    {
        return typeOf(TopNRankingNumberNode.class);
    }

    public static Pattern<DistinctLimitNode> distinctLimit()
    {
        return typeOf(DistinctLimitNode.class);
    }

    public static Pattern<IntersectNode> intersect()
    {
        return typeOf(IntersectNode.class);
    }

    public static Pattern<ExceptNode> except()
    {
        return typeOf(ExceptNode.class);
    }

    public static Pattern<GroupIdNode> groupId()
    {
        return typeOf(GroupIdNode.class);
    }

    public static Pattern<CTEScanNode> cteScan()
    {
        return typeOf(CTEScanNode.class);
    }

    public static Pattern<CacheTableFinishNode> cacheTableFinishNodePattern()
    {
        return typeOf(CacheTableFinishNode.class);
    }

    public static Pattern<CacheTableWriterNode> cacheTableWriterNodePattern()
    {
        return typeOf(CacheTableWriterNode.class);
    }

    public static Property<PlanNode, Lookup, PlanNode> source()
    {
        return optionalProperty(
                "source",
                (node, lookup) -> {
                    if (node.getSources().size() == 1) {
                        PlanNode source = getOnlyElement(node.getSources());
                        return Optional.of(lookup.resolve(source));
                    }
                    return Optional.empty();
                });
    }

    public static Property<PlanNode, Lookup, List<PlanNode>> sources()
    {
        return property(
                "sources",
                (PlanNode node, Lookup lookup) -> node.getSources().stream()
                        .map(source -> lookup.resolve(source))
                        .collect(toImmutableList()));
    }

    public static class TableWriter
    {
        public static Property<TableWriterNode, Lookup, TableWriterNode.WriterTarget> target()
        {
            return property("target", TableWriterNode::getTarget);
        }
    }

    /**
     * Find an optional source PlanNode of a given type. If the optional
     * source node is found, it will be passed to the next pattern otherwise the parent node itself will be passed
     * to the next pattern.
     * <p>
     * Use this method with the {@link io.hetu.core.matching.pattern.OptionalCapturePattern} to capture the optional node.
     *
     * @param expectedClass the class of the optional node
     * @param <T> the expected node type
     * @return a Property to extract the optional node
     */
    public static <T extends PlanNode> Property<PlanNode, Lookup, PlanNode> optionalSource(Class<T> expectedClass)
    {
        return optionalProperty(
                "optionalSource",
                (node, lookup) -> {
                    if (node.getSources().size() == 1) {
                        PlanNode source = lookup.resolve(getOnlyElement(node.getSources()));
                        if (source.getClass().equals(expectedClass)) {
                            return Optional.of(source);
                        }
                    }
                    return Optional.of(node);
                });
    }

    /**
     * Match any PlanNode objects.
     *
     * @return TypeOfPattern of PlanNode
     */
    public static Pattern<PlanNode> anyPlan()
    {
        return typeOf(PlanNode.class);
    }

    public static class Aggregation
    {
        public static Property<AggregationNode, Lookup, List<Symbol>> groupingColumns()
        {
            return property("groupingKeys", AggregationNode::getGroupingKeys);
        }

        public static Property<AggregationNode, Lookup, AggregationNode.Step> step()
        {
            return property("step", AggregationNode::getStep);
        }
    }

    public static class Apply
    {
        public static Property<ApplyNode, Lookup, List<Symbol>> correlation()
        {
            return property("correlation", ApplyNode::getCorrelation);
        }

        public static Property<ApplyNode, Lookup, PlanNode> subQuery()
        {
            return property("subquery",
                    (node, lookup) -> lookup.resolve(node.getSubquery()));
        }

        public static Property<ApplyNode, Lookup, PlanNode> input()
        {
            return property("input",
                    (node, lookup) -> lookup.resolve(node.getInput()));
        }
    }

    public static class Exchange
    {
        public static Property<ExchangeNode, Lookup, ExchangeNode.Scope> scope()
        {
            return property("scope", ExchangeNode::getScope);
        }
    }

    public static class Join
    {
        public static Property<JoinNode, Lookup, JoinNode.Type> type()
        {
            return property("type", JoinNode::getType);
        }

        public static Property<JoinNode, Lookup, PlanNode> left()
        {
            return property("left", (JoinNode joinNode, Lookup lookup) -> lookup.resolve(joinNode.getLeft()));
        }

        public static Property<JoinNode, Lookup, PlanNode> right()
        {
            return property("right", (JoinNode joinNode, Lookup lookup) -> lookup.resolve(joinNode.getRight()));
        }
    }

    public static class LateralJoin
    {
        public static Property<LateralJoinNode, Lookup, List<Symbol>> correlation()
        {
            return property("correlation", LateralJoinNode::getCorrelation);
        }

        public static Property<LateralJoinNode, Lookup, PlanNode> subquery()
        {
            return property("subquery", LateralJoinNode::getSubquery);
        }

        public static Property<LateralJoinNode, Lookup, Expression> filter()
        {
            return property("filter", LateralJoinNode::getFilter);
        }
    }

    public static class Limit
    {
        public static Property<LimitNode, Lookup, Long> count()
        {
            return property("count", LimitNode::getCount);
        }
    }

    public static class Sample
    {
        public static Property<SampleNode, Lookup, Double> sampleRatio()
        {
            return property("sampleRatio", SampleNode::getSampleRatio);
        }

        public static Property<SampleNode, Lookup, SampleNode.Type> sampleType()
        {
            return property("sampleType", SampleNode::getSampleType);
        }
    }

    public static class TopN
    {
        public static Property<TopNNode, Lookup, TopNNode.Step> step()
        {
            return property("step", TopNNode::getStep);
        }

        public static Property<TopNNode, Lookup, Long> count()
        {
            return property("count", TopNNode::getCount);
        }
    }

    public static class Values
    {
        public static Property<ValuesNode, Lookup, List<List<RowExpression>>> rows()
        {
            return property("rows", ValuesNode::getRows);
        }
    }

    public static class SemiJoin
    {
        public static Property<SemiJoinNode, Lookup, PlanNode> getSource()
        {
            return property(
                    "source",
                    (SemiJoinNode semiJoin, Lookup lookup) -> lookup.resolve(semiJoin.getSource()));
        }

        public static Property<SemiJoinNode, Lookup, PlanNode> getFilteringSource()
        {
            return property(
                    "filteringSource",
                    (SemiJoinNode semiJoin, Lookup lookup) -> lookup.resolve(semiJoin.getFilteringSource()));
        }
    }
}
