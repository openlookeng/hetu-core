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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.sql.planner.optimizations.PlanNodeSearcher;
import io.prestosql.sql.planner.plan.DynamicFilterSourceNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.RemoteSourceNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.util.MorePredicates;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.prestosql.util.MorePredicates.isInstanceOfAny;

/**
 * This class must be public as it is accessed via join compiler reflection.
 */
public final class JoinUtils
{
    private JoinUtils() {}

    public static List<Page> channelsToPages(List<List<Block>> channels)
    {
        if (channels.isEmpty()) {
            return ImmutableList.of();
        }

        int pagesCount = channels.get(0).size();
        ImmutableList.Builder<Page> pagesBuilder = ImmutableList.builderWithExpectedSize(pagesCount);
        for (int pageIndex = 0; pageIndex < pagesCount; ++pageIndex) {
            Block[] blocks = new Block[channels.size()];
            for (int channelIndex = 0; channelIndex < blocks.length; ++channelIndex) {
                blocks[channelIndex] = channels.get(channelIndex).get(pageIndex);
            }
            pagesBuilder.add(new Page(blocks));
        }
        return pagesBuilder.build();
    }

    public static boolean isBuildSideReplicated(PlanNode node)
    {
        checkArgument(isInstanceOfAny(JoinNode.class, SemiJoinNode.class).test(node));
        if (node instanceof JoinNode) {
            return PlanNodeSearcher.searchFrom(((JoinNode) node).getRight())
                    .recurseOnlyWhen(
                            MorePredicates.<PlanNode>isInstanceOfAny(ProjectNode.class)
                                    .or(JoinUtils::isLocalRepartitionExchange)
                                    .or(JoinUtils::isLocalGatherExchange))  // used in cross join case
                    .where(joinNode -> isRemoteReplicatedExchange(joinNode) || isRemoteReplicatedSourceNode(joinNode))
                    .matches();
        }
        return PlanNodeSearcher.searchFrom(((SemiJoinNode) node).getFilteringSource())
                .recurseOnlyWhen(
                        MorePredicates.<PlanNode>isInstanceOfAny(ProjectNode.class)
                                .or(JoinUtils::isLocalGatherExchange))
                .where(joinNode -> isRemoteReplicatedExchange(joinNode) || isRemoteReplicatedSourceNode(joinNode))
                .matches();
    }

    private static boolean isRemoteReplicatedExchange(PlanNode node)
    {
        if (!(node instanceof ExchangeNode)) {
            return false;
        }

        ExchangeNode exchangeNode = (ExchangeNode) node;
        return exchangeNode.getScope() == REMOTE && exchangeNode.getType() == REPLICATE;
    }

    private static boolean isRemoteReplicatedSourceNode(PlanNode node)
    {
        if (!(node instanceof RemoteSourceNode)) {
            return false;
        }

        RemoteSourceNode remoteSourceNode = (RemoteSourceNode) node;
        return remoteSourceNode.getExchangeType() == REPLICATE;
    }

    private static boolean isLocalRepartitionExchange(PlanNode node)
    {
        if (!(node instanceof ExchangeNode)) {
            return false;
        }

        ExchangeNode exchangeNode = (ExchangeNode) node;
        return exchangeNode.getScope() == LOCAL && exchangeNode.getType() == REPARTITION;
    }

    private static boolean isLocalGatherExchange(PlanNode node)
    {
        if (!(node instanceof ExchangeNode)) {
            return false;
        }

        ExchangeNode exchangeNode = (ExchangeNode) node;
        return exchangeNode.getScope() == LOCAL && exchangeNode.getType() == GATHER;
    }

    public static Map<String, Symbol> getJoinDynamicFilters(JoinNode joinNode)
    {
        List<DynamicFilterSourceNode> dynamicFilterSourceNodes = PlanNodeSearcher.searchFrom(joinNode.getRight())
                .where(DynamicFilterSourceNode.class::isInstance)
                .recurseOnlyWhen(node -> node instanceof ExchangeNode || node instanceof ProjectNode)
                .findAll();
        Map<String, Symbol> dynamicFilters = joinNode.getDynamicFilters();
        // This method maybe called before AddJoinDynamicFilterSource has rewritten join dynamic filters to DynamicFilterSourceNode
        if (dynamicFilterSourceNodes.isEmpty()) {
            return dynamicFilters;
        }
        verify(
                dynamicFilters.isEmpty(),
                "Dynamic filters %s present in a join with a DynamicFilterSourceNode on it's build side", dynamicFilters);
        verify(dynamicFilterSourceNodes.size() == 1, "Expected only 1 dynamic filter source node");
        return dynamicFilterSourceNodes.get(0).getDynamicFilters();
    }

    public static Optional<String> getSemiJoinDynamicFilterId(SemiJoinNode semiJoinNode)
    {
        List<DynamicFilterSourceNode> dynamicFilterSourceNodes = PlanNodeSearcher.searchFrom(semiJoinNode.getFilteringSource())
                .where(DynamicFilterSourceNode.class::isInstance)
                .recurseOnlyWhen(node -> node instanceof ExchangeNode || node instanceof ProjectNode)
                .findAll();
        Optional<String> dynamicFilterId = semiJoinNode.getDynamicFilterId();
        // This method maybe called before AddSemiJoinDynamicFilterSource has rewritten join dynamic filters to DynamicFilterSourceNode
        if (dynamicFilterSourceNodes.isEmpty()) {
            return dynamicFilterId;
        }
        verify(
                !dynamicFilterId.isPresent(),
                "Dynamic filter %s present in a semi join with a DynamicFilterSourceNode on it's filtering source side", dynamicFilterId);
        verify(dynamicFilterSourceNodes.size() == 1, "Expected only 1 dynamic filter source node");
        return Optional.of(getOnlyElement(dynamicFilterSourceNodes.get(0).getDynamicFilters().keySet()));
    }
}
