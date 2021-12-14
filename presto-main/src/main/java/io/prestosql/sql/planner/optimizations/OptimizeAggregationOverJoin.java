/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.prestosql.sql.planner.optimizations;

import io.airlift.log.Logger;
import io.hetu.core.spi.cube.io.CubeMetaStore;
import io.prestosql.Session;
import io.prestosql.cube.CubeManager;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.sql.planner.iterative.Plans;
import io.prestosql.sql.planner.iterative.Rule;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.prestosql.SystemSessionProperties.isEnableStarTreeIndex;
import static io.prestosql.cube.CubeManager.STAR_TREE;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static io.prestosql.sql.planner.plan.Patterns.anyPlan;
import static io.prestosql.sql.planner.plan.Patterns.join;
import static io.prestosql.sql.planner.plan.Patterns.optionalSource;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

/*
 * Captures AggregationNode over JoinNode and rewrites the TableScan
 * using Cube table if matches.
 *
 * <p>
 * Transforms:
 * <pre>
 * AggregationNode
 * |- ProjectNode[Optional]
 * .  |- ProjectNode[Optional]
 * .  .  |- FilterNode[Optional]
 * .  .  .  |- JoinNode
 * .  .  .  .  [More Joins]
 * .  .  .  .  .
 * .  .  .  .  |- ProjectNode[Optional] - Left
 * .  .  .  .  .  |- TableScanNode [Fact Table]
 * .  .  .  .  |- ProjectNode[Optional] - Right
 * .  .  .  .  .  |- TableScanNode
 * </pre>
 * Into:
 * |- ProjectNode[Optional]
 * .  |- ProjectNode[Optional]
 * .  .  |- FilterNode[Optional]
 * .  .  .  |- JoinNode
 * .  .  .  .  [More Joins]
 * .  .  .  .  .
 * .  .  .  .  |- ProjectNode[Optional] - Left
 * .  .  .  .  .  |- TableScanNode [Cube]
 * .  .  .  .  |- ProjectNode[Optional] - Right
 * .  .  .  .  .  |- TableScanNode
 * </pre>
 */
public class OptimizeAggregationOverJoin
        implements Rule<AggregationNode>
{
    private static final Logger LOGGER = Logger.get(OptimizeAggregationOverJoin.class);

    private static final Capture<JoinNode> JOIN_NODE = newCapture();

    private static final Capture<Optional<PlanNode>> OPTIONAL_PRE_PROJECT_ONE = newCapture();

    private static final Capture<Optional<PlanNode>> OPTIONAL_PRE_PROJECT_TWO = newCapture();

    private static final Capture<Optional<PlanNode>> OPTIONAL_FILTER = newCapture();

    private static final Pattern<AggregationNode> PATTERN = joinSubTree();

    private final CubeManager cubeManager;

    private volatile CubeMetaStore cubeMetaStore;

    private final Metadata metadata;

    public OptimizeAggregationOverJoin(CubeManager cubeManager, Metadata metadata)
    {
        this.cubeManager = requireNonNull(cubeManager, "cubeManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    private static Pattern<AggregationNode> joinSubTree()
    {
        return aggregation().matching(CubeOptimizerUtil::isSupportedAggregation)
                .with(optionalSource(ProjectNode.class).matching(anyPlan().capturedAsIf(node -> node instanceof ProjectNode, OPTIONAL_PRE_PROJECT_ONE)
                        .with(optionalSource(ProjectNode.class).matching(anyPlan().capturedAsIf(node -> node instanceof ProjectNode, OPTIONAL_PRE_PROJECT_TWO)
                                .with(optionalSource(FilterNode.class).matching(anyPlan().capturedAsIf(x -> x instanceof FilterNode, OPTIONAL_FILTER)
                                        .with(source().matching(join().capturedAs(JOIN_NODE)))))))));
    }

    @Override
    public boolean isEnabled(Session session)
    {
        if (isEnableStarTreeIndex(session) && this.cubeManager.getCubeProvider(STAR_TREE).isPresent()) {
            if (this.cubeMetaStore == null) {
                // Creating CubeMetaStore in the constructor is too early. By that time, plugins are not loaded
                // That's why, the cubeMetaStore is lazy loaded here
                synchronized (this) {
                    if (this.cubeMetaStore == null) {
                        Optional<CubeMetaStore> optionalCubeMetaStore = this.cubeManager.getMetaStore(STAR_TREE);
                        if (!optionalCubeMetaStore.isPresent()) {
                            return false;
                        }
                        this.cubeMetaStore = optionalCubeMetaStore.get();
                    }
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        long startOptimizationTime = System.currentTimeMillis();

        Optional<PlanNode> preProjectNodeOne = captures.get(OPTIONAL_PRE_PROJECT_ONE);
        Optional<PlanNode> preProjectNodeTwo = captures.get(OPTIONAL_PRE_PROJECT_TWO);

        List<ProjectNode> projectNodes = new ArrayList<>();
        if ((preProjectNodeOne.isPresent() && !(preProjectNodeOne.get() instanceof ProjectNode)) ||
                (preProjectNodeTwo.isPresent() && !(preProjectNodeTwo.get() instanceof ProjectNode))) {
            return Result.empty();
        }
        preProjectNodeOne.map(ProjectNode.class::cast).map(projectNodes::add);
        preProjectNodeTwo.map(ProjectNode.class::cast).map(projectNodes::add);

        Optional<PlanNode> filterNode = captures.get(OPTIONAL_FILTER);
        JoinNode joinNode = (JoinNode) Plans.resolveGroupReferences(captures.get(JOIN_NODE), context.getLookup());

        try {
            Optional<PlanNode> optimized = CubeOptimizer.forPlan(context,
                    metadata,
                    cubeMetaStore,
                    node,
                    projectNodes,
                    filterNode.map(FilterNode.class::cast).orElse(null),
                    joinNode).optimize();
            return optimized.map(Result::ofPlanNode).orElseGet(Result::empty);
        }
        catch (RuntimeException ex) {
            LOGGER.warn("Encountered exception '" + ex.getMessage() + "' while applying the CubeOptimizer", ex);
            return Result.empty();
        }
        finally {
            long endOptimizationTime = System.currentTimeMillis();
            LOGGER.debug("Star-tree total optimization time: %d millis", (endOptimizationTime - startOptimizationTime));
        }
    }
}
