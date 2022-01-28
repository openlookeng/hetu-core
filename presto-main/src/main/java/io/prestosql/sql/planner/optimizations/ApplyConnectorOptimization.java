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
package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.spi.ConnectorPlanOptimizer;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.ExceptNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.GroupIdNode;
import io.prestosql.spi.plan.IntersectNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.MarkDistinctNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.TopNNode;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ApplyConnectorOptimization
        implements PlanOptimizer
{
    static final Set<Class<? extends PlanNode>> CONNECTOR_ACCESSIBLE_PLAN_NODES = ImmutableSet.of(
            AggregationNode.class,
            TableScanNode.class,
            LimitNode.class,
            ExceptNode.class,
            FilterNode.class,
            IntersectNode.class,
            MarkDistinctNode.class,
            JoinNode.class,
            WindowNode.class,
            ProjectNode.class,
            TopNNode.class,
            UnionNode.class,
            GroupIdNode.class);

    // for a leaf node that doesn't not belong to any connector (e.g., ValuesNode)
    private static final CatalogName EMPTY_CATALOG_NAME = new CatalogName("$internal$" + ApplyConnectorOptimization.class + "_CATALOG");

    private final Supplier<Map<CatalogName, Set<ConnectorPlanOptimizer>>> connectorOptimizersSupplier;

    public ApplyConnectorOptimization(Supplier<Map<CatalogName, Set<ConnectorPlanOptimizer>>> connectorOptimizersSupplier)
    {
        this.connectorOptimizersSupplier = requireNonNull(connectorOptimizersSupplier, "connectorOptimizerSupplier is null");
    }

    @Override
    public PlanNode optimize(PlanNode inputPlan, Session session, TypeProvider types, PlanSymbolAllocator planSymbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(inputPlan, "inputPlan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(idAllocator, "idAllocator is null");

        PlanNode plan = inputPlan;
        Map<CatalogName, Set<ConnectorPlanOptimizer>> connectorOptimizers = connectorOptimizersSupplier.get();

        if (connectorOptimizers.isEmpty()) {
            return plan;
        }

        // retrieve all the connectors
        ImmutableSet.Builder<CatalogName> catalogNames = ImmutableSet.builder();
        getAllCatalogNames(plan, catalogNames);

        for (CatalogName catalogName : catalogNames.build()) {
            Set<ConnectorPlanOptimizer> optimizers = connectorOptimizers.get(catalogName);
            if (optimizers == null) {
                continue;
            }

            ImmutableMap.Builder<PlanNode, ConnectorPlanNodeContext> contextMapBuilder = ImmutableMap.builder();
            buildConnectorPlanContext(plan, null, contextMapBuilder);
            Map<PlanNode, ConnectorPlanNodeContext> contextMap = contextMapBuilder.build();

            Map<PlanNode, PlanNode> updates = new HashMap<>();

            Map<String, Type> typeMap = new HashMap<>();
            for (Map.Entry<Symbol, Type> entry : types.allTypes().entrySet()) {
                typeMap.put(entry.getKey().getName(), entry.getValue());
            }

            for (PlanNode node : contextMap.keySet()) {
                ConnectorPlanNodeContext context = contextMap.get(node);
                if (!context.isClosure(catalogName) ||
                        !context.getParent().isPresent() ||
                        contextMap.get(context.getParent().get()).isClosure(catalogName)) {
                    continue;
                }

                PlanNode newNode = node;

                for (ConnectorPlanOptimizer optimizer : optimizers) {
                    newNode = optimizer.optimize(newNode, session.toConnectorSession(catalogName), typeMap, planSymbolAllocator, idAllocator);
                }

                if (node != newNode) {
                    checkState(
                            containsAll(ImmutableSet.copyOf(newNode.getOutputSymbols()), node.getOutputSymbols()),
                            "the connector optimizer from %s returns a node that does not cover all output before optimization",
                            catalogName);
                    updates.put(node, newNode);
                }
            }

            Queue<PlanNode> originalNodes = new LinkedList<>(updates.keySet());
            while (!originalNodes.isEmpty()) {
                PlanNode originalNode = originalNodes.poll();

                if (!contextMap.get(originalNode).getParent().isPresent()) {
                    plan = updates.get(originalNode);
                    continue;
                }

                PlanNode originalParent = contextMap.get(originalNode).getParent().get();

                ImmutableList.Builder<PlanNode> newChildren = ImmutableList.builder();
                originalParent.getSources().forEach(child -> newChildren.add(updates.getOrDefault(child, child)));
                PlanNode newParent = originalParent.replaceChildren(newChildren.build());

                updates.put(originalParent, newParent);

                originalNodes.add(originalParent);
            }
        }

        return plan;
    }

    private static void getAllCatalogNames(PlanNode node, ImmutableSet.Builder<CatalogName> builder)
    {
        if (node.getSources().isEmpty()) {
            if (node instanceof TableScanNode) {
                builder.add(((TableScanNode) node).getTable().getCatalogName());
            }
            else {
                builder.add(EMPTY_CATALOG_NAME);
            }
            return;
        }

        for (PlanNode child : node.getSources()) {
            getAllCatalogNames(child, builder);
        }
    }

    private static ConnectorPlanNodeContext buildConnectorPlanContext(
            PlanNode node,
            PlanNode parent,
            ImmutableMap.Builder<PlanNode, ConnectorPlanNodeContext> contextBuilder)
    {
        Set<CatalogName> catalogNames;
        Set<Class<? extends PlanNode>> planNodeTypes;

        if (node.getSources().isEmpty()) {
            if (node instanceof TableScanNode) {
                catalogNames = ImmutableSet.of(((TableScanNode) node).getTable().getCatalogName());
                planNodeTypes = ImmutableSet.of(TableScanNode.class);
            }
            else {
                catalogNames = ImmutableSet.of(EMPTY_CATALOG_NAME);
                planNodeTypes = ImmutableSet.of(node.getClass());
            }
        }
        else {
            catalogNames = new HashSet<>();
            planNodeTypes = new HashSet<>();

            for (PlanNode child : node.getSources()) {
                ConnectorPlanNodeContext childContext = buildConnectorPlanContext(child, node, contextBuilder);
                catalogNames.addAll(childContext.getReachableConnectors());
                planNodeTypes.addAll(childContext.getReachablePlanNodeTypes());
            }
            planNodeTypes.add(node.getClass());
        }

        ConnectorPlanNodeContext connectorPlanNodeContext = new ConnectorPlanNodeContext(
                parent,
                catalogNames,
                planNodeTypes);

        contextBuilder.put(node, connectorPlanNodeContext);
        return connectorPlanNodeContext;
    }

    /**
     * Extra information needed for a plan node
     */
    private static final class ConnectorPlanNodeContext
    {
        private final PlanNode parent;
        private final Set<CatalogName> reachableConnectors;
        private final Set<Class<? extends PlanNode>> reachablePlanNodeTypes;

        ConnectorPlanNodeContext(PlanNode parent, Set<CatalogName> reachableConnectors, Set<Class<? extends PlanNode>> reachablePlanNodeTypes)
        {
            this.parent = parent;
            this.reachableConnectors = requireNonNull(reachableConnectors, "reachableConnectors is null");
            this.reachablePlanNodeTypes = requireNonNull(reachablePlanNodeTypes, "reachablePlanNodeTypes is null");
            checkArgument(!reachableConnectors.isEmpty(), "encountered a PlanNode that reaches no connector");
            checkArgument(!reachablePlanNodeTypes.isEmpty(), "encountered a PlanNode that reaches no plan node");
        }

        Optional<PlanNode> getParent()
        {
            return Optional.ofNullable(parent);
        }

        public Set<CatalogName> getReachableConnectors()
        {
            return reachableConnectors;
        }

        private Set<Class<? extends PlanNode>> getReachablePlanNodeTypes()
        {
            return reachablePlanNodeTypes;
        }

        boolean isClosure(CatalogName catalogName)
        {
            if (reachableConnectors.size() != 1 || !reachableConnectors.contains(catalogName)) {
                return false;
            }

            return containsAll(CONNECTOR_ACCESSIBLE_PLAN_NODES, reachablePlanNodeTypes);
        }
    }

    private static <T> boolean containsAll(Set<T> container, Collection<T> test)
    {
        for (T element : test) {
            if (!container.contains(element)) {
                return false;
            }
        }
        return true;
    }
}
