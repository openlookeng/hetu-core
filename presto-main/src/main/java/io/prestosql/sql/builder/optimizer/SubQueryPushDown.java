/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.sql.builder.optimizer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.SubQueryApplicationResult;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.builder.PushDownConstant;
import io.prestosql.sql.builder.SqlQueryBuilder;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.GroupIdNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.OutputNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.SimplePlanRewriter;
import io.prestosql.sql.planner.plan.SortNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.plan.TopNNode;
import io.prestosql.sql.planner.plan.UnionNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;
import java.util.WeakHashMap;

import static io.prestosql.sql.planner.plan.ChildReplacer.replaceChildren;
import static java.util.Objects.requireNonNull;

/**
 * Legacy optimizer to push sub-query with join down to the connector.
 */
public class SubQueryPushDown
        implements PlanOptimizer
{
    private static final Logger logger = Logger.get(SubQueryPushDown.class);

    private final Metadata metadata;

    public SubQueryPushDown(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new OptimizedPlanRewriter(session, metadata, symbolAllocator, idAllocator, types), plan);
    }

    private static class OptimizedPlanRewriter
            extends SimplePlanRewriter<Void>
    {
        private final Session session;

        private final Metadata metadata;

        private final SqlQueryBuilder sqlQueryBuilder;

        private final Map<PlanNode, PlanNode> cache = new WeakHashMap<>();

        private final SymbolAllocator symbolAllocator;

        private final PlanNodeIdAllocator idAllocator;

        private final TypeProvider typeProvider;

        private OptimizedPlanRewriter(Session session, Metadata metadata, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator,
                TypeProvider typeProvider)
        {
            this.session = session;
            this.metadata = metadata;
            this.sqlQueryBuilder = new SqlQueryBuilder(metadata, session);
            this.symbolAllocator = symbolAllocator;
            this.idAllocator = idAllocator;
            this.typeProvider = typeProvider;
        }

        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<Void> context)
        {
            PlanNode source = node.getSource();
            Stack<PlanNode> stack = new Stack<>();
            stack.push(node);

            // Skip identity project nodes which are used to select sub-set of the source node
            while (source instanceof ProjectNode && isIdentity(((ProjectNode) source))) {
                stack.push(source);
                source = ((ProjectNode) source).getSource();
            }

            if (source instanceof SortNode) {
                PlanNode output = context.rewrite(((SortNode) source).getSource(), context.get());
                source = replaceChildren(source, ImmutableList.of(output));
            }
            else if (source instanceof TopNNode) {
                PlanNode output = context.rewrite(source, context.get());
                if (output instanceof TableScanNode) {
                    // TopNN is rewritten
                    source = replaceChildren(source, ImmutableList.of(output));
                }
                else {
                    source = output;
                }
            }
            else {
                source = context.rewrite(source, context.get());
            }
            while (!stack.empty()) {
                PlanNode parent = stack.pop();
                source = replaceChildren(parent, ImmutableList.of(source));
            }
            return source;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, SimplePlanRewriter.RewriteContext<Void> context)
        {
            return visitNode(node, context);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            return visitNode(node, context);
        }

        @Override
        public PlanNode visitLimit(LimitNode node, RewriteContext<Void> context)
        {
            return visitNode(node, context);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            return visitNode(node, context);
        }

        @Override
        public PlanNode visitSort(SortNode node, RewriteContext<Void> context)
        {
            return visitNode(node, context);
        }

        @Override
        public PlanNode visitTopN(TopNNode node, RewriteContext<Void> context)
        {
            return visitNode(node, context);
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            return visitNode(node, context);
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Void> context)
        {
            return visitNode(node, context);
        }

        @Override
        public PlanNode visitUnion(UnionNode node, RewriteContext<Void> context)
        {
            return visitNode(node, context);
        }

        private <T extends PlanNode> PlanNode visitNode(T node, SimplePlanRewriter.RewriteContext<Void> context)
        {
            // Do not process a sub-tree if it is already processed
            PlanNode rewrittenNode = this.cache.get(node);
            if (rewrittenNode != null) {
                return rewrittenNode;
            }
            // Build SQL query from the sub-tree
            Optional<SqlQueryBuilder.Result> builderResult = sqlQueryBuilder.build(node);

            if (builderResult.isPresent()) {
                Map<String, Type> types = new HashMap<>();
                for (Symbol symbol : node.getOutputSymbols()) {
                    types.put(symbol.getName().toLowerCase(Locale.ENGLISH), typeProvider.get(symbol));
                }
                SqlQueryBuilder.Result result = builderResult.get();
                Optional<PlanNode> output = build(node, result.getQuery(), result.getTableHandle(), types);
                rewrittenNode = output.orElseGet(() -> context.defaultRewrite(node, context.get()));
            }
            else {
                // process grouping function, for now grouping function is not supported
                if (node instanceof ProjectNode && sqlQueryBuilder.isGroupByWithGroupingFunction(node)) {
                    PlanNode planNode = ((ProjectNode) node).getSource();
                    if (!(planNode instanceof AggregationNode)) {
                        this.cache.put(node, node);
                        return node;
                    }
                    AggregationNode aggregationNode = (AggregationNode) planNode;
                    planNode = aggregationNode.getSource();
                    if (!(planNode instanceof GroupIdNode)) {
                        this.cache.put(node, node);
                        return node;
                    }
                    PlanNode newSourceGroupIdNode = context.defaultRewrite(planNode, context.get());
                    PlanNode newAggNode = aggregationNode.replaceChildren(Collections.singletonList(newSourceGroupIdNode));
                    rewrittenNode = node.replaceChildren(Collections.singletonList(newAggNode));
                }
                else {
                    rewrittenNode = context.defaultRewrite(node, context.get());
                }
            }
            this.cache.put(node, rewrittenNode);
            return rewrittenNode;
        }

        private Optional<PlanNode> build(PlanNode root, String sql, TableHandle tableHandle,
                Map<String, Type> types)
        {
            Map<String, Type> newTypesMap = new HashMap<>();
            types.forEach((key, value) -> newTypesMap.put(this.sqlQueryBuilder.aliasBack(key).orElse(key), value));
            types = newTypesMap;
            Optional<SubQueryApplicationResult<TableHandle>> result = metadata.applySubQuery(session, tableHandle, sql,
                    types);
            if (result.isPresent()) {
                SubQueryApplicationResult<TableHandle> applicationResult = result.get();
                ImmutableMap.Builder<Symbol, ColumnHandle> columnHandleBuilder = new ImmutableMap.Builder<>();
                ImmutableList.Builder<Symbol> symbolsBuilder = new ImmutableList.Builder<>();
                ImmutableMap.Builder<Symbol, Expression> assignmentsBuilder = new ImmutableMap.Builder<>();

                Map<String, ColumnHandle> assignments = applicationResult.getAssignments();

                for (Symbol symbol : root.getOutputSymbols()) {
                    String name = symbol.getName().toLowerCase(Locale.ENGLISH);
                    // only the group id will cause alias translation
                    Optional<String> aliasBack = this.sqlQueryBuilder.aliasBack(name);
                    String newName = aliasBack.orElse(name);
                    ColumnHandle columnHandle = assignments.get(newName);

                    if (columnHandle == null) {
                        if (newName.toLowerCase(Locale.ENGLISH).contains(PushDownConstant.GROUPING_COLUMN_INDEX_ALIAS)) {
                            // only the group id will cause column drop
                            continue;
                        }
                        else {
                            return Optional.empty();
                        }
                    }

                    Type prestoType = types.get(newName);
                    Type dbType = applicationResult.getType(newName);

                    if (dbType.equals(prestoType)) {
                        Symbol scanActSymbol;
                        if (!name.equals(newName)) {
                            scanActSymbol = symbolAllocator.newSymbol(newName, dbType);
                        }
                        else {
                            scanActSymbol = symbol;
                        }
                        assignmentsBuilder.put(symbol, new SymbolReference(scanActSymbol.getName()));
                        symbolsBuilder.add(scanActSymbol);
                        columnHandleBuilder.put(scanActSymbol, columnHandle);
                    }
                    else {
                        // Database returns a type different from Presto's expected type
                        Symbol scanNewSymbol = symbolAllocator.newSymbol(newName, dbType);
                        symbolsBuilder.add(scanNewSymbol);
                        columnHandleBuilder.put(scanNewSymbol, columnHandle);
                        assignmentsBuilder.put(symbol, new Cast(new SymbolReference(scanNewSymbol.getName()), prestoType.toString()));
                    }
                }

                PlanNode output = new ProjectNode(this.idAllocator.getNextId(),
                        new TableScanNode(root.getId(),
                                result.get().getHandle(),
                                symbolsBuilder.build(),
                                columnHandleBuilder.build(),
                                TupleDomain.all(),
                                Optional.empty(),
                                0, 0),
                        new Assignments(assignmentsBuilder.build()));

                return Optional.of(output);
            }
            else {
                return Optional.empty();
            }
        }
    }

    private static boolean isIdentity(ProjectNode node)
    {
        for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
            Expression expression = entry.getValue();
            Symbol symbol = entry.getKey();
            if (!(expression instanceof SymbolReference && ((SymbolReference) expression).getName()
                    .replaceAll("\"", "")
                    .equals(symbol.getName().replaceAll("\"", "")))) {
                return false;
            }
        }
        return true;
    }
}
