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
package io.prestosql.plugin.jdbc.optimization;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.expressions.LogicalRowExpressions;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.optimization.JdbcQueryGeneratorResult.GeneratedSql;
import io.prestosql.spi.ConnectorPlanOptimizer;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.SymbolAllocator;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.FunctionMetadataManager;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.GroupIdNode;
import io.prestosql.spi.plan.MarkDistinctNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.PlanVisitor;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.DeterminismEvaluator;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.sql.QueryGenerator;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.UnknownType;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.jdbc.optimization.JdbcPlanOptimizerUtils.getGroupingSetColumn;
import static io.prestosql.plugin.jdbc.optimization.JdbcPlanOptimizerUtils.replaceGroupingSetColumns;
import static io.prestosql.spi.function.OperatorType.CAST;

public class JdbcPlanOptimizer
        implements ConnectorPlanOptimizer
{
    private static final Logger log = Logger.get(JdbcPlanOptimizer.class);
    private static final Set<Class<? extends PlanNode>> UNSUPPORTED_ROOT_NODE = ImmutableSet.of(GroupIdNode.class, MarkDistinctNode.class);

    private final JdbcClient client;
    private final BaseJdbcConfig config;
    private final TypeManager typeManager;
    private final Optional<QueryGenerator<JdbcQueryGeneratorResult, JdbcConverterContext>> queryGenerator;
    private final StandardFunctionResolution functionResolution;
    private final LogicalRowExpressions logicalRowExpressions;

    @Inject
    public JdbcPlanOptimizer(
            JdbcClient client,
            TypeManager typeManager,
            BaseJdbcConfig config,
            RowExpressionService rowExpressionService,
            DeterminismEvaluator determinismEvaluator,
            FunctionMetadataManager functionManager,
            StandardFunctionResolution functionResolution)
    {
        this.client = client;
        this.config = config;
        this.typeManager = typeManager;
        this.functionResolution = functionResolution;
        this.queryGenerator = client.getQueryGenerator(determinismEvaluator, rowExpressionService, functionManager, functionResolution);
        this.logicalRowExpressions = new LogicalRowExpressions(
                determinismEvaluator,
                functionResolution,
                functionManager);
    }

    @Override
    public PlanNode optimize(
            PlanNode maxSubPlan,
            ConnectorSession session,
            Map<String, Type> types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        if (!config.isPushDownEnable() || !queryGenerator.isPresent()) {
            return maxSubPlan;
        }
        // Some node cannot be push down root node.
        if (UNSUPPORTED_ROOT_NODE.contains(maxSubPlan.getClass())) {
            return maxSubPlan;
        }
        return maxSubPlan.accept(new Visitor(idAllocator, types, session, symbolAllocator), null);
    }

    private static PlanNode replaceChildren(PlanNode node, List<PlanNode> children)
    {
        List<PlanNode> childrenNodes = node.getSources();
        for (int i = 0; i < childrenNodes.size(); i++) {
            if (children.get(i) != childrenNodes.get(i)) {
                return node.replaceChildren(children);
            }
        }
        return node;
    }

    private class Visitor
            extends PlanVisitor<PlanNode, Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final ConnectorSession session;
        private final Map<String, Type> types;
        private final SymbolAllocator symbolAllocator;
        private final IdentityHashMap<FilterNode, Void> filtersSplitUp = new IdentityHashMap<>();

        public Visitor(
                PlanNodeIdAllocator idAllocator,
                Map<String, Type> types,
                ConnectorSession session,
                SymbolAllocator symbolAllocator)
        {
            this.idAllocator = idAllocator;
            this.types = types;
            this.session = session;
            this.symbolAllocator = symbolAllocator;
        }

        @Override
        public PlanNode visitPlan(PlanNode node, Void context)
        {
            Optional<PlanNode> pushDownPlan = tryCreatingNewScanNode(node);
            return pushDownPlan.orElseGet(() -> replaceChildren(
                    node, node.getSources().stream().map(source -> source.accept(this, null)).collect(toImmutableList())));
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Void context)
        {
            if (filtersSplitUp.containsKey(node)) {
                return this.visitPlan(node, context);
            }
            filtersSplitUp.put(node, null);
            FilterNode nodeToRecurseInto = node;
            List<RowExpression> pushable = new ArrayList<>();
            List<RowExpression> nonPushable = new ArrayList<>();

            for (RowExpression conjunct : LogicalRowExpressions.extractConjuncts(node.getPredicate())) {
                try {
                    JdbcConverterContext jdbcConverterContext = new JdbcConverterContext();
                    conjunct.accept(queryGenerator.get().getConverter(), jdbcConverterContext);
                    pushable.add(conjunct);
                }
                catch (PrestoException pe) {
                    nonPushable.add(conjunct);
                }
            }
            if (!pushable.isEmpty()) {
                FilterNode pushableFilter = new FilterNode(idAllocator.getNextId(), node.getSource(), logicalRowExpressions.combineConjuncts(pushable));
                Optional<FilterNode> nonPushableFilter = nonPushable.isEmpty() ? Optional.empty() : Optional.of(new FilterNode(idAllocator.getNextId(), pushableFilter, logicalRowExpressions.combineConjuncts(nonPushable)));

                filtersSplitUp.put(pushableFilter, null);
                if (nonPushableFilter.isPresent()) {
                    FilterNode nonPushableFilterNode = nonPushableFilter.get();
                    filtersSplitUp.put(nonPushableFilterNode, null);
                    nodeToRecurseInto = nonPushableFilterNode;
                }
                else {
                    nodeToRecurseInto = pushableFilter;
                }
            }
            return this.visitFilter(nodeToRecurseInto, context);
        }

        private Optional<PlanNode> tryCreatingNewScanNode(PlanNode node)
        {
            Optional<JdbcQueryGeneratorResult> result = queryGenerator.get().generate(node, typeManager);
            if (!result.isPresent()) {
                return Optional.empty();
            }

            Map<String, ColumnHandle> columns;
            JdbcQueryGeneratorContext context = result.get().getContext();
            GeneratedSql generatedSql = result.get().getGeneratedSql();
            if (!generatedSql.isPushDown()) {
                return Optional.empty();
            }

            JdbcQueryGeneratorContext.GroupIdNodeInfo groupIdNodeInfo = context.getGroupIdNodeInfo();
            String sql = generatedSql.getSql();
            // replace grouping sets column
            if (groupIdNodeInfo.isGroupByComplexOperation()) {
                sql = replaceGroupingSetColumns(sql);
            }

            try {
                columns = client.getColumns(session, sql, types);
            }
            catch (PrestoException e) {
                log.warn("query push down failed for [%s]", e.getMessage());
                return Optional.empty();
            }
            if (columns.isEmpty()) {
                log.debug("Get columns from generated sql failed.");
                return Optional.empty();
            }

            ImmutableList.Builder<Symbol> scanOutputs = new ImmutableList.Builder<>();
            ImmutableMap.Builder<Symbol, ColumnHandle> columnHandles = new ImmutableMap.Builder<>();
            ImmutableMap.Builder<Symbol, RowExpression> assignments = new ImmutableMap.Builder<>();

            for (Symbol symbol : node.getOutputSymbols()) {
                String name = symbol.getName().toLowerCase(Locale.ENGLISH);
                String aliasName = groupIdNodeInfo.isGroupByComplexOperation()
                        ? getGroupingSetColumn(name)
                        : name;
                if (!types.containsKey(name) || !columns.containsKey(aliasName)) {
                    log.debug("Get type of column [%s] failed", name);
                    return Optional.empty();
                }
                Type prestoType = types.get(name);
                Type jdbcType = ((JdbcColumnHandle) columns.get(aliasName)).getColumnType();

                if (prestoType.equals(jdbcType)) {
                    scanOutputs.add(symbol);
                    columnHandles.put(symbol, columns.get(aliasName));
                    assignments.put(symbol, new VariableReferenceExpression(symbol.getName(), prestoType));
                }
                else {
                    if (prestoType instanceof UnknownType) {
                        log.debug("Can't cast from type[%s] to type[%s]", jdbcType.getDisplayName(), prestoType.getDisplayName());
                        return Optional.empty();
                    }
                    // If Jdbc return a different type from Presto's expected type, add a CAST expression
                    Symbol scanSymbol = symbolAllocator.newSymbol(symbol.getName(), jdbcType);
                    scanOutputs.add(scanSymbol);
                    columnHandles.put(scanSymbol, columns.get(aliasName));
                    assignments.put(symbol, new CallExpression(
                            CAST.name(),
                            functionResolution.castFunction(jdbcType.getTypeSignature(), prestoType.getTypeSignature()),
                            prestoType,
                            ImmutableList.of(new VariableReferenceExpression(scanSymbol.getName(), jdbcType)),
                            Optional.empty()));
                }
            }

            checkState(context.getCatalogName().isPresent(), "CatalogName is null");
            checkState(context.getSchemaTableName().isPresent(), "schemaTableName is null");
            checkState(context.getTransaction().isPresent(), "transaction is null");
            TableHandle newTableHandle = new TableHandle(
                    context.getCatalogName().get(),
                    new JdbcTableHandle(
                            context.getSchemaTableName().get(),
                            context.getRemoteCatalogName(),
                            context.getRemoteSchemaName(),
                            context.getRemoteTableName(),
                            TupleDomain.all(),
                            OptionalLong.empty(),
                            Optional.of(new GeneratedSql(sql, true)),
                            false),
                    context.getTransaction().get(),
                    Optional.empty());
            return Optional.of(
                    new ProjectNode(
                            this.idAllocator.getNextId(),
                            new TableScanNode(
                                    idAllocator.getNextId(),
                                    newTableHandle,
                                    scanOutputs.build(),
                                    columnHandles.build(),
                                    TupleDomain.all(),
                                    Optional.empty(),
                                    ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT,
                                    new UUID(0, 0),
                                    0,
                                    false),
                            new Assignments(assignments.build())));
        }
    }
}
