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

package io.hetu.core.plugin.datacenter.optimization;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.hetu.core.plugin.datacenter.DataCenterColumn;
import io.hetu.core.plugin.datacenter.DataCenterColumnHandle;
import io.hetu.core.plugin.datacenter.DataCenterConfig;
import io.hetu.core.plugin.datacenter.DataCenterTableHandle;
import io.hetu.core.plugin.datacenter.client.DataCenterClient;
import io.hetu.core.plugin.datacenter.client.DataCenterStatementClientFactory;
import io.prestosql.plugin.jdbc.optimization.JdbcQueryGeneratorContext;
import io.prestosql.plugin.jdbc.optimization.JdbcQueryGeneratorResult;
import io.prestosql.spi.ConnectorPlanOptimizer;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.SymbolAllocator;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
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
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.sql.RowExpressionUtils;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.UnknownType;
import okhttp3.OkHttpClient;

import javax.inject.Inject;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.jdbc.optimization.JdbcPlanOptimizerUtils.getGroupingSetColumn;
import static io.prestosql.plugin.jdbc.optimization.JdbcPlanOptimizerUtils.replaceGroupingSetColumns;

public class DataCenterPlanOptimizer
        implements ConnectorPlanOptimizer
{
    private static final Logger log = Logger.get(DataCenterPlanOptimizer.class);

    private static final String DATACENTER_CATALOG_PREFIX = "dc.";
    private static final Set<Class<? extends PlanNode>> UNSUPPORTED_ROOT_NODE = ImmutableSet.of(GroupIdNode.class, MarkDistinctNode.class);

    private final DataCenterClient client;
    private final DataCenterConfig config;
    private final TypeManager typeManager;
    private final DataCenterQueryGenerator queryGenerator;

    @Inject
    public DataCenterPlanOptimizer(
            TypeManager typeManager,
            DataCenterConfig config,
            DataCenterQueryGenerator query)
    {
        OkHttpClient httpClient = DataCenterStatementClientFactory.newHttpClient(config);
        this.client = new DataCenterClient(config, httpClient, typeManager);
        this.config = config;
        this.typeManager = typeManager;
        this.queryGenerator = query;
    }

    @Override
    public PlanNode optimize(PlanNode maxSubPlan, ConnectorSession session, Map<String, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        if (!config.isQueryPushDownEnabled()) {
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
        for (int i = 0; i < node.getSources().size(); i++) {
            if (children.get(i) != node.getSources().get(i)) {
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

            for (RowExpression conjunct : RowExpressionUtils.extractConjuncts(node.getPredicate())) {
                try {
                    conjunct.accept(queryGenerator.getConverter(), null);
                    pushable.add(conjunct);
                }
                catch (PrestoException pe) {
                    nonPushable.add(conjunct);
                }
            }
            if (!pushable.isEmpty()) {
                FilterNode pushableFilter = new FilterNode(idAllocator.getNextId(), node.getSource(), RowExpressionUtils.combineConjuncts(pushable));
                Optional<FilterNode> nonPushableFilter = nonPushable.isEmpty() ? Optional.empty() : Optional.of(new FilterNode(idAllocator.getNextId(), pushableFilter, RowExpressionUtils.combineConjuncts(nonPushable)));

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
            Optional<JdbcQueryGeneratorResult> result = queryGenerator.generate(node, typeManager);
            if (!result.isPresent()) {
                return Optional.empty();
            }

            JdbcQueryGeneratorContext context = result.get().getContext();
            JdbcQueryGeneratorResult.GeneratedSql generatedSql = result.get().getGeneratedSql();
            if (!generatedSql.isPushDown()) {
                return Optional.empty();
            }

            JdbcQueryGeneratorContext.GroupIdNodeInfo groupIdNodeInfo = context.getGroupIdNodeInfo();
            String sql = generatedSql.getSql();
            // replace grouping sets column
            if (groupIdNodeInfo.isGroupByComplexOperation()) {
                sql = replaceGroupingSetColumns(sql);
            }

            if (sql.getBytes(StandardCharsets.ISO_8859_1).length >= config.getRemoteHttpServerMaxRequestHeaderSize().toBytes()) {
                log.debug("Generated sql is too long, push down failed.");
                return Optional.empty();
            }
            List<DataCenterColumn> columnsList;
            try {
                columnsList = client.getColumns(sql);
            }
            catch (PrestoException e) {
                log.warn("query push down failed for [%s]", e.getMessage());
                return Optional.empty();
            }
            if (columnsList.isEmpty()) {
                log.debug("Get columns from generated sql failed.");
                return Optional.empty();
            }

            Map<String, ColumnHandle> columns = new HashMap<>();
            IntStream.range(0, columnsList.size()).forEach(i -> {
                DataCenterColumn column = columnsList.get(i);
                columns.put(column.getName(), new DataCenterColumnHandle(column.getName(), column.getType(), i));
            });
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
                Type dcType = ((DataCenterColumnHandle) columns.get(aliasName)).getColumnType();

                if (prestoType.equals(dcType)) {
                    scanOutputs.add(symbol);
                    columnHandles.put(symbol, columns.get(aliasName));
                    assignments.put(symbol, new VariableReferenceExpression(symbol.getName(), prestoType));
                }
                else {
                    if (prestoType instanceof UnknownType) {
                        log.debug("Can't cast from type[%s] to type[%s]", dcType.getDisplayName(), prestoType.getDisplayName());
                        return Optional.empty();
                    }
                    // If Jdbc return a different type from Presto's expected type, add a CAST expression
                    Symbol scanSymbol = symbolAllocator.newSymbol(symbol.getName(), dcType);
                    scanOutputs.add(scanSymbol);
                    columnHandles.put(scanSymbol, columns.get(aliasName));
                    assignments.put(symbol, new CallExpression(
                            Signature.internalOperator(OperatorType.CAST, prestoType.getTypeSignature(), ImmutableList.of(dcType.getTypeSignature())),
                            prestoType,
                            ImmutableList.of(new VariableReferenceExpression(scanSymbol.getName(), dcType)),
                            Optional.empty()));
                }
            }

            checkState(context.getCatalogName().isPresent(), "CatalogName is null");
            checkState(context.getSchemaTableName().isPresent(), "schemaTableName is null");
            checkState(context.getTransaction().isPresent(), "transaction is null");
            CatalogName catalogName = context.getCatalogName().get();
            String tableCatalogName = catalogName.getCatalogName().startsWith(DATACENTER_CATALOG_PREFIX)
                    ? catalogName.getCatalogName().substring(DATACENTER_CATALOG_PREFIX.length())
                    : catalogName.getCatalogName();

            TableHandle newTableHandle = new TableHandle(
                    catalogName,
                    new DataCenterTableHandle(
                            tableCatalogName,
                            context.getSchemaTableName().get().getSchemaName(),
                            context.getSchemaTableName().get().getTableName(),
                            OptionalLong.empty(),
                            sql),
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
