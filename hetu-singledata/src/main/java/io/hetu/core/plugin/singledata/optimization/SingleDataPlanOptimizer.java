/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.hetu.core.plugin.singledata.optimization;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.hetu.core.plugin.singledata.BaseSingleDataClient;
import io.hetu.core.plugin.singledata.SingleDataSplit;
import io.hetu.core.plugin.singledata.SingleDataTableHandle;
import io.prestosql.expressions.LogicalRowExpressions;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.optimization.BaseJdbcRowExpressionConverter;
import io.prestosql.plugin.jdbc.optimization.JdbcConverterContext;
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
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.PlanVisitor;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.DeterminismEvaluator;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.sql.RowExpressionConverter;
import io.prestosql.spi.sql.expression.Types;
import io.prestosql.spi.type.Type;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static io.hetu.core.plugin.singledata.SingleDataUtils.quote;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_QUERY_GENERATOR_FAILURE;
import static java.lang.String.format;

public class SingleDataPlanOptimizer
        implements ConnectorPlanOptimizer
{
    private static final Logger LOGGER = Logger.get(SingleDataPlanOptimizer.class);
    private static final String LEFT_JOIN_TABLE_ALIAS = "hetu_left";
    private static final String RIGHT_JOIN_TABLE_ALIAS = "hetu_right";

    private final BaseSingleDataClient client;
    private final LogicalRowExpressions logicalRowExpressions;
    private final RowExpressionConverter<JdbcConverterContext> converter;

    @Inject
    public SingleDataPlanOptimizer(
            JdbcClient client,
            RowExpressionService rowExpressionService,
            DeterminismEvaluator determinismEvaluator,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution functionResolution)
    {
        checkState(client instanceof BaseSingleDataClient, "Need SingleDataClient");
        this.client = (BaseSingleDataClient) client;
        this.converter = new BaseJdbcRowExpressionConverter(functionMetadataManager, functionResolution, rowExpressionService, determinismEvaluator);
        this.logicalRowExpressions = new LogicalRowExpressions(
                determinismEvaluator,
                functionResolution,
                functionMetadataManager);
    }

    @Override
    public PlanNode optimize(
            PlanNode maxSubPlan,
            ConnectorSession session,
            Map<String, Type> types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        return maxSubPlan.accept(new Visitor(session, idAllocator, new SingleDataPushDownContextGenerator()), null);
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
        private final ConnectorSession session;
        private final PlanNodeIdAllocator idAllocator;
        private final SingleDataPushDownContextGenerator generator;

        public Visitor(
                ConnectorSession session,
                PlanNodeIdAllocator idAllocator,
                SingleDataPushDownContextGenerator generator)
        {
            this.session = session;
            this.idAllocator = idAllocator;
            this.generator = generator;
        }

        @Override
        public PlanNode visitPlan(PlanNode node, Void context)
        {
            Optional<PlanNode> pushDownPlan = tryCreatingNewNode(node);
            return pushDownPlan.orElseGet(() -> replaceChildren(
                    node, node.getSources().stream().map(source -> source.accept(this, null)).collect(Collectors.toList())));
        }

        private Optional<PlanNode> tryCreatingNewNode(PlanNode node)
        {
            Optional<SingleDataPushDownContext> context = node.accept(generator, true);
            if (!context.isPresent()) {
                return Optional.empty();
            }
            else {
                SingleDataPushDownContext pushDownContext = context.get();
                Optional<List<SingleDataSplit>> splits = client.tryGetSplits(session, pushDownContext);
                if (!splits.isPresent() || splits.get().isEmpty()) {
                    return Optional.empty();
                }

                JdbcTableHandle oldJdbcTableHandle = pushDownContext.getTableHandle();

                SingleDataTableHandle singleDataTableHandle = new SingleDataTableHandle(oldJdbcTableHandle, splits.get());
                TableHandle newTableHandle = new TableHandle(pushDownContext.getCatalogName(), singleDataTableHandle,
                        pushDownContext.getTransaction(), Optional.empty());

                // build new column
                ImmutableList.Builder<Symbol> scanOutputs = new ImmutableList.Builder<>();
                ImmutableMap.Builder<Symbol, ColumnHandle> columnHandles = new ImmutableMap.Builder<>();
                ImmutableMap.Builder<Symbol, RowExpression> assignments = new ImmutableMap.Builder<>();

                for (Map.Entry<Symbol, ColumnHandle> entry : pushDownContext.getColumns().entrySet()) {
                    Symbol symbol = entry.getKey();
                    JdbcColumnHandle oldColumn = (JdbcColumnHandle) entry.getValue();
                    JdbcColumnHandle newColumn = new JdbcColumnHandle(symbol.getName(), oldColumn.getJdbcTypeHandle(),
                            oldColumn.getColumnType(), oldColumn.isNullable());
                    scanOutputs.add(symbol);
                    columnHandles.put(symbol, newColumn);
                    assignments.put(symbol, new VariableReferenceExpression(symbol.getName(), newColumn.getColumnType()));
                }

                PlanNode result = new ProjectNode(
                        idAllocator.getNextId(),
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
                        new Assignments(assignments.build()));

                if (pushDownContext.getNoPushableFilter().isPresent()) {
                    result = new FilterNode(idAllocator.getNextId(), result, pushDownContext.getNoPushableFilter().get());
                }
                if (pushDownContext.getLimit().isPresent()) {
                    result = new LimitNode(idAllocator.getNextId(), result, pushDownContext.getLimit().getAsLong(), false);
                }
                return Optional.of(result);
            }
        }
    }

    /**
     * Generate SingleDataPushDownContext
     * support: LimitNode FilterNode tableScanNode JoinNode
     */
    private class SingleDataPushDownContextGenerator
            extends PlanVisitor<Optional<SingleDataPushDownContext>, Boolean>
    {
        Set<FilterNode> analysedFilterNodes = new HashSet<>();
        Map<FilterNode, List<RowExpression>> pushableFilters = new HashMap<>();
        Map<FilterNode, List<RowExpression>> nonPushableFilters = new HashMap<>();

        public SingleDataPushDownContextGenerator() {}

        @Override
        public Optional<SingleDataPushDownContext> visitPlan(PlanNode node, Boolean isRoot)
        {
            LOGGER.debug("Cannot push down node %s", node.getClass().getSimpleName());
            return Optional.empty();
        }

        @Override
        public Optional<SingleDataPushDownContext> visitFilter(FilterNode filterNode, Boolean isRoot)
        {
            Optional<SingleDataPushDownContext> sourceContext = filterNode.getSource().accept(this, false);
            if (!sourceContext.isPresent() || sourceContext.get().isHasFilter() || sourceContext.get().getLimit().isPresent()) {
                return Optional.empty();
            }

            List<RowExpression> pushable;
            List<RowExpression> nonPushable;

            if (analysedFilterNodes.contains(filterNode)) {
                pushable = pushableFilters.get(filterNode);
                nonPushable = nonPushableFilters.get(filterNode);
            }
            else {
                pushable = new ArrayList<>();
                nonPushable = new ArrayList<>();

                for (RowExpression conjunct : LogicalRowExpressions.extractConjuncts(filterNode.getPredicate())) {
                    try {
                        conjunct.accept(converter, new JdbcConverterContext());
                        pushable.add(conjunct);
                        continue;
                    }
                    catch (PrestoException e) {
                        LOGGER.debug("cannot push down rowExpression [%s], reserve it.", conjunct.getClass().getSimpleName());
                    }
                    nonPushable.add(conjunct);
                }

                analysedFilterNodes.add(filterNode);
                pushableFilters.put(filterNode, pushable);
                nonPushableFilters.put(filterNode, nonPushable);
            }

            if (pushable.isEmpty()) {
                return Optional.empty();
            }
            else {
                RowExpression pushableExpression = logicalRowExpressions.combineConjuncts(pushable);
                RowExpression nonPushableExpression = logicalRowExpressions.combineConjuncts(nonPushable);
                String pushableFilter = pushableExpression.accept(converter, new JdbcConverterContext());
                SingleDataPushDownContext singleDataPushDownContext = sourceContext.get();
                singleDataPushDownContext.setFilter(pushableFilter);
                singleDataPushDownContext.setHasFilter(true);
                singleDataPushDownContext.setNoPushableFilter(nonPushableExpression);
                return Optional.of(singleDataPushDownContext);
            }
        }

        @Override
        public Optional<SingleDataPushDownContext> visitLimit(LimitNode limitNode, Boolean isRoot)
        {
            if (limitNode.isPartial()) {
                LOGGER.debug("Partial limit is not available in push down");
                return Optional.empty();
            }
            Optional<SingleDataPushDownContext> sourceContext = limitNode.getSource().accept(this, false);
            if (!sourceContext.isPresent() || sourceContext.get().getLimit().isPresent()) {
                return Optional.empty();
            }
            return Optional.of(sourceContext.get().setLimit(limitNode.getCount()));
        }

        @Override
        public Optional<SingleDataPushDownContext> visitTableScan(TableScanNode tableScanNode, Boolean isRoot)
        {
            checkState(tableScanNode.getTable().getConnectorHandle() instanceof JdbcTableHandle,
                    "Expected to find JdbcTableHandle for the scan node");
            if (isRoot) {
                return Optional.empty();
            }
            JdbcTableHandle jdbcTableHandle = (JdbcTableHandle) tableScanNode.getTable().getConnectorHandle();
            if (jdbcTableHandle instanceof SingleDataTableHandle
                    || jdbcTableHandle.getGeneratedSql().isPresent()
                    || jdbcTableHandle.getLimit().isPresent()) {
                // already push down
                return Optional.empty();
            }

            SingleDataPushDownContext pushDownContext = new SingleDataPushDownContext();
            pushDownContext.setCatalogName(tableScanNode.getTable().getCatalogName());
            Map<Symbol, ColumnHandle> columns = new HashMap<>(tableScanNode.getAssignments());
            pushDownContext.setColumns(columns);
            pushDownContext.setTransaction(tableScanNode.getTable().getTransaction());
            pushDownContext.setTableHandle(jdbcTableHandle);
            if (jdbcTableHandle.getLimit().isPresent()) {
                pushDownContext.setLimit(jdbcTableHandle.getLimit().getAsLong());
            }
            return Optional.of(pushDownContext);
        }

        @Override
        public Optional<SingleDataPushDownContext> visitJoin(JoinNode joinNode, Boolean isRoot)
        {
            if (!isRoot || !checkJoinNode(joinNode)) {
                return Optional.empty();
            }

            TableScanNode leftNode = (TableScanNode) joinNode.getLeft();
            TableScanNode rightNode = (TableScanNode) joinNode.getRight();

            Map<Symbol, String> leftSymbolToColumnName = new HashMap<>();
            Map<Symbol, String> rightSymbolToColumnName = new HashMap<>();
            leftNode.getAssignments().forEach((key, value) -> leftSymbolToColumnName.put(key, value.getColumnName()));
            rightNode.getAssignments().forEach((key, value) -> rightSymbolToColumnName.put(key, value.getColumnName()));

            List<String> leftJoinColumns = new ArrayList<>();
            List<String> rightJoinColumns = new ArrayList<>();
            joinNode.getCriteria().forEach(equiJoinClause -> {
                leftJoinColumns.add(leftSymbolToColumnName.get(equiJoinClause.getLeft()));
                rightJoinColumns.add(rightSymbolToColumnName.get(equiJoinClause.getRight()));
            });

            TableHandle leftTable = leftNode.getTable();
            TableHandle rightTable = rightNode.getTable();

            JdbcTableHandle leftTableHandle = (JdbcTableHandle) leftTable.getConnectorHandle();
            JdbcTableHandle rightTableHandle = (JdbcTableHandle) rightTable.getConnectorHandle();

            String joinType = joinNode.isCrossJoin() ? Types.JoinType.CROSS.getJoinLabel() : Types.JoinType.valueOf(joinNode.getType().toString()).getJoinLabel();

            List<Symbol> outputSymbols = joinNode.getOutputSymbols();
            Map<Symbol, ColumnHandle> columns = getColumns(outputSymbols, leftNode.getAssignments(), rightNode.getAssignments());
            String joinSql = buildJoinSql(
                    outputSymbols,
                    leftTableHandle.getTableName(),
                    rightTableHandle.getTableName(),
                    leftNode.getAssignments(),
                    rightNode.getAssignments(),
                    joinType,
                    leftJoinColumns,
                    rightJoinColumns);

            if (!client.checkJoinPushDown(leftTableHandle, rightTableHandle, joinSql)) {
                return Optional.empty();
            }

            SingleDataPushDownContext context = new SingleDataPushDownContext();
            context.setHasJoin(true);
            context.setJoinSql(joinSql);
            context.setTransaction(((TableScanNode) joinNode.getLeft()).getTable().getTransaction());
            context.setCatalogName(((TableScanNode) joinNode.getLeft()).getTable().getCatalogName());
            context.setTableHandle((JdbcTableHandle) ((TableScanNode) joinNode.getLeft()).getTable().getConnectorHandle());
            context.setColumns(columns);
            return Optional.of(context);
        }

        private Map<Symbol, ColumnHandle> getColumns(
                List<Symbol> outputs,
                Map<Symbol, ColumnHandle> leftColumns,
                Map<Symbol, ColumnHandle> rightColumns)
        {
            Map<Symbol, ColumnHandle> columns = new LinkedHashMap<>();
            for (Symbol symbol : outputs) {
                if (leftColumns.containsKey(symbol)) {
                    columns.put(symbol, leftColumns.get(symbol));
                }
                else if (rightColumns.containsKey(symbol)) {
                    columns.put(symbol, rightColumns.get(symbol));
                }
                else {
                    throw new PrestoException(JDBC_QUERY_GENERATOR_FAILURE, format("Unknown symbol [%s] in join node", symbol.getName()));
                }
            }
            return columns;
        }

        private boolean checkJoinNode(JoinNode node)
        {
            if (node.getFilter().isPresent()) {
                return false;
            }
            if (!(node.getLeft() instanceof TableScanNode && node.getRight() instanceof TableScanNode)) {
                return false;
            }
            TableScanNode leftNode = (TableScanNode) node.getLeft();
            TableScanNode rightNode = (TableScanNode) node.getRight();

            TableHandle leftTable = leftNode.getTable();
            TableHandle rightTable = rightNode.getTable();
            if (!leftTable.getCatalogName().equals(rightTable.getCatalogName())) {
                return false;
            }

            if (!(leftTable.getConnectorHandle() instanceof JdbcTableHandle && rightTable.getConnectorHandle() instanceof JdbcTableHandle)) {
                return false;
            }

            JdbcTableHandle leftTableHandle = (JdbcTableHandle) leftTable.getConnectorHandle();
            JdbcTableHandle rightTableHandle = (JdbcTableHandle) rightTable.getConnectorHandle();
            if (!leftTableHandle.getSchemaTableName().getSchemaName().equals(rightTableHandle.getSchemaTableName().getSchemaName())) {
                return false;
            }

            return !(leftTableHandle instanceof SingleDataTableHandle) && !(rightTableHandle instanceof SingleDataTableHandle)
                    && !leftTableHandle.getLimit().isPresent() && !leftTableHandle.getGeneratedSql().isPresent()
                    && !rightTableHandle.getLimit().isPresent() && !rightTableHandle.getGeneratedSql().isPresent();
        }

        private String buildJoinSql(
                List<Symbol> outputs,
                String leftTableName,
                String rightTableName,
                Map<Symbol, ColumnHandle> leftAssignment,
                Map<Symbol, ColumnHandle> rightAssignment,
                String joinType,
                List<String> leftJoinColumns,
                List<String> rightJoinColumns)
        {
            StringBuilder sql = new StringBuilder("SELECT ");
            if (outputs == null || outputs.isEmpty()) {
                sql.append("null");
            }
            else {
                List<String> columnExpressions = new ArrayList<>();
                for (Symbol symbol : outputs) {
                    if (leftAssignment.containsKey(symbol)) {
                        columnExpressions.add(LEFT_JOIN_TABLE_ALIAS + "." + quote(leftAssignment.get(symbol).getColumnName()) + " AS " + symbol.getName());
                    }
                    else {
                        columnExpressions.add(RIGHT_JOIN_TABLE_ALIAS + "." + quote(rightAssignment.get(symbol).getColumnName()) + " AS " + symbol.getName());
                    }
                }
                sql.append(Joiner.on(", ").join(columnExpressions));
            }
            sql.append(" FROM ").append(leftTableName).append(" ").append(LEFT_JOIN_TABLE_ALIAS).append(" ").append(joinType)
                    .append(" ").append(rightTableName).append(" ").append(RIGHT_JOIN_TABLE_ALIAS).append(" ON ");

            List<String> joinClauses = new ArrayList<>();
            for (int i = 0; i < leftJoinColumns.size(); i++) {
                joinClauses.add(LEFT_JOIN_TABLE_ALIAS + "." + quote(leftJoinColumns.get(i)) + " = "
                        + RIGHT_JOIN_TABLE_ALIAS + "." + quote(rightJoinColumns.get(i)));
            }
            sql.append(Joiner.on(" AND ").join(joinClauses));

            return sql.toString();
        }
    }
}
