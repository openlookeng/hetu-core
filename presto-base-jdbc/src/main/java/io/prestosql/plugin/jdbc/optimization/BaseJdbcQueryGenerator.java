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
package io.prestosql.plugin.jdbc.optimization;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.optimization.JdbcQueryGeneratorResult.GeneratedSql;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.GroupIdNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.MarkDistinctNode;
import io.prestosql.spi.plan.OrderingScheme;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanVisitor;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.TopNNode;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.sql.QueryGenerator;
import io.prestosql.spi.sql.RowExpressionConverter;
import io.prestosql.spi.sql.SqlStatementWriter;
import io.prestosql.spi.sql.expression.OrderBy;
import io.prestosql.spi.sql.expression.Selection;
import io.prestosql.spi.sql.expression.Types;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_QUERY_GENERATOR_FAILURE;
import static io.prestosql.plugin.jdbc.optimization.JdbcPlanOptimizerUtils.frameBound;
import static io.prestosql.plugin.jdbc.optimization.JdbcPlanOptimizerUtils.getDerivedTable;
import static io.prestosql.plugin.jdbc.optimization.JdbcPlanOptimizerUtils.getProjectSelections;
import static io.prestosql.plugin.jdbc.optimization.JdbcPlanOptimizerUtils.getSelectionsFromSymbolsMap;
import static io.prestosql.plugin.jdbc.optimization.JdbcPlanOptimizerUtils.isAggregationDistinct;
import static io.prestosql.plugin.jdbc.optimization.JdbcPlanOptimizerUtils.isSameCatalog;
import static io.prestosql.plugin.jdbc.optimization.JdbcPlanOptimizerUtils.quote;
import static io.prestosql.plugin.jdbc.optimization.JdbcPushDownModule.DEFAULT;
import static io.prestosql.plugin.jdbc.optimization.JdbcPushDownModule.FULL_PUSHDOWN;
import static io.prestosql.plugin.jdbc.optimization.JdbcQueryGeneratorContext.buildAsNewTable;
import static io.prestosql.plugin.jdbc.optimization.JdbcQueryGeneratorContext.buildFrom;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class BaseJdbcQueryGenerator
        implements QueryGenerator<JdbcQueryGeneratorResult>
{
    protected static final Logger log = Logger.get(BaseJdbcQueryGenerator.class);
    protected static final String GENERATE_FAILED_LOG = "JDBC query generator failed for [%s]";

    protected final String quote;
    protected final JdbcPushDownModule pushDownModule;
    protected final RowExpressionConverter converter;
    protected final SqlStatementWriter statementWriter;

    public BaseJdbcQueryGenerator(
            JdbcPushDownParameter pushDownParameter,
            RowExpressionConverter converter,
            SqlStatementWriter statementWriter)
    {
        this.quote = pushDownParameter.getIdentifierQuote();
        this.pushDownModule = pushDownParameter.getPushDownModuleParameter() == DEFAULT ? FULL_PUSHDOWN : pushDownParameter.getPushDownModuleParameter();
        this.converter = converter;
        this.statementWriter = statementWriter;
    }

    @Override
    public RowExpressionConverter getConverter()
    {
        return converter;
    }

    @Override
    public Optional<JdbcQueryGeneratorResult> generate(PlanNode plan, TypeManager typeManager)
    {
        try {
            Optional<JdbcQueryGeneratorContext> context = requireNonNull(plan.accept(getVisitor(typeManager), null),
                    "Resulting context is null");
            return context.map(jdbcQueryGeneratorContext -> new JdbcQueryGeneratorResult(buildSql(jdbcQueryGeneratorContext), jdbcQueryGeneratorContext));
        }
        catch (PrestoException e) {
            log.debug(e, "Possibly benign error when pushing plan into scan node %s", plan);
            return Optional.empty();
        }
    }

    protected PlanVisitor<Optional<JdbcQueryGeneratorContext>, Void> getVisitor(TypeManager typeManager)
    {
        return new BaseJdbcPlanVisitor(typeManager);
    }

    protected GeneratedSql buildSql(JdbcQueryGeneratorContext context)
    {
        String sql = statementWriter.select(ImmutableList.copyOf(context.getSelections().values()));

        checkArgument(context.getFrom().isPresent(), "From expression must not be empty");
        sql = statementWriter.from(sql, context.getFrom().get());

        if (context.getFilter().isPresent()) {
            sql = statementWriter.filter(sql, context.getFilter().get());
        }

        if (!context.getGroupByColumns().isEmpty()) {
            sql = statementWriter.groupBy(sql, context.getGroupByColumns());
        }

        if (context.getOrderBy().isPresent()) {
            sql = statementWriter.orderBy(sql, context.getOrderBy().get());
        }

        if (context.getLimit().isPresent()) {
            sql = statementWriter.limit(sql, context.getLimit().getAsLong());
        }

        boolean isPushDown = context.isHasPushDown();

        return new GeneratedSql(sql, isPushDown);
    }

    protected class BaseJdbcPlanVisitor
            extends PlanVisitor<Optional<JdbcQueryGeneratorContext>, Void>
    {
        protected int derivedTableIdentifier = 1;
        protected TypeManager typeManager;

        public BaseJdbcPlanVisitor(TypeManager typeManager)
        {
            this.typeManager = typeManager;
        }

        @Override
        public Optional<JdbcQueryGeneratorContext> visitPlan(PlanNode node, Void contextIn)
        {
            log.debug(GENERATE_FAILED_LOG, "Don't know how to handle plan node of type " + node);
            return Optional.empty();
        }

        @Override
        public Optional<JdbcQueryGeneratorContext> visitMarkDistinct(MarkDistinctNode node, Void contextIn)
        {
            return node.getSource().accept(this, contextIn);
        }

        @Override
        public Optional<JdbcQueryGeneratorContext> visitFilter(FilterNode node, Void contextIn)
        {
            checkAvailable(node);
            Optional<JdbcQueryGeneratorContext> sourceContext = node.getSource().accept(this, contextIn);
            if (!sourceContext.isPresent()) {
                return Optional.empty();
            }
            JdbcQueryGeneratorContext context = sourceContext.get();

            String filter = node.getPredicate().accept(converter, null);

            return Optional.of(buildAsNewTable(context)
                    .setSelections(getProjectSelections(context.getSelections()))
                    .setFrom(getDerivedTable(buildSql(context).getSql(), derivedTableIdentifier++))
                    .setFilter(Optional.of(filter))
                    .setOutputColumns(node.getOutputSymbols())
                    .setHasPushDown(true)
                    .build());
        }

        @Override
        public Optional<JdbcQueryGeneratorContext> visitJoin(JoinNode node, Void contextIn)
        {
            checkAvailable(node);
            Optional<JdbcQueryGeneratorContext> leftSourceContext = node.getLeft().accept(this, contextIn);
            if (!leftSourceContext.isPresent()) {
                return Optional.empty();
            }
            Optional<JdbcQueryGeneratorContext> rightSourceContext = node.getRight().accept(this, contextIn);
            if (!rightSourceContext.isPresent()) {
                return Optional.empty();
            }

            JdbcQueryGeneratorContext leftContext = leftSourceContext.get();
            JdbcQueryGeneratorContext rightContext = rightSourceContext.get();

            if (!leftContext.getCatalogName().isPresent()
                    || !rightContext.getCatalogName().isPresent()
                    || !leftContext.getCatalogName().equals(rightContext.getCatalogName())) {
                log.debug(GENERATE_FAILED_LOG, "Jdbc Generator can only push down join node with same catalog");
                return Optional.empty();
            }

            LinkedHashMap<String, Selection> newSelections = new LinkedHashMap<>();
            newSelections.putAll(getProjectSelections(leftContext.getSelections()));
            newSelections.putAll(getProjectSelections(rightContext.getSelections()));

            // create a derived table as from
            String from = statementWriter.join((node.isCrossJoin()
                            ? Types.JoinType.CROSS
                            : Types.JoinType.valueOf(node.getType().toString())).getJoinLabel(),
                    buildSql(leftContext).getSql(),
                    buildSql(rightContext).getSql(),
                    node.getCriteria().stream().map(JoinNode.EquiJoinClause::toString).collect(toList()),
                    node.getFilter().map(filter -> filter.accept(converter, null)),
                    derivedTableIdentifier++);

            JdbcQueryGeneratorContext.Builder contextBuilder = buildAsNewTable(leftContext)
                    .setSelections(newSelections)
                    .setFrom(Optional.of(from))
                    .setHasPushDown(true)
                    .setOutputColumns(node.getOutputSymbols());

            return Optional.of(contextBuilder.build());
        }

        @Override
        public Optional<JdbcQueryGeneratorContext> visitUnion(UnionNode node, Void contextIn)
        {
            checkAvailable(node);
            List<PlanNode> sources = node.getSources();
            if (sources == null || sources.size() < 2) {
                log.debug(GENERATE_FAILED_LOG, "Does not support tables' num smaller than 2 in union node");
                return Optional.empty();
            }
            List<Optional<JdbcQueryGeneratorContext>> sourceContexts = sources.stream()
                    .map(planNode -> planNode.accept(this, contextIn))
                    .collect(toList());
            if (!sourceContexts.stream().allMatch(Optional::isPresent)) {
                return Optional.empty();
            }
            List<JdbcQueryGeneratorContext> contexts = sourceContexts.stream()
                    .map(Optional::get)
                    .collect(toList());
            if (!isSameCatalog(contexts)) {
                log.debug(GENERATE_FAILED_LOG, "Union push down just support all sources in same catalog");
                return Optional.empty();
            }
            // sort sources' selection
            String from = statementWriter.union(IntStream.range(0, sources.size())
                    .mapToObj(i -> statementWriter.from(
                            statementWriter.select(getSelectionsFromSymbolsMap(node.sourceSymbolMap(i))),
                            getDerivedTable(buildSql(contexts.get(i)).getSql(), derivedTableIdentifier++).get()))
                    .collect(toList()), derivedTableIdentifier++);

            LinkedHashMap<String, Selection> newSelections = new LinkedHashMap<>();
            node.getOutputSymbols().forEach(symbol -> newSelections.put(symbol.getName(), new Selection(symbol.getName(), symbol.getName())));
            // select first source as base context
            JdbcQueryGeneratorContext baseContext = contexts.get(0);
            return Optional.of(buildAsNewTable(baseContext)
                    .setSelections(newSelections)
                    .setFrom(Optional.of(from))
                    .setHasPushDown(true)
                    .build());
        }

        @Override
        public Optional<JdbcQueryGeneratorContext> visitProject(ProjectNode node, Void contextIn)
        {
            checkAvailable(node);
            Optional<JdbcQueryGeneratorContext> sourceContext = node.getSource().accept(this, contextIn);
            if (!sourceContext.isPresent()) {
                return Optional.empty();
            }
            JdbcQueryGeneratorContext context = sourceContext.get();

            Map<Symbol, RowExpression> assignments = node.getAssignments().getMap();

            LinkedHashMap<String, Selection> newSelections = new LinkedHashMap<>(getProjectSelections(context.getSelections()));

            for (Map.Entry<Symbol, RowExpression> entry : assignments.entrySet()) {
                newSelections.put(entry.getKey().getName(), new Selection(entry.getValue().accept(converter, null), entry.getKey().getName()));
            }

            return Optional.of(buildAsNewTable(context)
                    .setHasPushDown(true)
                    .setFrom(getDerivedTable(buildSql(context).getSql(), derivedTableIdentifier++))
                    .setSelections(newSelections)
                    .setOutputColumns(node.getOutputSymbols())
                    .build());
        }

        @Override
        public Optional<JdbcQueryGeneratorContext> visitAggregation(AggregationNode node, Void contextIn)
        {
            checkAvailable(node);
            // visit the child project node
            Optional<JdbcQueryGeneratorContext> sourceContext = node.getSource().accept(this, contextIn);
            if (!sourceContext.isPresent()) {
                return Optional.empty();
            }
            checkArgument(!node.getStep().isOutputPartial(), "partial aggregations are not support in Jdbc pushdown framework");

            LinkedHashMap<String, Selection> newSelections = new LinkedHashMap<>();
            LinkedHashSet<String> groupByColumns = new LinkedHashSet<>();
            for (Symbol outputColumn : node.getOutputSymbols()) {
                AggregationNode.Aggregation aggregation = node.getAggregations().get(outputColumn);

                if (aggregation != null) {
                    if (aggregation.getFilter().isPresent() || aggregation.getOrderingScheme().isPresent()) {
                        log.debug(GENERATE_FAILED_LOG, "Not support aggregation node " + node);
                        return Optional.empty();
                    }
                    Type returnType = typeManager.getType(aggregation.getSignature().getReturnType());
                    String aggExpression = statementWriter.aggregation(
                            aggregation.getSignature().getName(),
                            aggregation.getArguments().stream()
                                    .map(rowExpression -> rowExpression.accept(converter, null))
                                    .collect(toList()),
                            isAggregationDistinct(aggregation));
                    String castAggExpression = statementWriter.castAggregationType(aggExpression, converter, returnType);
                    newSelections.put(outputColumn.getName(), new Selection(castAggExpression, outputColumn.getName()));
                }
                else {
                    // group by output
                    newSelections.put(outputColumn.getName(), new Selection(outputColumn.getName()));
                    groupByColumns.add(outputColumn.getName());
                }
            }

            // If groupIdSymbol is not empty, remove groupId column and add GROUPING SETS
            Optional<Symbol> groupIdSymbol = node.getGroupIdSymbol();
            if (groupIdSymbol.isPresent() && sourceContext.get().getGroupIdNodeInfo().getGroupingElementStore().containsKey(groupIdSymbol.get())) {
                JdbcQueryGeneratorContext.GroupIdNodeInfo groupIdNodeInfo = sourceContext.get().getGroupIdNodeInfo();
                String idElementString = groupIdNodeInfo.getGroupingElementStore().get(groupIdSymbol.get());
                Optional<String> eleStr = Optional.of(idElementString);
                Optional<Selection> selectionOptional = Optional.empty();
                for (Map.Entry<String, Selection> entry : newSelections.entrySet()) {
                    String selectStr = entry.getValue().getExpression();
                    if (selectStr.equals(groupIdSymbol.get().getName())) {
                        selectionOptional = Optional.of(entry.getValue());
                    }
                }
                selectionOptional.ifPresent(selection -> newSelections.remove(selection.getAlias()));
                groupIdNodeInfo.setGroupByComplexOperation(true);
                return Optional.of(buildAsNewTable(sourceContext.get())
                        .setFrom(getDerivedTable(buildSql(sourceContext.get()).getSql(), derivedTableIdentifier++))
                        .setSelections(newSelections)
                        .setGroupIdNodeInfo(groupIdNodeInfo)
                        .setGroupByColumns(ImmutableSet.of(eleStr.get()))
                        .build());
            }

            JdbcQueryGeneratorContext context = sourceContext.get();
            return Optional.of(buildAsNewTable(context)
                    .setFrom(getDerivedTable(buildSql(context).getSql(), derivedTableIdentifier++))
                    .setSelections(newSelections)
                    .setGroupByColumns(groupByColumns)
                    .setHasPushDown(true)
                    .build());
        }

        @Override
        public Optional<JdbcQueryGeneratorContext> visitTableScan(TableScanNode node, Void contextIn)
        {
            checkAvailable(node);
            checkArgument(node.getTable().getConnectorHandle() instanceof JdbcTableHandle,
                    "Expected to find jdbc table handle for the scan node");
            TupleDomain<ColumnHandle> constraint = node.getEnforcedConstraint();
            if (constraint != null && constraint.getDomains().isPresent()) {
                if (!constraint.getDomains().get().isEmpty()) {
                    // Predicate is pushed down
                    throw new PrestoException(JDBC_QUERY_GENERATOR_FAILURE, "Cannot push down table scan with predicates pushed down");
                }
            }
            TableHandle tableHandle = node.getTable();
            JdbcTableHandle jdbcTableHandle = (JdbcTableHandle) node.getTable().getConnectorHandle();
            checkArgument(!jdbcTableHandle.getGeneratedSql().isPresent(), "Jdbc tableHandle should not have sql before pushdown");
            LinkedHashMap<String, Selection> selections = new LinkedHashMap<>();
            node.getOutputSymbols().forEach(outputColumn -> {
                JdbcColumnHandle jdbcColumn = (JdbcColumnHandle) node.getAssignments().get(outputColumn);
                selections.put(outputColumn.getName(), new Selection(jdbcColumn.getColumnName(), outputColumn.getName()));
            });
            StringBuilder table = new StringBuilder();
            if (!isNullOrEmpty(jdbcTableHandle.getCatalogName())) {
                table.append(quote(quote, jdbcTableHandle.getCatalogName())).append('.');
            }
            if (!isNullOrEmpty(jdbcTableHandle.getSchemaName())) {
                table.append(quote(quote, jdbcTableHandle.getSchemaName())).append('.');
            }
            table.append(quote(quote, jdbcTableHandle.getTableName()));

            JdbcQueryGeneratorContext.Builder contextBuilder = new JdbcQueryGeneratorContext.Builder()
                    .setCatalogName(Optional.of(tableHandle.getCatalogName()))
                    .setTransaction(Optional.of(tableHandle.getTransaction()))
                    .setSchemaTableName(Optional.of(jdbcTableHandle.getSchemaTableName()))
                    .setSelections(selections)
                    .setFrom(Optional.of(table.toString()));
            // If LIMIT has been push down, add it to context
            if (jdbcTableHandle.getLimit().isPresent()) {
                contextBuilder.setLimit(jdbcTableHandle.getLimit());
                contextBuilder.setHasPushDown(true);
            }

            return Optional.of(contextBuilder.build());
        }

        @Override
        public Optional<JdbcQueryGeneratorContext> visitWindow(WindowNode node, Void contextIn)
        {
            checkAvailable(node);
            Optional<JdbcQueryGeneratorContext> sourceContext = node.getSource().accept(this, contextIn);
            if (!sourceContext.isPresent()) {
                return Optional.empty();
            }
            JdbcQueryGeneratorContext context = sourceContext.get();

            List<String> partitionBy = node.getPartitionBy().stream().map(Symbol::getName).collect(toList());

            Optional<String> orderBy = Optional.empty();
            if (node.getOrderingScheme().isPresent()) {
                OrderingScheme scheme = node.getOrderingScheme().get();
                orderBy = Optional.of(statementWriter.orderBy("", scheme.getOrderBy().stream()
                        .map(symbol -> new OrderBy(symbol.getName(), scheme.getOrdering(symbol)))
                        .collect(toList())));
            }

            LinkedHashMap<String, Selection> newSelections = new LinkedHashMap<>(getProjectSelections(context.getSelections()));

            for (Map.Entry<Symbol, WindowNode.Function> functionEntry : node.getWindowFunctions().entrySet()) {
                Symbol windowFunctionColumnName = functionEntry.getKey();
                WindowNode.Function windowFunction = functionEntry.getValue();
                WindowNode.Frame frame = windowFunction.getFrame();

                io.prestosql.spi.sql.expression.Types.WindowFrameType windowFrameType;
                if (frame.getType() == Types.WindowFrameType.RANGE) {
                    windowFrameType = io.prestosql.spi.sql.expression.Types.WindowFrameType.RANGE;
                }
                else if (frame.getType() == Types.WindowFrameType.ROWS) {
                    windowFrameType = io.prestosql.spi.sql.expression.Types.WindowFrameType.ROWS;
                }
                else {
                    throw new PrestoException(JDBC_QUERY_GENERATOR_FAILURE, "Does not support unknown frame type in " + node.getClass().getName());
                }

                Optional<String> startBound;
                Optional<String> endBound;
                Types.FrameBoundType startType = frame.getStartType();
                Types.FrameBoundType endType = frame.getEndType();
                if (frame.getStartValue().isPresent() && frame.getOriginalEndValue().isPresent()) {
                    if (!frame.getOriginalStartValue().isPresent() || !frame.getOriginalEndValue().isPresent()) {
                        throw new PrestoException(JDBC_QUERY_GENERATOR_FAILURE, "Does not support unknown 2 frame bound value in " + node.getClass().getName());
                    }
                    Optional<String> startValue = Optional.of(frame.getOriginalStartValue().get());
                    startBound = Optional.of(frameBound(startType, startValue));
                    Optional<String> endValue = Optional.of(frame.getOriginalEndValue().get());
                    endBound = Optional.of(frameBound(endType, endValue));
                }
                else if (frame.getStartValue().isPresent() && !frame.getEndValue().isPresent()) {
                    if (!frame.getOriginalStartValue().isPresent()) {
                        throw new PrestoException(JDBC_QUERY_GENERATOR_FAILURE, "Does not support start frame bound value in " + node.getClass().getName());
                    }
                    Optional<String> startValue = Optional.of(frame.getOriginalStartValue().get());
                    startBound = Optional.of(frameBound(startType, startValue));
                    endBound = Optional.of(frameBound(endType, Optional.empty()));
                }
                else if (!frame.getStartValue().isPresent() && !frame.getEndValue().isPresent()) {
                    startBound = Optional.of(frameBound(startType, Optional.empty()));
                    endBound = Optional.of(frameBound(endType, Optional.empty()));
                }
                else {
                    throw new PrestoException(JDBC_QUERY_GENERATOR_FAILURE, "Does not support unknown frame start and end value in " + node.getClass().getName());
                }
                Optional<String> frameStr = startBound.map(s -> statementWriter.windowFrame(windowFrameType, s, endBound));

                List<RowExpression> expArgs = windowFunction.getArguments();
                List<String> functionArgs = expArgs.stream().map(expression -> expression.accept(converter, null)).collect(toList());

                String columnStr = statementWriter.window(windowFunction.getSignature().getName(), functionArgs, partitionBy, orderBy, frameStr);
                newSelections.put(windowFunctionColumnName.toString(), new Selection(columnStr, windowFunctionColumnName.toString()));
            }

            return Optional.of(buildAsNewTable(context)
                    .setFrom(getDerivedTable(buildSql(context).getSql(), derivedTableIdentifier++))
                    .setHasPushDown(true)
                    .setSelections(newSelections)
                    .setOutputColumns(node.getOutputSymbols())
                    .build());
        }

        @Override
        public Optional<JdbcQueryGeneratorContext> visitLimit(LimitNode node, Void contextIn)
        {
            checkAvailable(node);
            if (node.isPartial()) {
                throw new PrestoException(NOT_SUPPORTED, "Jdbc query generator cannot handle partial limit");
            }
            Optional<JdbcQueryGeneratorContext> sourceContext = node.getSource().accept(this, contextIn);
            if (!sourceContext.isPresent()) {
                return Optional.empty();
            }
            JdbcQueryGeneratorContext context = sourceContext.get();
            return Optional.of(buildFrom(context)
                    .setHasPushDown(true)
                    .setLimit(OptionalLong.of(node.getCount()))
                    .setOutputColumns(node.getOutputSymbols())
                    .build());
        }

        @Override
        public Optional<JdbcQueryGeneratorContext> visitTopN(TopNNode node, Void contextIn)
        {
            checkAvailable(node);
            Optional<JdbcQueryGeneratorContext> sourceContext = node.getSource().accept(this, contextIn);
            if (!sourceContext.isPresent()) {
                return Optional.empty();
            }
            if (!node.getStep().equals(TopNNode.Step.SINGLE)) {
                throw new PrestoException(NOT_SUPPORTED, "JDBC query generator can only push single logical topN");
            }
            JdbcQueryGeneratorContext context = sourceContext.get();
            OrderingScheme scheme = node.getOrderingScheme();
            return Optional.of(buildAsNewTable(context)
                    .setFrom(getDerivedTable(buildSql(context).getSql(), derivedTableIdentifier++))
                    .setHasPushDown(true)
                    .setSelections(getProjectSelections(context.getSelections()))
                    .setLimit(OptionalLong.of(node.getCount()))
                    .setOrderBy(Optional.of(scheme.getOrderBy().stream()
                            .map(symbol -> new OrderBy(symbol.getName(), scheme.getOrdering(symbol)))
                            .collect(toList())))
                    .setOutputColumns(node.getOutputSymbols())
                    .build());
        }

        @Override
        public Optional<JdbcQueryGeneratorContext> visitGroupId(GroupIdNode node, Void contextIn)
        {
            checkAvailable(node);
            Optional<JdbcQueryGeneratorContext> sourceContext = node.getSource().accept(this, contextIn);
            if (!sourceContext.isPresent()) {
                return Optional.empty();
            }
            JdbcQueryGeneratorContext.GroupIdNodeInfo groupIdNodeInfo = sourceContext.get().getGroupIdNodeInfo();

            String groupingsSets = statementWriter.groupingsSets(node.getGroupingSets().stream()
                    .map(list -> list.stream().map(Symbol::getName).collect(toList()))
                    .collect(toList()));

            Symbol groupIdSymbol = node.getGroupIdSymbol();

            groupIdNodeInfo.getGroupingElementStore().put(groupIdSymbol, groupingsSets);

            LinkedHashMap<String, Selection> newSelections = new LinkedHashMap<>();
            node.getGroupingColumns().forEach((key, value) -> newSelections.put(key.getName(), new Selection(value.getName(), key.getName())));
            node.getAggregationArguments().forEach(symbol -> newSelections.put(symbol.getName(), new Selection(symbol.getName())));

            return Optional.of(buildAsNewTable(sourceContext.get())
                    .setFrom(getDerivedTable(buildSql(sourceContext.get()).getSql(), derivedTableIdentifier++))
                    .setGroupIdNodeInfo(groupIdNodeInfo)
                    .setSelections(newSelections)
                    .build());
        }

        protected void checkAvailable(PlanNode node)
        {
            if (!pushDownModule.isAvailable(node)) {
                throw new PrestoException(JDBC_QUERY_GENERATOR_FAILURE, format("The node [%s] is not support to push down in mode [%s]", node.getClass().getSimpleName(), pushDownModule));
            }
        }
    }
}
