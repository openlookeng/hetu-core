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
package io.prestosql.sql.builder;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.sql.SqlQueryWriter;
import io.prestosql.spi.sql.expression.OrderBy;
import io.prestosql.spi.sql.expression.Selection;
import io.prestosql.spi.sql.expression.Types;
import io.prestosql.sql.planner.OrderingScheme;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.ExceptNode;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.GroupIdNode;
import io.prestosql.sql.planner.plan.IntersectNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.OffsetNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanVisitor;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.RowNumberNode;
import io.prestosql.sql.planner.plan.SetOperationNode;
import io.prestosql.sql.planner.plan.SortNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.plan.TopNNode;
import io.prestosql.sql.planner.plan.UnionNode;
import io.prestosql.sql.planner.plan.UnnestNode;
import io.prestosql.sql.planner.plan.ValuesNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SortItem;
import io.prestosql.sql.tree.SubscriptExpression;
import io.prestosql.sql.tree.SymbolReference;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.stream.Collectors;

import static io.prestosql.sql.tree.WindowFrame.Type.RANGE;
import static io.prestosql.sql.tree.WindowFrame.Type.ROWS;

public class SqlQueryBuilder
        extends PlanVisitor<String, SqlQueryBuilder.Context>
{
    private static final Logger logger = Logger.get(SqlQueryBuilder.class);

    private final Metadata metadata;
    private final Session session;
    private final Map<PlanNode, CacheValue> cache = new WeakHashMap<>();
    private GroupIdNodeInfo groupIdNodeInfo;

    public SqlQueryBuilder(Metadata metadata, Session session)
    {
        this.metadata = metadata;
        this.session = session;
        this.groupIdNodeInfo = new GroupIdNodeInfo();
    }

    public Optional<Result> build(PlanNode root)
    {
        TableHandle tableHandle;
        String sql;
        CacheValue cacheValue = this.cache.get(root);

        if (cacheValue != null) {
            tableHandle = cacheValue.tableHandle;
            sql = cacheValue.query;
        }
        else {
            // Build SQL query from the sub-tree
            try {
                Context context = new Context();
                sql = root.accept(this, context);
                tableHandle = context.tableHandle;
                this.cache.put(root, new CacheValue(sql, tableHandle));
            }
            catch (Exception ex) {
                this.cache.put(root, CacheValue.EMPTY_VALUE);
                if (logger.isDebugEnabled()) {
                    logger.debug("query build failed... cause by %s\n", ex.getMessage());
                }
                return Optional.empty();
            }
        }
        if (sql != null && tableHandle != null) {
            if (isGroupByComplexOperation()) {
                Set<String> aliasSet = this.groupIdNodeInfo.aliasGraph.getAliasColumns();
                List<String> aliaList = new ArrayList<>(aliasSet);
                aliaList.sort((o1, o2) -> Integer.compare(o2.length(), o1.length()));
                for (String alias : aliaList) {
                    Optional<String> leafSource = this.groupIdNodeInfo.aliasGraph.findLeafSource(alias);
                    if (leafSource.isPresent()) {
                        sql = sql.replace(alias, leafSource.get());
                    }
                    else {
                        // group by with complex op does not support alias, return empty not to do subquery-pushdown
                        return Optional.empty();
                    }
                }
            }
            return Optional.of(new Result(sql, tableHandle));
        }
        return Optional.empty();
    }

    public boolean isGroupByComplexOperation()
    {
        return this.groupIdNodeInfo.isGroupByComplexOperation;
    }

    public boolean isGroupByWithGroupingFunction(PlanNode proNode)
    {
        return this.groupIdNodeInfo.groupingProjectNodes.contains(proNode);
    }

    public Optional<String> aliasBack(String aliasName)
    {
        return this.groupIdNodeInfo.aliasGraph.findLeafSource(aliasName);
    }

    @Override
    protected String visitPlan(PlanNode node, SqlQueryBuilder.Context context)
    {
        throw new UnsupportedOperationException("VisitPlan does not support " + node.getClass().getName());
    }

    @Override
    public String visitWindow(WindowNode node, SqlQueryBuilder.Context context)
    {
        String from = visitChild(node, node.getSource(), context);

        List<Symbol> partitionBySymbolsList = node.getPartitionBy();
        List<String> partitionBy = partitionBySymbolsList.stream().map(Symbol::toString).collect(Collectors.toList());

        Optional<OrderingScheme> orderingSchemeOptional = node.getOrderingScheme();
        Optional<String> orderBy;
        if (orderingSchemeOptional.isPresent()) {
            OrderingScheme orderingScheme = orderingSchemeOptional.get();
            Map<Symbol, SortOrder> orderingMap = orderingScheme.getOrderings();
            List<OrderBy> orders = new ArrayList<>();
            orderingMap.forEach((key, value) -> {
                orders.add(new OrderBy(key.toString(), value));
            });
            orderBy = Optional.of(context.queryWriter.orderBy(orders));
        }
        else {
            orderBy = Optional.empty();
        }

        // do not find if there will be multiple frame in one window, wait to verify
        List<WindowNode.Frame> framesList = node.getFrames();
        WindowNode.Frame frame = framesList.get(0);
        Types.WindowFrameType windowFrameType;
        if (frame.getType() == RANGE) {
            windowFrameType = Types.WindowFrameType.RANGE;
        }
        else if (frame.getType() == ROWS) {
            windowFrameType = Types.WindowFrameType.ROWS;
        }
        else {
            throw new UnsupportedOperationException("Does not support unknown frame type in " + node.getClass().getName());
        }

        Optional<String> startBound;
        Optional<String> endBound;
        Types.FrameBoundType startType = Types.FrameBoundType.valueOf(frame.getStartType().name());
        Types.FrameBoundType endType = Types.FrameBoundType.valueOf(frame.getEndType().name());
        if (frame.getStartValue().isPresent() && frame.getEndValue().isPresent()) {
            if (!frame.getOriginalStartValue().isPresent() || !frame.getOriginalEndValue().isPresent()) {
                throw new UnsupportedOperationException("Does not support unknown 2 frame bound value in " + node.getClass().getName());
            }
            Optional<String> startValue = Optional.of(visitExpression(node, frame.getOriginalStartValue().get(), context));
            startBound = Optional.of(context.queryWriter.frameBound(startType, startValue));
            Optional<String> endValue = Optional.of(visitExpression(node, frame.getOriginalEndValue().get(), context));
            endBound = Optional.of(context.queryWriter.frameBound(endType, endValue));
        }
        else if (frame.getStartValue().isPresent() && !frame.getEndValue().isPresent()) {
            if (!frame.getOriginalStartValue().isPresent()) {
                throw new UnsupportedOperationException("Does not support unknown start frame bound value in " + node.getClass().getName());
            }
            Optional<String> startValue = Optional.of(visitExpression(node, frame.getOriginalStartValue().get(), context));
            startBound = Optional.of(context.queryWriter.frameBound(startType, startValue));
            endBound = Optional.of(context.queryWriter.frameBound(endType, Optional.empty()));
        }
        else if (!frame.getStartValue().isPresent() && !frame.getEndValue().isPresent()) {
            startBound = Optional.of(context.queryWriter.frameBound(startType, Optional.empty()));
            endBound = Optional.of(context.queryWriter.frameBound(endType, Optional.empty()));
        }
        else {
            throw new UnsupportedOperationException("Does not support unknown frame start and end value in " + node.getClass().getName());
        }
        Optional<String> frameStr = startBound.map(s -> context.queryWriter.windowFrame(windowFrameType, s, endBound));

        // function name and signature
        WindowNode.Function function = null;
        Symbol windowsFunctionColumnName = null;
        Map<Symbol, WindowNode.Function> functionMap = node.getWindowFunctions();
        for (Map.Entry<Symbol, WindowNode.Function> entry : functionMap.entrySet()) {
            // there is only one key-value in this map
            windowsFunctionColumnName = entry.getKey();
            function = entry.getValue();
        }
        if (function == null || windowsFunctionColumnName == null) {
            throw new UnsupportedOperationException("Does not support null function name in " + node.getClass().getName());
        }
        List<Expression> expArgs = function.getArguments();
        List<String> functionArgs = expArgs.stream().map(expression -> visitExpression(node, expression, context)).collect(Collectors.toList());

        String windows = context.queryWriter.window(partitionBy, orderBy, frameStr);

        String columnStr = context.queryWriter.formatWindowColumn(function.getSignature().getName(), functionArgs, windows) + " AS " + windowsFunctionColumnName.toString();

        List<Symbol> outputSymbolsList = node.getOutputSymbols();
        List<Symbol> newSymbolList = new ArrayList<>();
        for (Symbol symbol : outputSymbolsList) {
            if (!symbol.toString().equals(windowsFunctionColumnName.toString())) {
                newSymbolList.add(symbol);
            }
        }
        newSymbolList.add(new Symbol(columnStr));
        List<Selection> selections = newSymbolList.stream().map(symbol -> new Selection(symbol.toString())).collect(Collectors.toList());

        return context.queryWriter.select(selections, from);
    }

    @Override
    public String visitGroupId(GroupIdNode node, SqlQueryBuilder.Context context)
    {
        // from must be first
        String from = visitChild(node, node.getSource(), context);

        // group id save
        List<List<Symbol>> groSets = node.getGroupingSets();
        List<List<String>> strGroSet = groSets.stream().map(list -> list.stream().map(Symbol::getName).collect(Collectors.toList())).collect(Collectors.toList());
        String elementIdStr = context.queryWriter.groupByIdElement(strGroSet);
        Symbol groIdSym = node.getGroupIdSymbol();
        this.groupIdNodeInfo.groupingElementStore.put(groIdSym, elementIdStr);

        // alias source string
        Map<Symbol, Symbol> groColMap = node.getGroupingColumns();
        // we sure that the group id node is not the leaf node
        Set<Symbol> inputSymSet = node.getInputSymbols();
        List<Selection> sourceSelections = new ArrayList<>();
        Map<String, String> aliasMap = new HashMap<>();
        for (Symbol symbol : inputSymSet) {
            boolean isAlias = false;
            for (Map.Entry<Symbol, Symbol> entry : groColMap.entrySet()) {
                Symbol key = entry.getKey();
                Symbol value = entry.getValue();
                if (symbol.getName().equals(value.getName())) {
                    sourceSelections.add(new Selection(symbol.getName(), key.getName()));
                    aliasMap.put(key.getName(), symbol.getName());
                    isAlias = true;
                }
            }
            if (!isAlias) {
                sourceSelections.add(new Selection(symbol.getName()));
            }
        }
        String aliasSourceFromString = context.queryWriter.select(sourceSelections, from);
        this.groupIdNodeInfo.aliasGraph.addNewAliasRelations(aliasMap);
        List<Symbol> nodeOutputSymSet = node.getOutputSymbols();
        List<Selection> selections = nodeOutputSymSet.stream().filter(symbol -> !symbol.getName().equals(groIdSym.getName()))
                .map(symbol -> new Selection(symbol.getName())).collect(Collectors.toList());
        return context.queryWriter.select(selections, aliasSourceFromString);
    }

    @Override
    public String visitValues(ValuesNode node, SqlQueryBuilder.Context context)
    {
        throw new UnsupportedOperationException("Does not support " + node.getClass().getName());
    }

    private String visitSetOperation(SetOperationNode node, Types.SetOperator type, SqlQueryBuilder.Context context)
    {
        if (node.getSources().size() < 2) {
            throw new UnsupportedOperationException("Does not support tables' num smaller than 2 in union node: " + node.getClass().getName());
        }
        List<PlanNode> sourceNodes = node.getSources();
        List<String> fromList = new ArrayList<>();
        List<TableHandle> tableHandles = new ArrayList<>();
        List<Context> contexts = new ArrayList<>();
        Map<String, Integer> firstSourceColumnCount = new HashMap<>();
        for (int index = 0; index < sourceNodes.size(); index++) {
            // if there is one child fail to rewrite, this union node will stop rewriting
            PlanNode childPlanNode = sourceNodes.get(index);
            Context temp = new Context();
            String from = visitChild(node, childPlanNode, temp);
            List<String> selectionsStr = new ArrayList<>();
            List<Symbol> outList = node.sourceOutputLayout(index);
            for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                selectionsStr.add(outList.get(i).getName());
            }
            // to deal with the same name in selections
            Map<String, Integer> countSelectMap = new HashMap<>();
            List<Selection> selectionsOrder = new ArrayList<>();
            for (String str : selectionsStr) {
                if (countSelectMap.containsKey(str)) {
                    int preCount = countSelectMap.get(str);
                    countSelectMap.put(str, preCount + 1);
                    String nowAliasName = str + (preCount + 1);
                    selectionsOrder.add(new Selection(str, nowAliasName));
                }
                else {
                    countSelectMap.put(str, 1);
                    selectionsOrder.add(new Selection(str));
                }
            }
            // cache the first source column count
            if (index == 0) {
                firstSourceColumnCount.putAll(countSelectMap);
            }
            String orderedFrom = temp.queryWriter.select(selectionsOrder, from);
            fromList.add(orderedFrom);
            tableHandles.add(temp.getTableHandle());
            contexts.add(temp);
        }
        if (!isOneDataSourceCatalogName(tableHandles)) {
            throw new UnsupportedOperationException("Does not support multiple catalog source in union node: " + node.getClass().getName());
        }
        Context cc = contexts.get(0);
        context.setTableHandle(cc.tableHandle);
        context.setQueryWriter(cc.queryWriter);

        // FOR Set operators, select first source's symbol as set's output symbol
        List<Selection> selections = new ArrayList<>();
        for (Map.Entry<Symbol, SymbolReference> entry : node.sourceSymbolMap(0).entrySet()) {
            String sourceName = entry.getValue().getName();
            String aliasName = entry.getKey().getName();
            int count = firstSourceColumnCount.get(sourceName);
            if (count == 1) {
                selections.add(new Selection(sourceName, aliasName));
            }
            else {
                String aliasSourceName = sourceName + count;
                selections.add(new Selection(aliasSourceName, aliasName));
                count--;
                firstSourceColumnCount.put(sourceName, count);
            }
        }

        return context.queryWriter.setOperator(selections, type, fromList);
    }

    private boolean isOneDataSourceCatalogName(List<TableHandle> tableHandles)
    {
        String fName = tableHandles.get(0).getCatalogName().getCatalogName();
        for (TableHandle tableHandle : tableHandles) {
            if (!fName.equals(tableHandle.getCatalogName().getCatalogName())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String visitUnion(UnionNode node, SqlQueryBuilder.Context context)
    {
        // union distinct have rewritten to union all with group by, so currently, union just only rewrite to union all
        return visitSetOperation(node, Types.SetOperator.UNION_ALL, context);
    }

    @Override
    public String visitIntersect(IntersectNode node, SqlQueryBuilder.Context context)
    {
        return visitSetOperation(node, Types.SetOperator.INTERSECT_DISTINCT, context);
    }

    @Override
    public String visitExcept(ExceptNode node, SqlQueryBuilder.Context context)
    {
        return visitSetOperation(node, Types.SetOperator.EXCEPT_DISTINCT, context);
    }

    @Override
    public String visitUnnest(UnnestNode node, SqlQueryBuilder.Context context)
    {
        throw new UnsupportedOperationException("Does not support " + node.getClass().getName());
    }

    @Override
    public String visitRowNumber(RowNumberNode node, SqlQueryBuilder.Context context)
    {
        throw new UnsupportedOperationException("Does not support " + node.getClass().getName());
    }

    @Override
    public String visitOffset(OffsetNode node, SqlQueryBuilder.Context context)
    {
        throw new UnsupportedOperationException("Does not support " + node.getClass().getName());
    }

    @Override
    public String visitJoin(JoinNode node, SqlQueryBuilder.Context context)
    {
        Context leftContext = new Context();
        String left = visitChild(node, node.getLeft(), leftContext);

        Context rightContext = new Context();
        String right = visitChild(node, node.getRight(), rightContext);

        if (!leftContext.tableHandle.getCatalogName().equals(rightContext.tableHandle.getCatalogName())) {
            throw new UnsupportedOperationException("Cannot push the query joining sources from two different catalogs");
        }

        context.setTableHandle(leftContext.tableHandle);
        context.setQueryWriter(leftContext.queryWriter);

        Types.JoinType type;

        // Add join type
        if (node.isCrossJoin()) {
            type = Types.JoinType.CROSS;
        }
        else {
            type = Types.JoinType.valueOf(node.getType().toString());
        }

        String leftName = context.queryWriter.queryAlias(node.getLeft().getId().toString());
        String rightName = context.queryWriter.queryAlias(node.getRight().getId().toString());
        Map<String, Selection> fullNames = new HashMap<>();
        node.getLeft().getOutputSymbols()
                .forEach(symbol -> {
                    String symbolName = symbol.getName();
                    fullNames.put(symbolName, new Selection(context.queryWriter.qualifiedName(leftName, symbolName), symbolName));
                });
        node.getRight().getOutputSymbols()
                .forEach(symbol -> {
                    String symbolName = symbol.getName();
                    fullNames.put(symbolName, new Selection(context.queryWriter.qualifiedName(rightName, symbolName), symbolName));
                });
        List<String> criteria = node.getCriteria()
                .stream()
                .map(JoinNode.EquiJoinClause::toExpression)
                .map(exp -> visitExpression(node, exp, context, fullNames))
                .collect(Collectors.toList());

        Optional<String> filter = node.getFilter().map(exp -> visitExpression(node, exp, context, fullNames));

        return context.queryWriter.join(symbols(node), type, left, leftName, right, rightName, criteria, filter);
    }

    @Override
    public String visitProject(ProjectNode node, SqlQueryBuilder.Context context)
    {
        String from = visitChild(node, node.getSource(), context);

        List<Selection> symbols = new ArrayList<>(node.getAssignments().size());
        for (Map.Entry<Symbol, Expression> assignment : node.getAssignments().entrySet()) {
            Expression expr = assignment.getValue();
            if (expr instanceof SubscriptExpression && isGroupByComplexOperation()) {
                SubscriptExpression eer = (SubscriptExpression) expr;
                String indexStr = eer.getIndex().toString();
                if (indexStr.contains("groupid") && indexStr.contains("1")) {
                    this.groupIdNodeInfo.groupingProjectNodes.add(node);
                    logger.info("for now we do not support grouping function in group by clause!");
                }
            }
            String builtExpression = visitExpression(node, assignment.getValue(), context);
            symbols.add(new Selection(builtExpression, assignment.getKey().getName()));
        }
        return context.queryWriter.select(symbols, from);
    }

    @Override
    public String visitAggregation(AggregationNode node, SqlQueryBuilder.Context context)
    {
        String from = visitChild(node, node.getSource(), context);

        Map<Symbol, AggregationNode.Aggregation> aggregations = node.getAggregations();
        List<Selection> symbols = new ArrayList<>(aggregations.size());
        for (Symbol symbol : node.getOutputSymbols()) {
            AggregationNode.Aggregation aggregation = aggregations.get(symbol);
            if (aggregation == null) {
                symbols.add(new Selection(symbol.getName()));
            }
            else {
                Signature signature = aggregation.getSignature();
                FunctionCall functionCall = new FunctionCall(Optional.empty(),
                        QualifiedName.of(signature.getName()),
                        Optional.empty(),
                        aggregation.getFilter().map(Symbol::toSymbolReference),
                        aggregation.getOrderingScheme().map(SqlQueryBuilder::sortItemToSortOrder),
                        aggregation.isDistinct(),
                        aggregation.getArguments());
                String expression = visitExpression(node, new Cast(functionCall, aggregation.getSignature().getReturnType().toString()), context);
                symbols.add(new Selection(expression, symbol.getName()));
            }
        }

        // remove groupid column and add GROUPING SETS
        Optional<Symbol> groupIdSymbolOp = node.getGroupIdSymbol();
        if (groupIdSymbolOp.isPresent() && this.groupIdNodeInfo.groupingElementStore.containsKey(groupIdSymbolOp.get())) {
            String idElementString = this.groupIdNodeInfo.groupingElementStore.get(groupIdSymbolOp.get());
            Optional<String> eleStr = Optional.of(idElementString);
            Optional<Selection> selectionOptional = Optional.empty();
            for (Selection s : symbols) {
                String selectStr = s.getExpression();
                if (selectStr.equals(groupIdSymbolOp.get().getName())) {
                    selectionOptional = Optional.of(s);
                }
            }
            selectionOptional.ifPresent(symbols::remove);
            String reSql = context.queryWriter.aggregation(symbols, Optional.empty(), eleStr, from);
            this.groupIdNodeInfo.isGroupByComplexOperation = true;
            return reSql;
        }
        else {
            List<String> groupingKeys = node.getGroupingSets()
                    .getGroupingKeys()
                    .stream()
                    .map(Symbol::getName)
                    .collect(Collectors.toList());
            return context.queryWriter.aggregation(symbols, Optional.of(groupingKeys), Optional.empty(), from);
        }
    }

    @Override
    public String visitSort(SortNode node, SqlQueryBuilder.Context context)
    {
        String from = visitChild(node, node.getSource(), context);
        OrderingScheme scheme = node.getOrderingScheme();
        List<OrderBy> orderings = scheme
                .getOrderBy()
                .stream()
                .map(symbol -> new OrderBy(symbol.getName(), scheme.getOrdering(symbol)))
                .collect(Collectors.toList());
        return context.queryWriter.sort(symbols(node), orderings, from);
    }

    @Override
    public String visitTopN(TopNNode node, SqlQueryBuilder.Context context)
    {
        String from = visitChild(node, node.getSource(), context);
        OrderingScheme scheme = node.getOrderingScheme();
        List<OrderBy> orderings = scheme
                .getOrderBy()
                .stream()
                .map(symbol -> new OrderBy(symbol.getName(), scheme.getOrdering(symbol)))
                .collect(Collectors.toList());
        return context.queryWriter.topN(symbols(node), orderings, node.getCount(), from);
    }

    @Override
    public String visitFilter(FilterNode node, SqlQueryBuilder.Context context)
    {
        String from = visitChild(node, node.getSource(), context);
        List<Selection> symbols = new ArrayList<>();
        symbols.add(new Selection("*"));
        return context.queryWriter.filter(symbols, visitExpression(node, node.getPredicate(), context), from);
    }

    @Override
    public String visitLimit(LimitNode node, SqlQueryBuilder.Context context)
    {
        String from = visitChild(node, node.getSource(), context);
        return context.queryWriter.limit(symbols(node), node.getCount(), from);
    }

    @Override
    public String visitTableScan(TableScanNode node, SqlQueryBuilder.Context context)
    {
        TupleDomain<ColumnHandle> constraint = node.getEnforcedConstraint();
        if (constraint != null && constraint.getDomains().isPresent()) {
            if (!constraint.getDomains().get().isEmpty()) {
                // Predicate is pushed down
                throw new UnsupportedOperationException("Cannot push down table scan with predicates pushed down");
            }
        }

        // The qualified name can be just the table name in unsupported connectors
        // However, they will be ignored in the ConnectorMetadata#applySubQuery method.
        Optional<SqlQueryWriter> queryWriter = metadata.getSqlQueryWriter(session, node.getTable());
        if (queryWriter.isPresent()) {
            context.setTableHandle(node.getTable());
            context.setQueryWriter(queryWriter.get());
        }
        else {
            // This connector does not support query rewriting
            throw new UnsupportedOperationException(node.getTable().getCatalogName().getCatalogName() + " does not support query rewriting");
        }

        String qualifiedName = node.getTable().getConnectorHandle().getSchemaPrefixedTableName();
        if (node.getAssignments().isEmpty()) {
            return qualifiedName;
        }

        try {
            List<Selection> symbols = new ArrayList<>(node.getAssignments().size());

            //add symbols by output symbols's order
            for (Symbol outputSymbol : node.getOutputSymbols()) {
                ColumnHandle column = node.getAssignments().get(outputSymbol);
                symbols.add(new Selection(column.getColumnName(), outputSymbol.getName()));
            }
            return context.queryWriter.select(symbols, qualifiedName);
        }
        catch (NotImplementedException ex) {
            // TableScanNode with this column handle cannot be used to push query down
            throw new UnsupportedOperationException("A ColumnHandle of " + qualifiedName + " does not support query push down");
        }
    }

    private String visitChild(PlanNode parent, PlanNode child, SqlQueryBuilder.Context context)
    {
        try {
            String result = child.accept(this, context);
            this.cache.put(child, new CacheValue(result, context.tableHandle));
            return result;
        }
        catch (UnsupportedOperationException ex) {
            this.cache.put(child, CacheValue.EMPTY_VALUE);
            this.cache.put(parent, CacheValue.EMPTY_VALUE);
            throw ex;
        }
    }

    private String visitExpression(PlanNode parent, Expression expression, Context context)
    {
        try {
            return ExpressionFormatter.formatExpression(context.queryWriter, expression, Optional.empty());
        }
        catch (UnsupportedOperationException ex) {
            this.cache.put(parent, CacheValue.EMPTY_VALUE);
            throw ex;
        }
    }

    private String visitExpression(PlanNode parent, Expression expression, Context context, Map<String, Selection> qualifiedNames)
    {
        try {
            return ExpressionFormatter.formatExpression(context.queryWriter, expression, Optional.empty(), qualifiedNames);
        }
        catch (UnsupportedOperationException ex) {
            this.cache.put(parent, CacheValue.EMPTY_VALUE);
            throw ex;
        }
    }

    private static List<Selection> symbols(PlanNode node)
    {
        return node.getOutputSymbols()
                .stream()
                .map(symbol -> new Selection(symbol.getName()))
                .collect(Collectors.toList());
    }

    public static class Result
    {
        private final String query;
        private final TableHandle tableHandle;

        public Result(String query, TableHandle tableHandle)
        {
            this.query = query;
            this.tableHandle = tableHandle;
        }

        public String getQuery()
        {
            return query;
        }

        public TableHandle getTableHandle()
        {
            return tableHandle;
        }
    }

    private static class CacheValue
    {
        private final String query;
        private final TableHandle tableHandle;
        private static final CacheValue EMPTY_VALUE = new CacheValue(null, null);

        private CacheValue(String query, TableHandle tableHandle)
        {
            this.query = query;
            this.tableHandle = tableHandle;
        }
    }

    public static class Context
    {
        private TableHandle tableHandle;
        private SqlQueryWriter queryWriter;

        public TableHandle getTableHandle()
        {
            return tableHandle;
        }

        public void setTableHandle(TableHandle tableHandle)
        {
            this.tableHandle = tableHandle;
        }

        private void setQueryWriter(SqlQueryWriter queryWriter)
        {
            this.queryWriter = queryWriter;
        }
    }

    private class GroupIdNodeInfo
    {
        private boolean isGroupByComplexOperation;
        private Map<Symbol, String> groupingElementStore;
        private AliasGraph aliasGraph;
        private Set<PlanNode> groupingProjectNodes;

        GroupIdNodeInfo()
        {
            this.groupingElementStore = new HashMap<>();
            this.aliasGraph = new AliasGraph();
            this.groupingProjectNodes = new HashSet<>();
        }
    }

    private class AliasGraph
    {
        // the graph should not have cycle
        private Map<String, String> nearDirectedNode;

        public AliasGraph(Map<String, String> init)
        {
            this.nearDirectedNode = new HashMap<>();
            this.nearDirectedNode.putAll(init);
        }

        AliasGraph()
        {
            this.nearDirectedNode = new HashMap<>();
        }

        /**
         * this method do not support graph has circle
         */
        public Optional<String> findLeafSource(String nodeName)
        {
            if (this.nearDirectedNode.containsKey(nodeName)) {
                String value = this.nearDirectedNode.get(nodeName);
                if (!this.nearDirectedNode.containsKey(value)) {
                    return Optional.of(value);
                }
                return findLeafSource(value);
            }
            else {
                return Optional.empty();
            }
        }

        public void addNewAliasRelation(String alias, String nearSource)
        {
            this.nearDirectedNode.put(alias, nearSource);
        }

        public void addNewAliasRelations(Map<String, String> addition)
        {
            this.nearDirectedNode.putAll(addition);
        }

        public Map<String, String> getNearDirectedNode()
        {
            return nearDirectedNode;
        }

        public Set<String> getAliasColumns()
        {
            return new HashSet<>(this.nearDirectedNode.keySet());
        }
    }

    private static io.prestosql.sql.tree.OrderBy sortItemToSortOrder(OrderingScheme orderingScheme)
    {
        ImmutableList.Builder<SortItem> builder = new ImmutableList.Builder<>();
        for (Symbol symbol : orderingScheme.getOrderBy()) {
            SortOrder sortOrder = orderingScheme.getOrdering(symbol);
            SortItem sortItem;
            switch (sortOrder) {
                case ASC_NULLS_LAST:
                    sortItem = new SortItem(symbol.toSymbolReference(), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.LAST);
                    break;
                case ASC_NULLS_FIRST:
                    sortItem = new SortItem(symbol.toSymbolReference(), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.FIRST);
                    break;
                case DESC_NULLS_FIRST:
                    sortItem = new SortItem(symbol.toSymbolReference(), SortItem.Ordering.DESCENDING, SortItem.NullOrdering.FIRST);
                    break;
                case DESC_NULLS_LAST:
                    sortItem = new SortItem(symbol.toSymbolReference(), SortItem.Ordering.DESCENDING, SortItem.NullOrdering.LAST);
                    break;
                default:
                    throw new UnsupportedOperationException(sortOrder + " is not supported");
            }
            builder.add(sortItem);
        }
        return new io.prestosql.sql.tree.OrderBy(builder.build());
    }
}
