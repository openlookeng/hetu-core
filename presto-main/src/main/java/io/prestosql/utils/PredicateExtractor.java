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
package io.prestosql.utils;

import io.airlift.log.Logger;
import io.prestosql.execution.SqlStageExecution;
import io.prestosql.metadata.TableHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.NotExpression;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.TimeLiteral;
import io.prestosql.sql.tree.TimestampLiteral;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

public class PredicateExtractor
{
    private static final Logger LOG = Logger.get(PredicateExtractor.class);

    public static class Tuple<T1, T2>
    {
        public final T1 first;
        public final T2 second;

        public Tuple(T1 v1, T2 v2)
        {
            first = v1;
            second = v2;
        }
    }

    private PredicateExtractor()
    {
    }

    private static List<PlanNode> getFilterNode(SqlStageExecution stage)
    {
        PlanFragment fragment = stage.getFragment();
        PlanNode root = fragment.getRoot();
        List<PlanNode> result = new LinkedList<>();

        Queue<PlanNode> queue = new LinkedList<>();
        queue.add(root);

        while (!queue.isEmpty()) {
            PlanNode node = queue.poll();
            if (node instanceof FilterNode
                    || node instanceof TableScanNode) {
                result.add(node);
            }

            queue.addAll(node.getSources());
        }

        return result;
    }

    public static boolean isSplitFilterApplicable(SqlStageExecution stage)
    {
        List<PlanNode> filterNodeOptional = getFilterNode(stage);

        if (filterNodeOptional.isEmpty()) {
            return false;
        }

        PlanNode node = filterNodeOptional.get(0);

        if (node instanceof FilterNode) {
            FilterNode filterNode = (FilterNode) node;
            PlanNode sourceNode = filterNode.getSource();
            if (!(sourceNode instanceof TableScanNode)) {
                return false;
            }

            //if a catalog name starts with a $, it's not an normal query, could be something like show tables;
            TableHandle table = ((TableScanNode) sourceNode).getTable();
            String catalogName = table.getCatalogName().getCatalogName();
            if (catalogName.startsWith("$")) {
                return false;
            }

            /* (!(table.getConnectorHandle().isFilterSupported()
             *   && (isSupportedExpression(filterNode.getPredicate())
             *       || (((TableScanNode) sourceNode).getPredicate().isPresent()
             *           && isSupportedExpression(((TableScanNode) sourceNode).getPredicate().get())))))
             */
            if (!table.getConnectorHandle().isFilterSupported()) {
                return false;
            }

            if (!isSupportedExpression(filterNode.getPredicate())
                    && (!((TableScanNode) sourceNode).getPredicate().isPresent()
                    || !isSupportedExpression(((TableScanNode) sourceNode).getPredicate().get()))) {
                return false;
            }
        }

        if (node instanceof TableScanNode) {
            TableScanNode tableScanNode = (TableScanNode) node;
            //if a catalog name starts with a $, it's not an normal query, could be something like show tables;
            TableHandle table = tableScanNode.getTable();
            String catalogName = table.getCatalogName().getCatalogName();
            if (catalogName.startsWith("$")) {
                return false;
            }

            if (!table.getConnectorHandle().isFilterSupported()) {
                return false;
            }

            if (!tableScanNode.getPredicate().isPresent()
                    || !isSupportedExpression(tableScanNode.getPredicate().get())) {
                return false;
            }
        }

        return true;
    }

    private static boolean isSupportedExpression(Expression predicate)
    {
        if (predicate instanceof LogicalBinaryExpression) {
            LogicalBinaryExpression lbExpression = (LogicalBinaryExpression) predicate;
            if ((lbExpression.getOperator() == LogicalBinaryExpression.Operator.AND) ||
                    (lbExpression.getOperator() == LogicalBinaryExpression.Operator.OR)) {
                return isSupportedExpression(lbExpression.getRight()) && isSupportedExpression(lbExpression.getLeft());
            }
        }
        if (predicate instanceof ComparisonExpression) {
            ComparisonExpression comparisonExpression = (ComparisonExpression) predicate;
            switch (comparisonExpression.getOperator()) {
                case EQUAL:
                case GREATER_THAN:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN_OR_EQUAL:
                    return true;
                default:
                    return false;
            }
        }
        if (predicate instanceof InPredicate) {
            return true;
        }

        if (predicate instanceof NotExpression) {
            return true;
        }

        if (predicate instanceof BetweenPredicate) {
            return true;
        }

        return false;
    }

    /**
     * Get the expression and column name assignment map, in case some columns are
     * renamed which results in index not loading correctly.
     *
     * @param stage stage object
     * @return Pair of: Expression and a column name assignment map
     */
    public static Tuple<Optional<Expression>, Map<Symbol, ColumnHandle>> getExpression(SqlStageExecution stage)
    {
        List<PlanNode> filterNodeOptional = getFilterNode(stage);

        if (filterNodeOptional.size() == 0) {
            return new Tuple<>(Optional.empty(), new HashMap<>());
        }

        if (filterNodeOptional.get(0) instanceof FilterNode) {
            FilterNode filterNode = (FilterNode) filterNodeOptional.get(0);
            if (filterNode.getSource() instanceof TableScanNode) {
                TableScanNode tableScanNode = (TableScanNode) filterNode.getSource();
                if (tableScanNode.getPredicate().isPresent()
                        && isSupportedExpression(tableScanNode.getPredicate().get())) { /* if total filter is not supported use the filterNode */
                    return new Tuple<>(tableScanNode.getPredicate(), tableScanNode.getAssignments());
                }

                return new Tuple<>(Optional.of(filterNode.getPredicate()), tableScanNode.getAssignments());
            }

            return new Tuple<>(Optional.empty(), new HashMap<>());
        }

        if (filterNodeOptional.get(0) instanceof TableScanNode) {
            TableScanNode tableScanNode = (TableScanNode) filterNodeOptional.get(0);
            if (tableScanNode.getPredicate().isPresent()) {
                return new Tuple<>(tableScanNode.getPredicate(), tableScanNode.getAssignments());
            }
        }

        return new Tuple<>(Optional.empty(), new HashMap<>());
    }

    public static Optional<String> getFullyQualifiedName(SqlStageExecution stage)
    {
        List<PlanNode> filterNodeOptional = getFilterNode(stage);

        if (filterNodeOptional.size() == 0) {
            return Optional.empty();
        }

        TableScanNode tableScanNode;
        if (filterNodeOptional.get(0) instanceof FilterNode) {
            FilterNode filterNode = (FilterNode) filterNodeOptional.get(0);
            tableScanNode = (TableScanNode) filterNode.getSource();
        }
        else {
            tableScanNode = (TableScanNode) filterNodeOptional.get(0);
        }

        String fullQualifiedTableName = tableScanNode.getTable().getFullyQualifiedName();

        return Optional.of(fullQualifiedTableName);
    }

    protected static Predicate processComparisonExpression(ComparisonExpression comparisonExpression, String fullQualifiedTableName, Map<Symbol, ColumnHandle> assignments)
    {
        Predicate pred = null;
        switch (comparisonExpression.getOperator()) {
            case EQUAL:
            case GREATER_THAN:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN_OR_EQUAL:
                pred = buildPredicate(comparisonExpression, fullQualifiedTableName, assignments);
                break;
            default:
                LOG.warn("ComparisonExpression %s, not supported", comparisonExpression.getOperator().toString());
                break;
        }
        return pred;
    }

    private static Predicate buildPredicate(ComparisonExpression expression, String fullQualifiedTableName, Map<Symbol, ColumnHandle> assignments)
    {
        Predicate predicate = new Predicate();
        predicate.setTableName(fullQualifiedTableName);

        Expression leftExpression = extractExpression(expression.getLeft());

        // if not SymbolReference, return null or throw PrestoException
        // skip the predicate
        if (leftExpression instanceof SymbolReference == false) {
            LOG.warn("Invalid Left of expression %s, should be an SymbolReference", leftExpression.toString());
            return null;
        }
        String columnName = ((SymbolReference) leftExpression).getName();
        Symbol columnSymbol = new Symbol(columnName);
        if (assignments.containsKey(columnSymbol)) {
            columnName = assignments.get(columnSymbol).getColumnName();
        }
        predicate.setColumnName(columnName);

        // Get column value type
        Expression rightExpression = expression.getRight();
        Object object = extract(rightExpression);
        if (object == null) {
            return null;
        }
        predicate.setValue(object);

        // set compare operator
        predicate.setOperator(expression.getOperator());

        return predicate;
    }

    private static Expression extractExpression(Expression expression)
    {
        if (expression instanceof Cast) {
            // extract the inner expression for CAST expressions
            return extractExpression(((Cast) expression).getExpression());
        }
        else {
            return expression;
        }
    }

    private static Object extract(Expression expression)
    {
        if (expression instanceof Cast) {
            return extract(((Cast) expression).getExpression());
        }
        else if (expression instanceof BooleanLiteral) {
            Boolean value = ((BooleanLiteral) expression).getValue();
            return value;
        }
        else if (expression instanceof DecimalLiteral) {
            String value = ((DecimalLiteral) expression).getValue();
            return new BigDecimal(value);
        }
        else if (expression instanceof DoubleLiteral) {
            double value = ((DoubleLiteral) expression).getValue();
            return value;
        }
        else if (expression instanceof LongLiteral) {
            Long value = ((LongLiteral) expression).getValue();
            return value;
        }
        else if (expression instanceof StringLiteral) {
            String value = ((StringLiteral) expression).getValue();
            return value;
        }
        else if (expression instanceof TimeLiteral) {
            String value = ((TimeLiteral) expression).getValue();
            return value;
        }
        else if (expression instanceof TimestampLiteral) {
            String value = ((TimestampLiteral) expression).getValue();
            return Timestamp.valueOf(value).getTime();
        }
        else if (expression instanceof GenericLiteral) {
            GenericLiteral genericLiteral = (GenericLiteral) expression;

            if (genericLiteral.getType().equalsIgnoreCase("bigint")) {
                return Long.valueOf(genericLiteral.getValue());
            }
            else if (genericLiteral.getType().equalsIgnoreCase("real")) {
                return (long) Float.floatToIntBits(Float.parseFloat(genericLiteral.getValue()));
            }
            else if (genericLiteral.getType().equalsIgnoreCase("tinyint")) {
                return Byte.valueOf(genericLiteral.getValue()).longValue();
            }
            else if (genericLiteral.getType().equalsIgnoreCase("smallint")) {
                return Short.valueOf(genericLiteral.getValue()).longValue();
            }
            else if (genericLiteral.getType().equalsIgnoreCase("date")) {
                return LocalDate.parse(genericLiteral.getValue()).toEpochDay();
            }
            else {
                LOG.warn("Not Implemented Exception: %s", genericLiteral.toString());
                return null;
            }
        }
        else {
            LOG.warn("Not Implemented Exception: %s", expression.toString());
            return null;
        }
    }
}
