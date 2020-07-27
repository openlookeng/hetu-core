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
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.TableScanNode;
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
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.TimeLiteral;
import io.prestosql.sql.tree.TimestampLiteral;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;

public class PredicateExtractor
{
    private static final Logger LOG = Logger.get(PredicateExtractor.class);

    private PredicateExtractor()
    {
    }

    private static Optional<FilterNode> getFilterNode(SqlStageExecution stage)
    {
        PlanFragment fragment = stage.getFragment();
        PlanNode root = fragment.getRoot();

        Queue<PlanNode> queue = new LinkedList<>();
        queue.add(root);

        while (!queue.isEmpty()) {
            PlanNode node = queue.poll();
            if (node instanceof FilterNode) {
                return Optional.of((FilterNode) node);
            }

            queue.addAll(node.getSources());
        }

        return Optional.empty();
    }

    public static boolean isSplitFilterApplicable(SqlStageExecution stage)
    {
        Optional<FilterNode> filterNodeOptional = getFilterNode(stage);

        if (!filterNodeOptional.isPresent()) {
            return false;
        }

        FilterNode filterNode = filterNodeOptional.get();

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

        boolean supported = table.getConnectorHandle().isFilterSupported();
        if (!supported || !isSupportedExpression(filterNode.getPredicate())) {
            return false;
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

        return false;
    }

    public static Optional<Expression> getExpression(SqlStageExecution stage)
    {
        Optional<FilterNode> filterNodeOptional = getFilterNode(stage);

        if (!filterNodeOptional.isPresent()) {
            return Optional.empty();
        }

        FilterNode filterNode = filterNodeOptional.get();
        Expression predicate = filterNode.getPredicate();

        return Optional.of(predicate);
    }

    public static Optional<String> getFullyQualifiedName(SqlStageExecution stage)
    {
        Optional<FilterNode> filterNodeOptional = getFilterNode(stage);

        if (!filterNodeOptional.isPresent()) {
            return Optional.empty();
        }

        FilterNode filterNode = filterNodeOptional.get();
        TableScanNode tableScanNode = (TableScanNode) filterNode.getSource();
        String fullQualifiedTableName = tableScanNode.getTable().getFullyQualifiedName();

        return Optional.of(fullQualifiedTableName);
    }

    public static Predicate processComparisonExpression(ComparisonExpression comparisonExpression, String fullQualifiedTableName)
    {
        Predicate pred = null;
        switch (comparisonExpression.getOperator()) {
            case EQUAL:
            case GREATER_THAN:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN_OR_EQUAL:
                pred = buildPredicate(comparisonExpression, fullQualifiedTableName);
                break;
            default:
                LOG.warn("ComparisonExpression %s, not supported", comparisonExpression.getOperator().toString());
                break;
        }
        return pred;
    }

    private static Predicate buildPredicate(ComparisonExpression expression, String fullQualifiedTableName)
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
