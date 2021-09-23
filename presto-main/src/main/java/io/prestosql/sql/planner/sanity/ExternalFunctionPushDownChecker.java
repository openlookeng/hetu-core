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
package io.prestosql.sql.planner.sanity;

import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.expressions.DefaultRowExpressionTraversalVisitor;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.ErrorCodeSupplier;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;
import io.prestosql.sql.planner.plan.SpatialJoinNode;
import io.prestosql.sql.planner.plan.StatisticAggregations;
import io.prestosql.sql.planner.plan.TableDeleteNode;
import io.prestosql.sql.planner.plan.TableFinishNode;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.sql.planner.plan.VacuumTableNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.prestosql.spi.connector.CatalogSchemaName.DEFAULT_NAMESPACE;
import static io.prestosql.sql.relational.OriginalExpressionUtils.isOriginalExpression;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * To check external function call expression appear in the plan tree.
 * If it appears, throw exception to show that we do not support to execute external function now.
 */
public class ExternalFunctionPushDownChecker
        implements PlanSanityChecker.Checker
{
    @Override
    public void validate(PlanNode planNode, Session session, Metadata metadata, TypeAnalyzer typeAnalyzer, TypeProvider types, WarningCollector warningCollector)
    {
        Set<String> set = new HashSet<>();
        planNode.accept(new ExternalFunctionFinder(), set);
        if (set.size() > 0) {
            String allErrorFun = set.stream().map(String::toString).collect(Collectors.joining(", "));
            throw new IllegalExternalFunctionUsageException(
                    GENERIC_USER_ERROR,
                    format("The external function %s does not support to push down to data source for this query.", allErrorFun));
        }
    }

    private static class ExternalFunctionFinder
            extends InternalPlanVisitor<Void, Set<String>>
    {
        @Override
        public Void visitPlan(PlanNode node, Set<String> context)
        {
            for (PlanNode planNode : node.getSources()) {
                planNode.accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Set<String> context)
        {
            for (PlanNode planNode : node.getSources()) {
                planNode.accept(this, context);
            }
            visitRowExpressions(context, node.getPredicate());
            return null;
        }

        @Override
        public Void visitValues(ValuesNode node, Set<String> context)
        {
            for (PlanNode planNode : node.getSources()) {
                planNode.accept(this, context);
            }
            for (List<RowExpression> rList : node.getRows()) {
                visitRowExpressions(context, rList);
            }
            return null;
        }

        @Override
        public Void visitProject(ProjectNode node, Set<String> context)
        {
            for (PlanNode planNode : node.getSources()) {
                planNode.accept(this, context);
            }
            visitRowExpressions(context, new ArrayList<>(node.getAssignments().getExpressions()));
            return null;
        }

        @Override
        public Void visitApply(ApplyNode node, Set<String> context)
        {
            for (PlanNode planNode : node.getSources()) {
                planNode.accept(this, context);
            }
            visitRowExpressions(context, new ArrayList<>(node.getSubqueryAssignments().getExpressions()));
            return null;
        }

        @Override
        public Void visitWindow(WindowNode node, Set<String> context)
        {
            for (PlanNode planNode : node.getSources()) {
                planNode.accept(this, context);
            }
            for (WindowNode.Function function : node.getWindowFunctions().values()) {
                visitRowExpressions(context, function.getArguments());
                visitRowExpressions(context, function.getFunctionCall());
            }
            return null;
        }

        @Override
        public Void visitJoin(JoinNode node, Set<String> context)
        {
            for (PlanNode planNode : node.getSources()) {
                planNode.accept(this, context);
            }
            node.getFilter().ifPresent(rowExpression -> visitRowExpressions(context, rowExpression));
            return null;
        }

        @Override
        public Void visitSpatialJoin(SpatialJoinNode node, Set<String> context)
        {
            for (PlanNode planNode : node.getSources()) {
                planNode.accept(this, context);
            }
            visitRowExpressions(context, node.getFilter());
            return null;
        }

        @Override
        public Void visitAggregation(AggregationNode node, Set<String> context)
        {
            for (PlanNode planNode : node.getSources()) {
                planNode.accept(this, context);
            }
            for (AggregationNode.Aggregation agg : node.getAggregations().values()) {
                visitRowExpressions(context, agg.getArguments());
                visitRowExpressions(context, agg.getFunctionCall());
            }
            return null;
        }

        @Override
        public Void visitTableFinish(TableFinishNode node, Set<String> context)
        {
            for (PlanNode planNode : node.getSources()) {
                planNode.accept(this, context);
            }
            Optional<StatisticAggregations> statisticAggregations = node.getStatisticsAggregation();
            if (statisticAggregations.isPresent()) {
                for (AggregationNode.Aggregation agg : statisticAggregations.get().getAggregations().values()) {
                    visitRowExpressions(context, agg.getArguments());
                    visitRowExpressions(context, agg.getFunctionCall());
                }
            }
            return null;
        }

        @Override
        public Void visitTableWriter(TableWriterNode node, Set<String> context)
        {
            for (PlanNode planNode : node.getSources()) {
                planNode.accept(this, context);
            }
            Optional<StatisticAggregations> statisticAggregations = node.getStatisticsAggregation();
            if (statisticAggregations.isPresent()) {
                for (AggregationNode.Aggregation agg : statisticAggregations.get().getAggregations().values()) {
                    visitRowExpressions(context, agg.getArguments());
                    visitRowExpressions(context, agg.getFunctionCall());
                }
            }
            return null;
        }

        @Override
        public Void visitVacuumTable(VacuumTableNode node, Set<String> context)
        {
            for (PlanNode planNode : node.getSources()) {
                planNode.accept(this, context);
            }
            Optional<StatisticAggregations> statisticAggregations = node.getStatisticsAggregation();
            if (statisticAggregations.isPresent()) {
                for (AggregationNode.Aggregation agg : statisticAggregations.get().getAggregations().values()) {
                    visitRowExpressions(context, agg.getArguments());
                    visitRowExpressions(context, agg.getFunctionCall());
                }
            }
            return null;
        }

        @Override
        public Void visitTableDelete(TableDeleteNode node, Set<String> context)
        {
            for (PlanNode planNode : node.getSources()) {
                planNode.accept(this, context);
            }
            node.getFilter().ifPresent(row -> visitRowExpressions(context, row));
            return null;
        }

        private static void visitRowExpressions(Set<String> context, List<RowExpression> rowExpressions)
        {
            requireNonNull(rowExpressions);
            requireNonNull(context);
            for (RowExpression row : rowExpressions) {
                if (!isOriginalExpression(row)) {
                    row.accept(new RowExpressionVisitor(), context);
                }
            }
        }

        private static void visitRowExpressions(Set<String> context, RowExpression... rowExpressions)
        {
            requireNonNull(rowExpressions);
            requireNonNull(context);
            visitRowExpressions(context, Arrays.asList(rowExpressions));
        }
    }

    private static class RowExpressionVisitor
            extends DefaultRowExpressionTraversalVisitor<Set<String>>
    {
        @Override
        public Void visitCall(CallExpression call, Set<String> context)
        {
            if (!isDefaultFunction(call)) {
                context.add(call.getDisplayName());
            }
            call.getArguments().forEach(argument -> {
                if (!isOriginalExpression(argument)) {
                    argument.accept(this, context);
                }
            });
            return null;
        }

        @Override
        public Void visitSpecialForm(SpecialForm specialForm, Set<String> context)
        {
            specialForm.getArguments().forEach(argument -> {
                if (!isOriginalExpression(argument)) {
                    argument.accept(this, context);
                }
            });
            return null;
        }

        private static boolean isDefaultFunction(CallExpression callExpression)
        {
            return DEFAULT_NAMESPACE.equals(callExpression.getFunctionHandle().getFunctionNamespace());
        }
    }

    public static class IllegalExternalFunctionUsageException
            extends PrestoException
    {
        public IllegalExternalFunctionUsageException(ErrorCodeSupplier errorCode, String message)
        {
            super(errorCode, message);
        }
    }
}
