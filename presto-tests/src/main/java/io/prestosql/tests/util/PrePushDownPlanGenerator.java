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
package io.prestosql.tests.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.cost.CostCalculator;
import io.prestosql.cost.StatsCalculator;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.builder.SqlQueryBuilder;
import io.prestosql.sql.builder.optimizer.SubQueryPushDown;
import io.prestosql.sql.planner.OptimizerStatsRecorder;
import io.prestosql.sql.planner.RuleStatsRecorder;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.iterative.IterativeOptimizer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.iterative.rule.CanonicalizeExpressions;
import io.prestosql.sql.planner.iterative.rule.DesugarAtTimeZone;
import io.prestosql.sql.planner.iterative.rule.DesugarCurrentPath;
import io.prestosql.sql.planner.iterative.rule.DesugarCurrentUser;
import io.prestosql.sql.planner.iterative.rule.DesugarLambdaExpression;
import io.prestosql.sql.planner.iterative.rule.DesugarRowSubscript;
import io.prestosql.sql.planner.iterative.rule.DesugarTryExpression;
import io.prestosql.sql.planner.iterative.rule.EvaluateZeroSample;
import io.prestosql.sql.planner.iterative.rule.ImplementExceptAsUnion;
import io.prestosql.sql.planner.iterative.rule.ImplementFilteredAggregations;
import io.prestosql.sql.planner.iterative.rule.ImplementIntersectAsUnion;
import io.prestosql.sql.planner.iterative.rule.ImplementLimitWithTies;
import io.prestosql.sql.planner.iterative.rule.ImplementOffset;
import io.prestosql.sql.planner.iterative.rule.InlineProjections;
import io.prestosql.sql.planner.iterative.rule.MergeFilters;
import io.prestosql.sql.planner.iterative.rule.MergeLimitOverProjectWithSort;
import io.prestosql.sql.planner.iterative.rule.MergeLimitWithDistinct;
import io.prestosql.sql.planner.iterative.rule.MergeLimitWithSort;
import io.prestosql.sql.planner.iterative.rule.MergeLimitWithTopN;
import io.prestosql.sql.planner.iterative.rule.MergeLimits;
import io.prestosql.sql.planner.iterative.rule.MultipleDistinctAggregationToMarkDistinct;
import io.prestosql.sql.planner.iterative.rule.PruneAggregationColumns;
import io.prestosql.sql.planner.iterative.rule.PruneAggregationSourceColumns;
import io.prestosql.sql.planner.iterative.rule.PruneCountAggregationOverScalar;
import io.prestosql.sql.planner.iterative.rule.PruneCrossJoinColumns;
import io.prestosql.sql.planner.iterative.rule.PruneFilterColumns;
import io.prestosql.sql.planner.iterative.rule.PruneIndexSourceColumns;
import io.prestosql.sql.planner.iterative.rule.PruneJoinChildrenColumns;
import io.prestosql.sql.planner.iterative.rule.PruneJoinColumns;
import io.prestosql.sql.planner.iterative.rule.PruneLimitColumns;
import io.prestosql.sql.planner.iterative.rule.PruneMarkDistinctColumns;
import io.prestosql.sql.planner.iterative.rule.PruneOrderByInAggregation;
import io.prestosql.sql.planner.iterative.rule.PruneOutputColumns;
import io.prestosql.sql.planner.iterative.rule.PruneProjectColumns;
import io.prestosql.sql.planner.iterative.rule.PruneSemiJoinColumns;
import io.prestosql.sql.planner.iterative.rule.PruneSemiJoinFilteringSourceColumns;
import io.prestosql.sql.planner.iterative.rule.PruneTableScanColumns;
import io.prestosql.sql.planner.iterative.rule.PruneTopNColumns;
import io.prestosql.sql.planner.iterative.rule.PruneValuesColumns;
import io.prestosql.sql.planner.iterative.rule.PruneWindowColumns;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughMarkDistinct;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughOffset;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughOuterJoin;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughProject;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughSemiJoin;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughUnion;
import io.prestosql.sql.planner.iterative.rule.PushOffsetThroughProject;
import io.prestosql.sql.planner.iterative.rule.RemoveAggregationInSemiJoin;
import io.prestosql.sql.planner.iterative.rule.RemoveFullSample;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantDistinctLimit;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantLimit;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantSort;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantTopN;
import io.prestosql.sql.planner.iterative.rule.RemoveTrivialFilters;
import io.prestosql.sql.planner.iterative.rule.RemoveUnreferencedScalarApplyNodes;
import io.prestosql.sql.planner.iterative.rule.RemoveUnreferencedScalarLateralNodes;
import io.prestosql.sql.planner.iterative.rule.RewriteSpatialPartitioningAggregation;
import io.prestosql.sql.planner.iterative.rule.SimplifyExpressions;
import io.prestosql.sql.planner.iterative.rule.SingleDistinctAggregationToGroupBy;
import io.prestosql.sql.planner.iterative.rule.TransformCorrelatedInPredicateToJoin;
import io.prestosql.sql.planner.iterative.rule.TransformCorrelatedLateralJoinToJoin;
import io.prestosql.sql.planner.iterative.rule.TransformCorrelatedScalarAggregationToJoin;
import io.prestosql.sql.planner.iterative.rule.TransformCorrelatedScalarSubquery;
import io.prestosql.sql.planner.iterative.rule.TransformCorrelatedSingleRowSubqueryToProject;
import io.prestosql.sql.planner.iterative.rule.TransformExistsApplyToLateralNode;
import io.prestosql.sql.planner.iterative.rule.TransformUncorrelatedInPredicateSubqueryToSemiJoin;
import io.prestosql.sql.planner.iterative.rule.TransformUncorrelatedLateralToJoin;
import io.prestosql.sql.planner.optimizations.CheckSubqueryNodesAreRewritten;
import io.prestosql.sql.planner.optimizations.ImplementIntersectAndExceptAsUnion;
import io.prestosql.sql.planner.optimizations.LimitPushDown;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import io.prestosql.sql.planner.optimizations.PredicatePushDown;
import io.prestosql.sql.planner.optimizations.PruneUnreferencedOutputs;
import io.prestosql.sql.planner.optimizations.SetFlatteningOptimizer;
import io.prestosql.sql.planner.optimizations.StatsRecordingPlanOptimizer;
import io.prestosql.sql.planner.optimizations.TransformQuantifiedComparisonApplyToLateralJoin;
import io.prestosql.sql.planner.optimizations.UnaliasSymbolReferences;
import io.prestosql.testing.LocalQueryRunner;
import io.prestosql.testing.QueryRunner;

import java.util.List;
import java.util.Set;

/**
 * The {@link LocalQueryRunner} uses all the optimizers available in Presto.
 * To test the {@link SqlQueryBuilder}, only the optimizers upto {@link SubQueryPushDown}
 * must be applied in the logical plan tree. This custom {@link QueryRunner} uses only the necessary optimizers.
 */
public class PrePushDownPlanGenerator
        extends LocalQueryRunner
{
    private final Metadata metadata = getMetadata();
    private final TypeAnalyzer typeAnalyzer = new TypeAnalyzer(getSqlParser(), metadata);
    private final StatsCalculator statsCalculator = getStatsCalculator();
    private final CostCalculator estimatedExchangesCostCalculator = getEstimatedExchangesCostCalculator();

    public PrePushDownPlanGenerator(Session defaultSession)
    {
        super(defaultSession);
    }

    @Override
    public List<PlanOptimizer> getPlanOptimizers(boolean forceSingleNode)
    {
        RuleStatsRecorder ruleStats = new RuleStatsRecorder();
        OptimizerStatsRecorder optimizerStats = new OptimizerStatsRecorder();

        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();

        Set<Rule<?>> predicatePushDownRules = ImmutableSet.of(
                new MergeFilters());

        Set<Rule<?>> columnPruningRules = ImmutableSet.of(
                new PruneAggregationColumns(),
                new PruneAggregationSourceColumns(),
                new PruneCrossJoinColumns(),
                new PruneFilterColumns(),
                new PruneIndexSourceColumns(),
                new PruneJoinChildrenColumns(),
                new PruneJoinColumns(),
                new PruneMarkDistinctColumns(),
                new PruneOutputColumns(),
                new PruneProjectColumns(),
                new PruneSemiJoinColumns(),
                new PruneSemiJoinFilteringSourceColumns(),
                new PruneTopNColumns(),
                new PruneValuesColumns(),
                new PruneWindowColumns(),
                new PruneLimitColumns(),
                new PruneTableScanColumns());

        IterativeOptimizer inlineProjections = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(
                        new InlineProjections(),
                        new RemoveRedundantIdentityProjections()));

        IterativeOptimizer simplifyOptimizer = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                new SimplifyExpressions(metadata, typeAnalyzer).rules());

        PlanOptimizer predicatePushDown = new StatsRecordingPlanOptimizer(optimizerStats, new PredicatePushDown(metadata, typeAnalyzer, false));

        builder.add(
                // Clean up all the sugar in expressions, e.g. AtTimeZone, must be run before all the other optimizers
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(new DesugarLambdaExpression().rules())
                                .addAll(new DesugarAtTimeZone(metadata, typeAnalyzer).rules())
                                .addAll(new DesugarCurrentUser(metadata).rules())
                                .addAll(new DesugarCurrentPath(metadata).rules())
                                .addAll(new DesugarTryExpression(metadata, typeAnalyzer).rules())
                                .addAll(new DesugarRowSubscript(typeAnalyzer).rules())
                                .build()),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        new CanonicalizeExpressions(metadata, typeAnalyzer).rules()),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(predicatePushDownRules)
                                .addAll(columnPruningRules)
                                // .addAll(projectionPushdownRules)
                                .addAll(ImmutableSet.of(
                                        new RemoveRedundantIdentityProjections(),
                                        new RemoveFullSample(),
                                        new EvaluateZeroSample(),
                                        new PushOffsetThroughProject(),
                                        new PushLimitThroughOffset(),
                                        new PushLimitThroughProject(),
                                        new MergeLimits(),
                                        new MergeLimitWithSort(),
                                        new MergeLimitOverProjectWithSort(),
                                        new MergeLimitWithTopN(),
                                        new PushLimitThroughMarkDistinct(),
                                        new PushLimitThroughOuterJoin(),
                                        new PushLimitThroughSemiJoin(),
                                        new PushLimitThroughUnion(),
                                        new RemoveTrivialFilters(),
                                        new RemoveRedundantLimit(),
                                        new RemoveRedundantSort(),
                                        new RemoveRedundantTopN(),
                                        new RemoveRedundantDistinctLimit(),
                                        new ImplementFilteredAggregations(),
                                        new SingleDistinctAggregationToGroupBy(),
                                        new MultipleDistinctAggregationToMarkDistinct(),
                                        new MergeLimitWithDistinct(),
                                        new PruneCountAggregationOverScalar(),
                                        new PruneOrderByInAggregation(metadata),
                                        new RewriteSpatialPartitioningAggregation(metadata)))
                                .build()),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new ImplementOffset(),
                                new ImplementLimitWithTies())),
                simplifyOptimizer,
                new UnaliasSymbolReferences(),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                new SetFlatteningOptimizer(),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableList.of(new ImplementIntersectAndExceptAsUnion()),
                        ImmutableSet.of(
                                new ImplementIntersectAsUnion(),
                                new ImplementExceptAsUnion())),
                new LimitPushDown(), // Run the LimitPushDown after flattening set operators to make it easier to do the set flattening
                new PruneUnreferencedOutputs(),
                inlineProjections,
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        columnPruningRules),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new TransformExistsApplyToLateralNode(metadata))),
                new TransformQuantifiedComparisonApplyToLateralJoin(metadata),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new RemoveUnreferencedScalarLateralNodes(),
                                new TransformUncorrelatedLateralToJoin(),
                                new TransformUncorrelatedInPredicateSubqueryToSemiJoin(),
                                new TransformCorrelatedScalarAggregationToJoin(metadata),
                                new TransformCorrelatedLateralJoinToJoin())),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new RemoveUnreferencedScalarApplyNodes(),
                                new TransformCorrelatedInPredicateToJoin(), // must be run after PruneUnreferencedOutputs
                                new TransformCorrelatedScalarSubquery(metadata), // must be run after TransformCorrelatedScalarAggregationToJoin
                                new TransformCorrelatedLateralJoinToJoin(),
                                new ImplementFilteredAggregations())),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new InlineProjections(),
                                new RemoveRedundantIdentityProjections(),
                                new TransformCorrelatedSingleRowSubqueryToProject(),
                                new RemoveAggregationInSemiJoin())),
                new CheckSubqueryNodesAreRewritten(),
                predicatePushDown,
                new PruneUnreferencedOutputs(), // Hetu: Prune unreferenced outputs to make the sub-query simple
                inlineProjections);              // Hetu: Remove redundant projects to make the sub-query simple
        // Optimizers upto JoinPushDown

        return builder.build();
    }
}
