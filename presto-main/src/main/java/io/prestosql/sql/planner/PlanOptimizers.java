/*
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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.cost.CostCalculator;
import io.prestosql.cost.CostCalculator.EstimatedExchanges;
import io.prestosql.cost.CostComparator;
import io.prestosql.cost.StatsCalculator;
import io.prestosql.cost.TaskCountEstimator;
import io.prestosql.cube.CubeManager;
import io.prestosql.execution.TaskManagerConfig;
import io.prestosql.execution.scheduler.NodeSchedulerConfig;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.split.PageSourceManager;
import io.prestosql.split.SplitManager;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.iterative.IterativeOptimizer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.iterative.rule.AddExchangeAboveCTENode;
import io.prestosql.sql.planner.iterative.rule.AddExchangesBelowPartialAggregationOverGroupIdRuleSet;
import io.prestosql.sql.planner.iterative.rule.AddIntermediateAggregations;
import io.prestosql.sql.planner.iterative.rule.CanonicalizeExpressions;
import io.prestosql.sql.planner.iterative.rule.CreatePartialTopN;
import io.prestosql.sql.planner.iterative.rule.DesugarAtTimeZone;
import io.prestosql.sql.planner.iterative.rule.DesugarCurrentPath;
import io.prestosql.sql.planner.iterative.rule.DesugarCurrentUser;
import io.prestosql.sql.planner.iterative.rule.DesugarLambdaExpression;
import io.prestosql.sql.planner.iterative.rule.DesugarRowSubscript;
import io.prestosql.sql.planner.iterative.rule.DesugarTryExpression;
import io.prestosql.sql.planner.iterative.rule.DetermineJoinDistributionType;
import io.prestosql.sql.planner.iterative.rule.DetermineSemiJoinDistributionType;
import io.prestosql.sql.planner.iterative.rule.EliminateCrossJoins;
import io.prestosql.sql.planner.iterative.rule.EvaluateEmptyIntersect;
import io.prestosql.sql.planner.iterative.rule.EvaluateZeroSample;
import io.prestosql.sql.planner.iterative.rule.ExtractSpatialJoins;
import io.prestosql.sql.planner.iterative.rule.GatherAndMergeWindows;
import io.prestosql.sql.planner.iterative.rule.HintedReorderJoins;
import io.prestosql.sql.planner.iterative.rule.ImplementBernoulliSampleAsFilter;
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
import io.prestosql.sql.planner.iterative.rule.MergePartialAggregationWithJoin;
import io.prestosql.sql.planner.iterative.rule.MergePartialAggregationWithJoinPushProject;
import io.prestosql.sql.planner.iterative.rule.MultipleDistinctAggregationToMarkDistinct;
import io.prestosql.sql.planner.iterative.rule.PruneAggregationColumns;
import io.prestosql.sql.planner.iterative.rule.PruneAggregationSourceColumns;
import io.prestosql.sql.planner.iterative.rule.PruneApplyColumns;
import io.prestosql.sql.planner.iterative.rule.PruneApplyCorrelation;
import io.prestosql.sql.planner.iterative.rule.PruneApplySourceColumns;
import io.prestosql.sql.planner.iterative.rule.PruneAssignUniqueIdColumns;
import io.prestosql.sql.planner.iterative.rule.PruneCountAggregationOverScalar;
import io.prestosql.sql.planner.iterative.rule.PruneCrossJoinColumns;
import io.prestosql.sql.planner.iterative.rule.PruneDeleteSourceColumns;
import io.prestosql.sql.planner.iterative.rule.PruneDistinctLimitSourceColumns;
import io.prestosql.sql.planner.iterative.rule.PruneEnforceSingleRowColumns;
import io.prestosql.sql.planner.iterative.rule.PruneExceptSourceColumns;
import io.prestosql.sql.planner.iterative.rule.PruneExchangeColumns;
import io.prestosql.sql.planner.iterative.rule.PruneExchangeSourceColumns;
import io.prestosql.sql.planner.iterative.rule.PruneFilterColumns;
import io.prestosql.sql.planner.iterative.rule.PruneGroupIdColumns;
import io.prestosql.sql.planner.iterative.rule.PruneGroupIdSourceColumns;
import io.prestosql.sql.planner.iterative.rule.PruneIndexJoinColumns;
import io.prestosql.sql.planner.iterative.rule.PruneIndexSourceColumns;
import io.prestosql.sql.planner.iterative.rule.PruneIntersectSourceColumns;
import io.prestosql.sql.planner.iterative.rule.PruneJoinChildrenColumns;
import io.prestosql.sql.planner.iterative.rule.PruneJoinColumns;
import io.prestosql.sql.planner.iterative.rule.PruneLimitColumns;
import io.prestosql.sql.planner.iterative.rule.PruneMarkDistinctColumns;
import io.prestosql.sql.planner.iterative.rule.PruneOffsetColumns;
import io.prestosql.sql.planner.iterative.rule.PruneOrderByInAggregation;
import io.prestosql.sql.planner.iterative.rule.PruneOutputColumns;
import io.prestosql.sql.planner.iterative.rule.PruneOutputSourceColumns;
import io.prestosql.sql.planner.iterative.rule.PruneProjectColumns;
import io.prestosql.sql.planner.iterative.rule.PruneRowNumberColumns;
import io.prestosql.sql.planner.iterative.rule.PruneSampleColumns;
import io.prestosql.sql.planner.iterative.rule.PruneSemiJoinColumns;
import io.prestosql.sql.planner.iterative.rule.PruneSemiJoinFilteringSourceColumns;
import io.prestosql.sql.planner.iterative.rule.PruneSortColumns;
import io.prestosql.sql.planner.iterative.rule.PruneSpatialJoinChildrenColumns;
import io.prestosql.sql.planner.iterative.rule.PruneSpatialJoinColumns;
import io.prestosql.sql.planner.iterative.rule.PruneTableExecuteSourceColumns;
import io.prestosql.sql.planner.iterative.rule.PruneTableScanColumns;
import io.prestosql.sql.planner.iterative.rule.PruneTableWriterSourceColumns;
import io.prestosql.sql.planner.iterative.rule.PruneTopNColumns;
import io.prestosql.sql.planner.iterative.rule.PruneTopNRankingColumns;
import io.prestosql.sql.planner.iterative.rule.PruneUnionColumns;
import io.prestosql.sql.planner.iterative.rule.PruneUnionSourceColumns;
import io.prestosql.sql.planner.iterative.rule.PruneUpdateSourceColumns;
import io.prestosql.sql.planner.iterative.rule.PruneValuesColumns;
import io.prestosql.sql.planner.iterative.rule.PruneWindowColumns;
import io.prestosql.sql.planner.iterative.rule.PushAggregationThroughOuterJoin;
import io.prestosql.sql.planner.iterative.rule.PushDeleteAsInsertIntoConnector;
import io.prestosql.sql.planner.iterative.rule.PushDeleteIntoConnector;
import io.prestosql.sql.planner.iterative.rule.PushLimitIntoTableScan;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughMarkDistinct;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughOffset;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughOuterJoin;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughProject;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughSemiJoin;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughUnion;
import io.prestosql.sql.planner.iterative.rule.PushOffsetThroughProject;
import io.prestosql.sql.planner.iterative.rule.PushPartialAggregationThroughExchange;
import io.prestosql.sql.planner.iterative.rule.PushPartialAggregationThroughJoin;
import io.prestosql.sql.planner.iterative.rule.PushPredicateIntoTableScan;
import io.prestosql.sql.planner.iterative.rule.PushPredicateIntoUpdateDelete;
import io.prestosql.sql.planner.iterative.rule.PushProjectionIntoTableScan;
import io.prestosql.sql.planner.iterative.rule.PushProjectionThroughExchange;
import io.prestosql.sql.planner.iterative.rule.PushProjectionThroughUnion;
import io.prestosql.sql.planner.iterative.rule.PushRemoteExchangeThroughAssignUniqueId;
import io.prestosql.sql.planner.iterative.rule.PushSampleIntoTableScan;
import io.prestosql.sql.planner.iterative.rule.PushTableWriteThroughUnion;
import io.prestosql.sql.planner.iterative.rule.PushTopNThroughOuterJoin;
import io.prestosql.sql.planner.iterative.rule.PushTopNThroughProject;
import io.prestosql.sql.planner.iterative.rule.PushTopNThroughUnion;
import io.prestosql.sql.planner.iterative.rule.RemoveAggregationInSemiJoin;
import io.prestosql.sql.planner.iterative.rule.RemoveDuplicateConditions;
import io.prestosql.sql.planner.iterative.rule.RemoveEmptyDelete;
import io.prestosql.sql.planner.iterative.rule.RemoveEmptyExceptBranches;
import io.prestosql.sql.planner.iterative.rule.RemoveEmptyUnionBranches;
import io.prestosql.sql.planner.iterative.rule.RemoveFullSample;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantDistinctLimit;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantEnforceSingleRowNode;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantExists;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantJoin;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantLimit;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantOffset;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantSort;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantTopN;
import io.prestosql.sql.planner.iterative.rule.RemoveTrivialFilters;
import io.prestosql.sql.planner.iterative.rule.RemoveUnreferencedScalarApplyNodes;
import io.prestosql.sql.planner.iterative.rule.RemoveUnreferencedScalarLateralNodes;
import io.prestosql.sql.planner.iterative.rule.RemoveUnsupportedDynamicFilters;
import io.prestosql.sql.planner.iterative.rule.ReorderJoins;
import io.prestosql.sql.planner.iterative.rule.RewriteSpatialPartitioningAggregation;
import io.prestosql.sql.planner.iterative.rule.SimplifyCountOverConstant;
import io.prestosql.sql.planner.iterative.rule.SimplifyExpressions;
import io.prestosql.sql.planner.iterative.rule.SimplifyRowExpressions;
import io.prestosql.sql.planner.iterative.rule.SingleDistinctAggregationToGroupBy;
import io.prestosql.sql.planner.iterative.rule.TablePushdown;
import io.prestosql.sql.planner.iterative.rule.TransformCorrelatedInPredicateToJoin;
import io.prestosql.sql.planner.iterative.rule.TransformCorrelatedLateralJoinToJoin;
import io.prestosql.sql.planner.iterative.rule.TransformCorrelatedScalarAggregationToJoin;
import io.prestosql.sql.planner.iterative.rule.TransformCorrelatedScalarSubquery;
import io.prestosql.sql.planner.iterative.rule.TransformCorrelatedSingleRowSubqueryToProject;
import io.prestosql.sql.planner.iterative.rule.TransformExistsApplyToLateralNode;
import io.prestosql.sql.planner.iterative.rule.TransformFilteringSemiJoinToInnerJoin;
import io.prestosql.sql.planner.iterative.rule.TransformUnCorrelatedInPredicateSubQuerySelfJoinToAggregate;
import io.prestosql.sql.planner.iterative.rule.TransformUnCorrelatedSubquerySelfJoinToWindowFunctions;
import io.prestosql.sql.planner.iterative.rule.TransformUncorrelatedInPredicateSubqueryToSemiJoin;
import io.prestosql.sql.planner.iterative.rule.TransformUncorrelatedLateralToJoin;
import io.prestosql.sql.planner.iterative.rule.TransformUncorrelatedSubquerySelfJoinAggregatesToWindowFunction;
import io.prestosql.sql.planner.iterative.rule.TranslateExpressions;
import io.prestosql.sql.planner.iterative.rule.UnwrapCastInComparison;
import io.prestosql.sql.planner.iterative.rule.UseNonPartitionedJoinLookupSource;
import io.prestosql.sql.planner.optimizations.AddCacheTableWriterAboveCTEOptimizer;
import io.prestosql.sql.planner.optimizations.AddExchanges;
import io.prestosql.sql.planner.optimizations.AddLocalExchanges;
import io.prestosql.sql.planner.optimizations.AddReuseExchange;
import io.prestosql.sql.planner.optimizations.AddSortBasedAggregation;
import io.prestosql.sql.planner.optimizations.AdjustApplicableOptimizationRule;
import io.prestosql.sql.planner.optimizations.ApplyConnectorOptimization;
import io.prestosql.sql.planner.optimizations.BeginTableWrite;
import io.prestosql.sql.planner.optimizations.CheckSubqueryNodesAreRewritten;
import io.prestosql.sql.planner.optimizations.HashGenerationOptimizer;
import io.prestosql.sql.planner.optimizations.ImplementIntersectAndExceptAsUnion;
import io.prestosql.sql.planner.optimizations.IndexJoinOptimizer;
import io.prestosql.sql.planner.optimizations.LimitPushDown;
import io.prestosql.sql.planner.optimizations.MetadataQueryOptimizer;
import io.prestosql.sql.planner.optimizations.OptimizeAggregationOverJoin;
import io.prestosql.sql.planner.optimizations.OptimizeMixedDistinctAggregations;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import io.prestosql.sql.planner.optimizations.PredicatePushDown;
import io.prestosql.sql.planner.optimizations.PruneCTENodes;
import io.prestosql.sql.planner.optimizations.PruneUnreferencedOutputs;
import io.prestosql.sql.planner.optimizations.ReplicateSemiJoinInDelete;
import io.prestosql.sql.planner.optimizations.ReplicateSemiJoinInUpdate;
import io.prestosql.sql.planner.optimizations.SetFlatteningOptimizer;
import io.prestosql.sql.planner.optimizations.StarTreeAggregationRule;
import io.prestosql.sql.planner.optimizations.StatsRecordingPlanOptimizer;
import io.prestosql.sql.planner.optimizations.TableDeleteOptimizer;
import io.prestosql.sql.planner.optimizations.TransformQuantifiedComparisonApplyToLateralJoin;
import io.prestosql.sql.planner.optimizations.UnaliasSymbolReferences;
import io.prestosql.sql.planner.optimizations.WindowFilterPushDown;
import io.prestosql.utils.HetuConfig;
import org.weakref.jmx.MBeanExporter;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.List;
import java.util.Set;

import static io.prestosql.sql.planner.ConnectorPlanOptimizerManager.PlanPhase.LOGICAL;
import static io.prestosql.sql.planner.ConnectorPlanOptimizerManager.PlanPhase.PHYSICAL;

public class PlanOptimizers
{
    private final List<PlanOptimizer> optimizers;
    private final RuleStatsRecorder ruleStats = new RuleStatsRecorder();
    private final OptimizerStatsRecorder optimizerStats = new OptimizerStatsRecorder();
    private final MBeanExporter exporter;

    @Inject
    public PlanOptimizers(
            Metadata metadata,
            TypeAnalyzer typeAnalyzer,
            FeaturesConfig featuresConfig,
            NodeSchedulerConfig nodeSchedulerConfig,
            InternalNodeManager nodeManager,
            TaskManagerConfig taskManagerConfig,
            MBeanExporter exporter,
            SplitManager splitManager,
            ConnectorPlanOptimizerManager planOptimizerManager,
            PageSourceManager pageSourceManager,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            @EstimatedExchanges CostCalculator estimatedExchangesCostCalculator,
            CostComparator costComparator,
            TaskCountEstimator taskCountEstimator,
            CubeManager cubeManager,
            HetuConfig hetuConfig)
    {
        this(metadata,
                typeAnalyzer,
                featuresConfig,
                taskManagerConfig,
                false,
                exporter,
                splitManager,
                planOptimizerManager,
                pageSourceManager,
                statsCalculator,
                costCalculator,
                estimatedExchangesCostCalculator,
                costComparator,
                taskCountEstimator,
                cubeManager,
                hetuConfig);
    }

    @PostConstruct
    public void initialize()
    {
        ruleStats.export(exporter);
        optimizerStats.export(exporter);
    }

    @PreDestroy
    public void destroy()
    {
        ruleStats.unexport(exporter);
        optimizerStats.unexport(exporter);
    }

    public PlanOptimizers(
            Metadata metadata,
            TypeAnalyzer typeAnalyzer,
            FeaturesConfig featuresConfig,
            TaskManagerConfig taskManagerConfig,
            boolean forceSingleNode,
            MBeanExporter exporter,
            SplitManager splitManager,
            ConnectorPlanOptimizerManager planOptimizerManager,
            PageSourceManager pageSourceManager,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            CostCalculator inputEstimatedExchangesCostCalculator,
            CostComparator costComparator,
            TaskCountEstimator taskCountEstimator,
            CubeManager cubeManager,
            HetuConfig hetuConfig)
    {
        CostCalculator estimatedExchangesCostCalculator = inputEstimatedExchangesCostCalculator;
        this.exporter = exporter;
        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();
        CostCalculationHandle costCalculationHandle = new CostCalculationHandle(statsCalculator, costCalculator, costComparator);

        builder.add(new AdjustApplicableOptimizationRule()); //This must be the first rule, as based on this next set of rule to apply will vary.

        builder.add(new PruneCTENodes(metadata, typeAnalyzer, false, true));
        Set<Rule<?>> predicatePushDownRules = ImmutableSet.of(
                new MergeFilters());

        // TODO: Once we've migrated handling all the plan node types, replace uses of PruneUnreferencedOutputs with an IterativeOptimizer containing these rules.
        Set<Rule<?>> columnPruningRules = ImmutableSet.of(
                new PruneAggregationColumns(),
                new PruneAggregationSourceColumns(),
                new PruneApplyColumns(),
                new PruneApplyCorrelation(),
                new PruneApplySourceColumns(),
                new PruneAssignUniqueIdColumns(),
                new PruneDeleteSourceColumns(),
                new PruneUpdateSourceColumns(),
                new PruneDistinctLimitSourceColumns(),
                new PruneEnforceSingleRowColumns(),
                new PruneExceptSourceColumns(),
                new PruneExchangeColumns(),
                new PruneExchangeSourceColumns(),
                new PruneFilterColumns(),
                new PruneGroupIdColumns(),
                new PruneGroupIdSourceColumns(),
                new PruneIndexJoinColumns(),
                new PruneIndexSourceColumns(),
                new PruneIntersectSourceColumns(),
                new PruneJoinChildrenColumns(),
                new PruneJoinColumns(),
                new PruneLimitColumns(),
                new PruneMarkDistinctColumns(),
                new PruneOffsetColumns(),
                new PruneOutputSourceColumns(),
                new PruneProjectColumns(),
                new PruneRowNumberColumns(),
                new PruneSampleColumns(),
                new PruneSemiJoinColumns(),
                new PruneSemiJoinFilteringSourceColumns(),
                new PruneSortColumns(),
                new PruneSpatialJoinChildrenColumns(),
                new PruneSpatialJoinColumns(),
                new PruneTableExecuteSourceColumns(),
                new PruneTableWriterSourceColumns(),
                new PruneTopNColumns(),
                new PruneTopNRankingColumns(),
                new PruneUnionColumns(),
                new PruneUnionSourceColumns(),
                new PruneValuesColumns(),
                new PruneWindowColumns(),
                new PruneCrossJoinColumns(),
                new PruneOutputColumns(),
                new PruneTableScanColumns());

        Set<Rule<?>> projectionPushdownRules = ImmutableSet.of(
                new PushProjectionIntoTableScan(metadata, typeAnalyzer),
                new PushProjectionThroughUnion(),
                new PushProjectionThroughExchange());

        IterativeOptimizer inlineProjections = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(
                        new InlineProjections(metadata),
                        new RemoveRedundantIdentityProjections()));

        IterativeOptimizer projectionPushDown = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                projectionPushdownRules);

        IterativeOptimizer simplifyOptimizer = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .addAll(new SimplifyExpressions(metadata, typeAnalyzer).rules())
                        .addAll(new UnwrapCastInComparison(metadata, typeAnalyzer).rules())
                        .addAll(new RemoveDuplicateConditions().rules())
                        .addAll(new CanonicalizeExpressions(metadata, typeAnalyzer).rules())
                        .build());

        IterativeOptimizer simplifyRowExpressionOptimizer = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                new SimplifyRowExpressions(metadata).rules(metadata));

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
                                .addAll(ImmutableSet.of(
                                        new RemoveRedundantIdentityProjections(),
                                        new RemoveEmptyUnionBranches(),
                                        new EvaluateEmptyIntersect(),
                                        new RemoveEmptyExceptBranches(),
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
                                        new RemoveRedundantOffset(),
                                        new RemoveRedundantSort(),
                                        new RemoveRedundantTopN(),
                                        new RemoveRedundantDistinctLimit(),
                                        new RemoveRedundantJoin(),
                                        new RemoveRedundantEnforceSingleRowNode(),
                                        new RemoveRedundantExists(),
                                        new ImplementFilteredAggregations(),
                                        new StarTreeAggregationRule(cubeManager, metadata),
                                        new OptimizeAggregationOverJoin(cubeManager, metadata),
                                        new SingleDistinctAggregationToGroupBy(),
                                        new MultipleDistinctAggregationToMarkDistinct(),
                                        new ImplementBernoulliSampleAsFilter(metadata),
                                        new MergeLimitWithDistinct(),
                                        new PruneCountAggregationOverScalar(metadata.getFunctionAndTypeManager()),
                                        new PruneOrderByInAggregation(metadata),
                                        new RewriteSpatialPartitioningAggregation(metadata)))
                                .build()),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new ImplementOffset(),
                                new ImplementLimitWithTies(metadata))),
                simplifyOptimizer,
                new UnaliasSymbolReferences(metadata),
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
                        ImmutableList.of(new ImplementIntersectAndExceptAsUnion(metadata.getFunctionAndTypeManager())),
                        ImmutableSet.of(
                                new ImplementIntersectAsUnion(metadata),
                                new ImplementExceptAsUnion(metadata))),
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
                                new TransformUnCorrelatedInPredicateSubQuerySelfJoinToAggregate(metadata),
                                new TransformUncorrelatedInPredicateSubqueryToSemiJoin(),
                                new TransformCorrelatedScalarAggregationToJoin(metadata),
                                new TransformCorrelatedLateralJoinToJoin())),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new RemoveUnreferencedScalarApplyNodes(),
                                new TransformCorrelatedInPredicateToJoin(metadata.getFunctionAndTypeManager()), // must be run after PruneUnreferencedOutputs
                                new TransformCorrelatedScalarSubquery(metadata), // must be run after TransformCorrelatedScalarAggregationToJoin
                                new TransformCorrelatedLateralJoinToJoin(),
                                new ImplementFilteredAggregations())),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new InlineProjections(metadata),
                                new RemoveRedundantIdentityProjections(),
                                new TransformCorrelatedSingleRowSubqueryToProject(),
                                new RemoveAggregationInSemiJoin())),
                new CheckSubqueryNodesAreRewritten());

        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(new TransformUncorrelatedSubquerySelfJoinAggregatesToWindowFunction(metadata),
                        new TransformUnCorrelatedSubquerySelfJoinToWindowFunctions(metadata))));

        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                new TranslateExpressions(metadata, new SqlParser()).rules(metadata)));

        builder.add(new StatsRecordingPlanOptimizer(
                        optimizerStats,
                        new PredicatePushDown(metadata, typeAnalyzer, costCalculationHandle, false, false, false)),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new TransformFilteringSemiJoinToInnerJoin(metadata))), // must run after PredicatePushDown
                new PruneUnreferencedOutputs(),
                inlineProjections,
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(projectionPushdownRules)
                                .add(new PushLimitIntoTableScan(metadata))
                                .add(new PushPredicateIntoTableScan(metadata, typeAnalyzer, true))
                                .add(new PushPredicateIntoUpdateDelete(metadata))
                                .add(new PushSampleIntoTableScan(metadata))
                                .build()),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(new PushDeleteAsInsertIntoConnector(metadata, true))),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(new PushDeleteAsInsertIntoConnector(metadata, false))),
                new PruneUnreferencedOutputs(),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new RemoveRedundantIdentityProjections(),
                                new PushAggregationThroughOuterJoin())),
                inlineProjections,
                simplifyRowExpressionOptimizer, // Re-run the SimplifyExpressions to simplify any recomposed expressions from other optimizations
                projectionPushDown,
                new UnaliasSymbolReferences(metadata), // Run again because predicate pushdown and projection pushdown might add more projections
                new PruneUnreferencedOutputs(), // Make sure to run this before index join. Filtered projections may not have all the columns.
                new IndexJoinOptimizer(metadata), // Run this after projections and filters have been fully simplified and pushed down
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new SimplifyCountOverConstant(metadata.getFunctionAndTypeManager()))),
                new LimitPushDown(), // Run LimitPushDown before WindowFilterPushDown
                new WindowFilterPushDown(metadata), // This must run after PredicatePushDown and LimitPushDown so that it squashes any successive filter nodes and limits
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                // add UnaliasSymbolReferences when it's ported
                                .add(new RemoveRedundantIdentityProjections())
                                .addAll(GatherAndMergeWindows.rules())
                                .build()),
                inlineProjections,
                new PruneUnreferencedOutputs(), // Make sure to run this at the end to help clean the plan for logging/execution and not remove info that other optimizers might need at an earlier point
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                new MetadataQueryOptimizer(metadata),
                new PruneCTENodes(metadata, typeAnalyzer, true, true),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new EliminateCrossJoins())), // This can pull up Filter and Project nodes from between Joins, so we need to push them down again
                new StatsRecordingPlanOptimizer(optimizerStats, new PredicatePushDown(metadata, typeAnalyzer, costCalculationHandle, true, false, true)),
                simplifyRowExpressionOptimizer, // Should be always run after PredicatePushDown
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new PushPredicateIntoTableScan(metadata, typeAnalyzer, true))),
                projectionPushDown,
                new PruneUnreferencedOutputs(),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new TablePushdown(metadata))), //Added in Presto for improving query latency

                // Because ReorderJoins runs only once,
                // PredicatePushDown, PruneUnreferenedOutputpus and RemoveRedundantIdentityProjections
                // need to run beforehand in order to produce an optimal join order
                // It also needs to run after EliminateCrossJoins so that its chosen order doesn't get undone.
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new ReorderJoins(costComparator, metadata))),
                new HintedReorderJoins(ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        costComparator,
                        metadata));

        builder.add(
                new ApplyConnectorOptimization(() -> planOptimizerManager.getOptimizers(LOGICAL)),
                projectionPushDown,
                new PruneUnreferencedOutputs());

        builder.add(new OptimizeMixedDistinctAggregations(metadata));
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(
                        new CreatePartialTopN(),
                        new PushTopNThroughProject(),
                        new PushTopNThroughOuterJoin(),
                        new PushTopNThroughUnion())));
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .add(new RemoveRedundantIdentityProjections())
                        .addAll(new ExtractSpatialJoins(metadata, splitManager, pageSourceManager, typeAnalyzer).rules())
                        .add(new InlineProjections(metadata))
                        .build()));

        if (!forceSingleNode) {
            builder.add(new ReplicateSemiJoinInDelete()); // Must run before AddExchanges
            builder.add(new ReplicateSemiJoinInUpdate()); // Must run before AddExchanges
            builder.add((new IterativeOptimizer(
                    ruleStats,
                    statsCalculator,
                    estimatedExchangesCostCalculator,
                    ImmutableSet.of(
                            new DetermineJoinDistributionType(costComparator, taskCountEstimator), // Must run before AddExchanges
                            // Must run before AddExchanges and after ReplicateSemiJoinInDelete
                            // to avoid temporarily having an invalid plan
                            new DetermineSemiJoinDistributionType(costComparator, taskCountEstimator)))));
            builder.add(
                    new IterativeOptimizer(
                            ruleStats,
                            statsCalculator,
                            estimatedExchangesCostCalculator,
                            ImmutableSet.of(new PushTableWriteThroughUnion()))); // Must run before AddExchanges
        }
        builder.add(new AddCacheTableWriterAboveCTEOptimizer(metadata));
        if (!forceSingleNode) {
            builder.add(new StatsRecordingPlanOptimizer(optimizerStats, new AddExchanges(metadata, typeAnalyzer, false)));
        }

        IterativeOptimizer pushdownRule = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(new PushPredicateIntoTableScan(metadata, typeAnalyzer, false)));

        IterativeOptimizer pushdownDeleteRule = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(new PushDeleteIntoConnector(metadata, false))); // Must run before AddExchanges
        IterativeOptimizer pushdownDeleteWithExchangeRule = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(new PushDeleteIntoConnector(metadata, true))); // Must run before AddExchanges
        estimatedExchangesCostCalculator = null; // Prevent accidental use after AddExchanges
        builder.add(
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(new RemoveEmptyDelete()))); // Run RemoveEmptyDelete after table scan is removed by PickTableLayout/AddExchanges

        builder.add(new StatsRecordingPlanOptimizer(optimizerStats, new PredicatePushDown(metadata, typeAnalyzer, costCalculationHandle, true, true, true))); // Run predicate push down one more time in case we can leverage new information from layouts' effective predicate
        builder.add(new RemoveUnsupportedDynamicFilters(metadata, statsCalculator));
        builder.add(simplifyRowExpressionOptimizer); // Should be always run after PredicatePushDown
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(new AddExchangeAboveCTENode())));
        builder.add(projectionPushDown);
        builder.add(inlineProjections);
        builder.add(new UnaliasSymbolReferences(metadata)); // Run unalias after merging projections to simplify projections more efficiently
        builder.add(new PruneUnreferencedOutputs());

        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .add(new RemoveRedundantIdentityProjections())
                        .add(new PushRemoteExchangeThroughAssignUniqueId())
                        .add(new InlineProjections(metadata))
                        .build()));
        builder.add(pushdownRule);
        builder.add(pushdownDeleteWithExchangeRule);
        builder.add(pushdownDeleteRule);

        builder.add(new AddSortBasedAggregation(metadata, statsCalculator, costCalculator, costComparator));
        // Optimizers above this don't understand local exchanges, so be careful moving this.
        builder.add(new AddLocalExchanges(metadata, typeAnalyzer));
        // UseNonPartitionedJoinLookupSource needs to run after AddLocalExchanges since it operates on ExchangeNodes added by this optimizer.
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(new UseNonPartitionedJoinLookupSource())));

        // Optimizers above this do not need to care about aggregations with the type other than SINGLE
        // This optimizer must be run after all exchange-related optimizers
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(
                        new PushPartialAggregationThroughJoin(),
                        new PushPartialAggregationThroughExchange(metadata),
                        new PruneJoinColumns(),
                        new MergePartialAggregationWithJoin(metadata),
                        new MergePartialAggregationWithJoinPushProject(metadata))));

        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                new AddExchangesBelowPartialAggregationOverGroupIdRuleSet(metadata, typeAnalyzer, taskCountEstimator, taskManagerConfig).rules()));

        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(
                        new AddIntermediateAggregations(),
                        new RemoveRedundantIdentityProjections())));

        builder.add(new AddReuseExchange(metadata));

        builder.add(
                new ApplyConnectorOptimization(() -> planOptimizerManager.getOptimizers(PHYSICAL)),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())));

        // DO NOT add optimizers that change the plan shape (computations) after this point

        // Precomputed hashes - this assumes that partitioning will not change
        builder.add(new HashGenerationOptimizer(metadata));

        builder.add(new TableDeleteOptimizer(metadata));
        builder.add(new BeginTableWrite(metadata, hetuConfig.getCachingUserName())); // HACK! see comments in BeginTableWrite

        // TODO: consider adding a formal final plan sanitization optimizer that prepares the plan for transmission/execution/logging
        // TODO: figure out how to improve the set flattening optimizer so that it can run at any point

        this.optimizers = builder.build();
    }

    public List<PlanOptimizer> get()
    {
        return optimizers;
    }

    public static class CostCalculationHandle
    {
        StatsCalculator statsCalculator;
        CostCalculator costCalculator;
        CostComparator costComparator;

        public CostCalculationHandle(StatsCalculator statsCalculator, CostCalculator costCalculator, CostComparator costComparator)
        {
            this.statsCalculator = statsCalculator;
            this.costCalculator = costCalculator;
            this.costComparator = costComparator;
        }

        public StatsCalculator getStatsCalculator()
        {
            return statsCalculator;
        }

        public CostCalculator getCostCalculator()
        {
            return costCalculator;
        }

        public CostComparator getCostComparator()
        {
            return costComparator;
        }
    }
}
