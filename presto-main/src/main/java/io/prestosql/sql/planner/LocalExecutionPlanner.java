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

import com.google.common.base.VerifyException;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.cache.CachedDataManager;
import io.prestosql.cache.elements.CachedDataStorage;
import io.prestosql.cube.CubeManager;
import io.prestosql.dynamicfilter.DynamicFilterCacheManager;
import io.prestosql.exchange.ExchangeManagerRegistry;
import io.prestosql.exchange.RetryPolicy;
import io.prestosql.execution.ExplainAnalyzeContext;
import io.prestosql.execution.StageId;
import io.prestosql.execution.TableExecuteContextManager;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskManagerConfig;
import io.prestosql.execution.buffer.OutputBuffer;
import io.prestosql.execution.buffer.OutputBufferInfo;
import io.prestosql.expressions.LogicalRowExpressions;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.index.IndexManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.AggregationOperator.AggregationOperatorFactory;
import io.prestosql.operator.AssignUniqueIdOperator;
import io.prestosql.operator.BatchDistinctLimitOperator;
import io.prestosql.operator.BatchLimitOperator;
import io.prestosql.operator.BloomFilterUtils;
import io.prestosql.operator.CacheTableFinishOperator;
import io.prestosql.operator.CacheTableWriterOperator;
import io.prestosql.operator.CommonTableExecutionContext;
import io.prestosql.operator.CubeFinishOperator.CubeFinishOperatorFactory;
import io.prestosql.operator.DeleteOperator.DeleteOperatorFactory;
import io.prestosql.operator.DevNullOperator.DevNullOperatorFactory;
import io.prestosql.operator.DriverFactory;
import io.prestosql.operator.DynamicFilterSourceOperator;
import io.prestosql.operator.EnforceSingleRowOperator;
import io.prestosql.operator.ExchangeClientSupplier;
import io.prestosql.operator.ExchangeOperator.ExchangeOperatorFactory;
import io.prestosql.operator.ExplainAnalyzeOperator.ExplainAnalyzeOperatorFactory;
import io.prestosql.operator.FilterAndProjectOperator;
import io.prestosql.operator.GroupIdOperator;
import io.prestosql.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import io.prestosql.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import io.prestosql.operator.HashSemiJoinOperator.HashSemiJoinOperatorFactory;
import io.prestosql.operator.JoinBridgeManager;
import io.prestosql.operator.JoinOperatorFactory;
import io.prestosql.operator.JoinOperatorFactory.OuterOperatorFactoryResult;
import io.prestosql.operator.LimitOperator.LimitOperatorFactory;
import io.prestosql.operator.LocalPlannerAware;
import io.prestosql.operator.LookupJoinOperatorFactory;
import io.prestosql.operator.LookupJoinOperators;
import io.prestosql.operator.LookupOuterOperator.LookupOuterOperatorFactory;
import io.prestosql.operator.LookupSourceFactory;
import io.prestosql.operator.MarkDistinctOperator.MarkDistinctOperatorFactory;
import io.prestosql.operator.MergeOperator.MergeOperatorFactory;
import io.prestosql.operator.NestedLoopJoinBridge;
import io.prestosql.operator.NestedLoopJoinPagesSupplier;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.OrderByOperator.OrderByOperatorFactory;
import io.prestosql.operator.OutputFactory;
import io.prestosql.operator.PagesIndex;
import io.prestosql.operator.PagesSpatialIndexFactory;
import io.prestosql.operator.PartitionFunction;
import io.prestosql.operator.PartitionedLookupSourceFactory;
import io.prestosql.operator.PipelineExecutionStrategy;
import io.prestosql.operator.RowNumberOperator;
import io.prestosql.operator.ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory;
import io.prestosql.operator.SetBuilderOperator.SetBuilderOperatorFactory;
import io.prestosql.operator.SetBuilderOperator.SetSupplier;
import io.prestosql.operator.SortAggregationOperator;
import io.prestosql.operator.SourceOperatorFactory;
import io.prestosql.operator.SpatialIndexBuilderOperator.SpatialIndexBuilderOperatorFactory;
import io.prestosql.operator.SpatialIndexBuilderOperator.SpatialPredicate;
import io.prestosql.operator.SpatialJoinOperator.SpatialJoinOperatorFactory;
import io.prestosql.operator.StageExecutionDescriptor;
import io.prestosql.operator.StatisticsWriterOperator.StatisticsWriterOperatorFactory;
import io.prestosql.operator.StreamingAggregationOperator.StreamingAggregationOperatorFactory;
import io.prestosql.operator.TableDeleteOperator.TableDeleteOperatorFactory;
import io.prestosql.operator.TableScanOperator.TableScanOperatorFactory;
import io.prestosql.operator.TableUpdateOperator;
import io.prestosql.operator.TaskContext;
import io.prestosql.operator.TaskOutputOperator.TaskOutputFactory;
import io.prestosql.operator.TopNOperator.TopNOperatorFactory;
import io.prestosql.operator.TopNRankingNumberOperator;
import io.prestosql.operator.UpdateIndexOperator;
import io.prestosql.operator.UpdateOperator;
import io.prestosql.operator.VacuumTableOperator;
import io.prestosql.operator.ValuesOperator.ValuesOperatorFactory;
import io.prestosql.operator.WindowFunctionDefinition;
import io.prestosql.operator.WindowOperator.WindowOperatorFactory;
import io.prestosql.operator.WorkProcessorPipelineSourceOperator;
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.operator.aggregation.LambdaProvider;
import io.prestosql.operator.aggregation.partial.PartialAggregationController;
import io.prestosql.operator.dynamicfilter.CrossRegionDynamicFilterOperator;
import io.prestosql.operator.exchange.LocalExchange.LocalExchangeFactory;
import io.prestosql.operator.exchange.LocalExchangeSinkOperator.LocalExchangeSinkOperatorFactory;
import io.prestosql.operator.exchange.LocalExchangeSourceOperator.LocalExchangeSourceOperatorFactory;
import io.prestosql.operator.exchange.LocalMergeSourceOperator.LocalMergeSourceOperatorFactory;
import io.prestosql.operator.exchange.PageChannelSelector;
import io.prestosql.operator.index.DynamicTupleFilterFactory;
import io.prestosql.operator.index.FieldSetFilteringRecordSet;
import io.prestosql.operator.index.IndexBuildDriverFactoryProvider;
import io.prestosql.operator.index.IndexJoinLookupStats;
import io.prestosql.operator.index.IndexLookupSourceFactory;
import io.prestosql.operator.index.IndexSourceOperator;
import io.prestosql.operator.output.PartitionedOutputOperator.PartitionedOutputFactory;
import io.prestosql.operator.output.PositionsAppenderFactory;
import io.prestosql.operator.project.CursorProcessor;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.operator.window.FrameInfo;
import io.prestosql.operator.window.WindowFunctionSupplier;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorIndex;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilterSupplier;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.FunctionMetadata;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.AggregationNode.Aggregation;
import io.prestosql.spi.plan.AggregationNode.Step;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.GroupIdNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.MarkDistinctNode;
import io.prestosql.spi.plan.OrderingScheme;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.TopNNode;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.plan.WindowNode.Frame;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.LambdaDefinitionExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.relation.VariableToChannelTranslator;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.PartitioningSpillerFactory;
import io.prestosql.spiller.SingleStreamSpillerFactory;
import io.prestosql.spiller.SpillerFactory;
import io.prestosql.split.MappedRecordSet;
import io.prestosql.split.PageSinkManager;
import io.prestosql.split.PageSourceProvider;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.gen.ExpressionCompiler;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.gen.JoinFilterFunctionCompiler;
import io.prestosql.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.prestosql.sql.gen.OrderingCompiler;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.sql.planner.optimizations.IndexJoinOptimizer;
import io.prestosql.sql.planner.plan.AssignUniqueId;
import io.prestosql.sql.planner.plan.AssignmentUtils;
import io.prestosql.sql.planner.plan.CacheTableFinishNode;
import io.prestosql.sql.planner.plan.CacheTableWriterNode;
import io.prestosql.sql.planner.plan.CreateIndexNode;
import io.prestosql.sql.planner.plan.CubeFinishNode;
import io.prestosql.sql.planner.plan.DeleteNode;
import io.prestosql.sql.planner.plan.DistinctLimitNode;
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.ExplainAnalyzeNode;
import io.prestosql.sql.planner.plan.IndexJoinNode;
import io.prestosql.sql.planner.plan.IndexSourceNode;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;
import io.prestosql.sql.planner.plan.OutputNode;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.sql.planner.plan.RemoteSourceNode;
import io.prestosql.sql.planner.plan.RowNumberNode;
import io.prestosql.sql.planner.plan.SampleNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SortNode;
import io.prestosql.sql.planner.plan.SpatialJoinNode;
import io.prestosql.sql.planner.plan.StatisticAggregationsDescriptor;
import io.prestosql.sql.planner.plan.StatisticsWriterNode;
import io.prestosql.sql.planner.plan.TableDeleteNode;
import io.prestosql.sql.planner.plan.TableExecuteHandle;
import io.prestosql.sql.planner.plan.TableExecuteNode;
import io.prestosql.sql.planner.plan.TableFinishNode;
import io.prestosql.sql.planner.plan.TableUpdateNode;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.sql.planner.plan.TableWriterNode.DeleteTarget;
import io.prestosql.sql.planner.plan.TopNRankingNumberNode;
import io.prestosql.sql.planner.plan.UnnestNode;
import io.prestosql.sql.planner.plan.UpdateIndexNode;
import io.prestosql.sql.planner.plan.UpdateNode;
import io.prestosql.sql.planner.plan.VacuumTableNode;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.relational.RowExpressionDeterminismEvaluator;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.statestore.listener.StateStoreListenerManager;
import io.prestosql.utils.HetuConfig;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Functions.forMap;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.DiscreteDomain.integers;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Range.closedOpen;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.SystemSessionProperties.getAdaptivePartialAggregationMinRows;
import static io.prestosql.SystemSessionProperties.getAdaptivePartialAggregationUniqueRowsRatioThreshold;
import static io.prestosql.SystemSessionProperties.getAggregationOperatorUnspillMemoryLimit;
import static io.prestosql.SystemSessionProperties.getCteMaxPrefetchQueueSize;
import static io.prestosql.SystemSessionProperties.getCteMaxQueueSize;
import static io.prestosql.SystemSessionProperties.getCteResultCacheThresholdSize;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringMaxPerDriverSize;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringMaxPerDriverValueCount;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringWaitTime;
import static io.prestosql.SystemSessionProperties.getFilterAndProjectMinOutputPageRowCount;
import static io.prestosql.SystemSessionProperties.getFilterAndProjectMinOutputPageSize;
import static io.prestosql.SystemSessionProperties.getRetryPolicy;
import static io.prestosql.SystemSessionProperties.getSpillOperatorThresholdReuseExchange;
import static io.prestosql.SystemSessionProperties.getTaskConcurrency;
import static io.prestosql.SystemSessionProperties.getTaskWriterCount;
import static io.prestosql.SystemSessionProperties.isAdaptivePartialAggregationEnabled;
import static io.prestosql.SystemSessionProperties.isCTEReuseEnabled;
import static io.prestosql.SystemSessionProperties.isCrossRegionDynamicFilterEnabled;
import static io.prestosql.SystemSessionProperties.isEnableDynamicFiltering;
import static io.prestosql.SystemSessionProperties.isNonBlockingSpillOrderby;
import static io.prestosql.SystemSessionProperties.isSpillEnabled;
import static io.prestosql.SystemSessionProperties.isSpillForOuterJoinEnabled;
import static io.prestosql.SystemSessionProperties.isSpillOrderBy;
import static io.prestosql.SystemSessionProperties.isSpillReuseExchange;
import static io.prestosql.SystemSessionProperties.isSpillToHdfsEnabled;
import static io.prestosql.SystemSessionProperties.isSpillWindowOperator;
import static io.prestosql.dynamicfilter.DynamicFilterCacheManager.createCacheKey;
import static io.prestosql.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static io.prestosql.expressions.RowExpressionNodeInliner.replaceExpression;
import static io.prestosql.operator.CommonTableExpressionOperator.CommonTableExpressionOperatorFactory;
import static io.prestosql.operator.CreateIndexOperator.CreateIndexOperatorFactory;
import static io.prestosql.operator.DistinctLimitOperator.DistinctLimitOperatorFactory;
import static io.prestosql.operator.NestedLoopBuildOperator.NestedLoopBuildOperatorFactory;
import static io.prestosql.operator.NestedLoopJoinOperator.NestedLoopJoinOperatorFactory;
import static io.prestosql.operator.PipelineExecutionStrategy.GROUPED_EXECUTION;
import static io.prestosql.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static io.prestosql.operator.TableFinishOperator.TableFinishOperatorFactory;
import static io.prestosql.operator.TableFinishOperator.TableFinisher;
import static io.prestosql.operator.TableWriterOperator.FRAGMENT_CHANNEL;
import static io.prestosql.operator.TableWriterOperator.ROW_COUNT_CHANNEL;
import static io.prestosql.operator.TableWriterOperator.STATS_START_CHANNEL;
import static io.prestosql.operator.TableWriterOperator.TableWriterOperatorFactory;
import static io.prestosql.operator.WindowFunctionDefinition.window;
import static io.prestosql.operator.unnest.UnnestOperator.UnnestOperatorFactory;
import static io.prestosql.spi.StandardErrorCode.COMPILER_ERROR;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT;
import static io.prestosql.spi.plan.AggregationNode.Step.FINAL;
import static io.prestosql.spi.plan.AggregationNode.Step.PARTIAL;
import static io.prestosql.spi.plan.JoinNode.Type.FULL;
import static io.prestosql.spi.plan.JoinNode.Type.INNER;
import static io.prestosql.spi.plan.JoinNode.Type.RIGHT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.TypeUtils.writeNativeValue;
import static io.prestosql.spi.util.Reflection.constructorMethodHandle;
import static io.prestosql.spiller.PartitioningSpillerFactory.unsupportedPartitioningSpillerFactory;
import static io.prestosql.sql.DynamicFilters.extractStaticFilters;
import static io.prestosql.sql.gen.LambdaBytecodeGenerator.compileLambdaProvider;
import static io.prestosql.sql.planner.RowExpressionInterpreter.Level.OPTIMIZED;
import static io.prestosql.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_PASSTHROUGH_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.prestosql.sql.planner.VariableReferenceSymbolConverter.toSymbol;
import static io.prestosql.sql.planner.VariableReferenceSymbolConverter.toVariableReference;
import static io.prestosql.sql.planner.plan.AssignmentUtils.identityAssignments;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.prestosql.sql.planner.plan.TableWriterNode.CreateTarget;
import static io.prestosql.sql.planner.plan.TableWriterNode.DeleteAsInsertTarget;
import static io.prestosql.sql.planner.plan.TableWriterNode.InsertTarget;
import static io.prestosql.sql.planner.plan.TableWriterNode.UpdateTarget;
import static io.prestosql.sql.planner.plan.TableWriterNode.VacuumTarget;
import static io.prestosql.sql.planner.plan.TableWriterNode.WriterTarget;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.util.SpatialJoinUtils.ST_CONTAINS;
import static io.prestosql.util.SpatialJoinUtils.ST_DISTANCE;
import static io.prestosql.util.SpatialJoinUtils.ST_INTERSECTS;
import static io.prestosql.util.SpatialJoinUtils.ST_WITHIN;
import static io.prestosql.util.SpatialJoinUtils.extractSupportedSpatialComparisons;
import static io.prestosql.util.SpatialJoinUtils.extractSupportedSpatialFunctions;
import static java.util.Objects.requireNonNull;
import static java.util.stream.IntStream.range;

public class LocalExecutionPlanner
{
    private static final Logger log = Logger.get(LocalExecutionPlanner.class);

    protected final Metadata metadata;
    protected final TypeAnalyzer typeAnalyzer;
    protected final Optional<ExplainAnalyzeContext> explainAnalyzeContext;
    protected final PageSourceProvider pageSourceProvider;
    protected final IndexManager indexManager;
    protected final NodePartitioningManager nodePartitioningManager;
    protected final PageSinkManager pageSinkManager;
    protected final ExchangeClientSupplier exchangeClientSupplier;
    protected final ExpressionCompiler expressionCompiler;
    protected final PageFunctionCompiler pageFunctionCompiler;
    protected final JoinFilterFunctionCompiler joinFilterFunctionCompiler;
    protected final DataSize maxIndexMemorySize;
    protected final IndexJoinLookupStats indexJoinLookupStats;
    protected final DataSize maxPartialAggregationMemorySize;
    protected final DataSize maxPagePartitioningBufferSize;
    protected final DataSize maxLocalExchangeBufferSize;
    protected final SpillerFactory spillerFactory;
    protected final SingleStreamSpillerFactory singleStreamSpillerFactory;
    protected final PartitioningSpillerFactory partitioningSpillerFactory;
    protected final PagesIndex.Factory pagesIndexFactory;
    protected final JoinCompiler joinCompiler;
    protected final LookupJoinOperators lookupJoinOperators;
    protected final OrderingCompiler orderingCompiler;
    protected final StateStoreProvider stateStoreProvider;
    protected final NodeInfo nodeInfo;
    protected final CubeManager cubeManager;
    protected final StateStoreListenerManager stateStoreListenerManager;
    protected final DynamicFilterCacheManager dynamicFilterCacheManager;
    protected final HeuristicIndexerManager heuristicIndexerManager;
    protected final FunctionResolution functionResolution;
    protected final LogicalRowExpressions logicalRowExpressions;
    protected final TaskManagerConfig taskManagerConfig;
    private final ExchangeManagerRegistry exchangeManagerRegistry;
    protected final TableExecuteContextManager tableExecuteContextManager;
    private final PositionsAppenderFactory positionsAppenderFactory = new PositionsAppenderFactory();
    private final CachedDataManager cachedDataManager;
    private final String userName;

    public Metadata getMetadata()
    {
        return metadata;
    }

    public TypeAnalyzer getTypeAnalyzer()
    {
        return typeAnalyzer;
    }

    public Optional<ExplainAnalyzeContext> getExplainAnalyzeContext()
    {
        return explainAnalyzeContext;
    }

    public PageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    public IndexManager getIndexManager()
    {
        return indexManager;
    }

    public NodePartitioningManager getNodePartitioningManager()
    {
        return nodePartitioningManager;
    }

    public PageSinkManager getPageSinkManager()
    {
        return pageSinkManager;
    }

    public ExchangeClientSupplier getExchangeClientSupplier()
    {
        return exchangeClientSupplier;
    }

    public ExpressionCompiler getExpressionCompiler()
    {
        return expressionCompiler;
    }

    public PageFunctionCompiler getPageFunctionCompiler()
    {
        return pageFunctionCompiler;
    }

    public JoinFilterFunctionCompiler getJoinFilterFunctionCompiler()
    {
        return joinFilterFunctionCompiler;
    }

    public DataSize getMaxIndexMemorySize()
    {
        return maxIndexMemorySize;
    }

    public IndexJoinLookupStats getIndexJoinLookupStats()
    {
        return indexJoinLookupStats;
    }

    public DataSize getMaxPartialAggregationMemorySize()
    {
        return maxPartialAggregationMemorySize;
    }

    public DataSize getMaxPagePartitioningBufferSize()
    {
        return maxPagePartitioningBufferSize;
    }

    public DataSize getMaxLocalExchangeBufferSize()
    {
        return maxLocalExchangeBufferSize;
    }

    public SpillerFactory getSpillerFactory()
    {
        return spillerFactory;
    }

    public SingleStreamSpillerFactory getSingleStreamSpillerFactory()
    {
        return singleStreamSpillerFactory;
    }

    public PartitioningSpillerFactory getPartitioningSpillerFactory()
    {
        return partitioningSpillerFactory;
    }

    public PagesIndex.Factory getPagesIndexFactory()
    {
        return pagesIndexFactory;
    }

    public JoinCompiler getJoinCompiler()
    {
        return joinCompiler;
    }

    public LookupJoinOperators getLookupJoinOperators()
    {
        return lookupJoinOperators;
    }

    public OrderingCompiler getOrderingCompiler()
    {
        return orderingCompiler;
    }

    public StateStoreProvider getStateStoreProvider()
    {
        return stateStoreProvider;
    }

    public NodeInfo getNodeInfo()
    {
        return nodeInfo;
    }

    public CubeManager getCubeManager()
    {
        return cubeManager;
    }

    public StateStoreListenerManager getStateStoreListenerManager()
    {
        return stateStoreListenerManager;
    }

    public DynamicFilterCacheManager getDynamicFilterCacheManager()
    {
        return dynamicFilterCacheManager;
    }

    public HeuristicIndexerManager getHeuristicIndexerManager()
    {
        return heuristicIndexerManager;
    }

    public TaskManagerConfig getTaskManagerConfig()
    {
        return taskManagerConfig;
    }

    @Inject
    public LocalExecutionPlanner(
            Metadata metadata,
            TypeAnalyzer typeAnalyzer,
            Optional<ExplainAnalyzeContext> explainAnalyzeContext,
            PageSourceProvider pageSourceProvider,
            IndexManager indexManager,
            NodePartitioningManager nodePartitioningManager,
            PageSinkManager pageSinkManager,
            ExchangeClientSupplier exchangeClientSupplier,
            ExpressionCompiler expressionCompiler,
            PageFunctionCompiler pageFunctionCompiler,
            JoinFilterFunctionCompiler joinFilterFunctionCompiler,
            IndexJoinLookupStats indexJoinLookupStats,
            TaskManagerConfig taskManagerConfig,
            SpillerFactory spillerFactory,
            SingleStreamSpillerFactory singleStreamSpillerFactory,
            PartitioningSpillerFactory partitioningSpillerFactory,
            PagesIndex.Factory pagesIndexFactory,
            JoinCompiler joinCompiler,
            LookupJoinOperators lookupJoinOperators,
            OrderingCompiler orderingCompiler,
            NodeInfo nodeInfo,
            StateStoreProvider stateStoreProvider,
            StateStoreListenerManager stateStoreListenerManager,
            DynamicFilterCacheManager dynamicFilterCacheManager,
            HeuristicIndexerManager heuristicIndexerManager,
            CubeManager cubeManager,
            ExchangeManagerRegistry exchangeManagerRegistry,
            TableExecuteContextManager tableExecuteContextManager,
            CachedDataManager cachedDataManager,
            HetuConfig hetuConfig)
    {
        this.explainAnalyzeContext = requireNonNull(explainAnalyzeContext, "explainAnalyzeContext is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.indexManager = requireNonNull(indexManager, "indexManager is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.exchangeClientSupplier = exchangeClientSupplier;
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        this.pageSinkManager = requireNonNull(pageSinkManager, "pageSinkManager is null");
        this.expressionCompiler = requireNonNull(expressionCompiler, "compiler is null");
        this.pageFunctionCompiler = requireNonNull(pageFunctionCompiler, "pageFunctionCompiler is null");
        this.joinFilterFunctionCompiler = requireNonNull(joinFilterFunctionCompiler, "compiler is null");
        this.indexJoinLookupStats = requireNonNull(indexJoinLookupStats, "indexJoinLookupStats is null");
        this.taskManagerConfig = taskManagerConfig;
        this.maxIndexMemorySize = requireNonNull(taskManagerConfig, "taskManagerConfig is null").getMaxIndexMemoryUsage();
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");
        this.partitioningSpillerFactory = requireNonNull(partitioningSpillerFactory, "partitioningSpillerFactory is null");
        this.maxPartialAggregationMemorySize = taskManagerConfig.getMaxPartialAggregationMemoryUsage();
        this.maxPagePartitioningBufferSize = taskManagerConfig.getMaxPagePartitioningBufferSize();
        this.maxLocalExchangeBufferSize = taskManagerConfig.getMaxLocalExchangeBufferSize();
        this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
        this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        this.lookupJoinOperators = requireNonNull(lookupJoinOperators, "lookupJoinOperators is null");
        this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
        this.stateStoreProvider = requireNonNull(stateStoreProvider, "stateStore is null");
        this.nodeInfo = nodeInfo;
        this.stateStoreListenerManager = requireNonNull(stateStoreListenerManager, "stateStoreListenerManager is null");
        this.dynamicFilterCacheManager = requireNonNull(dynamicFilterCacheManager, "dynamicFilterCacheManager is null");
        this.heuristicIndexerManager = requireNonNull(heuristicIndexerManager, "heuristicIndexerManager is null");
        this.cubeManager = requireNonNull(cubeManager, "cubeManager is null");
        this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager());
        this.logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(metadata), functionResolution, metadata.getFunctionAndTypeManager());
        this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
        this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
        this.cachedDataManager = requireNonNull(cachedDataManager, "cachedDataManager is null");
        this.userName = requireNonNull(hetuConfig, "hetuConfig is null").getCachingUserName();
    }

    public LocalExecutionPlan plan(
            TaskContext taskContext,
            PlanNode plan,
            TypeProvider types,
            PartitioningScheme partitioningScheme,
            StageExecutionDescriptor stageExecutionDescriptor,
            List<PlanNodeId> partitionedSourceOrder,
            OutputBuffer outputBuffer,
            Optional<PlanFragmentId> feederCTEId,
            Optional<PlanNodeId> feederCTEParentId,
            Map<String, CommonTableExecutionContext> cteCtx)
    {
        List<Symbol> outputLayout = partitioningScheme.getOutputLayout();

        if (partitioningScheme.getPartitioning().getHandle().equals(FIXED_BROADCAST_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(FIXED_ARBITRARY_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(SCALED_WRITER_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(SINGLE_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(COORDINATOR_DISTRIBUTION)) {
            return plan(taskContext, stageExecutionDescriptor, plan, outputLayout, types, partitionedSourceOrder, outputBuffer, new TaskOutputFactory(outputBuffer), feederCTEId, feederCTEParentId, cteCtx);
        }

        // We can convert the symbols directly into channels, because the root must be a sink and therefore the layout is fixed
        List<Integer> partitionChannels;
        List<Optional<NullableValue>> partitionConstants;
        List<Type> partitionChannelTypes;
        if (partitioningScheme.getHashColumn().isPresent()) {
            partitionChannels = ImmutableList.of(outputLayout.indexOf(partitioningScheme.getHashColumn().get()));
            partitionConstants = ImmutableList.of(Optional.empty());
            partitionChannelTypes = ImmutableList.of(BIGINT);
        }
        else {
            partitionChannels = partitioningScheme.getPartitioning().getArguments().stream()
                    .map(argument -> {
                        if (argument.isConstant()) {
                            return -1;
                        }
                        return outputLayout.indexOf(argument.getColumn());
                    })
                    .collect(toImmutableList());
            partitionConstants = partitioningScheme.getPartitioning().getArguments().stream()
                    .map(argument -> {
                        if (argument.isConstant()) {
                            return Optional.of(argument.getConstant());
                        }
                        return Optional.<NullableValue>empty();
                    })
                    .collect(toImmutableList());
            partitionChannelTypes = partitioningScheme.getPartitioning().getArguments().stream()
                    .map(argument -> {
                        if (argument.isConstant()) {
                            return argument.getConstant().getType();
                        }
                        return types.get(argument.getColumn());
                    })
                    .collect(toImmutableList());
        }

        PartitionFunction partitionFunction = nodePartitioningManager.getPartitionFunction(taskContext.getSession(), partitioningScheme, partitionChannelTypes);
        OptionalInt nullChannel = OptionalInt.empty();
        Set<Symbol> partitioningColumns = partitioningScheme.getPartitioning().getColumns();

        // partitioningColumns expected to have one column in the normal case, and zero columns when partitioning on a constant
        checkArgument(!partitioningScheme.isReplicateNullsAndAny() || partitioningColumns.size() <= 1);
        if (partitioningScheme.isReplicateNullsAndAny() && partitioningColumns.size() == 1) {
            nullChannel = OptionalInt.of(outputLayout.indexOf(getOnlyElement(partitioningColumns)));
        }

        return plan(
                taskContext,
                stageExecutionDescriptor,
                plan,
                outputLayout,
                types,
                partitionedSourceOrder,
                outputBuffer,
                new PartitionedOutputFactory(
                        partitionFunction,
                        partitionChannels,
                        partitionConstants,
                        partitioningScheme.isReplicateNullsAndAny(),
                        nullChannel,
                        outputBuffer,
                        maxPagePartitioningBufferSize,
                        positionsAppenderFactory),
                feederCTEId,
                feederCTEParentId,
                cteCtx);
    }

    public LocalExecutionPlan plan(
            TaskContext taskContext,
            StageExecutionDescriptor stageExecutionDescriptor,
            PlanNode plan,
            List<Symbol> outputLayout,
            TypeProvider types,
            List<PlanNodeId> partitionedSourceOrder,
            OutputBuffer outputBuffer,
            OutputFactory outputOperatorFactory,
            Optional<PlanFragmentId> feederCTEId,
            Optional<PlanNodeId> feederCTEParentId,
            Map<String, CommonTableExecutionContext> cteCtx)
    {
        Session session = taskContext.getSession();
        LocalExecutionPlanContext context = new LocalExecutionPlanContext(taskContext, types, metadata, dynamicFilterCacheManager, feederCTEId, feederCTEParentId, cteCtx);

        PhysicalOperation physicalOperation = plan.accept(new Visitor(session, stageExecutionDescriptor), context);

        Function<Page, Page> pagePreprocessor = enforceLayoutProcessor(outputLayout, physicalOperation.getLayout());

        List<Type> outputTypes = outputLayout.stream()
                .map(types::get)
                .collect(toImmutableList());

        context.addDriverFactory(
                context.isInputDriver(),
                true,
                ImmutableList.<OperatorFactory>builder()
                        .addAll(physicalOperation.getOperatorFactories())
                        .add(outputOperatorFactory.createOutputOperator(
                                context.getNextOperatorId(),
                                plan.getId(),
                                outputTypes,
                                pagePreprocessor,
                                taskContext))
                        .build(),
                context.getDriverInstanceCount(),
                physicalOperation.getPipelineExecutionStrategy());

        addLookupOuterDrivers(context);

        // notify operator factories that planning has completed
        context.getDriverFactories().stream()
                .map(DriverFactory::getOperatorFactories)
                .flatMap(List::stream)
                .filter(LocalPlannerAware.class::isInstance)
                .map(LocalPlannerAware.class::cast)
                .forEach(LocalPlannerAware::localPlannerComplete);

        // calculate total number of components to be captured and add to snapshotManager
        if (SystemSessionProperties.isSnapshotEnabled(session)) {
            taskContext.getSnapshotManager().setTotalComponents(calculateTotalCountOfTaskComponentToBeCaptured(taskContext, context, outputBuffer));
        }

        return new LocalExecutionPlan(context.getDriverFactories(), partitionedSourceOrder, stageExecutionDescriptor, feederCTEId);
    }

    private static int calculateTotalCountOfTaskComponentToBeCaptured(TaskContext taskContext, LocalExecutionPlanContext context, OutputBuffer outputBuffer)
    {
        int totalCount = 0;
        boolean outputPipelineIsTableScan = false;
        for (DriverFactory df : context.getDriverFactories()) {
            // Don't include operators in table-scan pipelines
            if (!isTableScanPipeline(df)) {
                if (isOuterJoinFromTableScanPipeline(df, context)) {
                    // See Gitee issue Checkpoint - handle LookupOuterOperator pipelines
                    // https://gitee.com/open_lookeng/dashboard/issues?id=I2LMIW
                    // For outer pipelines forked from table-scan, only count the lookup-outer operator
                    totalCount += df.getDriverInstances().orElse(1);
                }
                else {
                    totalCount += df.getDriverInstances().orElse(1) * df.getOperatorFactories().size();
                }
            }
            else if (df.isOutputDriver()) {
                outputPipelineIsTableScan = true;
            }
        }

        int stageId = taskContext.getTaskId().getStageId().getId();
        // No OutputBuffer state needs to be captured for stage 0
        // If output pipeline is not also source pipeline, then OutputBuffer state needs to be captured
        if (stageId > 0 && !outputPipelineIsTableScan) {
            // For partitioned output buffer, each partition has its own snapshot state, so need to count all of them.
            // For other output buffers, there is a single snapshot state, so use 1.
            OutputBufferInfo info = outputBuffer.getInfo();
            if (info.getType().equals("PARTITIONED")) {
                totalCount += info.getBuffers().size();
            }
            else {
                totalCount++;
            }
        }
        return totalCount;
    }

    private static boolean isTableScanPipeline(DriverFactory driverFactory)
    {
        OperatorFactory first = driverFactory.getOperatorFactories().get(0);
        return first instanceof TableScanOperatorFactory || first instanceof ScanFilterAndProjectOperatorFactory;
    }

    private static boolean isOuterJoinFromTableScanPipeline(DriverFactory driverFactory, LocalExecutionPlanContext context)
    {
        OperatorFactory first = driverFactory.getOperatorFactories().get(0);
        return first instanceof LookupOuterOperatorFactory && isTableScanPipeline(context.outerToJoinMap.get(driverFactory));
    }

    protected static void addLookupOuterDrivers(LocalExecutionPlanContext context)
    {
        // For an outer join on the lookup side (RIGHT or FULL) add an additional
        // driver to output the unused rows in the lookup source
        for (DriverFactory factory : context.getDriverFactories()) {
            List<OperatorFactory> operatorFactories = factory.getOperatorFactories();
            for (int i = 0; i < operatorFactories.size(); i++) {
                OperatorFactory operatorFactory = operatorFactories.get(i);
                if (!(operatorFactory instanceof JoinOperatorFactory)) {
                    continue;
                }

                JoinOperatorFactory lookupJoin = (JoinOperatorFactory) operatorFactory;
                Optional<OuterOperatorFactoryResult> outerOperatorFactoryResult = lookupJoin.createOuterOperatorFactory();
                if (outerOperatorFactoryResult.isPresent()) {
                    // Add a new driver to output the unmatched rows in an outer join.
                    // We duplicate all of the factories above the JoinOperator (the ones reading from the joins),
                    // and replace the JoinOperator with the OuterOperator (the one that produces unmatched rows).
                    ImmutableList.Builder<OperatorFactory> newOperators = ImmutableList.builder();
                    newOperators.add(outerOperatorFactoryResult.get().getOuterOperatorFactory());
                    operatorFactories.subList(i + 1, operatorFactories.size()).stream()
                            .map(OperatorFactory::duplicate)
                            .forEach(newOperators::add);

                    DriverFactory outerFactory = context.addDriverFactory(false, factory.isOutputDriver(), newOperators.build(), OptionalInt.of(1), outerOperatorFactoryResult.get().getBuildExecutionStrategy());
                    context.outerToJoinMap.put(outerFactory, factory);
                }
            }
        }
    }

    public static class LocalExecutionPlanContext
    {
        protected final TaskContext taskContext;
        protected final TypeProvider types;
        protected List<DriverFactory> driverFactories;
        protected final Optional<IndexSourceContext> indexSourceContext;

        // the collector is shared with all subContexts to allow local dynamic filtering
        // with multiple table scans (e.g. co-located joins).
        protected final LocalDynamicFiltersCollector dynamicFiltersCollector;

        // this is shared with all subContexts
        protected final AtomicInteger nextPipelineId;

        private int nextOperatorId;
        private boolean inputDriver = true;
        private OptionalInt driverInstanceCount = OptionalInt.empty();
        private Map<PlanNodeId, OperatorFactory> cteOperationMap = new HashMap<>();
        protected Map<String, CommonTableExecutionContext> cteCtx;
        private static Map<String, PhysicalOperation> sourceInitialized = new ConcurrentHashMap<>();
        private final PlanNodeId consumerId;
        protected final Optional<PlanFragmentId> feederCTEId;
        protected final Optional<PlanNodeId> feederCTEParentId;

        // Snapshot: record pipeline that corresponds to the lookup-outer pipeline.
        // This is used to help determine if a lookup-outer pipeline should be treated as a tabel-scan pipeine.
        private final Map<DriverFactory, DriverFactory> outerToJoinMap = new HashMap<>();

        public LocalExecutionPlanContext(TaskContext taskContext, TypeProvider types, Metadata metadata, DynamicFilterCacheManager dynamicFilterCacheManager,
                Optional<PlanFragmentId> feederCTEId,
                Optional<PlanNodeId> feederCTEParentId,
                Map<String, CommonTableExecutionContext> cteCtx)
        {
            this(taskContext, types, new ArrayList<>(), Optional.empty(), new LocalDynamicFiltersCollector(taskContext, Optional.of(metadata), dynamicFilterCacheManager), new AtomicInteger(0), feederCTEId, feederCTEParentId, cteCtx);
        }

        protected LocalExecutionPlanContext(
                TaskContext taskContext,
                TypeProvider types,
                List<DriverFactory> driverFactories,
                Optional<IndexSourceContext> indexSourceContext,
                LocalDynamicFiltersCollector dynamicFiltersCollector,
                AtomicInteger nextPipelineId,
                Optional<PlanFragmentId> feederCTEId,
                Optional<PlanNodeId> feederCTEParentId,
                Map<String, CommonTableExecutionContext> cteCtx)
        {
            this.taskContext = taskContext;
            this.types = types;
            this.driverFactories = driverFactories;
            this.indexSourceContext = indexSourceContext;
            this.dynamicFiltersCollector = dynamicFiltersCollector;
            this.nextPipelineId = nextPipelineId;
            this.consumerId = taskContext.getConsumerId();
            this.feederCTEId = feederCTEId;
            this.feederCTEParentId = feederCTEParentId;
            this.cteCtx = cteCtx;
        }

        public DriverFactory addDriverFactory(boolean inputDriver, boolean outputDriver, List<OperatorFactory> operatorFactories, OptionalInt driverInstances, PipelineExecutionStrategy pipelineExecutionStrategy)
        {
            if (pipelineExecutionStrategy == GROUPED_EXECUTION) {
                OperatorFactory firstOperatorFactory = operatorFactories.get(0);
                if (inputDriver) {
                    checkArgument(firstOperatorFactory instanceof ScanFilterAndProjectOperatorFactory || firstOperatorFactory instanceof TableScanOperatorFactory);
                }
                else {
                    checkArgument(firstOperatorFactory instanceof LocalExchangeSourceOperatorFactory || firstOperatorFactory instanceof LookupOuterOperatorFactory);
                }
            }

            List<OperatorFactory> factories = operatorFactories;
            if (SystemSessionProperties.isWorkProcessorPipelines(taskContext.getSession())) {
                factories = WorkProcessorPipelineSourceOperator.convertOperators(getNextOperatorId(), factories);
            }

            DriverFactory driverFactory = new DriverFactory(getNextPipelineId(), inputDriver, outputDriver, factories, driverInstances, pipelineExecutionStrategy);
            driverFactories.add(driverFactory);
            return driverFactory;
        }

        public List<DriverFactory> getDriverFactories()
        {
            return ImmutableList.copyOf(driverFactories);
        }

        public void setDriverFactories(List<DriverFactory> driverFactories)
        {
            this.driverFactories = driverFactories;
        }

        public Session getSession()
        {
            return taskContext.getSession();
        }

        public StageId getStageId()
        {
            return taskContext.getTaskId().getStageId();
        }

        public TaskId getTaskId()
        {
            return taskContext.getTaskId();
        }

        public TypeProvider getTypes()
        {
            return types;
        }

        public LocalDynamicFiltersCollector getDynamicFiltersCollector()
        {
            return dynamicFiltersCollector;
        }

        public Optional<IndexSourceContext> getIndexSourceContext()
        {
            return indexSourceContext;
        }

        private AtomicInteger getPipelineId()
        {
            return nextPipelineId;
        }

        public int getNextPipelineId()
        {
            return nextPipelineId.getAndIncrement();
        }

        public int getNextOperatorId()
        {
            return nextOperatorId++;
        }

        public boolean isInputDriver()
        {
            return inputDriver;
        }

        public void setInputDriver(boolean inputDriver)
        {
            this.inputDriver = inputDriver;
        }

        public LocalExecutionPlanContext createSubContext()
        {
            checkState(!indexSourceContext.isPresent(), "index build plan can not have sub-contexts");
            return new LocalExecutionPlanContext(taskContext, types, driverFactories, indexSourceContext, dynamicFiltersCollector, nextPipelineId, feederCTEId, feederCTEParentId, cteCtx);
        }

        public LocalExecutionPlanContext createIndexSourceSubContext(IndexSourceContext indexSourceContext)
        {
            return new LocalExecutionPlanContext(taskContext, types, driverFactories, Optional.of(indexSourceContext), dynamicFiltersCollector, nextPipelineId, feederCTEId, feederCTEParentId, cteCtx);
        }

        public OptionalInt getDriverInstanceCount()
        {
            return driverInstanceCount;
        }

        public void setDriverInstanceCount(int driverInstanceCount)
        {
            checkArgument(driverInstanceCount > 0, "driverInstanceCount must be > 0");
            if (this.driverInstanceCount.isPresent()) {
                checkState(this.driverInstanceCount.getAsInt() == driverInstanceCount, "driverInstance count already set to " + this.driverInstanceCount.getAsInt());
            }
            this.driverInstanceCount = OptionalInt.of(driverInstanceCount);
        }

        public CommonTableExecutionContext getRunningTask(String cteExecutorId, Set<PlanNodeId> consumers, PhysicalOperation source)
        {
            checkArgument(feederCTEParentId.isPresent(), "CTE parent Id must be there");
            if (source != null) {
                sourceInitialized.putIfAbsent(cteExecutorId, source);
            }

            return cteCtx.computeIfAbsent(cteExecutorId, k -> new CommonTableExecutionContext(cteExecutorId, consumers,
                    feederCTEParentId.get(), taskContext.getNotificationExecutor(),
                    taskContext.getTaskCount(),
                    getCteMaxQueueSize(getSession()),
                    getCteMaxPrefetchQueueSize(getSession())));
        }

        public String getCteId(PlanNodeId cteNodeId)
        {
            return taskContext.getQueryContext().getQueryId().toString() + "-#-" + taskContext.getTaskId().getId() + "-#-" + cteNodeId.toString();
        }

        public PlanNodeId getConsumerId()
        {
            checkArgument(consumerId != null, "Consumer Id cannot be null for CTE executor!");
            return consumerId;
        }

        public Optional<PlanFragmentId> getFeederCTEId()
        {
            return feederCTEId;
        }

        public Optional<PlanNodeId> getfeederCTEParentId()
        {
            return feederCTEParentId;
        }

        public boolean isCteInitialized(String cteId)
        {
            return sourceInitialized.containsKey(cteId);
        }
    }

    public static class IndexSourceContext
    {
        private final SetMultimap<Symbol, Integer> indexLookupToProbeInput;

        public IndexSourceContext(SetMultimap<Symbol, Integer> indexLookupToProbeInput)
        {
            this.indexLookupToProbeInput = ImmutableSetMultimap.copyOf(requireNonNull(indexLookupToProbeInput, "indexLookupToProbeInput is null"));
        }

        private SetMultimap<Symbol, Integer> getIndexLookupToProbeInput()
        {
            return indexLookupToProbeInput;
        }
    }

    public static class LocalExecutionPlan
    {
        private final List<DriverFactory> driverFactories;
        private final List<PlanNodeId> partitionedSourceOrder;
        private final StageExecutionDescriptor stageExecutionDescriptor;
        private final Optional<PlanFragmentId> feederCTEId;

        public LocalExecutionPlan(List<DriverFactory> driverFactories, List<PlanNodeId> partitionedSourceOrder,
                StageExecutionDescriptor stageExecutionDescriptor, Optional<PlanFragmentId> feederCTEId)
        {
            this.driverFactories = ImmutableList.copyOf(requireNonNull(driverFactories, "driverFactories is null"));
            this.partitionedSourceOrder = ImmutableList.copyOf(requireNonNull(partitionedSourceOrder, "partitionedSourceOrder is null"));
            this.stageExecutionDescriptor = requireNonNull(stageExecutionDescriptor, "stageExecutionDescriptor is null");
            this.feederCTEId = feederCTEId;
        }

        public List<DriverFactory> getDriverFactories()
        {
            return driverFactories;
        }

        public List<PlanNodeId> getPartitionedSourceOrder()
        {
            return partitionedSourceOrder;
        }

        public StageExecutionDescriptor getStageExecutionDescriptor()
        {
            return stageExecutionDescriptor;
        }

        public Optional<PlanFragmentId> getFeederCTEId()
        {
            return feederCTEId;
        }
    }

    public class Visitor
            extends InternalPlanVisitor<PhysicalOperation, LocalExecutionPlanContext>
    {
        protected final Session session;
        protected final StageExecutionDescriptor stageExecutionDescriptor;

        public Visitor(Session session, StageExecutionDescriptor stageExecutionDescriptor)
        {
            this.session = session;
            this.stageExecutionDescriptor = stageExecutionDescriptor;
        }

        @Override
        public PhysicalOperation visitRemoteSource(RemoteSourceNode node, LocalExecutionPlanContext context)
        {
            if (node.getOrderingScheme().isPresent()) {
                return createMergeSource(node, context);
            }

            return createRemoteSource(node, context);
        }

        private PhysicalOperation createMergeSource(RemoteSourceNode node, LocalExecutionPlanContext context)
        {
            checkArgument(node.getOrderingScheme().isPresent(), "orderingScheme is absent");

            // merging remote source must have a single driver
            context.setDriverInstanceCount(1);

            OrderingScheme orderingScheme = node.getOrderingScheme().get();
            ImmutableMap<Symbol, Integer> layout = makeLayout(node);
            List<Integer> sortChannels = getChannelsForSymbols(orderingScheme.getOrderBy(), layout);
            List<SortOrder> sortOrder = orderingScheme.getOrderingList();

            List<Type> types = getSourceOperatorTypes(node, context.getTypes());
            ImmutableList<Integer> outputChannels = IntStream.range(0, types.size())
                    .boxed()
                    .collect(toImmutableList());

            OperatorFactory operatorFactory = new MergeOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    exchangeClientSupplier,
                    orderingCompiler,
                    types,
                    outputChannels,
                    sortChannels,
                    sortOrder);

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        private PhysicalOperation createRemoteSource(RemoteSourceNode node, LocalExecutionPlanContext context)
        {
            if (!context.getDriverInstanceCount().isPresent()) {
                context.setDriverInstanceCount(getTaskConcurrency(session));
            }

            OperatorFactory operatorFactory = new ExchangeOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    exchangeClientSupplier,
                    node.getRetryPolicy(),
                    exchangeManagerRegistry);

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        @Override
        public PhysicalOperation visitExplainAnalyze(ExplainAnalyzeNode node, LocalExecutionPlanContext context)
        {
            ExplainAnalyzeContext analyzeContext = explainAnalyzeContext
                    .orElseThrow(() -> new IllegalStateException("ExplainAnalyze can only run on coordinator"));
            PhysicalOperation source = node.getSource().accept(this, context);
            OperatorFactory operatorFactory = new ExplainAnalyzeOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    analyzeContext.getQueryPerformanceFetcher(),
                    metadata,
                    node.isVerbose());
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitOutput(OutputNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            // Only create DynamicFilterOperator if it's enabled
            if (isCrossRegionDynamicFilterEnabled(session)) {
                String queryId = context.getSession().getQueryId().getId();
                List<Symbol> inputSymbols = node.getSource().getOutputSymbols();
                OperatorFactory operatorFactory = new CrossRegionDynamicFilterOperator.CrossRegionDynamicFilterOperatorFactory(context.getNextOperatorId(), node.getId(), queryId, inputSymbols, context.getTypes(), dynamicFilterCacheManager, node.getColumnNames(), node.getOutputSymbols());
                return new PhysicalOperation(operatorFactory, makeLayout(node.getSource()), context, source);
            }

            return source;
        }

        @Override
        public PhysicalOperation visitRowNumber(RowNumberNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Integer> partitionChannels = getChannelsForSymbols(partitionBySymbols, source.getLayout());

            List<Type> partitionTypes = partitionChannels.stream()
                    .map(channel -> source.getTypes().get(channel))
                    .collect(toImmutableList());

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            // compute the layout of the output from the window operator
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(source.getLayout());

            // row number function goes in the last channel
            int channel = source.getTypes().size();
            outputMappings.put(node.getRowNumberSymbol(), channel);

            Optional<Integer> hashChannel = node.getHashSymbol().map(channelGetter(source));
            OperatorFactory operatorFactory = new RowNumberOperator.RowNumberOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    outputChannels.build(),
                    partitionChannels,
                    partitionTypes,
                    node.getMaxRowCountPerPartition(),
                    hashChannel,
                    10_000,
                    joinCompiler);
            return new PhysicalOperation(operatorFactory, outputMappings.build(), context, source);
        }

        @Override
        public PhysicalOperation visitTopNRankingNumber(TopNRankingNumberNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Integer> partitionChannels = getChannelsForSymbols(partitionBySymbols, source.getLayout());
            List<Type> partitionTypes = partitionChannels.stream()
                    .map(channel -> source.getTypes().get(channel))
                    .collect(toImmutableList());

            List<Symbol> orderBySymbols = node.getOrderingScheme().getOrderBy();
            List<Integer> sortChannels = getChannelsForSymbols(orderBySymbols, source.getLayout());
            List<SortOrder> sortOrder = orderBySymbols.stream()
                    .map(symbol -> node.getOrderingScheme().getOrdering(symbol))
                    .collect(toImmutableList());

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            // compute the layout of the output from the window operator
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(source.getLayout());

            if (!node.isPartial() || !partitionChannels.isEmpty()) {
                // row number function goes in the last channel
                int channel = source.getTypes().size();
                outputMappings.put(node.getRowNumberSymbol(), channel);
            }

            Optional<Integer> hashChannel = node.getHashSymbol().map(channelGetter(source));
            OperatorFactory operatorFactory = new TopNRankingNumberOperator.TopNRankingNumberOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    outputChannels.build(),
                    partitionChannels,
                    partitionTypes,
                    sortChannels,
                    sortOrder,
                    node.getMaxRowCountPerPartition(),
                    node.isPartial(),
                    hashChannel,
                    1000,
                    joinCompiler,
                    node.getRankingFunction());

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitWindow(WindowNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Integer> partitionChannels = ImmutableList.copyOf(getChannelsForSymbols(partitionBySymbols, source.getLayout()));
            List<Integer> preGroupedChannels = ImmutableList.copyOf(getChannelsForSymbols(ImmutableList.copyOf(node.getPrePartitionedInputs()), source.getLayout()));

            List<Integer> sortChannels = ImmutableList.of();
            List<SortOrder> sortOrder = ImmutableList.of();

            if (node.getOrderingScheme().isPresent()) {
                OrderingScheme orderingScheme = node.getOrderingScheme().get();
                sortChannels = getChannelsForSymbols(orderingScheme.getOrderBy(), source.getLayout());
                sortOrder = orderingScheme.getOrderingList();
            }

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            ImmutableList.Builder<WindowFunctionDefinition> windowFunctionsBuilder = ImmutableList.builder();
            ImmutableList.Builder<Symbol> windowFunctionOutputSymbolsBuilder = ImmutableList.builder();
            for (Map.Entry<Symbol, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
                Optional<Integer> frameStartChannel = Optional.empty();
                Optional<Integer> frameEndChannel = Optional.empty();

                Frame frame = entry.getValue().getFrame();
                if (frame.getStartValue().isPresent()) {
                    frameStartChannel = Optional.of(source.getLayout().get(frame.getStartValue().get()));
                }
                if (frame.getEndValue().isPresent()) {
                    frameEndChannel = Optional.of(source.getLayout().get(frame.getEndValue().get()));
                }

                FrameInfo frameInfo = new FrameInfo(frame.getType(), frame.getStartType(), frameStartChannel, frame.getEndType(), frameEndChannel);

                FunctionHandle functionHandle = entry.getValue().getFunctionHandle();
                ImmutableList.Builder<Integer> arguments = ImmutableList.builder();
                for (RowExpression argument : entry.getValue().getArguments()) {
                    checkState(argument instanceof VariableReferenceExpression);
                    Symbol argumentSymbol = new Symbol(((VariableReferenceExpression) argument).getName());
                    arguments.add(source.getLayout().get(argumentSymbol));
                }
                Symbol symbol = entry.getKey();
                WindowFunctionSupplier windowFunctionSupplier = metadata.getFunctionAndTypeManager().getWindowFunctionImplementation(functionHandle);
                Type type = metadata.getType(entry.getValue().getFunctionCall().getType().getTypeSignature());
                windowFunctionsBuilder.add(window(windowFunctionSupplier, type, frameInfo, arguments.build()));
                windowFunctionOutputSymbolsBuilder.add(symbol);
            }

            List<Symbol> windowFunctionOutputSymbols = windowFunctionOutputSymbolsBuilder.build();

            // compute the layout of the output from the window operator
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            for (Symbol symbol : node.getSource().getOutputSymbols()) {
                outputMappings.put(symbol, source.getLayout().get(symbol));
            }

            // window functions go in remaining channels starting after the last channel from the source operator, one per channel
            int channel = source.getTypes().size();
            for (Symbol symbol : windowFunctionOutputSymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }

            OperatorFactory operatorFactory = new WindowOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    outputChannels.build(),
                    windowFunctionsBuilder.build(),
                    partitionChannels,
                    preGroupedChannels,
                    sortChannels,
                    sortOrder,
                    node.getPreSortedOrderPrefix(),
                    10_000,
                    pagesIndexFactory,
                    isSpillEnabled(session) && isSpillWindowOperator(session),
                    spillerFactory,
                    orderingCompiler,
                    isSpillToHdfsEnabled(session));

            return new PhysicalOperation(operatorFactory, outputMappings.build(), context, source);
        }

        @Override
        public PhysicalOperation visitTopN(TopNNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderingScheme().getOrderBy();

            List<Integer> sortChannels = new ArrayList<>();
            List<SortOrder> sortOrders = new ArrayList<>();
            for (Symbol symbol : orderBySymbols) {
                sortChannels.add(source.getLayout().get(symbol));
                sortOrders.add(node.getOrderingScheme().getOrdering(symbol));
            }

            OperatorFactory operator = new TopNOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    (int) node.getCount(),
                    sortChannels,
                    sortOrders);

            return new PhysicalOperation(operator, source.getLayout(), context, source);
        }

        @Override
        public PhysicalOperation visitSort(SortNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderingScheme().getOrderBy();

            List<Integer> orderByChannels = getChannelsForSymbols(orderBySymbols, source.getLayout());

            ImmutableList.Builder<SortOrder> sortOrder = ImmutableList.builder();
            for (Symbol symbol : orderBySymbols) {
                sortOrder.add(node.getOrderingScheme().getOrdering(symbol));
            }

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            boolean spillEnabled = isSpillEnabled(context.getSession()) && isSpillOrderBy(context.getSession());
            boolean spillNonBlocking = isNonBlockingSpillOrderby(context.getSession());
            boolean isSpillToHdfsEnabled = isSpillToHdfsEnabled(context.getSession());

            OperatorFactory operator = new OrderByOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    outputChannels.build(),
                    10_000,
                    orderByChannels,
                    sortOrder.build(),
                    pagesIndexFactory,
                    spillEnabled,
                    Optional.of(spillerFactory),
                    orderingCompiler,
                    spillNonBlocking,
                    isSpillToHdfsEnabled);

            return new PhysicalOperation(operator, source.getLayout(), context, source);
        }

        @Override
        public PhysicalOperation visitLimit(LimitNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);
            OperatorFactory operatorFactory = null;

            if (getRetryPolicy(context.getSession()) == RetryPolicy.TASK) {
                log.info("Batch Process -> Use Batch BatchLimitOperatorFactory");
                operatorFactory = new BatchLimitOperator.BatchLimitOperatorFactory(context.getNextOperatorId(), node.getId(), node.getAtomicCount());
            }
            else {
                operatorFactory = new LimitOperatorFactory(context.getNextOperatorId(), node.getId(), node.getCount());
            }

            return new PhysicalOperation(operatorFactory, source.getLayout(), context, source);
        }

        @Override
        public PhysicalOperation visitDistinctLimit(DistinctLimitNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            Optional<Integer> hashChannel = node.getHashSymbol().map(channelGetter(source));
            List<Integer> distinctChannels = getChannelsForSymbols(node.getDistinctSymbols(), source.getLayout());
            OperatorFactory operatorFactory = null;
            if (getRetryPolicy(context.getSession()) == RetryPolicy.TASK) {
                log.info("Batch Process -> Use Batch BatchDistinctLimitOperatorFactory");
                operatorFactory = new BatchDistinctLimitOperator.BatchDistinctLimitOperatorFactory(
                        context.getNextOperatorId(),
                        node.getId(),
                        source.getTypes(),
                        distinctChannels,
                        node.getAtomicLimit(),
                        hashChannel,
                        joinCompiler);
            }
            else {
                operatorFactory = new DistinctLimitOperatorFactory(
                        context.getNextOperatorId(),
                        node.getId(),
                        source.getTypes(),
                        distinctChannels,
                        node.getLimit(),
                        hashChannel,
                        joinCompiler);
            }

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitGroupId(GroupIdNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);
            Map<Symbol, Integer> newLayout = new HashMap<>();
            ImmutableList.Builder<Type> outputTypes = ImmutableList.builder();

            int outputChannel = 0;

            for (Symbol output : node.getGroupingSets().stream().flatMap(Collection::stream).collect(Collectors.toSet())) {
                newLayout.put(output, outputChannel++);
                outputTypes.add(source.getTypes().get(source.getLayout().get(node.getGroupingColumns().get(output))));
            }

            Map<Symbol, Integer> argumentMappings = new HashMap<>();
            for (Symbol output : node.getAggregationArguments()) {
                int inputChannel = source.getLayout().get(output);

                newLayout.put(output, outputChannel++);
                outputTypes.add(source.getTypes().get(inputChannel));
                argumentMappings.put(output, inputChannel);
            }

            // for every grouping set, create a mapping of all output to input channels (including arguments)
            ImmutableList.Builder<Map<Integer, Integer>> mappings = ImmutableList.builder();
            for (List<Symbol> groupingSet : node.getGroupingSets()) {
                ImmutableMap.Builder<Integer, Integer> setMapping = ImmutableMap.builder();

                for (Symbol output : groupingSet) {
                    setMapping.put(newLayout.get(output), source.getLayout().get(node.getGroupingColumns().get(output)));
                }

                for (Symbol output : argumentMappings.keySet()) {
                    setMapping.put(newLayout.get(output), argumentMappings.get(output));
                }

                mappings.add(setMapping.build());
            }

            newLayout.put(node.getGroupIdSymbol(), outputChannel);
            outputTypes.add(BIGINT);

            OperatorFactory groupIdOperatorFactory = new GroupIdOperator.GroupIdOperatorFactory(context.getNextOperatorId(),
                    node.getId(),
                    outputTypes.build(),
                    mappings.build());

            return new PhysicalOperation(groupIdOperatorFactory, newLayout, context, source);
        }

        @Override
        public PhysicalOperation visitAggregation(AggregationNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            if (node.getGroupingKeys().isEmpty()) {
                return planGlobalAggregation(node, source, context);
            }

            boolean spillEnabled = isSpillEnabled(context.getSession());
            DataSize unspillMemoryLimit = getAggregationOperatorUnspillMemoryLimit(context.getSession());

            return planGroupByAggregation(node, source, spillEnabled, unspillMemoryLimit, context);
        }

        @Override
        public PhysicalOperation visitCTEScan(CTEScanNode node, LocalExecutionPlanContext context)
        {
            CommonTableExecutionContext cteCtx;
            List<Type> outputTypes;
            PhysicalOperation source = null;
            Function<Page, Page> pagePreprocessor = null;

            synchronized (this.getClass()) {
                String cteId = context.getCteId(node.getId());
                int stageId = context.getTaskId().getStageId().getId();
                if (!context.isCteInitialized(cteId) && context.getFeederCTEId().isPresent()
                        && Integer.parseInt(context.getFeederCTEId().get().toString()) == stageId) {
                    source = node.getSource().accept(this, context);
                    pagePreprocessor = enforceLayoutProcessor(node.getOutputSymbols(), makeLayout(node.getSource()));
                }

                /* Note: this should always be comming from remote node! */
                checkArgument(context.cteOperationMap.get(node.getId()) == null, "Cte node can be only 1 in a stage");

                cteCtx = context.getRunningTask(cteId, node.getConsumerPlans(), source);
                outputTypes = getSymbolTypes(node.getOutputSymbols(), context.getTypes());
            }

            CommonTableExpressionOperatorFactory cteOperatorFactory = new CommonTableExpressionOperatorFactory(context.getNextOperatorId(),
                    node.getId(),
                    cteCtx,
                    outputTypes,
                    getFilterAndProjectMinOutputPageSize(session),
                    getFilterAndProjectMinOutputPageRowCount(session),
                    pagePreprocessor);

            cteOperatorFactory.addConsumer(context.getConsumerId());

            context.cteOperationMap.put(node.getId(), cteOperatorFactory);
            if (source == null) {
                return new PhysicalOperation(cteOperatorFactory,
                        makeLayout(node),
                        context,
                        UNGROUPED_EXECUTION);
            }

            return new PhysicalOperation(cteOperatorFactory,
                    makeLayout(node),
                    context,
                    source);
        }

        @Override
        public PhysicalOperation visitCreateIndex(CreateIndexNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new CreateIndexOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    node.getCreateIndexMetadata(),
                    heuristicIndexerManager);

            return new PhysicalOperation(operatorFactory, source.getLayout(), context, source);
        }

        @Override
        public PhysicalOperation visitMarkDistinct(MarkDistinctNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Integer> channels = getChannelsForSymbols(node.getDistinctSymbols(), source.getLayout());
            Optional<Integer> hashChannel = node.getHashSymbol().map(channelGetter(source));
            MarkDistinctOperatorFactory operator = new MarkDistinctOperatorFactory(context.getNextOperatorId(), node.getId(), source.getTypes(), channels, hashChannel, joinCompiler);
            return new PhysicalOperation(operator, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitSample(SampleNode node, LocalExecutionPlanContext context)
        {
            // For system sample, the splits are already filtered out, so no specific action needs to be taken here
            if (node.getSampleType() == SampleNode.Type.SYSTEM) {
                return node.getSource().accept(this, context);
            }

            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        public PhysicalOperation visitFilter(FilterNode node, LocalExecutionPlanContext context)
        {
            PlanNode sourceNode = node.getSource();

            RowExpression filterExpression = node.getPredicate();
            List<Symbol> outputSymbols = node.getOutputSymbols();

            return visitScanFilterAndProject(context, node.getId(), sourceNode, Optional.of(filterExpression), AssignmentUtils.identityAssignments(context.getTypes(), outputSymbols), outputSymbols);
        }

        @Override
        public PhysicalOperation visitProject(ProjectNode node, LocalExecutionPlanContext context)
        {
            PlanNode sourceNode;
            Optional<RowExpression> filterExpression = Optional.empty();
            if (node.getSource() instanceof FilterNode) {
                FilterNode filterNode = (FilterNode) node.getSource();
                sourceNode = filterNode.getSource();
                filterExpression = Optional.of(filterNode.getPredicate());
            }
            else {
                sourceNode = node.getSource();
            }

            List<Symbol> outputSymbols = node.getOutputSymbols();

            return visitScanFilterAndProject(context, node.getId(), sourceNode, filterExpression, node.getAssignments(), outputSymbols);
        }

        // TODO: This should be refactored, so that there's an optimizer that merges scan-filter-project into a single PlanNode
        private PhysicalOperation visitScanFilterAndProject(
                LocalExecutionPlanContext context,
                PlanNodeId planNodeId,
                PlanNode sourceNode,
                Optional<RowExpression> inputFilterExpression,
                Assignments assignments,
                List<Symbol> outputSymbols)
        {
            // if source is a table scan we fold it directly into the filter and project
            // otherwise we plan it as a normal operator
            Map<Symbol, Integer> sourceLayout;
            TableHandle table = null;
            List<ColumnHandle> columns = null;
            PhysicalOperation source = null;
            ReuseExchangeOperator.STRATEGY strategy = REUSE_STRATEGY_DEFAULT;
            UUID reuseTableScanMappingId = new UUID(0, 0);
            Integer consumerTableScanNodeCount = 0;
            Optional<RowExpression> filterExpression = inputFilterExpression;
            if (sourceNode instanceof TableScanNode) {
                TableScanNode tableScanNode = (TableScanNode) sourceNode;
                table = tableScanNode.getTable();

                // extract the column handles and channel to type mapping
                sourceLayout = new LinkedHashMap<>();
                columns = new ArrayList<>();
                int channel = 0;
                for (Symbol symbol : tableScanNode.getOutputSymbols()) {
                    columns.add(tableScanNode.getAssignments().get(symbol));

                    Integer input = channel;
                    sourceLayout.put(symbol, input);

                    channel++;
                }

                strategy = tableScanNode.getStrategy();
                reuseTableScanMappingId = tableScanNode.getReuseTableScanMappingId();
                consumerTableScanNodeCount = tableScanNode.getConsumerTableScanNodeCount();
            }
            //TODO: This is a simple hack, it will be replaced when we add ability to push down sampling into connectors.
            // SYSTEM sampling is performed in the coordinator by dropping some random splits so the SamplingNode can be skipped here.
            else if (sourceNode instanceof SampleNode) {
                SampleNode sampleNode = (SampleNode) sourceNode;
                checkArgument(sampleNode.getSampleType() == SampleNode.Type.SYSTEM, "%s sampling is not supported", sampleNode.getSampleType());
                return visitScanFilterAndProject(context,
                        planNodeId,
                        sampleNode.getSource(),
                        filterExpression,
                        assignments,
                        outputSymbols);
            }
            else {
                // plan source
                source = sourceNode.accept(this, context);
                sourceLayout = source.getLayout();
            }

            // filterExpression may contain large function calls; evaluate them before compiling.
            if (filterExpression.isPresent()) {
                filterExpression = Optional.of(bindChannels(filterExpression.get(), sourceLayout, context.getTypes()));
            }

            // build output mapping
            ImmutableMap.Builder<Symbol, Integer> outputMappingsBuilder = ImmutableMap.builder();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappingsBuilder.put(symbol, i);
            }
            Map<Symbol, Integer> outputMappings = outputMappingsBuilder.build();

            Optional<DynamicFilters.ExtractResult> extractDynamicFilterResult = filterExpression.map(DynamicFilters::extractDynamicFilters);

            List<List<DynamicFilters.Descriptor>> extractDynamicFilterUnionResult;
            if (isCTEReuseEnabled(session)) {
                extractDynamicFilterUnionResult = DynamicFilters.extractDynamicFiltersAsUnion(filterExpression, metadata);
            }
            else {
                if (extractDynamicFilterResult.isPresent()) {
                    extractDynamicFilterUnionResult = ImmutableList.of(extractDynamicFilterResult.get().getDynamicConjuncts());
                }
                else {
                    extractDynamicFilterUnionResult = ImmutableList.of();
                }
            }
            Optional<RowExpression> translatedFilter = extractStaticFilters(filterExpression, metadata);

            // TODO: Execution must be plugged in here
            Supplier<List<Map<ColumnHandle, DynamicFilter>>> dynamicFilterSupplier = getDynamicFilterSupplier(Optional.of(extractDynamicFilterUnionResult), sourceNode, context);
            Optional<DynamicFilterSupplier> dynamicFilter = Optional.empty();
            if ((dynamicFilterSupplier != null && extractDynamicFilterResult.isPresent() && !extractDynamicFilterResult.get().getDynamicConjuncts().isEmpty())
                    || (dynamicFilterSupplier != null && isCTEReuseEnabled(session) && !extractDynamicFilterUnionResult.isEmpty())) {
                dynamicFilter = Optional.of(new DynamicFilterSupplier(dynamicFilterSupplier, System.currentTimeMillis(), getDynamicFilteringWaitTime(session).toMillis()));
            }

            List<RowExpression> projections = new ArrayList<>();
            for (Symbol symbol : outputSymbols) {
                projections.add(assignments.get(symbol));
            }

            List<RowExpression> translatedProjections = projections.stream()
                    .map(expression -> bindChannels(expression, sourceLayout, context.getTypes()))
                    .collect(toImmutableList());

            try {
                if (columns != null) {
                    Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(translatedFilter, translatedProjections, sourceNode.getId());
                    Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(translatedFilter, translatedProjections, Optional.of(context.getStageId() + "_" + planNodeId));

                    boolean spillEnabled = isSpillEnabled(session) && isSpillReuseExchange(session);
                    int spillerThreshold = getSpillOperatorThresholdReuseExchange(session) * 1024 * 1024; //convert from MB to bytes

                    SourceOperatorFactory operatorFactory = new ScanFilterAndProjectOperatorFactory(
                            context.getSession(),
                            context.getNextOperatorId(),
                            planNodeId,
                            sourceNode,
                            pageSourceProvider,
                            cursorProcessor,
                            pageProcessor,
                            table,
                            columns,
                            dynamicFilter,
                            projections.stream().map(expression -> expression.getType()).collect(toImmutableList()),
                            stateStoreProvider,
                            metadata,
                            dynamicFilterCacheManager,
                            getFilterAndProjectMinOutputPageSize(session),
                            getFilterAndProjectMinOutputPageRowCount(session),
                            strategy, reuseTableScanMappingId, spillEnabled, Optional.of(spillerFactory), spillerThreshold, consumerTableScanNodeCount);

                    return new PhysicalOperation(operatorFactory, outputMappings, context, stageExecutionDescriptor.isScanGroupedExecution(sourceNode.getId()) ? GROUPED_EXECUTION : UNGROUPED_EXECUTION);
                }
                else {
                    Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(translatedFilter, translatedProjections, Optional.of(context.getStageId() + "_" + planNodeId));

                    OperatorFactory operatorFactory = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                            context.getNextOperatorId(),
                            planNodeId,
                            pageProcessor,
                            projections.stream().map(expression -> expression.getType()).collect(toImmutableList()),
                            getFilterAndProjectMinOutputPageSize(session),
                            getFilterAndProjectMinOutputPageRowCount(session));

                    return new PhysicalOperation(operatorFactory, outputMappings, context, source);
                }
            }
            catch (RuntimeException e) {
                throw new PrestoException(COMPILER_ERROR, "Execution Compiler failed.", e);
            }
        }

        protected Supplier<List<Map<ColumnHandle, DynamicFilter>>> getDynamicFilterSupplier(Optional<List<List<DynamicFilters.Descriptor>>> dynamicFilters, PlanNode sourceNode, LocalExecutionPlanContext context)
        {
            if (dynamicFilters.isPresent() && !dynamicFilters.get().isEmpty()) {
                log.debug("[TableScan] Dynamic filters: %s", dynamicFilters);
                if (sourceNode instanceof TableScanNode) {
                    TableScanNode tableScanNode = (TableScanNode) sourceNode;
                    LocalDynamicFiltersCollector collector = context.getDynamicFiltersCollector();
                    collector.initContext(dynamicFilters.get(), SymbolUtils.toLayOut(tableScanNode.getOutputSymbols()));
                    dynamicFilters.get().forEach(dynamicFilterList ->
                            dynamicFilterList.forEach(dynamicFilter -> dynamicFilterCacheManager.registerTask(createCacheKey(dynamicFilter.getId(), session.getQueryId().getId()), context.getTaskId())));
                    return () -> collector.getDynamicFilters(tableScanNode);
                }
            }
            else if (isCrossRegionDynamicFilterEnabled(context.getSession())) {
                if (sourceNode instanceof TableScanNode) {
                    LocalDynamicFiltersCollector collector = context.getDynamicFiltersCollector();
                    if (collector.checkTableIsDcTable((TableScanNode) sourceNode)) {
                        // if cross-region-dynamic-filter is enabled and the table is a dc table, should consider push down the cross region bloom filter to next cluster
                        return BloomFilterUtils.getCrossRegionDynamicFilterSupplier(dynamicFilterCacheManager, context.getSession().getQueryId().getId(), (TableScanNode) sourceNode);
                    }
                }
            }
            return null;
        }

        public RowExpression bindChannels(RowExpression inputExpression, Map<Symbol, Integer> sourceLayout, TypeProvider types)
        {
            RowExpression expression = inputExpression;
            Type type = expression.getType();
            Object value = new RowExpressionInterpreter(expression, metadata, session.toConnectorSession(), OPTIMIZED).optimize();
            if (value instanceof RowExpression) {
                RowExpression optimized = (RowExpression) value;
                // building channel info
                Map<VariableReferenceExpression, Integer> layout = new LinkedHashMap<>();
                sourceLayout.forEach((symbol, num) -> layout.put(toVariableReference(symbol, types.get(symbol)), num));
                expression = VariableToChannelTranslator.translate(optimized, layout);
            }
            else {
                expression = constant(value, type);
            }
            return expression;
        }

        @Override
        public PhysicalOperation visitTableScan(TableScanNode node, LocalExecutionPlanContext context)
        {
            List<ColumnHandle> columns = new ArrayList<>();
            for (Symbol symbol : node.getOutputSymbols()) {
                columns.add(node.getAssignments().get(symbol));
            }

            Assignments assignments = identityAssignments(context.getTypes(), node.getOutputSymbols());
            List<Type> types = assignments.getExpressions().stream()
                    .map(expression -> expression.getType())
                    .collect(Collectors.toList());

            boolean spillEnabled = isSpillEnabled(session) && isSpillReuseExchange(session);
            int spillerThreshold = getSpillOperatorThresholdReuseExchange(session) * 1024 * 1024; //convert from MB to bytes
            Integer consumerTableScanNodeCount = node.getConsumerTableScanNodeCount();

            OperatorFactory operatorFactory = new TableScanOperatorFactory(context.getSession(),
                    context.getNextOperatorId(),
                    node,
                    pageSourceProvider,
                    node.getTable(),
                    columns,
                    types,
                    stateStoreProvider,
                    metadata,
                    dynamicFilterCacheManager,
                    getFilterAndProjectMinOutputPageSize(session),
                    getFilterAndProjectMinOutputPageRowCount(session), node.getStrategy(), node.getReuseTableScanMappingId(), spillEnabled, Optional.of(spillerFactory), spillerThreshold, consumerTableScanNodeCount);
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, stageExecutionDescriptor.isScanGroupedExecution(node.getId()) ? GROUPED_EXECUTION : UNGROUPED_EXECUTION);
        }

        private Page valuesPage(ValuesNode node, LocalExecutionPlanContext context)
        {
            if (node.getRows().isEmpty()) {
                return null;
            }

            List<Type> outputTypes = getSymbolTypes(node.getOutputSymbols(), context.getTypes());
            PageBuilder pageBuilder = new PageBuilder(node.getRows().size(), outputTypes);
            for (List<RowExpression> row : node.getRows()) {
                pageBuilder.declarePosition();
                for (int i = 0; i < row.size(); i++) {
                    // evaluate the literal value
                    Object result = RowExpressionInterpreter.rowExpressionInterpreter(row.get(i), metadata, context.getSession().toConnectorSession()).evaluate();
                    writeNativeValue(outputTypes.get(i), pageBuilder.getBlockBuilder(i), result);
                }
            }
            return pageBuilder.build();
        }

        @Override
        public PhysicalOperation visitValues(ValuesNode node, LocalExecutionPlanContext context)
        {
            // a values node must have a single driver
            context.setDriverInstanceCount(1);

            List<Page> pages;
            int from = 0;
            if (SystemSessionProperties.isSnapshotEnabled(context.taskContext.getSession())) {
                ImmutableList.Builder builder = ImmutableList.builder();
                // Always include the data page, for debugging purposes
                Page page = valuesPage(node, context);
                if (page != null) {
                    builder.add(page);
                }
                if (node.getResumeSnapshotId() != null) {
                    builder.add(MarkerPage.resumePage(node.getResumeSnapshotId()));
                    if (page != null) {
                        from = 1; // Skip data page if resuming from a snapshot, because data page would have been sent
                    }
                }
                builder.add(MarkerPage.snapshotPage(node.getNextSnapshotId()));
                pages = builder.build();
            }
            else {
                Page page = valuesPage(node, context);
                pages = page == null ? ImmutableList.of() : ImmutableList.of(page);
            }

            OperatorFactory operatorFactory = new ValuesOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    pages,
                    from);
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        @Override
        public PhysicalOperation visitUnnest(UnnestNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            ImmutableList.Builder<Type> replicateTypes = ImmutableList.builder();
            for (Symbol symbol : node.getReplicateSymbols()) {
                replicateTypes.add(context.getTypes().get(symbol));
            }
            List<Symbol> unnestSymbols = ImmutableList.copyOf(node.getUnnestSymbols().keySet());
            ImmutableList.Builder<Type> unnestTypes = ImmutableList.builder();
            for (Symbol symbol : unnestSymbols) {
                unnestTypes.add(context.getTypes().get(symbol));
            }
            Optional<Symbol> ordinalitySymbol = node.getOrdinalitySymbol();
            Optional<Type> ordinalityType = ordinalitySymbol.map(context.getTypes()::get);
            ordinalityType.ifPresent(type -> checkState(type.equals(BIGINT), "Type of ordinalitySymbol must always be BIGINT."));

            List<Integer> replicateChannels = getChannelsForSymbols(node.getReplicateSymbols(), source.getLayout());
            List<Integer> unnestChannels = getChannelsForSymbols(unnestSymbols, source.getLayout());

            // Source channels are always laid out first, followed by the unnested symbols
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            int channel = 0;
            for (Symbol symbol : node.getReplicateSymbols()) {
                outputMappings.put(symbol, channel);
                channel++;
            }
            for (Symbol symbol : unnestSymbols) {
                for (Symbol unnestedSymbol : node.getUnnestSymbols().get(symbol)) {
                    outputMappings.put(unnestedSymbol, channel);
                    channel++;
                }
            }
            if (ordinalitySymbol.isPresent()) {
                outputMappings.put(ordinalitySymbol.get(), channel);
                channel++;
            }
            OperatorFactory operatorFactory = new UnnestOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    replicateChannels,
                    replicateTypes.build(),
                    unnestChannels,
                    unnestTypes.build(),
                    ordinalityType.isPresent());
            return new PhysicalOperation(operatorFactory, outputMappings.build(), context, source);
        }

        @Override
        public PhysicalOperation visitVacuumTable(VacuumTableNode node, LocalExecutionPlanContext context)
        {
            OperatorFactory dummyOperatorFactory = new DevNullOperatorFactory(context.getNextOperatorId(), node.getId());
            PhysicalOperation source = new PhysicalOperation(dummyOperatorFactory, makeLayoutFromOutputSymbols(node.getInputSymbols()),
                    context, stageExecutionDescriptor.isScanGroupedExecution(node.getId()) ? GROUPED_EXECUTION : UNGROUPED_EXECUTION);

            ImmutableMap.Builder<Symbol, Integer> outputMapping = ImmutableMap.builder();
            outputMapping.put(node.getOutputSymbols().get(0), ROW_COUNT_CHANNEL);
            outputMapping.put(node.getOutputSymbols().get(1), FRAGMENT_CHANNEL);

            OperatorFactory statisticsOperatorFactory = node.getStatisticsAggregation().map(aggregation -> {
                List<Symbol> groupingSymbols = aggregation.getGroupingSymbols();
                if (groupingSymbols.isEmpty()) {
                    return createAggregationOperatorFactory(
                            node.getId(),
                            aggregation.getAggregations(),
                            PARTIAL,
                            STATS_START_CHANNEL,
                            outputMapping,
                            source,
                            context,
                            true);
                }
                return createHashAggregationOperatorFactory(
                        node.getId(),
                        aggregation.getAggregations(),
                        ImmutableSet.of(),
                        groupingSymbols,
                        PARTIAL,
                        Optional.empty(),
                        Optional.empty(),
                        source,
                        false,
                        false,
                        false,
                        new DataSize(0, BYTE),
                        context,
                        STATS_START_CHANNEL,
                        outputMapping,
                        200,
                        // This aggregation must behave as INTERMEDIATE.
                        // Using INTERMEDIATE aggregation directly
                        // is not possible, as it doesn't accept raw input data.
                        // Disabling partial pre-aggregation memory limit effectively
                        // turns PARTIAL aggregation into INTERMEDIATE.
                        Optional.empty(),
                        true);
            }).orElse(new DevNullOperatorFactory(context.getNextOperatorId(), node.getId()));
            OperatorFactory operatorFactory = new VacuumTableOperator.VacuumTableOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    pageSourceProvider,
                    pageSinkManager,
                    node.getTarget(),
                    node.getTable(),
                    session,
                    statisticsOperatorFactory,
                    getSymbolTypes(node.getOutputSymbols(), context.getTypes()),
                    Optional.of(context.getTaskId()));
            return new PhysicalOperation(operatorFactory,
                    makeLayout(node),
                    context,
                    stageExecutionDescriptor.isScanGroupedExecution(node.getId()) ? GROUPED_EXECUTION : UNGROUPED_EXECUTION);
        }

        protected ImmutableMap<Symbol, Integer> makeLayout(PlanNode node)
        {
            return makeLayoutFromOutputSymbols(node.getOutputSymbols());
        }

        protected ImmutableMap<Symbol, Integer> makeLayoutFromOutputSymbols(List<Symbol> outputSymbols)
        {
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            int channel = 0;
            for (Symbol symbol : outputSymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }
            return outputMappings.build();
        }

        @Override
        public PhysicalOperation visitIndexSource(IndexSourceNode node, LocalExecutionPlanContext context)
        {
            checkState(context.getIndexSourceContext().isPresent(), "Must be in an index source context");
            IndexSourceContext indexSourceContext = context.getIndexSourceContext().get();

            SetMultimap<Symbol, Integer> indexLookupToProbeInput = indexSourceContext.getIndexLookupToProbeInput();
            checkState(indexLookupToProbeInput.keySet().equals(node.getLookupSymbols()));

            // Finalize the symbol lookup layout for the index source
            List<Symbol> lookupSymbolSchema = ImmutableList.copyOf(node.getLookupSymbols());

            // Identify how to remap the probe key Input to match the source index lookup layout
            ImmutableList.Builder<Integer> remappedProbeKeyChannelsBuilder = ImmutableList.builder();
            // Identify overlapping fields that can produce the same lookup symbol.
            // We will filter incoming keys to ensure that overlapping fields will have the same value.
            ImmutableList.Builder<Set<Integer>> overlappingFieldSetsBuilder = ImmutableList.builder();
            for (Symbol lookupSymbol : lookupSymbolSchema) {
                Set<Integer> potentialProbeInputs = indexLookupToProbeInput.get(lookupSymbol);
                checkState(!potentialProbeInputs.isEmpty(), "Must have at least one source from the probe input");
                if (potentialProbeInputs.size() > 1) {
                    overlappingFieldSetsBuilder.add(potentialProbeInputs.stream().collect(toImmutableSet()));
                }
                remappedProbeKeyChannelsBuilder.add(Iterables.getFirst(potentialProbeInputs, null));
            }
            List<Set<Integer>> overlappingFieldSets = overlappingFieldSetsBuilder.build();
            List<Integer> remappedProbeKeyChannels = remappedProbeKeyChannelsBuilder.build();
            Function<RecordSet, RecordSet> probeKeyNormalizer = recordSet -> {
                if (!overlappingFieldSets.isEmpty()) {
                    recordSet = new FieldSetFilteringRecordSet(metadata.getFunctionAndTypeManager(), recordSet, overlappingFieldSets);
                }
                return new MappedRecordSet(recordSet, remappedProbeKeyChannels);
            };

            // Declare the input and output schemas for the index and acquire the actual Index
            List<ColumnHandle> lookupSchema = Lists.transform(lookupSymbolSchema, forMap(node.getAssignments()));
            List<ColumnHandle> outputSchema = Lists.transform(node.getOutputSymbols(), forMap(node.getAssignments()));
            ConnectorIndex index = indexManager.getIndex(session, node.getIndexHandle(), lookupSchema, outputSchema);

            OperatorFactory operatorFactory = new IndexSourceOperator.IndexSourceOperatorFactory(context.getNextOperatorId(), node.getId(), index, probeKeyNormalizer);
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        /**
         * This method creates a mapping from each index source lookup symbol (directly applied to the index)
         * to the corresponding probe key Input
         */
        private SetMultimap<Symbol, Integer> mapIndexSourceLookupSymbolToProbeKeyInput(IndexJoinNode node, Map<Symbol, Integer> probeKeyLayout)
        {
            Set<Symbol> indexJoinSymbols = node.getCriteria().stream()
                    .map(IndexJoinNode.EquiJoinClause::getIndex)
                    .collect(toImmutableSet());

            // Trace the index join symbols to the index source lookup symbols
            // Map: Index join symbol => Index source lookup symbol
            Map<Symbol, Symbol> indexKeyTrace = IndexJoinOptimizer.IndexKeyTracer.trace(node.getIndexSource(), indexJoinSymbols);

            // Map the index join symbols to the probe key Input
            Multimap<Symbol, Integer> indexToProbeKeyInput = HashMultimap.create();
            for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
                indexToProbeKeyInput.put(clause.getIndex(), probeKeyLayout.get(clause.getProbe()));
            }

            // Create the mapping from index source look up symbol to probe key Input
            ImmutableSetMultimap.Builder<Symbol, Integer> builder = ImmutableSetMultimap.builder();
            for (Map.Entry<Symbol, Symbol> entry : indexKeyTrace.entrySet()) {
                Symbol indexJoinSymbol = entry.getKey();
                Symbol indexLookupSymbol = entry.getValue();
                builder.putAll(indexLookupSymbol, indexToProbeKeyInput.get(indexJoinSymbol));
            }
            return builder.build();
        }

        @Override
        public PhysicalOperation visitIndexJoin(IndexJoinNode node, LocalExecutionPlanContext context)
        {
            List<IndexJoinNode.EquiJoinClause> clauses = node.getCriteria();

            List<Symbol> probeSymbols = Lists.transform(clauses, IndexJoinNode.EquiJoinClause::getProbe);
            List<Symbol> indexSymbols = Lists.transform(clauses, IndexJoinNode.EquiJoinClause::getIndex);

            // Plan probe side
            PhysicalOperation probeSource = node.getProbeSource().accept(this, context);
            List<Integer> probeChannels = getChannelsForSymbols(probeSymbols, probeSource.getLayout());
            OptionalInt probeHashChannel = node.getProbeHashSymbol().map(channelGetter(probeSource))
                    .map(OptionalInt::of).orElse(OptionalInt.empty());

            // The probe key channels will be handed to the index according to probeSymbol order
            Map<Symbol, Integer> probeKeyLayout = new HashMap<>();
            for (int i = 0; i < probeSymbols.size(); i++) {
                // Duplicate symbols can appear and we only need to take take one of the Inputs
                probeKeyLayout.put(probeSymbols.get(i), i);
            }

            // Plan the index source side
            SetMultimap<Symbol, Integer> indexLookupToProbeInput = mapIndexSourceLookupSymbolToProbeKeyInput(node, probeKeyLayout);
            LocalExecutionPlanContext indexContext = context.createIndexSourceSubContext(new IndexSourceContext(indexLookupToProbeInput));
            PhysicalOperation indexSource = node.getIndexSource().accept(this, indexContext);
            List<Integer> indexOutputChannels = getChannelsForSymbols(indexSymbols, indexSource.getLayout());
            OptionalInt indexHashChannel = node.getIndexHashSymbol().map(channelGetter(indexSource))
                    .map(OptionalInt::of).orElse(OptionalInt.empty());

            // Identify just the join keys/channels needed for lookup by the index source (does not have to use all of them).
            Set<Symbol> indexSymbolsNeededBySource = IndexJoinOptimizer.IndexKeyTracer.trace(node.getIndexSource(), ImmutableSet.copyOf(indexSymbols)).keySet();

            Set<Integer> lookupSourceInputChannels = node.getCriteria().stream()
                    .filter(equiJoinClause -> indexSymbolsNeededBySource.contains(equiJoinClause.getIndex()))
                    .map(IndexJoinNode.EquiJoinClause::getProbe)
                    .map(probeKeyLayout::get)
                    .collect(toImmutableSet());

            Optional<DynamicTupleFilterFactory> dynamicTupleFilterFactory = Optional.empty();
            if (lookupSourceInputChannels.size() < probeKeyLayout.values().size()) {
                int[] nonLookupInputChannels = Ints.toArray(node.getCriteria().stream()
                        .filter(equiJoinClause -> !indexSymbolsNeededBySource.contains(equiJoinClause.getIndex()))
                        .map(IndexJoinNode.EquiJoinClause::getProbe)
                        .map(probeKeyLayout::get)
                        .collect(toImmutableList()));
                int[] nonLookupOutputChannels = Ints.toArray(node.getCriteria().stream()
                        .filter(equiJoinClause -> !indexSymbolsNeededBySource.contains(equiJoinClause.getIndex()))
                        .map(IndexJoinNode.EquiJoinClause::getIndex)
                        .map(indexSource.getLayout()::get)
                        .collect(toImmutableList()));

                int filterOperatorId = indexContext.getNextOperatorId();
                dynamicTupleFilterFactory = Optional.of(new DynamicTupleFilterFactory(
                        filterOperatorId,
                        node.getId(),
                        nonLookupInputChannels,
                        nonLookupOutputChannels,
                        indexSource.getTypes(),
                        pageFunctionCompiler));
            }

            IndexBuildDriverFactoryProvider indexBuildDriverFactoryProvider = new IndexBuildDriverFactoryProvider(
                    indexContext.getNextPipelineId(),
                    indexContext.getNextOperatorId(),
                    node.getId(),
                    indexContext.isInputDriver(),
                    indexSource.getTypes(),
                    indexSource.getOperatorFactories(),
                    dynamicTupleFilterFactory);

            IndexLookupSourceFactory indexLookupSourceFactory = new IndexLookupSourceFactory(
                    lookupSourceInputChannels,
                    indexOutputChannels,
                    indexHashChannel,
                    indexSource.getTypes(),
                    indexSource.getLayout(),
                    indexBuildDriverFactoryProvider,
                    maxIndexMemorySize,
                    indexJoinLookupStats,
                    SystemSessionProperties.isShareIndexLoading(session),
                    pagesIndexFactory,
                    joinCompiler);

            verify(probeSource.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION);
            verify(indexSource.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION);
            JoinBridgeManager<LookupSourceFactory> lookupSourceFactoryManager = new JoinBridgeManager<>(
                    false,
                    UNGROUPED_EXECUTION,
                    UNGROUPED_EXECUTION,
                    lifespan -> indexLookupSourceFactory,
                    indexLookupSourceFactory.getOutputTypes());

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(probeSource.getLayout());

            // inputs from index side of the join are laid out following the input from the probe side,
            // so adjust the channel ids but keep the field layouts intact
            int offset = probeSource.getTypes().size();
            for (Map.Entry<Symbol, Integer> entry : indexSource.getLayout().entrySet()) {
                Integer input = entry.getValue();
                outputMappings.put(entry.getKey(), offset + input);
            }

            OperatorFactory lookupJoinOperatorFactory;
            OptionalInt totalOperatorsCount = context.getDriverInstanceCount();
            switch (node.getType()) {
                case INNER:
                    lookupJoinOperatorFactory = lookupJoinOperators.innerJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeSource.getTypes(), probeChannels, probeHashChannel, Optional.empty(), totalOperatorsCount, unsupportedPartitioningSpillerFactory());
                    break;
                case SOURCE_OUTER:
                    lookupJoinOperatorFactory = lookupJoinOperators.probeOuterJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeSource.getTypes(), probeChannels, probeHashChannel, Optional.empty(), totalOperatorsCount, unsupportedPartitioningSpillerFactory());
                    break;
                default:
                    throw new AssertionError("Unknown type: " + node.getType());
            }
            return new PhysicalOperation(lookupJoinOperatorFactory, outputMappings.build(), context, probeSource);
        }

        @Override
        public PhysicalOperation visitJoin(JoinNode node, LocalExecutionPlanContext context)
        {
            if (node.isCrossJoin()) {
                return createNestedLoopJoin(node, context);
            }

            List<JoinNode.EquiJoinClause> clauses = node.getCriteria();

            // TODO: Execution must be plugged in here
            if (!node.getDynamicFilters().isEmpty()) {
                log.debug("[Join] Dynamic filters: %s", node.getDynamicFilters());
            }

            List<Symbol> leftSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getLeft);
            List<Symbol> rightSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getRight);

            switch (node.getType()) {
                case INNER:
                case LEFT:
                case RIGHT:
                case FULL:
                    return createLookupJoin(node, node.getLeft(), leftSymbols, node.getLeftHashSymbol(), node.getRight(), rightSymbols, node.getRightHashSymbol(), context);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        @Override
        public PhysicalOperation visitSpatialJoin(SpatialJoinNode node, LocalExecutionPlanContext context)
        {
            RowExpression filterExpression = node.getFilter();
            List<CallExpression> spatialFunctions = extractSupportedSpatialFunctions(filterExpression, metadata.getFunctionAndTypeManager());
            for (CallExpression spatialFunction : spatialFunctions) {
                Optional<PhysicalOperation> operation = tryCreateSpatialJoin(context, node, removeExpressionFromFilter(filterExpression, spatialFunction), spatialFunction, Optional.empty(), Optional.empty());
                if (operation.isPresent()) {
                    return operation.get();
                }
            }

            List<CallExpression> spatialComparisons = extractSupportedSpatialComparisons(filterExpression, metadata.getFunctionAndTypeManager());
            for (CallExpression spatialComparison : spatialComparisons) {
                FunctionMetadata functionMetadata = metadata.getFunctionAndTypeManager().getFunctionMetadata(spatialComparison.getFunctionHandle());
                checkArgument(functionMetadata.getOperatorType().isPresent() && functionMetadata.getOperatorType().get().isComparisonOperator());
                if (functionMetadata.getOperatorType().get() == LESS_THAN || functionMetadata.getOperatorType().get() == LESS_THAN_OR_EQUAL) {
                    // ST_Distance(a, b) <= r
                    RowExpression radius = spatialComparison.getArguments().get(1);
                    if (radius instanceof VariableReferenceExpression && node.getRight().getOutputSymbols().contains(toSymbol(((VariableReferenceExpression) radius)))) {
                        CallExpression spatialFunction = (CallExpression) spatialComparison.getArguments().get(0);
                        Optional<PhysicalOperation> operation = tryCreateSpatialJoin(
                                context, node, removeExpressionFromFilter(filterExpression, spatialComparison),
                                spatialFunction, Optional.of((VariableReferenceExpression) radius),
                                functionMetadata.getOperatorType());
                        if (operation.isPresent()) {
                            return operation.get();
                        }
                    }
                }
            }

            throw new VerifyException("No valid spatial relationship found for spatial join");
        }

        private Optional<PhysicalOperation> tryCreateSpatialJoin(
                LocalExecutionPlanContext context,
                SpatialJoinNode node,
                Optional<RowExpression> filterExpression,
                CallExpression spatialFunction,
                Optional<VariableReferenceExpression> radius,
                Optional<OperatorType> comparisonOperator)
        {
            List<RowExpression> arguments = spatialFunction.getArguments();
            verify(arguments.size() == 2);

            if (!(arguments.get(0) instanceof VariableReferenceExpression) || !(arguments.get(1) instanceof VariableReferenceExpression)) {
                return Optional.empty();
            }

            VariableReferenceExpression firstVariable = (VariableReferenceExpression) arguments.get(0);
            VariableReferenceExpression secondVariable = (VariableReferenceExpression) arguments.get(1);

            PlanNode probeNode = node.getLeft();
            Set<SymbolReference> probeSymbols = getSymbolReferences(probeNode.getOutputSymbols());

            PlanNode buildNode = node.getRight();
            Set<SymbolReference> buildSymbols = getSymbolReferences(buildNode.getOutputSymbols());

            if (probeSymbols.contains(new SymbolReference(firstVariable.getName())) && buildSymbols.contains(new SymbolReference(secondVariable.getName()))) {
                return Optional.of(createSpatialLookupJoin(
                        node,
                        probeNode,
                        toSymbol(firstVariable),
                        buildNode,
                        toSymbol(secondVariable),
                        radius.map(VariableReferenceSymbolConverter::toSymbol),
                        spatialTest(spatialFunction, true, comparisonOperator),
                        filterExpression,
                        context));
            }
            if (probeSymbols.contains(new SymbolReference(secondVariable.getName())) && buildSymbols.contains(new SymbolReference(firstVariable.getName()))) {
                return Optional.of(createSpatialLookupJoin(
                        node,
                        probeNode,
                        toSymbol(secondVariable),
                        buildNode,
                        toSymbol(firstVariable),
                        radius.map(VariableReferenceSymbolConverter::toSymbol),
                        spatialTest(spatialFunction, false, comparisonOperator),
                        filterExpression,
                        context));
            }
            return Optional.empty();
        }

        private Optional<RowExpression> removeExpressionFromFilter(RowExpression filter, RowExpression expression)
        {
            RowExpression updatedJoinFilter = replaceExpression(filter, ImmutableMap.of(expression, TRUE_CONSTANT));
            return updatedJoinFilter == TRUE_CONSTANT ? Optional.empty() : Optional.of(updatedJoinFilter);
        }

        private SpatialPredicate spatialTest(CallExpression functionCall, boolean probeFirst, Optional<OperatorType> comparisonOperator)
        {
            String[] names = functionCall.getDisplayName().split("\\.");
            switch (names[names.length - 1].toLowerCase(Locale.ENGLISH)) {
                case ST_CONTAINS:
                    if (probeFirst) {
                        return (buildGeometry, probeGeometry, radius) -> probeGeometry.contains(buildGeometry);
                    }
                    else {
                        return (buildGeometry, probeGeometry, radius) -> buildGeometry.contains(probeGeometry);
                    }
                case ST_WITHIN:
                    if (probeFirst) {
                        return (buildGeometry, probeGeometry, radius) -> probeGeometry.within(buildGeometry);
                    }
                    else {
                        return (buildGeometry, probeGeometry, radius) -> buildGeometry.within(probeGeometry);
                    }
                case ST_INTERSECTS:
                    return (buildGeometry, probeGeometry, radius) -> buildGeometry.intersects(probeGeometry);
                case ST_DISTANCE:
                    if (comparisonOperator.get() == LESS_THAN) {
                        return (buildGeometry, probeGeometry, radius) -> buildGeometry.distance(probeGeometry) < radius.getAsDouble();
                    }
                    else if (comparisonOperator.get() == LESS_THAN_OR_EQUAL) {
                        return (buildGeometry, probeGeometry, radius) -> buildGeometry.distance(probeGeometry) <= radius.getAsDouble();
                    }
                    else {
                        throw new UnsupportedOperationException("Unsupported comparison operator: " + comparisonOperator.get());
                    }
                default:
                    throw new UnsupportedOperationException("Unsupported spatial function: " + functionCall.getDisplayName());
            }
        }

        private Set<SymbolReference> getSymbolReferences(Collection<Symbol> symbols)
        {
            return symbols.stream().map(SymbolUtils::toSymbolReference).collect(toImmutableSet());
        }

        protected PhysicalOperation createNestedLoopJoin(JoinNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation probeSource = node.getLeft().accept(this, context);

            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = node.getRight().accept(this, buildContext);

            checkState(
                    buildSource.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION,
                    "Build source of a nested loop join is expected to be GROUPED_EXECUTION.");
            checkArgument(node.getType() == INNER, "NestedLoopJoin is only used for inner join");

            JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager = new JoinBridgeManager<>(
                    false,
                    probeSource.getPipelineExecutionStrategy(),
                    buildSource.getPipelineExecutionStrategy(),
                    lifespan -> new NestedLoopJoinPagesSupplier(),
                    buildSource.getTypes());
            NestedLoopBuildOperatorFactory nestedLoopBuildOperatorFactory = new NestedLoopBuildOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    nestedLoopJoinBridgeManager);

            int taskCount = buildContext.getDriverInstanceCount().orElse(1);
            checkArgument(taskCount == 1, "Expected local execution to not be parallel");

            ImmutableList.Builder<OperatorFactory> factoriesBuilder = ImmutableList.builder();
            factoriesBuilder.addAll(buildSource.getOperatorFactories());

            createDynamicFilter(node, context, taskCount).ifPresent(
                    filter -> {
                        List<DynamicFilterSourceOperator.Channel> filterBuildChannels = filter
                                .getBuildChannels()
                                .entrySet()
                                .stream()
                                .map(entry -> {
                                    String filterId = entry.getKey();
                                    int index = entry.getValue();
                                    Type type = buildSource.getTypes().get(index);
                                    return new DynamicFilterSourceOperator.Channel(filterId, type, index, context.getSession().getQueryId().toString());
                                })
                                .collect(Collectors.toList());
                        factoriesBuilder.add(
                                new DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory(
                                        buildContext.getNextOperatorId(),
                                        node.getId(),
                                        filter.getValueConsumer(), /** the consumer to process all values collected to build the dynamic filter */
                                        filterBuildChannels,
                                        getDynamicFilteringMaxPerDriverValueCount(buildContext.getSession()),
                                        getDynamicFilteringMaxPerDriverSize(buildContext.getSession())));
                    });

            context.addDriverFactory(
                    buildContext.isInputDriver(),
                    false,
                    factoriesBuilder
                            .add(nestedLoopBuildOperatorFactory)
                            .build(),
                    buildContext.getDriverInstanceCount(),
                    buildSource.getPipelineExecutionStrategy());

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(probeSource.getLayout());

            // inputs from build side of the join are laid out following the input from the probe side,
            // so adjust the channel ids but keep the field layouts intact
            int offset = probeSource.getTypes().size();
            for (Map.Entry<Symbol, Integer> entry : buildSource.getLayout().entrySet()) {
                outputMappings.put(entry.getKey(), offset + entry.getValue());
            }

            OperatorFactory operatorFactory = new NestedLoopJoinOperatorFactory(context.getNextOperatorId(), node.getId(), nestedLoopJoinBridgeManager);
            return new PhysicalOperation(operatorFactory, outputMappings.build(), context, probeSource);
        }

        private PhysicalOperation createSpatialLookupJoin(
                SpatialJoinNode node,
                PlanNode probeNode,
                Symbol probeSymbol,
                PlanNode buildNode,
                Symbol buildSymbol,
                Optional<Symbol> radiusSymbol,
                SpatialPredicate spatialRelationshipTest,
                Optional<RowExpression> joinFilter,
                LocalExecutionPlanContext context)
        {
            // Plan probe
            PhysicalOperation probeSource = probeNode.accept(this, context);

            // Plan build
            PagesSpatialIndexFactory pagesSpatialIndexFactory = createPagesSpatialIndexFactory(node,
                    buildNode,
                    buildSymbol,
                    radiusSymbol,
                    probeSource.getLayout(),
                    spatialRelationshipTest,
                    joinFilter,
                    context);

            OperatorFactory operator = createSpatialLookupJoin(node, probeNode, probeSource, probeSymbol, pagesSpatialIndexFactory, context);

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            List<Symbol> outputSymbols = node.getOutputSymbols();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappings.put(symbol, i);
            }

            return new PhysicalOperation(operator, outputMappings.build(), context, probeSource);
        }

        private OperatorFactory createSpatialLookupJoin(SpatialJoinNode node,
                PlanNode probeNode,
                PhysicalOperation probeSource,
                Symbol probeSymbol,
                PagesSpatialIndexFactory pagesSpatialIndexFactory,
                LocalExecutionPlanContext context)
        {
            List<Type> probeTypes = probeSource.getTypes();
            List<Symbol> probeOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> probeNode.getOutputSymbols().contains(symbol))
                    .collect(toImmutableList());
            List<Integer> probeOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(probeOutputSymbols, probeSource.getLayout()));
            Function<Symbol, Integer> probeChannelGetter = channelGetter(probeSource);
            int probeChannel = probeChannelGetter.apply(probeSymbol);

            Optional<Integer> partitionChannel = node.getLeftPartitionSymbol().map(probeChannelGetter::apply);

            return new SpatialJoinOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    node.getType(),
                    probeTypes,
                    probeOutputChannels,
                    probeChannel,
                    partitionChannel,
                    pagesSpatialIndexFactory);
        }

        private PagesSpatialIndexFactory createPagesSpatialIndexFactory(
                SpatialJoinNode node,
                PlanNode buildNode,
                Symbol buildSymbol,
                Optional<Symbol> radiusSymbol,
                Map<Symbol, Integer> probeLayout,
                SpatialPredicate spatialRelationshipTest,
                Optional<RowExpression> joinFilter,
                LocalExecutionPlanContext context)
        {
            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = buildNode.accept(this, buildContext);
            List<Symbol> buildOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> buildNode.getOutputSymbols().contains(symbol))
                    .collect(toImmutableList());
            Map<Symbol, Integer> buildLayout = buildSource.getLayout();
            List<Integer> buildOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(buildOutputSymbols, buildLayout));
            Function<Symbol, Integer> buildChannelGetter = channelGetter(buildSource);
            Integer buildChannel = buildChannelGetter.apply(buildSymbol);
            Optional<Integer> radiusChannel = radiusSymbol.map(buildChannelGetter::apply);

            Optional<JoinFilterFunctionFactory> filterFunctionFactory = joinFilter
                    .map(filterExpression -> compileJoinFilterFunction(
                            filterExpression,
                            probeLayout,
                            buildLayout,
                            context.getTypes(),
                            context.getSession()));

            Optional<Integer> partitionChannel = node.getRightPartitionSymbol().map(buildChannelGetter::apply);

            SpatialIndexBuilderOperatorFactory builderOperatorFactory = new SpatialIndexBuilderOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    buildSource.getTypes(),
                    buildOutputChannels,
                    buildChannel,
                    radiusChannel,
                    partitionChannel,
                    spatialRelationshipTest,
                    node.getKdbTree(),
                    filterFunctionFactory,
                    10_000,
                    pagesIndexFactory);

            context.addDriverFactory(
                    buildContext.isInputDriver(),
                    false,
                    ImmutableList.<OperatorFactory>builder()
                            .addAll(buildSource.getOperatorFactories())
                            .add(builderOperatorFactory)
                            .build(),
                    buildContext.getDriverInstanceCount(),
                    buildSource.getPipelineExecutionStrategy());

            return builderOperatorFactory.getPagesSpatialIndexFactory();
        }

        private PhysicalOperation createLookupJoin(JoinNode node,
                PlanNode probeNode,
                List<Symbol> probeSymbols,
                Optional<Symbol> probeHashSymbol,
                PlanNode buildNode,
                List<Symbol> buildSymbols,
                Optional<Symbol> buildHashSymbol,
                LocalExecutionPlanContext context)
        {
            // Plan probe
            PhysicalOperation probeSource = probeNode.accept(this, context);

            // Plan build
            boolean spillEnabled = isSpillEnabled(session)
                    && node.isSpillable().orElseThrow(() -> new IllegalArgumentException("spillable not yet set"))
                    && probeSource.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION;
            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory =
                    createLookupSourceFactory(node, buildNode, buildSymbols, buildHashSymbol, probeSource, context, spillEnabled);

            OperatorFactory operator = createLookupJoin(node, probeSource, probeSymbols, probeHashSymbol, lookupSourceFactory, context, spillEnabled);

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            List<Symbol> outputSymbols = node.getOutputSymbols();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappings.put(symbol, i);
            }

            return new PhysicalOperation(operator, outputMappings.build(), context, probeSource);
        }

        protected Optional<LocalDynamicFilter> createDynamicFilter(JoinNode node, LocalExecutionPlanContext context, int partitionCount)
        {
            if (!isEnableDynamicFiltering(context.getSession())) {
                return Optional.empty();
            }
            if (node.getDynamicFilters().isEmpty()) {
                return Optional.empty();
            }
            LocalDynamicFiltersCollector collector = context.getDynamicFiltersCollector();
            return LocalDynamicFilter
                    .create(node, partitionCount, context.getSession(), context.taskContext.getTaskId(), stateStoreProvider)
                    .map(filter -> {
                        // Intersect dynamic filters' predicates when they become ready,
                        // in order to support multiple join nodes in the same plan fragment.
                        addSuccessCallback(filter.getDynamicFilterResultFuture(), collector::intersectDynamicFilter);
                        return filter;
                    });
        }

        private Optional<LocalDynamicFilter> createDynamicFilter(SemiJoinNode node, LocalExecutionPlanContext context)
        {
            if (!node.getDynamicFilterId().isPresent()) {
                return Optional.empty();
            }
            LocalDynamicFiltersCollector collector = context.getDynamicFiltersCollector();
            return LocalDynamicFilter
                    .create(node, context.getSession(), context.taskContext.getTaskId(), stateStoreProvider)
                    .map(filter -> {
                        addSuccessCallback(filter.getDynamicFilterResultFuture(), collector::intersectDynamicFilter);
                        return filter;
                    });
        }

        private JoinBridgeManager<PartitionedLookupSourceFactory> createLookupSourceFactory(
                JoinNode node,
                PlanNode buildNode,
                List<Symbol> buildSymbols,
                Optional<Symbol> buildHashSymbol,
                PhysicalOperation probeSource,
                LocalExecutionPlanContext context,
                boolean spillEnabled)
        {
            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = buildNode.accept(this, buildContext);

            if (buildSource.getPipelineExecutionStrategy() == GROUPED_EXECUTION) {
                checkState(
                        probeSource.getPipelineExecutionStrategy() == GROUPED_EXECUTION,
                        "Build execution is GROUPED_EXECUTION. Probe execution is expected be GROUPED_EXECUTION, but is UNGROUPED_EXECUTION.");
            }

            List<Symbol> buildOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> node.getRight().getOutputSymbols().contains(symbol))
                    .collect(toImmutableList());
            List<Integer> buildOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(buildOutputSymbols, buildSource.getLayout()));
            List<Integer> buildChannels = ImmutableList.copyOf(getChannelsForSymbols(buildSymbols, buildSource.getLayout()));
            OptionalInt buildHashChannel = buildHashSymbol.map(channelGetter(buildSource))
                    .map(OptionalInt::of).orElse(OptionalInt.empty());

            boolean buildOuter = node.getType() == RIGHT || node.getType() == FULL;
            int taskCount = buildContext.getDriverInstanceCount().orElse(1);
            /* Spill can take outer */
            boolean canOuterSpill = isSpillForOuterJoinEnabled(session);
            boolean spillAllowed = spillEnabled;
            if (buildOuter && spillEnabled) {
                spillAllowed = canOuterSpill;
            }

            Optional<JoinFilterFunctionFactory> filterFunctionFactory = node.getFilter()
                    .map(filterExpression -> compileJoinFilterFunction(
                            filterExpression,
                            probeSource.getLayout(),
                            buildSource.getLayout(),
                            context.getTypes(),
                            context.getSession()));

            Optional<SortExpressionContext> sortExpressionContext = node.getFilter().flatMap(filter -> SortExpressionExtractor.extractSortExpression(metadata, node.getRightOutputSymbols(), filter));

            Optional<Integer> sortChannel = sortExpressionContext
                    .map(SortExpressionContext::getSortExpression)
                    .map(sortExpression -> sortExpressionAsSortChannel(sortExpression, probeSource.getLayout(), buildSource.getLayout(), context));

            List<JoinFilterFunctionFactory> searchFunctionFactories = sortExpressionContext
                    .map(SortExpressionContext::getSearchExpressions)
                    .map(searchExpressions -> searchExpressions.stream()
                            .map(searchExpression -> compileJoinFilterFunction(
                                    searchExpression,
                                    probeSource.getLayout(),
                                    buildSource.getLayout(),
                                    context.getTypes(),
                                    context.getSession()))
                            .collect(toImmutableList()))
                    .orElse(ImmutableList.of());

            ImmutableList<Type> buildOutputTypes = buildOutputChannels.stream()
                    .map(buildSource.getTypes()::get)
                    .collect(toImmutableList());
            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = new JoinBridgeManager<>(
                    buildOuter,
                    probeSource.getPipelineExecutionStrategy(),
                    buildSource.getPipelineExecutionStrategy(),
                    lifespan -> new PartitionedLookupSourceFactory(
                            buildSource.getTypes(),
                            buildOutputTypes,
                            buildChannels.stream()
                                    .map(buildSource.getTypes()::get)
                                    .collect(toImmutableList()),
                            taskCount,
                            buildSource.getLayout(),
                            buildOuter,
                            canOuterSpill),
                    buildOutputTypes);

            ImmutableList.Builder<OperatorFactory> factoriesBuilder = new ImmutableList.Builder();
            factoriesBuilder.addAll(buildSource.getOperatorFactories());

            createDynamicFilter(node, context, taskCount).ifPresent(
                    filter -> {
                        List<DynamicFilterSourceOperator.Channel> filterBuildChannels = filter
                                .getBuildChannels()
                                .entrySet()
                                .stream()
                                .map(entry -> {
                                    String filterId = entry.getKey();
                                    int index = entry.getValue();
                                    Type type = buildSource.getTypes().get(index);
                                    return new DynamicFilterSourceOperator.Channel(filterId, type, index, context.getSession().getQueryId().toString());
                                })
                                .collect(Collectors.toList());
                        factoriesBuilder.add(
                                new DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory(
                                        buildContext.getNextOperatorId(),
                                        node.getId(),
                                        filter.getValueConsumer(), /** the consumer to process all values collected to build the dynamic filter */
                                        filterBuildChannels,
                                        getDynamicFilteringMaxPerDriverValueCount(buildContext.getSession()),
                                        getDynamicFilteringMaxPerDriverSize(buildContext.getSession())));
                    });

            HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    lookupSourceFactoryManager,
                    buildOutputChannels,
                    buildChannels,
                    buildHashChannel,
                    filterFunctionFactory,
                    sortChannel,
                    searchFunctionFactories,
                    10_000,
                    pagesIndexFactory,
                    spillAllowed && taskCount > 1,
                    singleStreamSpillerFactory,
                    spillerFactory,
                    isSpillToHdfsEnabled(context.getSession()));

            factoriesBuilder.add(hashBuilderOperatorFactory);

            context.addDriverFactory(
                    buildContext.isInputDriver(),
                    false,
                    factoriesBuilder.build(),
                    buildContext.getDriverInstanceCount(),
                    buildSource.getPipelineExecutionStrategy());

            return lookupSourceFactoryManager;
        }

        protected JoinFilterFunctionFactory compileJoinFilterFunction(
                RowExpression filterExpression,
                Map<Symbol, Integer> probeLayout,
                Map<Symbol, Integer> buildLayout,
                TypeProvider types,
                Session session)
        {
            Map<Symbol, Integer> joinSourcesLayout = createJoinSourcesLayout(buildLayout, probeLayout);
            return joinFilterFunctionCompiler.compileJoinFilterFunction(bindChannels(filterExpression, joinSourcesLayout, types), buildLayout.size());
        }

        public int sortExpressionAsSortChannel(
                RowExpression sortExpression,
                Map<Symbol, Integer> probeLayout,
                Map<Symbol, Integer> buildLayout,
                LocalExecutionPlanContext context)
        {
            Map<Symbol, Integer> joinSourcesLayout = createJoinSourcesLayout(buildLayout, probeLayout);
            RowExpression rewrittenSortExpression = bindChannels(sortExpression, joinSourcesLayout, context.getTypes());
            checkArgument(rewrittenSortExpression instanceof InputReferenceExpression, "Unsupported expression type [%s]", rewrittenSortExpression);
            return ((InputReferenceExpression) rewrittenSortExpression).getField();
        }

        private OperatorFactory createLookupJoin(
                JoinNode node,
                PhysicalOperation probeSource,
                List<Symbol> probeSymbols,
                Optional<Symbol> probeHashSymbol,
                JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactoryManager,
                LocalExecutionPlanContext context,
                boolean spillEnabled)
        {
            List<Type> probeTypes = probeSource.getTypes();
            List<Symbol> probeOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> node.getLeft().getOutputSymbols().contains(symbol))
                    .collect(toImmutableList());
            List<Integer> probeOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(probeOutputSymbols, probeSource.getLayout()));
            List<Integer> probeJoinChannels = ImmutableList.copyOf(getChannelsForSymbols(probeSymbols, probeSource.getLayout()));
            OptionalInt probeHashChannel = probeHashSymbol.map(channelGetter(probeSource))
                    .map(OptionalInt::of).orElse(OptionalInt.empty());
            OptionalInt totalOperatorsCount = context.getDriverInstanceCount();
            checkState(!spillEnabled || totalOperatorsCount.isPresent(), "A fixed distribution is required for JOIN when spilling is enabled");

            switch (node.getType()) {
                case INNER:
                    return lookupJoinOperators.innerJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel, Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);
                case LEFT:
                    return lookupJoinOperators.probeOuterJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel, Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);
                case RIGHT:
                    return lookupJoinOperators.lookupOuterJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel, Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);
                case FULL:
                    return lookupJoinOperators.fullOuterJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel, Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        protected Map<Symbol, Integer> createJoinSourcesLayout(Map<Symbol, Integer> lookupSourceLayout, Map<Symbol, Integer> probeSourceLayout)
        {
            ImmutableMap.Builder<Symbol, Integer> joinSourcesLayout = ImmutableMap.builder();
            joinSourcesLayout.putAll(lookupSourceLayout);
            for (Map.Entry<Symbol, Integer> probeLayoutEntry : probeSourceLayout.entrySet()) {
                joinSourcesLayout.put(probeLayoutEntry.getKey(), probeLayoutEntry.getValue() + lookupSourceLayout.size());
            }
            return joinSourcesLayout.build();
        }

        @Override
        public PhysicalOperation visitSemiJoin(SemiJoinNode node, LocalExecutionPlanContext context)
        {
            // Plan probe
            PhysicalOperation probeSource = node.getSource().accept(this, context);

            // Plan build
            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = node.getFilteringSource().accept(this, buildContext);
            checkState(buildSource.getPipelineExecutionStrategy() == probeSource.getPipelineExecutionStrategy(), "build and probe have different pipelineExecutionStrategy");
            int partitionCount = buildContext.getDriverInstanceCount().orElse(1);
            checkArgument(partitionCount == 1, "Expected local execution to not be parallel");

            int probeChannel = probeSource.getLayout().get(node.getSourceJoinSymbol());
            int buildChannel = buildSource.getLayout().get(node.getFilteringSourceJoinSymbol());

            ImmutableList.Builder<OperatorFactory> buildOperatorFactories = new ImmutableList.Builder<>();
            buildOperatorFactories.addAll(buildSource.getOperatorFactories());

            node.getDynamicFilterId().ifPresent(filterId -> {
                // Add a DynamicFilterSourceOperatorFactory to build operator factories
                log.debug("[Semi-join] Dynamic filter: %s", filterId);
                LocalDynamicFilter filterConsumer = createDynamicFilter(node, context).orElse(null);
                if (filterConsumer != null) {
                    addSuccessCallback(filterConsumer.getDynamicFilterResultFuture(), context::getDynamicFiltersCollector);
                    buildOperatorFactories.add(new DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory(
                            buildContext.getNextOperatorId(),
                            node.getId(),
                            filterConsumer.getValueConsumer(),
                            ImmutableList.of(new DynamicFilterSourceOperator.Channel(filterId, buildSource.getTypes().get(buildChannel), buildChannel, context.getSession().getQueryId().toString())),
                            getDynamicFilteringMaxPerDriverValueCount(context.getSession()),
                            getDynamicFilteringMaxPerDriverSize(context.getSession())));
                }
            });

            Optional<Integer> buildHashChannel = node.getFilteringSourceHashSymbol().map(channelGetter(buildSource));
            Optional<Integer> probeHashChannel = node.getSourceHashSymbol().map(channelGetter(probeSource));

            SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    buildSource.getTypes().get(buildChannel),
                    buildChannel,
                    buildHashChannel,
                    10_000,
                    joinCompiler);
            buildOperatorFactories.add(setBuilderOperatorFactory);
            SetSupplier setProvider = setBuilderOperatorFactory.getSetProvider();
            context.addDriverFactory(
                    buildContext.isInputDriver(),
                    false,
                    buildOperatorFactories.build(),
                    buildContext.getDriverInstanceCount(),
                    buildSource.getPipelineExecutionStrategy());

            // Source channels are always laid out first, followed by the boolean output symbol
            Map<Symbol, Integer> outputMappings = ImmutableMap.<Symbol, Integer>builder()
                    .putAll(probeSource.getLayout())
                    .put(node.getSemiJoinOutput(), probeSource.getLayout().size())
                    .build();

            HashSemiJoinOperatorFactory operator = new HashSemiJoinOperatorFactory(context.getNextOperatorId(), node.getId(), setProvider, probeSource.getTypes(), probeChannel, probeHashChannel);
            return new PhysicalOperation(operator, outputMappings, context, probeSource);
        }

        @Override
        public PhysicalOperation visitTableWriter(TableWriterNode node, LocalExecutionPlanContext context)
        {
            // Set table writer count
            if (node.getPartitioningScheme().isPresent()) {
                PartitioningHandle partitioningHandle = node.getPartitioningScheme().get().getPartitioning().getHandle();
                if (partitioningHandle.equals(FIXED_HASH_DISTRIBUTION)) {
                    context.setDriverInstanceCount(getTaskWriterCount(session));
                }
                else {
                    context.setDriverInstanceCount(1);
                }
            }
            else {
                context.setDriverInstanceCount(getTaskWriterCount(session));
            }

            // serialize writes by forcing data through a single writer
            PhysicalOperation source = node.getSource().accept(this, context);

            ImmutableMap.Builder<Symbol, Integer> outputMapping = ImmutableMap.builder();
            outputMapping.put(node.getOutputSymbols().get(0), ROW_COUNT_CHANNEL);
            outputMapping.put(node.getOutputSymbols().get(1), FRAGMENT_CHANNEL);

            OperatorFactory statisticsAggregation = node.getStatisticsAggregation().map(aggregation -> {
                List<Symbol> groupingSymbols = aggregation.getGroupingSymbols();
                if (groupingSymbols.isEmpty()) {
                    return createAggregationOperatorFactory(
                            node.getId(),
                            aggregation.getAggregations(),
                            PARTIAL,
                            STATS_START_CHANNEL,
                            outputMapping,
                            source,
                            context,
                            true);
                }
                return createHashAggregationOperatorFactory(
                        node.getId(),
                        aggregation.getAggregations(),
                        ImmutableSet.of(),
                        groupingSymbols,
                        PARTIAL,
                        Optional.empty(),
                        Optional.empty(),
                        source,
                        false,
                        false,
                        false,
                        new DataSize(0, BYTE),
                        context,
                        STATS_START_CHANNEL,
                        outputMapping,
                        200,
                        // This aggregation must behave as INTERMEDIATE.
                        // Using INTERMEDIATE aggregation directly
                        // is not possible, as it doesn't accept raw input data.
                        // Disabling partial pre-aggregation memory limit effectively
                        // turns PARTIAL aggregation into INTERMEDIATE.
                        Optional.empty(),
                        true);
            }).orElse(new DevNullOperatorFactory(context.getNextOperatorId(), node.getId()));

            List<Integer> inputChannels = node.getColumns().stream()
                    .map(source::symbolToChannel)
                    .collect(toImmutableList());

            OperatorFactory operatorFactory = new TableWriterOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    pageSinkManager,
                    node.getTarget(),
                    inputChannels,
                    session,
                    statisticsAggregation,
                    getSymbolTypes(node.getOutputSymbols(), context.getTypes()),
                    Optional.of(context.getTaskId()));

            return new PhysicalOperation(operatorFactory, outputMapping.build(), context, source);
        }

        @Override
        public PhysicalOperation visitStatisticsWriterNode(StatisticsWriterNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            StatisticAggregationsDescriptor<Integer> descriptor = node.getDescriptor().map(symbol -> source.getLayout().get(symbol));

            OperatorFactory operatorFactory = new StatisticsWriterOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    computedStatistics -> metadata.finishStatisticsCollection(session, ((StatisticsWriterNode.WriteStatisticsHandle) node.getTarget()).getHandle(), computedStatistics),
                    node.isRowCountEnabled(),
                    descriptor);
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitTableFinish(TableFinishNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            ImmutableMap.Builder<Symbol, Integer> outputMapping = ImmutableMap.builder();

            OperatorFactory statisticsAggregation = node.getStatisticsAggregation().map(aggregation -> {
                List<Symbol> groupingSymbols = aggregation.getGroupingSymbols();
                if (groupingSymbols.isEmpty()) {
                    return createAggregationOperatorFactory(
                            node.getId(),
                            aggregation.getAggregations(),
                            FINAL,
                            0,
                            outputMapping,
                            source,
                            context,
                            true);
                }
                return createHashAggregationOperatorFactory(
                        node.getId(),
                        aggregation.getAggregations(),
                        ImmutableSet.of(),
                        groupingSymbols,
                        FINAL,
                        Optional.empty(),
                        Optional.empty(),
                        source,
                        false,
                        false,
                        false,
                        new DataSize(0, BYTE),
                        context,
                        0,
                        outputMapping,
                        200,
                        // final aggregation ignores partial pre-aggregation memory limit
                        Optional.empty(),
                        true);
            }).orElse(new DevNullOperatorFactory(context.getNextOperatorId(), node.getId()));

            Map<Symbol, Integer> aggregationOutput = outputMapping.build();
            StatisticAggregationsDescriptor<Integer> descriptor = node.getStatisticsAggregationDescriptor()
                    .map(desc -> desc.map(aggregationOutput::get))
                    .orElse(StatisticAggregationsDescriptor.empty());

            OperatorFactory operatorFactory = new TableFinishOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    createTableFinisher(session, node, metadata),
                    statisticsAggregation,
                    descriptor,
                    tableExecuteContextManager,
                    session);
            Map<Symbol, Integer> layout = ImmutableMap.of(node.getOutputSymbols().get(0), 0);

            return new PhysicalOperation(operatorFactory, layout, context, source);
        }

        @Override
        public PhysicalOperation visitCacheTableFinish(CacheTableFinishNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            ImmutableMap.Builder<Symbol, Integer> outputMapping = ImmutableMap.builder();
            StatisticAggregationsDescriptor<Integer> descriptor = StatisticAggregationsDescriptor.empty();

            List<Symbol> outputSymbols = node.getOutputSymbols();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMapping.put(symbol, i);
            }

            long thresholdSize = getCteResultCacheThresholdSize(session).toBytes();

            Identity identity = session.getIdentity();
            identity = new Identity(userName, identity.getGroups(), identity.getPrincipal(), identity.getRoles(), identity.getExtraCredentials());
            Session newSession = session.withUpdatedIdentity(identity);

            OperatorFactory operatorFactory = new CacheTableFinishOperator.CacheTableFinishOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    createTableFinisher(newSession, node, metadata),
                    descriptor,
                    tableExecuteContextManager,
                    session,
                    thresholdSize,
                    (endTime, size) -> {
                        CachedDataStorage cds = cachedDataManager.get(node.getCachedDataKey());
                        cds.commit(endTime, size);
                        log.info("Cache Write committed for CTE: %s with size(%d)",
                                node.getCachedDataKey().getName(), size);
                        return null;
                    },
                    () -> {
                        CachedDataStorage cds = cachedDataManager.get(node.getCachedDataKey());
                        cds.setNonCachable(true);
                        log.info("Cache Write aborted for CTE: %s for exceeding threshold size",
                                node.getCachedDataKey().getName());
                        return null;
                    });

            return new PhysicalOperation(operatorFactory, outputMapping.build(), context, source);
        }

        @Override
        public PhysicalOperation visitCacheTableWriter(CacheTableWriterNode node, LocalExecutionPlanContext context)
        {
            // Set table writer count
            if (node.getPartitioningScheme().isPresent()) {
                PartitioningHandle partitioningHandle = node.getPartitioningScheme().get().getPartitioning().getHandle();
                if (partitioningHandle.equals(FIXED_HASH_DISTRIBUTION)) {
                    context.setDriverInstanceCount(getTaskWriterCount(session));
                }
                else {
                    context.setDriverInstanceCount(1);
                }
            }
            else {
                context.setDriverInstanceCount(getTaskWriterCount(session));
            }

            // serialize writes by forcing data through a single writer
            PhysicalOperation source = node.getSource().accept(this, context);
            List<Integer> inputChannels = node.getColumns().stream()
                    .map(source::symbolToChannel)
                    .collect(toImmutableList());
            List<Symbol> outputSymbols = node.getOutputSymbols();

            ImmutableMap.Builder<Symbol, Integer> outputMapping = ImmutableMap.builder();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMapping.put(symbol, i);
            }

            long thresholdSize = getCteResultCacheThresholdSize(session).toBytes() / nodePartitioningManager.getNodeScheduler()
                    .getNodeManager().getActiveConnectorNodes(new CatalogName("hive")).size();

            OperatorFactory operatorFactory = new CacheTableWriterOperator.CacheTableWriterOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    pageSinkManager,
                    node.getTarget(),
                    inputChannels,
                    session,
                    Optional.of(context.getTaskId()),
                    thresholdSize);

            return new PhysicalOperation(operatorFactory, outputMapping.build(), context, source);
        }

        @Override
        public PhysicalOperation visitCubeFinish(CubeFinishNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);
            OperatorFactory operatorFactory = new CubeFinishOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    session,
                    metadata,
                    TypeProvider.viewOf(node.getPredicateColumnsType()),
                    cubeManager,
                    node.getMetadata());
            Map<Symbol, Integer> layout = ImmutableMap.of(node.getOutputSymbols().get(0), 0);
            return new PhysicalOperation(operatorFactory, layout, context, source);
        }

        @Override
        public PhysicalOperation visitDelete(DeleteNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new DeleteOperatorFactory(context.getNextOperatorId(), node.getId(), source.getLayout().get(node.getRowId()));

            Map<Symbol, Integer> layout = ImmutableMap.<Symbol, Integer>builder()
                    .put(node.getOutputSymbols().get(0), 0)
                    .put(node.getOutputSymbols().get(1), 1)
                    .build();

            return new PhysicalOperation(operatorFactory, layout, context, source);
        }

        @Override
        public PhysicalOperation visitUpdateIndex(UpdateIndexNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new UpdateIndexOperator.UpdateIndexOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    node.getUpdateIndexMetadata(),
                    heuristicIndexerManager);

            return new PhysicalOperation(operatorFactory, source.getLayout(), context, source);
        }

        @Override
        public PhysicalOperation visitUpdate(UpdateNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);
            List<Integer> channelNumbers = createColumnValueAndRowIdChannels(node.getSource().getOutputSymbols(), node.getColumnValueAndRowIdSymbols());
            OperatorFactory operatorFactory = new UpdateOperator.UpdateOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    channelNumbers,
                    node.getTarget().getUpdatedColumns());

            Map<Symbol, Integer> layout = ImmutableMap.<Symbol, Integer>builder()
                    .put(node.getOutputSymbols().get(0), 0)
                    .put(node.getOutputSymbols().get(1), 1)
                    .build();

            return new PhysicalOperation(operatorFactory, layout, context, source);
        }

        private List<Integer> createColumnValueAndRowIdChannels(List<Symbol> outputSymbols, List<Symbol> columnValueAndRowIdSymbols)
        {
            Integer[] columnValueAndRowIdChannels = new Integer[columnValueAndRowIdSymbols.size()];
            int symbolCounter = 0;
            // This depends on the outputSymbols being ordered as the blocks of the
            // resulting page are ordered.
            for (Symbol symbol : outputSymbols) {
                int index = columnValueAndRowIdSymbols.indexOf(symbol);
                if (index >= 0) {
                    columnValueAndRowIdChannels[index] = symbolCounter;
                }
                symbolCounter++;
            }
            checkArgument(symbolCounter == columnValueAndRowIdSymbols.size(), "symbolCounter %s should be columnValueAndRowIdChannels.size() %s", symbolCounter);
            return Arrays.asList(columnValueAndRowIdChannels);
        }

        @Override
        public PhysicalOperation visitTableDelete(TableDeleteNode node, LocalExecutionPlanContext context)
        {
            Optional<PhysicalOperation> source = Optional.empty();
            Optional<Map<Symbol, Integer>> sourceLayout = Optional.empty();
            if (node.getSource() != null) {
                source = Optional.of(node.getSource().accept(this, context));
                sourceLayout = Optional.of(source.get().getLayout());
            }
            OperatorFactory operatorFactory = new TableDeleteOperatorFactory(context.getNextOperatorId(), node.getId(), metadata, session, node.getTarget(),
                    sourceLayout, node.getFilter(), node.getAssignments(), context.getTypes());

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source, UNGROUPED_EXECUTION);
        }

        @Override
        public PhysicalOperation visitTableExecute(PlanNode planNode, LocalExecutionPlanContext context)
        {
            TableExecuteNode node = (TableExecuteNode) planNode;
            // Set table writer count
            context.setDriverInstanceCount(getTaskWriterCount(session));

            PhysicalOperation source = node.getSource().accept(this, context);

            ImmutableMap.Builder<Symbol, Integer> outputMapping = ImmutableMap.builder();
            outputMapping.put(node.getOutputSymbols().get(0), ROW_COUNT_CHANNEL);
            outputMapping.put(node.getOutputSymbols().get(1), FRAGMENT_CHANNEL);

            List<Integer> inputChannels = node.getColumns().stream()
                    .map(source::symbolToChannel)
                    .collect(toImmutableList());

            OperatorFactory operatorFactory = new TableWriterOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    pageSinkManager,
                    node.getTarget(),
                    inputChannels,
                    session,
                    new DevNullOperatorFactory(context.getNextOperatorId(), node.getId()), // statistics are not calculated
                    getSymbolTypes(node.getOutputSymbols(), context.getTypes()), Optional.empty());

            return new PhysicalOperation(operatorFactory, outputMapping.build(), context, source);
        }

        @Override
        public PhysicalOperation visitTableUpdate(TableUpdateNode node, LocalExecutionPlanContext context)
        {
            OperatorFactory operatorFactory = new TableUpdateOperator.TableUpdateOperatorFactory(context.getNextOperatorId(), node.getId(), metadata, session, node.getTarget());

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        @Override
        public PhysicalOperation visitUnion(UnionNode node, LocalExecutionPlanContext context)
        {
            throw new UnsupportedOperationException("Union node should not be present in a local execution plan");
        }

        @Override
        public PhysicalOperation visitEnforceSingleRow(EnforceSingleRowNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new EnforceSingleRowOperator.EnforceSingleRowOperatorFactory(context.getNextOperatorId(), node.getId());
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitAssignUniqueId(AssignUniqueId node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new AssignUniqueIdOperator.AssignUniqueIdOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId());
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitExchange(ExchangeNode node, LocalExecutionPlanContext context)
        {
            checkArgument(node.getScope() == LOCAL, "Only local exchanges are supported in the local planner");

            if (node.getOrderingScheme().isPresent()) {
                return createLocalMerge(node, context);
            }

            return createLocalExchange(node, context);
        }

        private PhysicalOperation createLocalMerge(ExchangeNode node, LocalExecutionPlanContext context)
        {
            checkArgument(node.getOrderingScheme().isPresent(), "orderingScheme is absent");
            checkState(node.getSources().size() == 1, "single source is expected");

            // local merge source must have a single driver
            context.setDriverInstanceCount(1);

            PlanNode sourceNode = getOnlyElement(node.getSources());
            LocalExecutionPlanContext subContext = context.createSubContext();
            PhysicalOperation source = sourceNode.accept(this, subContext);

            int operatorsCount = subContext.getDriverInstanceCount().orElse(1);
            List<Type> types = getSourceOperatorTypes(node, context.getTypes());
            // Snapshot: current implementation depends on the fact that local-merge only uses pass-through exchangers
            checkArgument(node.getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_PASSTHROUGH_DISTRIBUTION));
            LocalExchangeFactory exchangeFactory = new LocalExchangeFactory(
                    node.getPartitioningScheme().getPartitioning().getHandle(),
                    operatorsCount,
                    types,
                    ImmutableList.of(),
                    Optional.empty(),
                    source.getPipelineExecutionStrategy(),
                    maxLocalExchangeBufferSize,
                    true,
                    node.getAggregationType());

            List<OperatorFactory> operatorFactories = new ArrayList<>(source.getOperatorFactories());
            List<Symbol> expectedLayout = node.getInputs().get(0);
            Function<Page, Page> pagePreprocessor = enforceLayoutProcessor(expectedLayout, source.getLayout());
            operatorFactories.add(new LocalExchangeSinkOperatorFactory(
                    exchangeFactory,
                    subContext.getNextOperatorId(),
                    node.getId(),
                    exchangeFactory.newSinkFactoryId(),
                    pagePreprocessor));
            context.addDriverFactory(subContext.isInputDriver(), false, operatorFactories, subContext.getDriverInstanceCount(), source.getPipelineExecutionStrategy());
            // the main driver is not an input... the exchange sources are the input for the plan
            context.setInputDriver(false);

            OrderingScheme orderingScheme = node.getOrderingScheme().get();
            ImmutableMap<Symbol, Integer> layout = makeLayout(node);
            List<Integer> sortChannels = getChannelsForSymbols(orderingScheme.getOrderBy(), layout);
            List<SortOrder> orderings = orderingScheme.getOrderingList();
            OperatorFactory operatorFactory = new LocalMergeSourceOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    exchangeFactory,
                    types,
                    orderingCompiler,
                    sortChannels,
                    orderings);
            return new PhysicalOperation(operatorFactory, layout, context, UNGROUPED_EXECUTION);
        }

        private PhysicalOperation createLocalExchange(ExchangeNode node, LocalExecutionPlanContext context)
        {
            int driverInstanceCount;
            if (node.getType() == ExchangeNode.Type.GATHER) {
                driverInstanceCount = 1;
                context.setDriverInstanceCount(1);
            }
            else if (context.getDriverInstanceCount().isPresent()) {
                driverInstanceCount = context.getDriverInstanceCount().getAsInt();
            }
            else {
                driverInstanceCount = getTaskConcurrency(session);
                context.setDriverInstanceCount(driverInstanceCount);
            }

            List<Type> types = getSourceOperatorTypes(node, context.getTypes());
            List<Integer> channels = node.getPartitioningScheme().getPartitioning().getArguments().stream()
                    .map(argument -> node.getOutputSymbols().indexOf(argument.getColumn()))
                    .collect(toImmutableList());
            Optional<Integer> hashChannel = node.getPartitioningScheme().getHashColumn()
                    .map(symbol -> node.getOutputSymbols().indexOf(symbol));

            PipelineExecutionStrategy exchangeSourcePipelineExecutionStrategy = GROUPED_EXECUTION;
            List<DriverFactoryParameters> driverFactoryParametersList = new ArrayList<>();
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode sourceNode = node.getSources().get(i);

                LocalExecutionPlanContext subContext = context.createSubContext();
                PhysicalOperation source = sourceNode.accept(this, subContext);
                driverFactoryParametersList.add(new DriverFactoryParameters(subContext, source));

                if (source.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION) {
                    exchangeSourcePipelineExecutionStrategy = UNGROUPED_EXECUTION;
                }
            }

            LocalExchangeFactory localExchangeFactory = new LocalExchangeFactory(
                    node.getPartitioningScheme().getPartitioning().getHandle(),
                    driverInstanceCount,
                    types,
                    channels,
                    hashChannel,
                    exchangeSourcePipelineExecutionStrategy,
                    maxLocalExchangeBufferSize,
                    false,
                    node.getAggregationType());
            int totalSinkCount = 0;
            for (int i = 0; i < node.getSources().size(); i++) {
                DriverFactoryParameters driverFactoryParameters = driverFactoryParametersList.get(i);
                PhysicalOperation source = driverFactoryParameters.getSource();
                LocalExecutionPlanContext subContext = driverFactoryParameters.getSubContext();

                List<Symbol> expectedLayout = node.getInputs().get(i);
                Function<Page, Page> pagePreprocessor = enforceLayoutProcessor(expectedLayout, source.getLayout());
                List<OperatorFactory> operatorFactories = new ArrayList<>(source.getOperatorFactories());

                operatorFactories.add(new LocalExchangeSinkOperatorFactory(
                        localExchangeFactory,
                        subContext.getNextOperatorId(),
                        node.getId(),
                        localExchangeFactory.newSinkFactoryId(),
                        pagePreprocessor));
                context.addDriverFactory(
                        subContext.isInputDriver(),
                        false,
                        operatorFactories,
                        subContext.getDriverInstanceCount(),
                        exchangeSourcePipelineExecutionStrategy);

                // Snapshot: total number of sinks are the expected number of input channels for local-exchange source
                totalSinkCount += subContext.getDriverInstanceCount().orElse(1);
                // For each outer-join, also need to include the lookup-outer driver as a sink
                totalSinkCount += operatorFactories.stream()
                        .filter(factory -> factory instanceof LookupJoinOperatorFactory && ((LookupJoinOperatorFactory) factory).createOuterOperatorFactory().isPresent())
                        .count();
            }

            // the main driver is not an input... the exchange sources are the input for the plan
            context.setInputDriver(false);

            // instance count must match the number of partitions in the exchange
            verify(context.getDriverInstanceCount().getAsInt() == localExchangeFactory.getBufferCount(),
                    "driver instance count must match the number of exchange partitions");

            return new PhysicalOperation(new LocalExchangeSourceOperatorFactory(context.getNextOperatorId(), node.getId(), localExchangeFactory, totalSinkCount), makeLayout(node), context, exchangeSourcePipelineExecutionStrategy);
        }

        @Override
        public PhysicalOperation visitPlan(PlanNode node, LocalExecutionPlanContext context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        protected List<Type> getSourceOperatorTypes(PlanNode node, TypeProvider types)
        {
            return getSymbolTypes(node.getOutputSymbols(), types);
        }

        protected List<Type> getSymbolTypes(List<Symbol> symbols, TypeProvider types)
        {
            return symbols.stream()
                    .map(types::get)
                    .collect(toImmutableList());
        }

        protected AccumulatorFactory buildAccumulatorFactory(
                PhysicalOperation source,
                Aggregation aggregation)
        {
            InternalAggregationFunction internalAggregationFunction = metadata.getFunctionAndTypeManager().getAggregateFunctionImplementation(aggregation.getFunctionHandle());

            List<Integer> valueChannels = new ArrayList<>();
            for (RowExpression argument : aggregation.getArguments()) {
                if (!(argument instanceof LambdaDefinitionExpression)) {
                    checkArgument(argument instanceof VariableReferenceExpression, "argument must be variable reference");
                    valueChannels.add(source.getLayout().get(new Symbol(((VariableReferenceExpression) argument).getName())));
                }
            }

            List<LambdaProvider> lambdaProviders = new ArrayList<>();
            List<LambdaDefinitionExpression> lambdas = aggregation.getArguments().stream()
                    .filter(LambdaDefinitionExpression.class::isInstance)
                    .map(LambdaDefinitionExpression.class::cast)
                    .collect(toImmutableList());
            for (int i = 0; i < lambdas.size(); i++) {
                List<Class<?>> lambdaInterfaces = internalAggregationFunction.getLambdaInterfaces();
                Class<? extends LambdaProvider> lambdaProviderClass = compileLambdaProvider(lambdas.get(i), metadata, lambdaInterfaces.get(i));
                try {
                    lambdaProviders.add((LambdaProvider) constructorMethodHandle(lambdaProviderClass, ConnectorSession.class).invoke(session.toConnectorSession()));
                }
                catch (Throwable t) {
                    throw new RuntimeException(t);
                }
            }

            Optional<Integer> maskChannel = aggregation.getMask().map(value -> source.getLayout().get(value));
            List<SortOrder> sortOrders = ImmutableList.of();
            List<Symbol> sortKeys = ImmutableList.of();
            if (aggregation.getOrderingScheme().isPresent()) {
                OrderingScheme orderingScheme = aggregation.getOrderingScheme().get();
                sortKeys = orderingScheme.getOrderBy();
                sortOrders = sortKeys.stream()
                        .map(orderingScheme::getOrdering)
                        .collect(toImmutableList());
            }

            return internalAggregationFunction.bind(
                    valueChannels,
                    maskChannel,
                    source.getTypes(),
                    getChannelsForSymbols(sortKeys, source.getLayout()),
                    sortOrders,
                    pagesIndexFactory,
                    aggregation.isDistinct(),
                    joinCompiler,
                    lambdaProviders,
                    session);
        }

        private PhysicalOperation planGlobalAggregation(AggregationNode node, PhysicalOperation source, LocalExecutionPlanContext context)
        {
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            AggregationOperatorFactory operatorFactory = createAggregationOperatorFactory(
                    node.getId(),
                    node.getAggregations(),
                    node.getStep(),
                    0,
                    outputMappings,
                    source,
                    context,
                    node.getStep().isOutputPartial());
            return new PhysicalOperation(operatorFactory, outputMappings.build(), context, source);
        }

        private AggregationOperatorFactory createAggregationOperatorFactory(
                PlanNodeId planNodeId,
                Map<Symbol, Aggregation> aggregations,
                Step step,
                int startOutputChannel,
                ImmutableMap.Builder<Symbol, Integer> outputMappings,
                PhysicalOperation source,
                LocalExecutionPlanContext context,
                boolean useSystemMemory)
        {
            int outputChannel = startOutputChannel;
            ImmutableList.Builder<AccumulatorFactory> accumulatorFactories = ImmutableList.builder();
            for (Map.Entry<Symbol, Aggregation> entry : aggregations.entrySet()) {
                Symbol symbol = entry.getKey();
                Aggregation aggregation = entry.getValue();
                accumulatorFactories.add(buildAccumulatorFactory(source, aggregation));
                outputMappings.put(symbol, outputChannel); // one aggregation per channel
                outputChannel++;
            }
            return new AggregationOperatorFactory(context.getNextOperatorId(), planNodeId, step, accumulatorFactories.build(), useSystemMemory);
        }

        private PhysicalOperation planGroupByAggregation(
                AggregationNode node,
                PhysicalOperation source,
                boolean spillEnabled,
                DataSize unspillMemoryLimit,
                LocalExecutionPlanContext context)
        {
            ImmutableMap.Builder<Symbol, Integer> mappings = ImmutableMap.builder();
            OperatorFactory operatorFactory;

            if (node.getAggregationType().equals(AggregationNode.AggregationType.SORT_BASED)) {
                operatorFactory = createSortAggregationOperatorFactory(
                        node.getId(),
                        node.getAggregations(),
                        node.getGlobalGroupingSets(),
                        node.getGroupingKeys(),
                        node.getStep(),
                        node.getHashSymbol(),
                        node.getGroupIdSymbol(),
                        source,
                        node.hasDefaultOutput(),
                        spillEnabled,
                        unspillMemoryLimit,
                        context,
                        0,
                        mappings,
                        10_000,
                        Optional.of(maxPartialAggregationMemorySize),
                        node.getStep().isOutputPartial(),
                        node.getFinalizeSymbol());
            }
            else {
                operatorFactory = createHashAggregationOperatorFactory(
                        node.getId(),
                        node.getAggregations(),
                        node.getGlobalGroupingSets(),
                        node.getGroupingKeys(),
                        node.getStep(),
                        node.getHashSymbol(),
                        node.getGroupIdSymbol(),
                        source,
                        node.hasDefaultOutput(),
                        spillEnabled,
                        node.isStreamable(),
                        unspillMemoryLimit,
                        context,
                        0,
                        mappings,
                        10_000,
                        Optional.of(maxPartialAggregationMemorySize),
                        node.getStep().isOutputPartial());
            }
            return new PhysicalOperation(operatorFactory, mappings.build(), context, source);
        }

        private OperatorFactory createHashAggregationOperatorFactory(
                PlanNodeId planNodeId,
                Map<Symbol, Aggregation> aggregations,
                Set<Integer> globalGroupingSets,
                List<Symbol> groupBySymbols,
                Step step,
                Optional<Symbol> hashSymbol,
                Optional<Symbol> groupIdSymbol,
                PhysicalOperation source,
                boolean hasDefaultOutput,
                boolean spillEnabled,
                boolean isStreamable,
                DataSize unspillMemoryLimit,
                LocalExecutionPlanContext context,
                int startOutputChannel,
                ImmutableMap.Builder<Symbol, Integer> outputMappings,
                int expectedGroups,
                Optional<DataSize> maxPartialAggregationMemorySize,
                boolean useSystemMemory)
        {
            List<AccumulatorFactory> accumulatorFactories = new ArrayList<>();
            Optional<Integer> groupIdChannel = getOutputMappingAndGroupIdChannel(aggregations, groupBySymbols, hashSymbol, groupIdSymbol,
                    source, startOutputChannel, outputMappings, accumulatorFactories, Optional.empty(), Optional.empty());

            List<Integer> groupByChannels = getChannelsForSymbols(groupBySymbols, source.getLayout());
            List<Type> groupByTypes = groupByChannels.stream()
                    .map(entry -> source.getTypes().get(entry))
                    .collect(toImmutableList());

            if (isStreamable) {
                return new StreamingAggregationOperatorFactory(
                        context.getNextOperatorId(),
                        planNodeId,
                        source.getTypes(),
                        groupByTypes,
                        groupByChannels,
                        step,
                        accumulatorFactories,
                        joinCompiler);
            }
            else {
                Optional<Integer> hashChannel = hashSymbol.map(channelGetter(source));
                return new HashAggregationOperatorFactory(
                        context.getNextOperatorId(),
                        planNodeId,
                        groupByTypes,
                        groupByChannels,
                        ImmutableList.copyOf(globalGroupingSets),
                        step,
                        hasDefaultOutput,
                        accumulatorFactories,
                        hashChannel,
                        groupIdChannel,
                        expectedGroups,
                        maxPartialAggregationMemorySize,
                        spillEnabled,
                        unspillMemoryLimit,
                        spillerFactory,
                        joinCompiler,
                        useSystemMemory,
                        createPartialAggregationController(step, session));
            }
        }

        private OperatorFactory createSortAggregationOperatorFactory(
                PlanNodeId planNodeId,
                Map<Symbol, Aggregation> aggregations,
                Set<Integer> globalGroupingSets,
                List<Symbol> groupBySymbols,
                Step step,
                Optional<Symbol> hashSymbol,
                Optional<Symbol> groupIdSymbol,
                PhysicalOperation source,
                boolean hasDefaultOutput,
                boolean spillEnabled,
                DataSize unspillMemoryLimit,
                LocalExecutionPlanContext context,
                int startOutputChannel,
                ImmutableMap.Builder<Symbol, Integer> outputMappings,
                int expectedGroups,
                Optional<DataSize> maxPartialAggregationMemorySize,
                boolean useSystemMemory,
                Optional<Symbol> finalizeSymbol)
        {
            List<AccumulatorFactory> accumulatorFactories = new ArrayList<>();
            Optional<Integer> groupIdChannel = getOutputMappingAndGroupIdChannel(aggregations, groupBySymbols, hashSymbol, groupIdSymbol,
                    source, startOutputChannel, outputMappings, accumulatorFactories, Optional.of(step), finalizeSymbol);

            List<Integer> groupByChannels = getChannelsForSymbols(groupBySymbols, source.getLayout());
            List<Type> groupByTypes = groupByChannels.stream()
                    .map(entry -> source.getTypes().get(entry))
                    .collect(toImmutableList());

            Optional<Integer> hashChannel = hashSymbol.map(channelGetter(source));
            return new SortAggregationOperator.SortAggregationOperatorFactory(
                    context.getNextOperatorId(),
                    planNodeId,
                    groupByTypes,
                    groupByChannels,
                    ImmutableList.copyOf(globalGroupingSets),
                    step,
                    hasDefaultOutput,
                    accumulatorFactories,
                    hashChannel,
                    groupIdChannel,
                    expectedGroups,
                    maxPartialAggregationMemorySize,
                    spillEnabled,
                    unspillMemoryLimit,
                    spillerFactory,
                    joinCompiler,
                    useSystemMemory,
                    finalizeSymbol.isPresent() ? true : false,
                    createPartialAggregationController(step, session));
        }

        private Optional<Integer> getOutputMappingAndGroupIdChannel(Map<Symbol, Aggregation> aggregations,
                                                                     List<Symbol> groupBySymbols,
                                                                     Optional<Symbol> hashSymbol,
                                                                     Optional<Symbol> groupIdSymbol,
                                                                     PhysicalOperation source,
                                                                     int startOutputChannel,
                                                                     ImmutableMap.Builder<Symbol, Integer> outputMappings,
                                                                     List<AccumulatorFactory> accumulatorFactories,
                                                                     Optional<Step> step,
                                                                     Optional<Symbol> finalizeSymbol)
        {
            List<Symbol> aggregationOutputSymbols = new ArrayList<>();
            for (Map.Entry<Symbol, Aggregation> entry : aggregations.entrySet()) {
                Symbol symbol = entry.getKey();
                Aggregation aggregation = entry.getValue();
                accumulatorFactories.add(buildAccumulatorFactory(source, aggregation));
                aggregationOutputSymbols.add(symbol);
            }

            // add group-by key fields each in a separate channel
            int channel = startOutputChannel;
            Optional<Integer> groupIdChannel = Optional.empty();
            for (Symbol symbol : groupBySymbols) {
                outputMappings.put(symbol, channel);
                if (groupIdSymbol.isPresent() && groupIdSymbol.get().equals(symbol)) {
                    groupIdChannel = Optional.of(channel);
                }
                channel++;
            }

            // hashChannel follows the group by channels
            if (hashSymbol.isPresent()) {
                outputMappings.put(hashSymbol.get(), channel++);
            }

            // aggregations go in following channels
            for (Symbol symbol : aggregationOutputSymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }

            // finalizeValue follows the aggregations by channels
            if (finalizeSymbol.isPresent() && step.get().equals(PARTIAL)) {
                outputMappings.put(finalizeSymbol.get(), channel++);
            }
            return groupIdChannel;
        }
    }

    private static Optional<PartialAggregationController> createPartialAggregationController(AggregationNode.Step step, Session session)
    {
        return step.isOutputPartial() && isAdaptivePartialAggregationEnabled(session) ?
                Optional.of(new PartialAggregationController(
                        getAdaptivePartialAggregationMinRows(session),
                        getAdaptivePartialAggregationUniqueRowsRatioThreshold(session))) :
                Optional.empty();
    }

    private static TableFinisher createTableFinisher(Session session, CacheTableFinishNode node, Metadata metadata)
    {
        WriterTarget target = node.getTarget();
        return (fragments, computedStatistics, tableExecuteContext) -> {
            if (!(target instanceof CreateTarget)) {
                throw new PrestoException(NOT_SUPPORTED, "Result Cache Table is not support for: " + target.getClass().getName());
            }
            return metadata.finishCreateTable(session, ((CreateTarget) target).getHandle(), fragments, computedStatistics);
        };
    }

    private static TableFinisher createTableFinisher(Session session, TableFinishNode node, Metadata metadata)
    {
        WriterTarget target = node.getTarget();
        return (fragments, statistics, tableExecuteContext) -> {
            if (target instanceof CreateTarget) {
                return metadata.finishCreateTable(session, ((CreateTarget) target).getHandle(), fragments, statistics);
            }
            else if (target instanceof InsertTarget) {
                return metadata.finishInsert(session, ((InsertTarget) target).getHandle(), fragments, statistics);
            }
            else if (target instanceof UpdateTarget) {
                metadata.finishUpdate(session, ((UpdateTarget) target).getHandle(), fragments);
                return Optional.empty();
            }
            else if (target instanceof TableWriterNode.UpdateAsInsertTarget) {
                return metadata.finishUpdateAsInsert(session, ((TableWriterNode.UpdateAsInsertTarget) target).getHandle(), fragments, statistics);
            }
            else if (target instanceof VacuumTarget) {
                return metadata.finishVacuum(session, ((VacuumTarget) target).getHandle(), fragments, statistics);
            }
            else if (target instanceof DeleteTarget) {
                metadata.finishDelete(session, ((DeleteTarget) target).getHandle(), fragments);
                return Optional.empty();
            }
            else if (target instanceof DeleteAsInsertTarget) {
                metadata.finishDeleteAsInsert(session, ((DeleteAsInsertTarget) target).getHandle(), fragments, statistics);
                return Optional.empty();
            }
            else if (target instanceof TableWriterNode.TableExecuteTarget) {
                TableExecuteHandle tableExecuteHandle = ((TableWriterNode.TableExecuteTarget) target).getExecuteHandle();
                metadata.finishTableExecute(session, tableExecuteHandle, fragments, tableExecuteContext.getSplitsInfo());
                return Optional.empty();
            }
            else {
                throw new AssertionError("Unhandled target type: " + target.getClass().getName());
            }
        };
    }

    protected static Function<Page, Page> enforceLayoutProcessor(List<Symbol> expectedLayout, Map<Symbol, Integer> inputLayout)
    {
        int[] channels = expectedLayout.stream()
                .peek(symbol -> checkArgument(inputLayout.containsKey(symbol), "channel not found for symbol: %s", symbol))
                .mapToInt(inputLayout::get)
                .toArray();

        if (Arrays.equals(channels, range(0, inputLayout.size()).toArray())) {
            // this is an identity mapping
            return Function.identity();
        }

        return new PageChannelSelector(channels);
    }

    protected static List<Integer> getChannelsForSymbols(List<Symbol> symbols, Map<Symbol, Integer> layout)
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (Symbol symbol : symbols) {
            builder.add(layout.get(symbol));
        }
        return builder.build();
    }

    protected static Function<Symbol, Integer> channelGetter(PhysicalOperation source)
    {
        return input -> {
            checkArgument(source.getLayout().containsKey(input));
            return source.getLayout().get(input);
        };
    }

    /**
     * Encapsulates an physical operator plus the mapping of logical symbols to channel/field
     */
    public static class PhysicalOperation
    {
        private final List<OperatorFactory> operatorFactories;
        private final Map<Symbol, Integer> layout;
        private final List<Type> types;

        private final PipelineExecutionStrategy pipelineExecutionStrategy;

        public PhysicalOperation(OperatorFactory operatorFactory, Map<Symbol, Integer> layout, LocalExecutionPlanContext context, PipelineExecutionStrategy pipelineExecutionStrategy)
        {
            this(operatorFactory, layout, context, Optional.empty(), pipelineExecutionStrategy);
        }

        public PhysicalOperation(OperatorFactory operatorFactory, Map<Symbol, Integer> layout, LocalExecutionPlanContext context, PhysicalOperation source)
        {
            this(operatorFactory, layout, context, Optional.of(requireNonNull(source, "source is null")), source.getPipelineExecutionStrategy());
        }

        private PhysicalOperation(
                OperatorFactory operatorFactory,
                Map<Symbol, Integer> layout,
                LocalExecutionPlanContext context,
                Optional<PhysicalOperation> source,
                PipelineExecutionStrategy pipelineExecutionStrategy)
        {
            requireNonNull(operatorFactory, "operatorFactory is null");
            requireNonNull(layout, "layout is null");
            requireNonNull(context, "context is null");
            requireNonNull(source, "source is null");
            requireNonNull(pipelineExecutionStrategy, "pipelineExecutionStrategy is null");

            this.operatorFactories = ImmutableList.<OperatorFactory>builder()
                    .addAll(source.map(PhysicalOperation::getOperatorFactories).orElse(ImmutableList.of()))
                    .add(operatorFactory)
                    .build();
            this.layout = ImmutableMap.copyOf(layout);
            this.types = toTypes(layout, context);
            this.pipelineExecutionStrategy = pipelineExecutionStrategy;
        }

        private static List<Type> toTypes(Map<Symbol, Integer> layout, LocalExecutionPlanContext context)
        {
            // verify layout covers all values
            int channelCount = layout.values().stream().mapToInt(Integer::intValue).max().orElse(-1) + 1;
            checkArgument(
                    layout.size() == channelCount && ImmutableSet.copyOf(layout.values()).containsAll(ContiguousSet.create(closedOpen(0, channelCount), integers())),
                    "Layout does not have a symbol for every output channel: %s", layout);
            Map<Integer, Symbol> channelLayout = ImmutableBiMap.copyOf(layout).inverse();

            return range(0, channelCount)
                    .mapToObj(channelLayout::get)
                    .map(context.getTypes()::get)
                    .collect(toImmutableList());
        }

        public int symbolToChannel(Symbol input)
        {
            checkArgument(layout.containsKey(input));
            return layout.get(input);
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public Map<Symbol, Integer> getLayout()
        {
            return layout;
        }

        public List<OperatorFactory> getOperatorFactories()
        {
            return operatorFactories;
        }

        public PipelineExecutionStrategy getPipelineExecutionStrategy()
        {
            return pipelineExecutionStrategy;
        }
    }

    protected static class DriverFactoryParameters
    {
        private final LocalExecutionPlanContext subContext;
        private final PhysicalOperation source;

        public DriverFactoryParameters(LocalExecutionPlanContext subContext, PhysicalOperation source)
        {
            this.subContext = subContext;
            this.source = source;
        }

        public LocalExecutionPlanContext getSubContext()
        {
            return subContext;
        }

        public PhysicalOperation getSource()
        {
            return source;
        }
    }
}
