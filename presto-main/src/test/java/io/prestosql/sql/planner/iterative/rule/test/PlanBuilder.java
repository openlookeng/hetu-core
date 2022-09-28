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
package io.prestosql.sql.planner.iterative.rule.test;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import io.prestosql.Session;
import io.prestosql.exchange.RetryPolicy;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.IndexHandle;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.AggregationNode.Aggregation;
import io.prestosql.spi.plan.AggregationNode.Step;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.ExceptNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.IntersectNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.MarkDistinctNode;
import io.prestosql.spi.plan.OrderingScheme;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.TopNNode;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.analyzer.ExpressionAnalyzer;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.OrderingSchemeUtils;
import io.prestosql.sql.planner.Partitioning;
import io.prestosql.sql.planner.PartitioningScheme;
import io.prestosql.sql.planner.SymbolUtils;
import io.prestosql.sql.planner.TestingConnectorIndexHandle;
import io.prestosql.sql.planner.TestingConnectorTransactionHandle;
import io.prestosql.sql.planner.TestingWriterTarget;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.AssignUniqueId;
import io.prestosql.sql.planner.plan.DeleteNode;
import io.prestosql.sql.planner.plan.DistinctLimitNode;
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.IndexJoinNode;
import io.prestosql.sql.planner.plan.IndexSourceNode;
import io.prestosql.sql.planner.plan.LateralJoinNode;
import io.prestosql.sql.planner.plan.OffsetNode;
import io.prestosql.sql.planner.plan.OutputNode;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.sql.planner.plan.RemoteSourceNode;
import io.prestosql.sql.planner.plan.RowNumberNode;
import io.prestosql.sql.planner.plan.SampleNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SortNode;
import io.prestosql.sql.planner.plan.TableFinishNode;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.sql.planner.plan.TableWriterNode.DeleteTarget;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.relational.SqlToRowExpressionTranslator;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.testing.TestingHandle;
import io.prestosql.testing.TestingMetadata.TestingTableHandle;
import io.prestosql.testing.TestingTransactionHandle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.metadata.FunctionAndTypeManager.qualifyObjectName;
import static io.prestosql.spi.function.FunctionKind.SCALAR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.prestosql.sql.relational.Expressions.call;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.util.MoreLists.nElements;
import static java.lang.String.format;
import static java.util.Collections.emptyList;

public class PlanBuilder
{
    private final PlanNodeIdAllocator idAllocator;
    private final Metadata metadata;
    private final Map<Symbol, Type> symbols = new HashMap<>();
    private final Map<String, Type> variables = new HashMap<>();

    public static Assignments assignment(Symbol symbol, Expression expression)
    {
        return Assignments.builder().put(symbol, OriginalExpressionUtils.castToRowExpression(expression)).build();
    }

    public static Assignments assignment(Symbol symbol, RowExpression expression)
    {
        return Assignments.builder().put(symbol, expression).build();
    }

    public static Assignments assignment(Symbol symbol1, Expression expression1, Symbol symbol2, Expression expression2)
    {
        return Assignments.builder().put(symbol1, OriginalExpressionUtils.castToRowExpression(expression1)).put(symbol2, OriginalExpressionUtils.castToRowExpression(expression2)).build();
    }

    public static Assignments assignment(Symbol symbol1, RowExpression expression1, Symbol symbol2, RowExpression expression2)
    {
        return Assignments.builder().put(symbol1, expression1).put(symbol2, expression2).build();
    }

    public PlanBuilder(PlanNodeIdAllocator idAllocator, Metadata metadata)
    {
        this.idAllocator = idAllocator;
        this.metadata = metadata;
    }

    public OutputNode output(List<String> columnNames, List<Symbol> outputs, PlanNode source)
    {
        return new OutputNode(
                idAllocator.getNextId(),
                source,
                columnNames,
                outputs);
    }

    public OutputNode output(Consumer<OutputBuilder> outputBuilderConsumer)
    {
        OutputBuilder outputBuilder = new OutputBuilder();
        outputBuilderConsumer.accept(outputBuilder);
        return outputBuilder.build();
    }

    public class OutputBuilder
    {
        private PlanNode source;
        private List<String> columnNames = new ArrayList<>();
        private List<Symbol> outputs = new ArrayList<>();

        public OutputBuilder source(PlanNode source)
        {
            this.source = source;
            return this;
        }

        public OutputBuilder column(Symbol symbol)
        {
            return column(symbol, symbol.getName());
        }

        public OutputBuilder column(Symbol symbol, String columnName)
        {
            outputs.add(symbol);
            columnNames.add(columnName);
            return this;
        }

        protected OutputNode build()
        {
            return new OutputNode(idAllocator.getNextId(), source, columnNames, outputs);
        }
    }

    public ValuesNode values(Symbol... columns)
    {
        return values(idAllocator.getNextId(), columns);
    }

    public ValuesNode values(PlanNodeId id, Symbol... columns)
    {
        return values(id, 0, columns);
    }

    public ValuesNode values(int rows, Symbol... columns)
    {
        return values(idAllocator.getNextId(), rows, columns);
    }

    public ValuesNode values(PlanNodeId id, int rows, Symbol... columns)
    {
        return values(
                id,
                ImmutableList.copyOf(columns),
                nElements(rows, row -> nElements(columns.length, cell -> OriginalExpressionUtils.castToRowExpression(new NullLiteral()))));
    }

    public ValuesNode values(List<Symbol> columns, List<List<RowExpression>> rows)
    {
        return values(idAllocator.getNextId(), columns, rows);
    }

    public ValuesNode values(PlanNodeId id, List<Symbol> columns, List<List<RowExpression>> rows)
    {
        return new ValuesNode(id, columns, rows);
    }

    public EnforceSingleRowNode enforceSingleRow(PlanNode source)
    {
        return new EnforceSingleRowNode(idAllocator.getNextId(), source);
    }

    public SortNode sort(List<Symbol> orderBy, PlanNode source)
    {
        return new SortNode(
                idAllocator.getNextId(),
                source,
                new OrderingScheme(
                        orderBy,
                        Maps.toMap(orderBy, Functions.constant(SortOrder.ASC_NULLS_FIRST))),
                false);
    }

    public OffsetNode offset(long rowCount, PlanNode source)
    {
        return new OffsetNode(idAllocator.getNextId(), source, rowCount);
    }

    public LimitNode limit(long limit, PlanNode source)
    {
        return limit(limit, ImmutableList.of(), source);
    }

    public LimitNode limit(long limit, List<Symbol> tiesResolvers, PlanNode source)
    {
        Optional<OrderingScheme> tiesResolvingScheme = Optional.empty();
        if (!tiesResolvers.isEmpty()) {
            tiesResolvingScheme = Optional.of(
                    new OrderingScheme(
                            tiesResolvers,
                            Maps.toMap(tiesResolvers, Functions.constant(SortOrder.ASC_NULLS_FIRST))));
        }
        return new LimitNode(
                idAllocator.getNextId(),
                source,
                limit,
                tiesResolvingScheme,
                false);
    }

    public TopNNode topN(long count, List<Symbol> orderBy, PlanNode source)
    {
        return topN(count, orderBy, TopNNode.Step.SINGLE, source);
    }

    public TopNNode topN(long count, List<Symbol> orderBy, TopNNode.Step step, PlanNode source)
    {
        return new TopNNode(
                idAllocator.getNextId(),
                source,
                count,
                new OrderingScheme(
                        orderBy,
                        Maps.toMap(orderBy, Functions.constant(SortOrder.ASC_NULLS_FIRST))),
                step);
    }

    public SampleNode sample(double sampleRatio, SampleNode.Type type, PlanNode source)
    {
        return new SampleNode(idAllocator.getNextId(), source, sampleRatio, type);
    }

    public ProjectNode project(Assignments assignments, PlanNode source)
    {
        return new ProjectNode(idAllocator.getNextId(), source, assignments);
    }

    public MarkDistinctNode markDistinct(Symbol markerSymbol, List<Symbol> distinctSymbols, PlanNode source)
    {
        return new MarkDistinctNode(idAllocator.getNextId(), source, markerSymbol, distinctSymbols, Optional.empty());
    }

    public MarkDistinctNode markDistinct(Symbol markerSymbol, List<Symbol> distinctSymbols, Symbol hashSymbol, PlanNode source)
    {
        return new MarkDistinctNode(idAllocator.getNextId(), source, markerSymbol, distinctSymbols, Optional.of(hashSymbol));
    }

    public FilterNode filter(Expression predicate, PlanNode source)
    {
        return new FilterNode(idAllocator.getNextId(), source, OriginalExpressionUtils.castToRowExpression(predicate));
    }

    public FilterNode filter(RowExpression predicate, PlanNode source)
    {
        return new FilterNode(idAllocator.getNextId(), source, predicate);
    }

    public AggregationNode aggregation(Consumer<AggregationBuilder> aggregationBuilderConsumer)
    {
        AggregationBuilder aggregationBuilder = new AggregationBuilder();
        aggregationBuilderConsumer.accept(aggregationBuilder);
        return aggregationBuilder.build();
    }

    public DistinctLimitNode distinctLimit(long count, List<Symbol> distinctSymbols, PlanNode source)
    {
        return new DistinctLimitNode(
                idAllocator.getNextId(),
                source,
                count,
                false,
                distinctSymbols,
                Optional.empty());
    }

    public CallExpression binaryOperation(OperatorType operatorType, RowExpression left, RowExpression right)
    {
        FunctionHandle functionHandle = metadata.getFunctionAndTypeManager().resolveOperatorFunctionHandle(operatorType, fromTypes(left.getType(), right.getType()));
        return call(operatorType.name(), functionHandle, left.getType(), left, right);
    }

    public static CallExpression comparison(OperatorType operatorType, RowExpression left, RowExpression right)
    {
        Signature signature = Signature.internalOperator(operatorType, BOOLEAN.getTypeSignature(), left.getType().getTypeSignature(), right.getType().getTypeSignature());
        return call(operatorType.name(), new BuiltInFunctionHandle(signature), BOOLEAN, left, right);
    }

    public class AggregationBuilder
    {
        private PlanNode source;
        private Map<Symbol, Aggregation> assignments = new HashMap<>();
        private AggregationNode.GroupingSetDescriptor groupingSets;
        private List<Symbol> preGroupedSymbols = new ArrayList<>();
        private Step step = Step.SINGLE;
        private Optional<Symbol> hashSymbol = Optional.empty();
        private Optional<Symbol> groupIdSymbol = Optional.empty();
        private Session session = testSessionBuilder().build();

        private Optional<PlanNodeId> nodeId = Optional.empty();

        public AggregationBuilder source(PlanNode source)
        {
            this.source = source;
            return this;
        }

        public AggregationBuilder nodeId(PlanNodeId nodeId)
        {
            this.nodeId = Optional.of(nodeId);
            return this;
        }

        public AggregationBuilder addAggregation(Symbol output, Expression expression, List<Type> inputTypes)
        {
            return addAggregation(output, expression, inputTypes, Optional.empty());
        }

        public AggregationBuilder addAggregation(Symbol output, Expression expression, List<Type> inputTypes, Symbol mask)
        {
            return addAggregation(output, expression, inputTypes, Optional.of(mask));
        }

        private AggregationBuilder addAggregation(Symbol output, Expression expression, List<Type> inputTypes, Optional<Symbol> mask)
        {
            checkArgument(expression instanceof FunctionCall);
            FunctionCall aggregation = (FunctionCall) expression;
            FunctionHandle functionHandle = metadata.getFunctionAndTypeManager().resolveFunction(
                    session.getTransactionId(),
                    qualifyObjectName(aggregation.getName()),
                    TypeSignatureProvider.fromTypes(inputTypes));

            return addAggregation(output, new Aggregation(
                    new CallExpression(
                            aggregation.getName().getSuffix(),
                            functionHandle,
                            metadata.getType(metadata.getFunctionAndTypeManager().getFunctionMetadata(functionHandle).getReturnType()),
                            aggregation.getArguments().stream().map(OriginalExpressionUtils::castToRowExpression).collect(toImmutableList())),
                    aggregation.getArguments().stream().map(OriginalExpressionUtils::castToRowExpression).collect(toImmutableList()),
                    aggregation.isDistinct(),
                    aggregation.getFilter().map(SymbolUtils::from),
                    aggregation.getOrderBy().map(OrderingSchemeUtils::fromOrderBy),
                    mask));
        }

        public AggregationBuilder addAggregation(Symbol output, Aggregation aggregation)
        {
            assignments.put(output, aggregation);
            return this;
        }

        public AggregationBuilder addAggregation(Symbol output, RowExpression expression)
        {
            return addAggregation(output, expression, Optional.empty(), Optional.empty(), false, Optional.empty());
        }

        public AggregationBuilder addAggregation(
                Symbol output,
                RowExpression expression,
                Optional<Symbol> filter,
                Optional<OrderingScheme> orderingScheme,
                boolean isDistinct,
                Optional<Symbol> mask)
        {
            checkArgument(expression instanceof CallExpression);
            CallExpression call = (CallExpression) expression;
            return addAggregation(output,
                    new Aggregation(call, call.getArguments(), isDistinct, filter, orderingScheme, mask));
        }

        public AggregationBuilder globalGrouping()
        {
            groupingSets(AggregationNode.singleGroupingSet(ImmutableList.of()));
            return this;
        }

        public AggregationBuilder singleGroupingSet(Symbol... symbols)
        {
            groupingSets(AggregationNode.singleGroupingSet(ImmutableList.copyOf(symbols)));
            return this;
        }

        public AggregationBuilder groupingSets(AggregationNode.GroupingSetDescriptor groupingSets)
        {
            checkState(this.groupingSets == null, "groupingSets already defined");
            this.groupingSets = groupingSets;
            return this;
        }

        public AggregationBuilder preGroupedSymbols(Symbol... symbols)
        {
            checkState(this.preGroupedSymbols.isEmpty(), "preGroupedSymbols already defined");
            this.preGroupedSymbols = ImmutableList.copyOf(symbols);
            return this;
        }

        public AggregationBuilder step(Step step)
        {
            this.step = step;
            return this;
        }

        public AggregationBuilder hashSymbol(Symbol hashSymbol)
        {
            this.hashSymbol = Optional.of(hashSymbol);
            return this;
        }

        public AggregationBuilder groupIdSymbol(Symbol groupIdSymbol)
        {
            this.groupIdSymbol = Optional.of(groupIdSymbol);
            return this;
        }

        protected AggregationNode build()
        {
            checkState(groupingSets != null, "No grouping sets defined; use globalGrouping/groupingKeys method");
            return new AggregationNode(
                    nodeId.orElse(idAllocator.getNextId()),
                    source,
                    assignments,
                    groupingSets,
                    preGroupedSymbols,
                    step,
                    hashSymbol,
                    groupIdSymbol,
                    AggregationNode.AggregationType.HASH,
                    Optional.empty());
        }
    }

    public ApplyNode apply(Assignments subqueryAssignments, List<Symbol> correlation, PlanNode input, PlanNode subquery)
    {
        NullLiteral originSubquery = new NullLiteral(); // does not matter for tests
        return new ApplyNode(idAllocator.getNextId(), input, subquery, subqueryAssignments, correlation, originSubquery);
    }

    public AssignUniqueId assignUniqueId(Symbol unique, PlanNode source)
    {
        return new AssignUniqueId(idAllocator.getNextId(), source, unique);
    }

    public LateralJoinNode lateral(List<Symbol> correlation, PlanNode input, PlanNode subquery)
    {
        return lateral(correlation, input, LateralJoinNode.Type.INNER, TRUE_LITERAL, subquery);
    }

    public LateralJoinNode lateral(List<Symbol> correlation, PlanNode input, LateralJoinNode.Type type, Expression filter, PlanNode subquery)
    {
        NullLiteral originSubquery = new NullLiteral(); // does not matter for tests
        return new LateralJoinNode(idAllocator.getNextId(), input, subquery, correlation, type, filter, originSubquery);
    }

    public TableScanNode tableScan(List<Symbol> symbols, Map<Symbol, ColumnHandle> assignments)
    {
        return tableScan(
                new TableHandle(new CatalogName("testConnector"),
                        new TestingTableHandle(),
                        TestingTransactionHandle.create(),
                        Optional.of(TestingHandle.INSTANCE)),
                symbols,
                assignments);
    }

    public TableScanNode tableScan(
            TableHandle tableHandle,
            List<Symbol> symbols,
            Map<Symbol, ColumnHandle> assignments)
    {
        return tableScan(tableHandle, symbols, assignments, TupleDomain.all());
    }

    public TableScanNode tableScan(
            TableHandle tableHandle,
            List<Symbol> symbols,
            Map<Symbol, ColumnHandle> assignments,
            TupleDomain<ColumnHandle> enforcedConstraint)
    {
        return new TableScanNode(
                idAllocator.getNextId(),
                tableHandle,
                symbols,
                assignments,
                enforcedConstraint,
                Optional.empty(),
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT,
                new UUID(0, 0),
                0,
                false);
    }

    public TableFinishNode tableDelete(SchemaTableName schemaTableName, PlanNode deleteSource, Symbol deleteRowId)
    {
        DeleteTarget deleteTarget = new DeleteTarget(
                new TableHandle(
                        new CatalogName("testConnector"),
                        new TestingTableHandle(),
                        TestingTransactionHandle.create(),
                        Optional.of(TestingHandle.INSTANCE)),
                schemaTableName);
        return new TableFinishNode(
                idAllocator.getNextId(),
                exchange(e -> e
                        .addSource(new DeleteNode(
                                idAllocator.getNextId(),
                                deleteSource,
                                deleteTarget,
                                deleteRowId,
                                ImmutableList.of(deleteRowId)))
                        .addInputsSet(deleteRowId)
                        .singleDistributionPartitioningScheme(deleteRowId)),
                deleteTarget,
                deleteRowId,
                Optional.empty(),
                Optional.empty());
    }

    public ExchangeNode gatheringExchange(ExchangeNode.Scope scope, PlanNode child)
    {
        return exchange(builder -> builder.type(ExchangeNode.Type.GATHER)
                .scope(scope)
                .singleDistributionPartitioningScheme(child.getOutputSymbols())
                .addSource(child)
                .addInputsSet(child.getOutputSymbols()));
    }

    public SemiJoinNode semiJoin(
            Symbol sourceJoinSymbol,
            Symbol filteringSourceJoinSymbol,
            Symbol semiJoinOutput,
            Optional<Symbol> sourceHashSymbol,
            Optional<Symbol> filteringSourceHashSymbol,
            PlanNode source,
            PlanNode filteringSource)
    {
        return semiJoin(
                source,
                filteringSource,
                sourceJoinSymbol,
                filteringSourceJoinSymbol,
                semiJoinOutput,
                sourceHashSymbol,
                filteringSourceHashSymbol,
                Optional.empty(),
                Optional.empty());
    }

    public SemiJoinNode semiJoin(
            PlanNode source,
            PlanNode filteringSource,
            Symbol sourceJoinSymbol,
            Symbol filteringSourceJoinSymbol,
            Symbol semiJoinOutput,
            Optional<Symbol> sourceHashSymbol,
            Optional<Symbol> filteringSourceHashSymbol,
            Optional<SemiJoinNode.DistributionType> distributionType)
    {
        return new SemiJoinNode(
                idAllocator.getNextId(),
                source,
                filteringSource,
                sourceJoinSymbol,
                filteringSourceJoinSymbol,
                semiJoinOutput,
                sourceHashSymbol,
                filteringSourceHashSymbol,
                distributionType,
                Optional.empty());
    }

    public SemiJoinNode semiJoin(
            PlanNode source,
            PlanNode filteringSource,
            Symbol sourceJoinSymbol,
            Symbol filteringSourceJoinSymbol,
            Symbol semiJoinOutput,
            Optional<Symbol> sourceHashSymbol,
            Optional<Symbol> filteringSourceHashSymbol,
            Optional<SemiJoinNode.DistributionType> distributionType,
            Optional<String> dynamicFilterId)
    {
        return new SemiJoinNode(
                idAllocator.getNextId(),
                source,
                filteringSource,
                sourceJoinSymbol,
                filteringSourceJoinSymbol,
                semiJoinOutput,
                sourceHashSymbol,
                filteringSourceHashSymbol,
                distributionType,
                dynamicFilterId);
    }

    public IndexSourceNode indexSource(
            TableHandle tableHandle,
            Set<Symbol> lookupSymbols,
            List<Symbol> outputSymbols,
            Map<Symbol, ColumnHandle> assignments,
            TupleDomain<ColumnHandle> effectiveTupleDomain)
    {
        return new IndexSourceNode(
                idAllocator.getNextId(),
                new IndexHandle(
                        tableHandle.getCatalogName(),
                        TestingConnectorTransactionHandle.INSTANCE,
                        TestingConnectorIndexHandle.INSTANCE),
                tableHandle,
                lookupSymbols,
                outputSymbols,
                assignments,
                effectiveTupleDomain);
    }

    public ExchangeNode exchange(Consumer<ExchangeBuilder> exchangeBuilderConsumer)
    {
        ExchangeBuilder exchangeBuilder = new ExchangeBuilder();
        exchangeBuilderConsumer.accept(exchangeBuilder);
        return exchangeBuilder.build();
    }

    public class ExchangeBuilder
    {
        private ExchangeNode.Type type = ExchangeNode.Type.GATHER;
        private ExchangeNode.Scope scope = ExchangeNode.Scope.REMOTE;
        private PartitioningScheme partitioningScheme;
        private OrderingScheme orderingScheme;
        private List<PlanNode> sources = new ArrayList<>();
        private List<List<Symbol>> inputs = new ArrayList<>();

        public ExchangeBuilder type(ExchangeNode.Type type)
        {
            this.type = type;
            return this;
        }

        public ExchangeBuilder scope(ExchangeNode.Scope scope)
        {
            this.scope = scope;
            return this;
        }

        public ExchangeBuilder singleDistributionPartitioningScheme(Symbol... outputSymbols)
        {
            return singleDistributionPartitioningScheme(Arrays.asList(outputSymbols));
        }

        public ExchangeBuilder singleDistributionPartitioningScheme(List<Symbol> outputSymbols)
        {
            return partitioningScheme(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), outputSymbols));
        }

        public ExchangeBuilder fixedHashDistributionParitioningScheme(List<Symbol> outputSymbols, List<Symbol> partitioningSymbols)
        {
            return partitioningScheme(new PartitioningScheme(Partitioning.create(
                    FIXED_HASH_DISTRIBUTION,
                    ImmutableList.copyOf(partitioningSymbols)),
                    ImmutableList.copyOf(outputSymbols)));
        }

        public ExchangeBuilder fixedHashDistributionParitioningScheme(List<Symbol> outputSymbols, List<Symbol> partitioningSymbols, Symbol hashSymbol)
        {
            return partitioningScheme(new PartitioningScheme(Partitioning.create(
                    FIXED_HASH_DISTRIBUTION,
                    ImmutableList.copyOf(partitioningSymbols)),
                    ImmutableList.copyOf(outputSymbols),
                    Optional.of(hashSymbol)));
        }

        public ExchangeBuilder partitioningScheme(PartitioningScheme partitioningScheme)
        {
            this.partitioningScheme = partitioningScheme;
            return this;
        }

        public ExchangeBuilder addSource(PlanNode source)
        {
            this.sources.add(source);
            return this;
        }

        public ExchangeBuilder addInputsSet(Symbol... inputs)
        {
            return addInputsSet(Arrays.asList(inputs));
        }

        public ExchangeBuilder addInputsSet(List<Symbol> inputs)
        {
            this.inputs.add(inputs);
            return this;
        }

        public ExchangeBuilder orderingScheme(OrderingScheme orderingScheme)
        {
            this.orderingScheme = orderingScheme;
            return this;
        }

        protected ExchangeNode build()
        {
            return new ExchangeNode(idAllocator.getNextId(), type, scope, partitioningScheme, sources, inputs, Optional.ofNullable(orderingScheme), AggregationNode.AggregationType.HASH);
        }
    }

    public JoinNode join(JoinNode.Type joinType, PlanNode left, PlanNode right, JoinNode.EquiJoinClause... criteria)
    {
        return join(joinType, left, right, Optional.empty(), criteria);
    }

    public JoinNode join(JoinNode.Type joinType, PlanNode left, PlanNode right, RowExpression filter, JoinNode.EquiJoinClause... criteria)
    {
        return join(joinType, left, right, Optional.of(filter), criteria);
    }

    private JoinNode join(JoinNode.Type joinType, PlanNode left, PlanNode right, Optional<RowExpression> filter, JoinNode.EquiJoinClause... criteria)
    {
        return join(
                joinType,
                left,
                right,
                ImmutableList.copyOf(criteria),
                ImmutableList.<Symbol>builder()
                        .addAll(left.getOutputSymbols())
                        .addAll(right.getOutputSymbols())
                        .build(),
                filter,
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());
    }

    public JoinNode join(JoinNode.Type type, PlanNode left, PlanNode right, List<JoinNode.EquiJoinClause> criteria, List<Symbol> outputSymbols, Optional<RowExpression> filter)
    {
        return join(type, left, right, criteria, outputSymbols, filter, Optional.empty(), Optional.empty());
    }

    public JoinNode join(
            JoinNode.Type type,
            PlanNode left,
            PlanNode right,
            List<JoinNode.EquiJoinClause> criteria,
            List<Symbol> outputSymbols,
            Optional<RowExpression> filter,
            Optional<Symbol> leftHashSymbol,
            Optional<Symbol> rightHashSymbol)
    {
        return join(type, left, right, criteria, outputSymbols, filter, leftHashSymbol, rightHashSymbol, Optional.empty(), ImmutableMap.of());
    }

    public JoinNode join(
            JoinNode.Type type,
            PlanNode left,
            PlanNode right,
            List<JoinNode.EquiJoinClause> criteria,
            List<Symbol> outputSymbols,
            Optional<RowExpression> filter,
            Optional<Symbol> leftHashSymbol,
            Optional<Symbol> rightHashSymbol,
            Map<String, Symbol> dynamicFilters)
    {
        return join(type, left, right, criteria, outputSymbols, filter, leftHashSymbol, rightHashSymbol, Optional.empty(), dynamicFilters);
    }

    public JoinNode join(
            JoinNode.Type type,
            PlanNode left,
            PlanNode right,
            List<JoinNode.EquiJoinClause> criteria,
            List<Symbol> outputSymbols,
            Optional<RowExpression> filter,
            Optional<Symbol> leftHashSymbol,
            Optional<Symbol> rightHashSymbol,
            Optional<JoinNode.DistributionType> distributionType,
            Map<String, Symbol> dynamicFilters)
    {
        return new JoinNode(idAllocator.getNextId(), type, left, right, criteria, outputSymbols, filter, leftHashSymbol, rightHashSymbol, distributionType, Optional.empty(), dynamicFilters);
    }

    public PlanNode indexJoin(IndexJoinNode.Type type, TableScanNode probe, TableScanNode index)
    {
        return new IndexJoinNode(
                idAllocator.getNextId(),
                type,
                probe,
                index,
                emptyList(),
                Optional.empty(),
                Optional.empty());
    }

    public UnionNode union(ListMultimap<Symbol, Symbol> outputsToInputs, List<PlanNode> sources)
    {
        List<Symbol> outputs = ImmutableList.copyOf(outputsToInputs.keySet());
        return new UnionNode(idAllocator.getNextId(), sources, outputsToInputs, outputs);
    }

    public IntersectNode intersect(ListMultimap<Symbol, Symbol> outputsToInputs, List<PlanNode> sources)
    {
        List<Symbol> outputs = ImmutableList.copyOf(outputsToInputs.keySet());
        return new IntersectNode(idAllocator.getNextId(), sources, outputsToInputs, outputs);
    }

    public ExceptNode except(ListMultimap<Symbol, Symbol> outputsToInputs, List<PlanNode> sources)
    {
        List<Symbol> outputs = ImmutableList.copyOf(outputsToInputs.keySet());
        return new ExceptNode(idAllocator.getNextId(), sources, outputsToInputs, outputs);
    }

    public TableWriterNode tableWriter(List<Symbol> columns, List<String> columnNames, PlanNode source)
    {
        return new TableWriterNode(
                idAllocator.getNextId(),
                source,
                new TestingWriterTarget(),
                symbol("partialrows", BIGINT),
                symbol("fragment", VARBINARY),
                columns,
                columnNames,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public Symbol symbol(String name)
    {
        return symbol(name, BIGINT);
    }

    public Symbol symbol(String name, Type type)
    {
        Symbol symbol = new Symbol(name);

        Type old = symbols.put(symbol, type);
        if (old != null && !old.equals(type)) {
            throw new IllegalArgumentException(format("Symbol '%s' already registered with type '%s'", name, old));
        }

        if (old == null) {
            symbols.put(symbol, type);
        }

        return symbol;
    }

    public VariableReferenceExpression variable(String name)
    {
        return variable(name, BIGINT);
    }

    public VariableReferenceExpression variable(VariableReferenceExpression variable)
    {
        return variable(variable.getName(), variable.getType());
    }

    public VariableReferenceExpression variable(String name, Type type)
    {
        Type old = variables.put(name, type);
        if (old != null && !old.equals(type)) {
            throw new IllegalArgumentException(format("Variable '%s' already registered with type '%s'", name, old));
        }

        if (old == null) {
            variables.put(name, type);
        }
        return new VariableReferenceExpression(name, type);
    }

    public WindowNode window(WindowNode.Specification specification, Map<Symbol, WindowNode.Function> functions, PlanNode source)
    {
        return new WindowNode(
                idAllocator.getNextId(),
                source,
                specification,
                ImmutableMap.copyOf(functions),
                Optional.empty(),
                ImmutableSet.of(),
                0);
    }

    public WindowNode window(WindowNode.Specification specification, Map<Symbol, WindowNode.Function> functions, Symbol hashSymbol, PlanNode source)
    {
        return new WindowNode(
                idAllocator.getNextId(),
                source,
                specification,
                ImmutableMap.copyOf(functions),
                Optional.of(hashSymbol),
                ImmutableSet.of(),
                0);
    }

    public RowNumberNode rowNumber(List<Symbol> partitionBy, Optional<Integer> maxRowCountPerPartition, Symbol rowNumberSymbol, PlanNode source)
    {
        return new RowNumberNode(
                idAllocator.getNextId(),
                source,
                partitionBy,
                rowNumberSymbol,
                maxRowCountPerPartition,
                Optional.empty());
    }

    public RemoteSourceNode remoteSourceNode(List<PlanFragmentId> fragmentIds, List<Symbol> symbols, ExchangeNode.Type exchangeType)
    {
        return new RemoteSourceNode(idAllocator.getNextId(), fragmentIds, symbols, Optional.empty(), exchangeType, RetryPolicy.NONE);
    }

    public RowExpression rowExpression(String sql)
    {
        Expression expression = expression(sql);
        Map<NodeRef<Expression>, Type> expressionTypes = ExpressionAnalyzer.analyzeExpressions(
                TEST_SESSION,
                metadata,
                new SqlParser(),
                getTypes(),
                ImmutableList.of(expression),
                ImmutableList.of(),
                WarningCollector.NOOP,
                false
        ).getExpressionTypes();
        return SqlToRowExpressionTranslator.translate(
                expression,
                SCALAR,
                expressionTypes,
                ImmutableMap.of(),
                metadata.getFunctionAndTypeManager(),
                TEST_SESSION,
                false);
    }

    public static RowExpression castToRowExpression(String sql)
    {
        return OriginalExpressionUtils.castToRowExpression(ExpressionUtils.rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(sql)));
    }

    public static Expression expression(String sql)
    {
        return ExpressionUtils.rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(sql));
    }

    public static List<Expression> expressions(String... expressions)
    {
        return Stream.of(expressions)
                .map(PlanBuilder::expression)
                .collect(toImmutableList());
    }

    public static List<RowExpression> originalExpressions(String... expressions)
    {
        return Stream.of(expressions)
                .map(PlanBuilder::expression)
                .map(OriginalExpressionUtils::castToRowExpression)
                .collect(toImmutableList());
    }

    public static List<RowExpression> constantExpressions(Type type, Object... values)
    {
        return Stream.of(values)
                .map(value -> constant(value, type))
                .collect(toImmutableList());
    }

    public TypeProvider getTypes()
    {
        return TypeProvider.copyOf(symbols);
    }

    public PlanNodeIdAllocator getIdAllocator()
    {
        return idAllocator;
    }
}
