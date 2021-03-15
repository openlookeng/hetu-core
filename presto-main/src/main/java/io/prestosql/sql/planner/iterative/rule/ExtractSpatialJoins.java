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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.prestosql.Session;
import io.prestosql.execution.Lifespan;
import io.prestosql.geospatial.KdbTree;
import io.prestosql.geospatial.KdbTreeUtils;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.CastType;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Split;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.FunctionMetadata;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.split.PageSourceManager;
import io.prestosql.split.SplitManager;
import io.prestosql.split.SplitSource;
import io.prestosql.split.SplitSource.SplitBatch;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.iterative.Rule.Context;
import io.prestosql.sql.planner.iterative.Rule.Result;
import io.prestosql.sql.planner.plan.SpatialJoinNode;
import io.prestosql.sql.planner.plan.UnnestNode;
import io.prestosql.sql.relational.Expressions;
import io.prestosql.util.SpatialJoinUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.SystemSessionProperties.getSpatialPartitioningTableName;
import static io.prestosql.SystemSessionProperties.isSpatialJoinEnabled;
import static io.prestosql.expressions.RowExpressionNodeInliner.replaceExpression;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.spi.StandardErrorCode.INVALID_SPATIAL_PARTITIONING;
import static io.prestosql.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.prestosql.spi.plan.JoinNode.Type.INNER;
import static io.prestosql.spi.plan.JoinNode.Type.LEFT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.planner.SymbolsExtractor.extractUnique;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.join;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.util.SpatialJoinUtils.extractSupportedSpatialComparisons;
import static io.prestosql.util.SpatialJoinUtils.extractSupportedSpatialFunctions;
import static io.prestosql.util.SpatialJoinUtils.getFlippedFunctionHandle;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Applies to broadcast spatial joins, inner and left, expressed via ST_Contains,
 * ST_Intersects and ST_Distance functions.
 * <p>
 * For example:
 * <ul>
 * <li>SELECT ... FROM a, b WHERE ST_Contains(b.geometry, a.geometry)</li>
 * <li>SELECT ... FROM a, b WHERE ST_Intersects(b.geometry, a.geometry)</li>
 * <li>SELECT ... FROM a, b WHERE ST_Distance(b.geometry, a.geometry) <= 300</li>
 * <li>SELECT ... FROM a, b WHERE 15.5 > ST_Distance(b.geometry, a.geometry)</li>
 * </ul>
 * <p>
 * Joins expressed via ST_Contains and ST_Intersects functions must match all of
 * the following criteria:
 * <p>
 * - arguments of the spatial function are non-scalar expressions;
 * - one of the arguments uses symbols from left side of the join, the other from right.
 * <p>
 * Joins expressed via ST_Distance function must use less than or less than or equals operator
 * to compare ST_Distance value with a radius and must match all of the following criteria:
 * <p>
 * - arguments of the spatial function are non-scalar expressions;
 * - one of the arguments uses symbols from left side of the join, the other from right;
 * - radius is either scalar expression or uses symbols only from the right (build) side of the join.
 * <p>
 * For inner join, replaces cross join node and a qualifying filter on top with a single
 * spatial join node.
 * <p>
 * For both inner and left joins, pushes non-trivial expressions of the spatial function
 * arguments and radius into projections on top of join child nodes.
 * <p>
 * Examples:
 * <pre>
 * Point-in-polygon inner join
 *      ST_Contains(ST_GeometryFromText(a.wkt), ST_Point(b.longitude, b.latitude))
 * becomes a spatial join
 *      ST_Contains(st_geometryfromtext, st_point)
 * with st_geometryfromtext -> 'ST_GeometryFromText(a.wkt)' and
 * st_point -> 'ST_Point(b.longitude, b.latitude)' projections on top of child nodes.
 *
 * Distance query
 *      ST_Distance(ST_Point(a.lon, a.lat), ST_Point(b.lon, b.lat)) <= 10 / (111.321 * cos(radians(b.lat)))
 * becomes a spatial join
 *      ST_Distance(st_point_a, st_point_b) <= radius
 * with st_point_a -> 'ST_Point(a.lon, a.lat)', st_point_b -> 'ST_Point(b.lon, b.lat)'
 * and radius -> '10 / (111.321 * cos(radians(b.lat)))' projections on top of child nodes.
 * </pre>
 */
public class ExtractSpatialJoins
{
    private static final TypeSignature SPHERICAL_GEOGRAPHY_TYPE_SIGNATURE = parseTypeSignature("SphericalGeography");
    private static final String KDB_TREE_TYPENAME = "KdbTree";

    private final Metadata metadata;
    private final SplitManager splitManager;
    private final PageSourceManager pageSourceManager;
    private final TypeAnalyzer typeAnalyzer;

    public ExtractSpatialJoins(Metadata metadata, SplitManager splitManager, PageSourceManager pageSourceManager, TypeAnalyzer typeAnalyzer)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceManager = requireNonNull(pageSourceManager, "pageSourceManager is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                new ExtractSpatialInnerJoin(metadata, splitManager, pageSourceManager, typeAnalyzer),
                new ExtractSpatialLeftJoin(metadata, splitManager, pageSourceManager, typeAnalyzer));
    }

    @VisibleForTesting
    public static final class ExtractSpatialInnerJoin
            implements Rule<FilterNode>
    {
        private static final Capture<JoinNode> JOIN = newCapture();
        private static final Pattern<FilterNode> PATTERN = filter()
                .with(source().matching(join().capturedAs(JOIN).matching(JoinNode::isCrossJoin)));

        private final Metadata metadata;
        private final SplitManager splitManager;
        private final PageSourceManager pageSourceManager;
        private final TypeAnalyzer typeAnalyzer;

        public ExtractSpatialInnerJoin(Metadata metadata, SplitManager splitManager, PageSourceManager pageSourceManager, TypeAnalyzer typeAnalyzer)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.splitManager = requireNonNull(splitManager, "splitManager is null");
            this.pageSourceManager = requireNonNull(pageSourceManager, "pageSourceManager is null");
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return isSpatialJoinEnabled(session);
        }

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(FilterNode node, Captures captures, Context context)
        {
            JoinNode joinNode = captures.get(JOIN);
            RowExpression filter = node.getPredicate();
            List<CallExpression> spatialFunctions = extractSupportedSpatialFunctions(filter, metadata.getFunctionAndTypeManager());
            for (CallExpression spatialFunction : spatialFunctions) {
                Result result = tryCreateSpatialJoin(context, joinNode, filter, node.getId(), node.getOutputSymbols(), spatialFunction, Optional.empty(), metadata, splitManager, pageSourceManager, typeAnalyzer);
                if (!result.isEmpty()) {
                    return result;
                }
            }

            List<CallExpression> spatialComparisons = extractSupportedSpatialComparisons(filter, metadata.getFunctionAndTypeManager());
            for (CallExpression spatialComparison : spatialComparisons) {
                Result result = tryCreateSpatialJoin(context, joinNode, filter, node.getId(), node.getOutputSymbols(), spatialComparison, metadata, splitManager, pageSourceManager, typeAnalyzer);
                if (!result.isEmpty()) {
                    return result;
                }
            }

            return Result.empty();
        }
    }

    @VisibleForTesting
    public static final class ExtractSpatialLeftJoin
            implements Rule<JoinNode>
    {
        private static final Pattern<JoinNode> PATTERN = join().matching(node -> node.getCriteria().isEmpty() && node.getFilter().isPresent() && node.getType() == LEFT);

        private final Metadata metadata;
        private final SplitManager splitManager;
        private final PageSourceManager pageSourceManager;
        private final TypeAnalyzer typeAnalyzer;

        public ExtractSpatialLeftJoin(Metadata metadata, SplitManager splitManager, PageSourceManager pageSourceManager, TypeAnalyzer typeAnalyzer)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.splitManager = requireNonNull(splitManager, "splitManager is null");
            this.pageSourceManager = requireNonNull(pageSourceManager, "pageSourceManager is null");
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return isSpatialJoinEnabled(session);
        }

        @Override
        public Pattern<JoinNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(JoinNode joinNode, Captures captures, Context context)
        {
            checkArgument(joinNode.getFilter().isPresent());
            RowExpression filter = joinNode.getFilter().get();
            List<CallExpression> spatialFunctions = extractSupportedSpatialFunctions(filter, metadata.getFunctionAndTypeManager());
            for (CallExpression spatialFunction : spatialFunctions) {
                Result result = tryCreateSpatialJoin(context, joinNode, filter, joinNode.getId(), joinNode.getOutputSymbols(), spatialFunction, Optional.empty(), metadata, splitManager, pageSourceManager, typeAnalyzer);
                if (!result.isEmpty()) {
                    return result;
                }
            }

            List<CallExpression> spatialComparisons = extractSupportedSpatialComparisons(filter, metadata.getFunctionAndTypeManager());
            for (CallExpression spatialComparison : spatialComparisons) {
                Result result = tryCreateSpatialJoin(context, joinNode, filter, joinNode.getId(), joinNode.getOutputSymbols(), spatialComparison, metadata, splitManager, pageSourceManager, typeAnalyzer);
                if (!result.isEmpty()) {
                    return result;
                }
            }

            return Result.empty();
        }
    }

    private static Result tryCreateSpatialJoin(
            Context context,
            JoinNode joinNode,
            RowExpression filter,
            PlanNodeId nodeId,
            List<Symbol> outputSymbols,
            CallExpression spatialComparison,
            Metadata metadata,
            SplitManager splitManager,
            PageSourceManager pageSourceManager,
            TypeAnalyzer typeAnalyzer)
    {
        FunctionMetadata spatialComparisonMetadata = metadata.getFunctionAndTypeManager().getFunctionMetadata(spatialComparison.getFunctionHandle());
        checkArgument(spatialComparison.getArguments().size() == 2 && spatialComparisonMetadata.getOperatorType().isPresent());
        PlanNode leftNode = joinNode.getLeft();
        PlanNode rightNode = joinNode.getRight();

        List<Symbol> leftSymbols = leftNode.getOutputSymbols();
        List<Symbol> rightSymbols = rightNode.getOutputSymbols();

        RowExpression radius;
        Optional<Symbol> newRadiusSymbol;
        CallExpression newComparison;
        OperatorType operatorType = spatialComparisonMetadata.getOperatorType().get();
        if (operatorType.equals(OperatorType.LESS_THAN) || operatorType.equals(OperatorType.LESS_THAN_OR_EQUAL)) {
            // ST_Distance(a, b) <= r
            radius = spatialComparison.getArguments().get(1);
            Set<Symbol> radiusSymbols = extractUnique(radius);
            if (radiusSymbols.isEmpty() || (rightSymbols.containsAll(radiusSymbols) && containsNone(leftSymbols, radiusSymbols))) {
                newRadiusSymbol = newRadiusSymbol(context, radius);
                newComparison = new CallExpression(
                        spatialComparison.getDisplayName(),
                        spatialComparison.getFunctionHandle(),
                        spatialComparison.getType(),
                        ImmutableList.of(spatialComparison.getArguments().get(0), mapToExpression(newRadiusSymbol, radius, context)),
                        Optional.empty());
            }
            else {
                return Result.empty();
            }
        }
        else {
            // r >= ST_Distance(a, b)
            radius = spatialComparison.getArguments().get(0);
            Set<Symbol> radiusSymbols = extractUnique(radius);
            if (radiusSymbols.isEmpty() || (rightSymbols.containsAll(radiusSymbols) && containsNone(leftSymbols, radiusSymbols))) {
                OperatorType newOperatorType = SpatialJoinUtils.flip(operatorType);
                FunctionHandle flippedHandle = getFlippedFunctionHandle(spatialComparison, metadata.getFunctionAndTypeManager());
                newRadiusSymbol = newRadiusSymbol(context, radius);
                newComparison = new CallExpression(
                        newOperatorType.name(),
                        flippedHandle,
                        spatialComparison.getType(),
                        ImmutableList.of(spatialComparison.getArguments().get(1), mapToExpression(newRadiusSymbol, radius, context)), Optional.empty());
            }
            else {
                return Result.empty();
            }
        }

        RowExpression newFilter = replaceExpression(filter, ImmutableMap.of(spatialComparison, newComparison));
        PlanNode newRightNode = newRadiusSymbol.map(symbol -> addProjection(context, rightNode, symbol, radius)).orElse(rightNode);

        JoinNode newJoinNode = new JoinNode(
                joinNode.getId(),
                joinNode.getType(),
                leftNode,
                newRightNode,
                joinNode.getCriteria(),
                joinNode.getOutputSymbols(),
                Optional.of(newFilter),
                joinNode.getLeftHashSymbol(),
                joinNode.getRightHashSymbol(),
                joinNode.getDistributionType(),
                joinNode.isSpillable(),
                joinNode.getDynamicFilters());

        return tryCreateSpatialJoin(context, newJoinNode, newFilter, nodeId, outputSymbols, (CallExpression) newComparison.getArguments().get(0), Optional.of(newComparison.getArguments().get(1)), metadata, splitManager, pageSourceManager, typeAnalyzer);
    }

    private static Result tryCreateSpatialJoin(
            Context context,
            JoinNode joinNode,
            RowExpression filter,
            PlanNodeId nodeId,
            List<Symbol> outputSymbols,
            CallExpression spatialFunction,
            Optional<RowExpression> radius,
            Metadata metadata,
            SplitManager splitManager,
            PageSourceManager pageSourceManager,
            TypeAnalyzer typeAnalyzer)
    {
        // TODO Add support for distributed left spatial joins
        Optional<String> spatialPartitioningTableName = joinNode.getType() == INNER ? getSpatialPartitioningTableName(context.getSession()) : Optional.empty();
        Optional<KdbTree> kdbTree = spatialPartitioningTableName.map(tableName -> loadKdbTree(tableName, context.getSession(), metadata, splitManager, pageSourceManager, nodeId));

        List<RowExpression> arguments = spatialFunction.getArguments();
        verify(arguments.size() == 2);

        RowExpression firstArgument = arguments.get(0);
        RowExpression secondArgument = arguments.get(1);

        Type sphericalGeographyType = metadata.getType(SPHERICAL_GEOGRAPHY_TYPE_SIGNATURE);
        if (firstArgument.getType().equals(sphericalGeographyType) || secondArgument.getType().equals(sphericalGeographyType)) {
            if (joinNode.getType() != INNER) {
                return Result.empty();
            }
        }

        Set<Symbol> firstSymbols = extractUnique(firstArgument);
        Set<Symbol> secondSymbols = extractUnique(secondArgument);

        if (firstSymbols.isEmpty() || secondSymbols.isEmpty()) {
            return Result.empty();
        }

        Optional<Symbol> newFirstSymbol = newGeometrySymbol(context, firstArgument);
        Optional<Symbol> newSecondSymbol = newGeometrySymbol(context, secondArgument);

        PlanNode leftNode = joinNode.getLeft();
        PlanNode rightNode = joinNode.getRight();

        PlanNode newLeftNode;
        PlanNode newRightNode;

        // Check if the order of arguments of the spatial function matches the order of join sides
        int alignment = checkAlignment(joinNode, firstSymbols, secondSymbols);
        if (alignment > 0) {
            newLeftNode = newFirstSymbol.map(symbol -> addProjection(context, leftNode, symbol, firstArgument)).orElse(leftNode);
            newRightNode = newSecondSymbol.map(symbol -> addProjection(context, rightNode, symbol, secondArgument)).orElse(rightNode);
        }
        else if (alignment < 0) {
            newLeftNode = newSecondSymbol.map(symbol -> addProjection(context, leftNode, symbol, secondArgument)).orElse(leftNode);
            newRightNode = newFirstSymbol.map(symbol -> addProjection(context, rightNode, symbol, firstArgument)).orElse(rightNode);
        }
        else {
            return Result.empty();
        }

        RowExpression newFirstArgument = mapToExpression(newFirstSymbol, firstArgument, context);
        RowExpression newSecondArgument = mapToExpression(newSecondSymbol, secondArgument, context);

        Optional<Symbol> leftPartitionSymbol = Optional.empty();
        Optional<Symbol> rightPartitionSymbol = Optional.empty();
        if (kdbTree.isPresent()) {
            leftPartitionSymbol = Optional.of(context.getSymbolAllocator().newSymbol("pid", INTEGER));
            rightPartitionSymbol = Optional.of(context.getSymbolAllocator().newSymbol("pid", INTEGER));

            if (alignment > 0) {
                newLeftNode = addPartitioningNodes(metadata, context, newLeftNode, leftPartitionSymbol.get(), kdbTree.get(), newFirstArgument, Optional.empty());
                newRightNode = addPartitioningNodes(metadata, context, newRightNode, rightPartitionSymbol.get(), kdbTree.get(), newSecondArgument, radius);
            }
            else {
                newLeftNode = addPartitioningNodes(metadata, context, newLeftNode, leftPartitionSymbol.get(), kdbTree.get(), newSecondArgument, Optional.empty());
                newRightNode = addPartitioningNodes(metadata, context, newRightNode, rightPartitionSymbol.get(), kdbTree.get(), newFirstArgument, radius);
            }
        }

        CallExpression newSpatialFunction = new CallExpression(
                spatialFunction.getDisplayName(),
                spatialFunction.getFunctionHandle(),
                spatialFunction.getType(),
                ImmutableList.of(newFirstArgument, newSecondArgument),
                Optional.empty());

        RowExpression newFilter = replaceExpression(filter, ImmutableMap.of(spatialFunction, newSpatialFunction));

        return Result.ofPlanNode(new SpatialJoinNode(
                nodeId,
                SpatialJoinNode.Type.fromJoinNodeType(joinNode.getType()),
                newLeftNode,
                newRightNode,
                outputSymbols,
                newFilter,
                leftPartitionSymbol,
                rightPartitionSymbol,
                kdbTree.map(KdbTreeUtils::toJson)));
    }

    private static KdbTree loadKdbTree(String tableName, Session session, Metadata metadata, SplitManager splitManager, PageSourceManager pageSourceManager, PlanNodeId nodeId)
    {
        QualifiedObjectName name = toQualifiedObjectName(tableName, session.getCatalog().get(), session.getSchema().get());
        TableHandle tableHandle = metadata.getTableHandle(session, name)
                .orElseThrow(() -> new PrestoException(INVALID_SPATIAL_PARTITIONING, format("Table not found: %s", name)));
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        List<ColumnHandle> visibleColumnHandles = columnHandles.values().stream()
                .filter(handle -> !metadata.getColumnMetadata(session, tableHandle, handle).isHidden())
                .collect(toImmutableList());
        checkSpatialPartitioningTable(visibleColumnHandles.size() == 1, "Expected single column for table %s, but found %s columns", name, columnHandles.size());

        ColumnHandle kdbTreeColumn = Iterables.getOnlyElement(visibleColumnHandles);

        Optional<KdbTree> kdbTree = Optional.empty();
        try (SplitSource splitSource = splitManager.getSplits(session, tableHandle, UNGROUPED_SCHEDULING, null, Optional.empty(), Collections.emptyMap(), ImmutableSet.of(), false, nodeId)) {
            while (!Thread.currentThread().isInterrupted()) {
                SplitBatch splitBatch = getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), 1000));
                List<Split> splits = splitBatch.getSplits();

                for (Split split : splits) {
                    try (ConnectorPageSource pageSource = pageSourceManager.createPageSource(session, split, tableHandle, ImmutableList.of(kdbTreeColumn), Optional.empty())) {
                        do {
                            getFutureValue(pageSource.isBlocked());
                            Page page = pageSource.getNextPage();
                            if (page != null && page.getPositionCount() > 0) {
                                checkSpatialPartitioningTable(!kdbTree.isPresent(), "Expected exactly one row for table %s, but found more", name);
                                checkSpatialPartitioningTable(page.getPositionCount() == 1, "Expected exactly one row for table %s, but found %s rows", name, page.getPositionCount());
                                String kdbTreeJson = VARCHAR.getSlice(page.getBlock(0), 0).toStringUtf8();
                                try {
                                    kdbTree = Optional.of(KdbTreeUtils.fromJson(kdbTreeJson));
                                }
                                catch (IllegalArgumentException e) {
                                    checkSpatialPartitioningTable(false, "Invalid JSON string for KDB tree: %s", e.getMessage());
                                }
                            }
                        }
                        while (!pageSource.isFinished());
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }

                if (splitBatch.isLastBatch()) {
                    break;
                }
            }
        }

        checkSpatialPartitioningTable(kdbTree.isPresent(), "Expected exactly one row for table %s, but got none", name);
        return kdbTree.get();
    }

    private static void checkSpatialPartitioningTable(boolean condition, String message, Object... arguments)
    {
        if (!condition) {
            throw new PrestoException(INVALID_SPATIAL_PARTITIONING, format(message, arguments));
        }
    }

    private static QualifiedObjectName toQualifiedObjectName(String name, String catalog, String schema)
    {
        ImmutableList<String> ids = ImmutableList.copyOf(Splitter.on('.').split(name));
        if (ids.size() == 3) {
            return new QualifiedObjectName(ids.get(0), ids.get(1), ids.get(2));
        }

        if (ids.size() == 2) {
            return new QualifiedObjectName(catalog, ids.get(0), ids.get(1));
        }

        if (ids.size() == 1) {
            return new QualifiedObjectName(catalog, schema, ids.get(0));
        }

        throw new PrestoException(INVALID_SPATIAL_PARTITIONING, format("Invalid name: %s", name));
    }

    private static int checkAlignment(JoinNode joinNode, Set<Symbol> maybeLeftSymbols, Set<Symbol> maybeRightSymbols)
    {
        List<Symbol> leftSymbols = joinNode.getLeft().getOutputSymbols();
        List<Symbol> rightSymbols = joinNode.getRight().getOutputSymbols();

        if (leftSymbols.containsAll(maybeLeftSymbols)
                && containsNone(leftSymbols, maybeRightSymbols)
                && rightSymbols.containsAll(maybeRightSymbols)
                && containsNone(rightSymbols, maybeLeftSymbols)) {
            return 1;
        }

        if (leftSymbols.containsAll(maybeRightSymbols)
                && containsNone(leftSymbols, maybeLeftSymbols)
                && rightSymbols.containsAll(maybeLeftSymbols)
                && containsNone(rightSymbols, maybeRightSymbols)) {
            return -1;
        }

        return 0;
    }

    private static RowExpression mapToExpression(Optional<Symbol> optionalSymbol, RowExpression defaultExpression, Context context)
    {
        return optionalSymbol.map(symbol -> (RowExpression) new VariableReferenceExpression(symbol.getName(), context.getSymbolAllocator().getTypes().get(symbol))).orElse(defaultExpression);
    }

    private static Optional<Symbol> newGeometrySymbol(Context context, RowExpression expression)
    {
        if (expression instanceof VariableReferenceExpression) {
            return Optional.empty();
        }

        return Optional.of(context.getSymbolAllocator().newSymbol(expression));
    }

    private static Optional<Symbol> newRadiusSymbol(Context context, RowExpression expression)
    {
        if (expression instanceof VariableReferenceExpression) {
            return Optional.empty();
        }

        return Optional.of(context.getSymbolAllocator().newSymbol(expression));
    }

    private static PlanNode addProjection(Context context, PlanNode node, Symbol symbol, RowExpression expression)
    {
        Assignments.Builder projections = Assignments.builder();
        TypeProvider typeProvider = context.getSymbolAllocator().getTypes();
        for (Symbol outputSymbol : node.getOutputSymbols()) {
            projections.put(outputSymbol, new VariableReferenceExpression(outputSymbol.getName(), typeProvider.get(outputSymbol)));
        }

        projections.put(symbol, expression);
        return new ProjectNode(context.getIdAllocator().getNextId(), node, projections.build());
    }

    private static PlanNode addPartitioningNodes(Metadata metadata, Context context, PlanNode node, Symbol partitionSymbol, KdbTree kdbTree, RowExpression geometry, Optional<RowExpression> radius)
    {
        Assignments.Builder projections = Assignments.builder();
        TypeProvider typeProvider = context.getSymbolAllocator().getTypes();
        for (Symbol outputSymbol : node.getOutputSymbols()) {
            projections.put(outputSymbol, new VariableReferenceExpression(outputSymbol.getName(), typeProvider.get(outputSymbol)));
        }

        ConstantExpression kdbConstant = Expressions.constant(utf8Slice(KdbTreeUtils.toJson(kdbTree)), VARCHAR);
        FunctionHandle castFunctionHandle = metadata.getFunctionAndTypeManager().lookupCast(CastType.CAST, VARCHAR.getTypeSignature(), parseTypeSignature(KDB_TREE_TYPENAME));
        ImmutableList.Builder partitioningArgumentsBuilder = ImmutableList.builder()
                .add(new CallExpression(CastType.CAST.name(), castFunctionHandle, metadata.getType(parseTypeSignature(KDB_TREE_TYPENAME)), ImmutableList.of(kdbConstant), Optional.empty()))
                .add(geometry);

        radius.map(partitioningArgumentsBuilder::add);
        List<RowExpression> partitioningArguments = partitioningArgumentsBuilder.build();

        String spatialPartitionsFunctionName = "spatial_partitions";

        FunctionHandle functionHandle = metadata.getFunctionAndTypeManager().lookupFunction(spatialPartitionsFunctionName,
                fromTypes(partitioningArguments.stream().map(RowExpression::getType).collect(toImmutableList())));
        CallExpression partitioningFunction = new CallExpression(
                spatialPartitionsFunctionName,
                functionHandle,
                new ArrayType(INTEGER), partitioningArguments,
                Optional.empty());

        Symbol partitionsSymbol = context.getSymbolAllocator().newSymbol(partitioningFunction);
        projections.put(partitionsSymbol, partitioningFunction);

        return new UnnestNode(
                context.getIdAllocator().getNextId(),
                new ProjectNode(context.getIdAllocator().getNextId(), node, projections.build()),
                node.getOutputSymbols(),
                ImmutableMap.of(partitionsSymbol, ImmutableList.of(partitionSymbol)),
                Optional.empty());
    }

    private static boolean containsNone(Collection<Symbol> values, Collection<Symbol> testValues)
    {
        return values.stream().noneMatch(ImmutableSet.copyOf(testValues)::contains);
    }
}
