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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.hetu.core.spi.cube.CubeMetadata;
import io.hetu.core.spi.cube.CubeStatus;
import io.prestosql.Session;
import io.prestosql.cost.CachingCostProvider;
import io.prestosql.cost.CachingStatsProvider;
import io.prestosql.cost.CostCalculator;
import io.prestosql.cost.CostProvider;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.cost.StatsCalculator;
import io.prestosql.cost.StatsProvider;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.NewTableLayout;
import io.prestosql.metadata.TableMetadata;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.cube.CubeUpdateMetadata;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.statistics.TableStatisticsMetadata;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.ExpressionFormatter;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.analyzer.Analysis;
import io.prestosql.sql.analyzer.Field;
import io.prestosql.sql.analyzer.RelationId;
import io.prestosql.sql.analyzer.RelationType;
import io.prestosql.sql.analyzer.Scope;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.StatisticsAggregationPlanner.TableStatisticAggregation;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import io.prestosql.sql.planner.plan.CubeFinishNode;
import io.prestosql.sql.planner.plan.DeleteNode;
import io.prestosql.sql.planner.plan.ExplainAnalyzeNode;
import io.prestosql.sql.planner.plan.OutputNode;
import io.prestosql.sql.planner.plan.StatisticAggregations;
import io.prestosql.sql.planner.plan.StatisticAggregationsDescriptor;
import io.prestosql.sql.planner.plan.StatisticsWriterNode;
import io.prestosql.sql.planner.plan.TableFinishNode;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.sql.planner.plan.TableWriterNode.VacuumTargetReference;
import io.prestosql.sql.planner.plan.VacuumTableNode;
import io.prestosql.sql.planner.sanity.PlanSanityChecker;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.tree.Analyze;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.CreateTableAsSelect;
import io.prestosql.sql.tree.Delete;
import io.prestosql.sql.tree.Explain;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.IfExpression;
import io.prestosql.sql.tree.Insert;
import io.prestosql.sql.tree.InsertCube;
import io.prestosql.sql.tree.LambdaArgumentDeclaration;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QuerySpecification;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.Update;
import io.prestosql.sql.tree.VacuumTable;
import io.prestosql.type.TypeCoercion;
import io.prestosql.utils.OptimizerUtils;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Streams.zip;
import static io.prestosql.metadata.MetadataUtil.toSchemaTableName;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.StandardErrorCode.QUERY_REJECTED;
import static io.prestosql.spi.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.spi.statistics.TableStatisticType.ROW_COUNT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.ParsingUtil.createParsingOptions;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.prestosql.sql.planner.plan.TableWriterNode.CreateReference;
import static io.prestosql.sql.planner.plan.TableWriterNode.InsertReference;
import static io.prestosql.sql.planner.plan.TableWriterNode.WriterTarget;
import static io.prestosql.sql.planner.sanity.PlanSanityChecker.DISTRIBUTED_PLAN_SANITY_CHECKER;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class LogicalPlanner
{
    public enum Stage
    {
        CREATED, OPTIMIZED, OPTIMIZED_AND_VALIDATED
    }

    private final PlanNodeIdAllocator idAllocator;

    private final Session session;
    private final List<PlanOptimizer> planOptimizers;
    private final PlanSanityChecker planSanityChecker;
    protected final PlanSymbolAllocator planSymbolAllocator = new PlanSymbolAllocator();
    private final Metadata metadata;
    private final TypeCoercion typeCoercion;
    private final TypeAnalyzer typeAnalyzer;
    private final StatisticsAggregationPlanner statisticsAggregationPlanner;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final WarningCollector warningCollector;
    private final Map<QualifiedName, Integer> namedSubPlan = new HashMap<>();
    private final UniqueIdAllocator uniqueIdAllocator = new UniqueIdAllocator();

    public LogicalPlanner(Session session,
            List<PlanOptimizer> planOptimizers,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata,
            TypeAnalyzer typeAnalyzer,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            WarningCollector warningCollector)
    {
        this(session, planOptimizers, DISTRIBUTED_PLAN_SANITY_CHECKER, idAllocator, metadata, typeAnalyzer, statsCalculator, costCalculator, warningCollector);
    }

    public LogicalPlanner(Session session,
            List<PlanOptimizer> planOptimizers,
            PlanSanityChecker planSanityChecker,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata,
            TypeAnalyzer typeAnalyzer,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            WarningCollector warningCollector)
    {
        this.session = requireNonNull(session, "session is null");
        this.planOptimizers = requireNonNull(planOptimizers, "planOptimizers is null");
        this.planSanityChecker = requireNonNull(planSanityChecker, "planSanityChecker is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeCoercion = new TypeCoercion(metadata::getType);
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        this.statisticsAggregationPlanner = new StatisticsAggregationPlanner(planSymbolAllocator, metadata);
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    }

    public Plan plan(Analysis analysis)
    {
        return plan(analysis, Stage.OPTIMIZED_AND_VALIDATED);
    }

    public Plan plan(Analysis analysis, Stage stage)
    {
        PlanNode root = planStatement(analysis, analysis.getStatement());

        planSanityChecker.validateIntermediatePlan(root, session, metadata, typeAnalyzer, planSymbolAllocator.getTypes(), warningCollector);

        if (stage.ordinal() >= Stage.OPTIMIZED.ordinal()) {
            for (PlanOptimizer optimizer : planOptimizers) {
                if (OptimizerUtils.isEnabledLegacy(optimizer, session, root)) {
                    root = optimizer.optimize(root, session, planSymbolAllocator.getTypes(), planSymbolAllocator, idAllocator,
                            warningCollector);
                    requireNonNull(root, format("%s returned a null plan", optimizer.getClass().getName()));
                }
            }
        }

        if (stage.ordinal() >= Stage.OPTIMIZED_AND_VALIDATED.ordinal()) {
            // make sure we produce a valid plan after optimizations run. This is mainly to catch programming errors
            planSanityChecker.validateFinalPlan(root, session, metadata, typeAnalyzer, planSymbolAllocator.getTypes(), warningCollector);
        }

        TypeProvider types = planSymbolAllocator.getTypes();
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, types);
        CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.empty(), session, types);
        return new Plan(root, types, StatsAndCosts.create(root, statsProvider, costProvider));
    }

    public PlanNode planStatement(Analysis analysis, Statement statement)
    {
        if ((statement instanceof CreateTableAsSelect) && analysis.isCreateTableAsSelectNoOp()) {
            checkState(analysis.getCreateTableDestination().isPresent(), "Table destination is missing");
            Symbol symbol = planSymbolAllocator.newSymbol("rows", BIGINT);
            PlanNode source = new ValuesNode(idAllocator.getNextId(), ImmutableList.of(symbol), ImmutableList.of(ImmutableList.of(new ConstantExpression(0L, BIGINT))));
            return new OutputNode(idAllocator.getNextId(), source, ImmutableList.of("rows"), ImmutableList.of(symbol));
        }
        return createOutputPlan(planStatementWithoutOutput(analysis, statement), analysis);
    }

    private RelationPlan planStatementWithoutOutput(Analysis analysis, Statement statement)
    {
        if (statement instanceof CreateTableAsSelect) {
            if (analysis.isCreateTableAsSelectNoOp()) {
                throw new PrestoException(NOT_SUPPORTED, "CREATE TABLE IF NOT EXISTS is not supported in this context " + statement.getClass().getSimpleName());
            }
            return createTableCreationPlan(analysis, ((CreateTableAsSelect) statement).getQuery());
        }
        else if (statement instanceof Analyze) {
            return createAnalyzePlan(analysis, (Analyze) statement);
        }
        else if (statement instanceof Insert) {
            checkState(analysis.getInsert().isPresent(), "Insert handle is missing");
            return createInsertPlan(analysis, (Insert) statement);
        }
        else if (statement instanceof InsertCube) {
            checkState(analysis.getCubeInsert().isPresent(), "Cube insert handle is missing");
            return createInsertCubePlan(analysis, (InsertCube) statement);
        }
        else if (statement instanceof Delete) {
            return createDeletePlan(analysis, (Delete) statement);
        }
        else if (statement instanceof Update) {
            return createUpdatePlan(analysis, (Update) statement);
        }
        else if (statement instanceof VacuumTable) {
            return createVacuumTablePlan(analysis, (VacuumTable) statement);
        }
        else if (statement instanceof Query) {
            return createRelationPlan(analysis, statement);
        }
        else if (statement instanceof Explain && ((Explain) statement).isAnalyze()) {
            return createExplainAnalyzePlan(analysis, (Explain) statement);
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported statement type " + statement.getClass().getSimpleName());
        }
    }

    private RelationPlan createExplainAnalyzePlan(Analysis analysis, Explain statement)
    {
        RelationPlan underlyingPlan = planStatementWithoutOutput(analysis, statement.getStatement());
        PlanNode root = underlyingPlan.getRoot();
        Scope scope = analysis.getScope(statement);
        Symbol outputSymbol = planSymbolAllocator.newSymbol(scope.getRelationType().getFieldByIndex(0));
        root = new ExplainAnalyzeNode(idAllocator.getNextId(), root, outputSymbol, statement.isVerbose());
        return new RelationPlan(root, scope, ImmutableList.of(outputSymbol));
    }

    private RelationPlan createAnalyzePlan(Analysis analysis, Analyze analyzeStatement)
    {
        TableHandle targetTable = analysis.getAnalyzeTarget().get();

        // Plan table scan
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, targetTable);
        ImmutableList.Builder<Symbol> tableScanOutputs = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, ColumnHandle> symbolToColumnHandle = ImmutableMap.builder();
        ImmutableMap.Builder<String, Symbol> columnNameToSymbol = ImmutableMap.builder();
        TableMetadata tableMetadata = metadata.getTableMetadata(session, targetTable);
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            Symbol symbol = planSymbolAllocator.newSymbol(column.getName(), column.getType());
            tableScanOutputs.add(symbol);
            symbolToColumnHandle.put(symbol, columnHandles.get(column.getName()));
            columnNameToSymbol.put(column.getName(), symbol);
        }

        TableStatisticsMetadata tableStatisticsMetadata = metadata.getStatisticsCollectionMetadata(
                session,
                targetTable.getCatalogName().getCatalogName(),
                tableMetadata.getMetadata());

        TableStatisticAggregation tableStatisticAggregation = statisticsAggregationPlanner.createStatisticsAggregation(tableStatisticsMetadata, columnNameToSymbol.build());
        StatisticAggregations statisticAggregations = tableStatisticAggregation.getAggregations();
        List<Symbol> groupingSymbols = statisticAggregations.getGroupingSymbols();

        PlanNode planNode = new StatisticsWriterNode(
                idAllocator.getNextId(),
                new AggregationNode(
                        idAllocator.getNextId(),
                        TableScanNode.newInstance(idAllocator.getNextId(), targetTable, tableScanOutputs.build(), symbolToColumnHandle.build(), ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT, new UUID(0, 0), 0, false),
                        statisticAggregations.getAggregations(),
                        singleGroupingSet(groupingSymbols),
                        ImmutableList.of(),
                        AggregationNode.Step.SINGLE,
                        Optional.empty(),
                        Optional.empty()),
                new StatisticsWriterNode.WriteStatisticsReference(targetTable),
                planSymbolAllocator.newSymbol("rows", BIGINT),
                tableStatisticsMetadata.getTableStatistics().contains(ROW_COUNT),
                tableStatisticAggregation.getDescriptor());
        return new RelationPlan(planNode, analysis.getScope(analyzeStatement), planNode.getOutputSymbols());
    }

    private RelationPlan createTableCreationPlan(Analysis analysis, Query query)
    {
        QualifiedObjectName destination = analysis.getCreateTableDestination().get();

        RelationPlan plan = createRelationPlan(analysis, query);

        ConnectorTableMetadata tableMetadata = createTableMetadata(
                destination,
                getOutputTableColumns(plan, analysis.getColumnAliases()),
                analysis.getCreateTableProperties(),
                analysis.getParameters(),
                analysis.getCreateTableComment());
        analysis.setCreateTableMetadata(tableMetadata);
        Optional<NewTableLayout> newTableLayout = metadata.getNewTableLayout(session, destination.getCatalogName(), tableMetadata);

        List<String> columnNames = tableMetadata.getColumns().stream()
                .filter(column -> !column.isHidden())
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());

        TableStatisticsMetadata statisticsMetadata = metadata.getStatisticsCollectionMetadataForWrite(session, destination.getCatalogName(), tableMetadata);

        return createTableWriterPlan(
                analysis,
                plan,
                new CreateReference(destination.getCatalogName(), tableMetadata, newTableLayout),
                columnNames,
                newTableLayout,
                statisticsMetadata);
    }

    private RelationPlan createInsertPlan(Analysis analysis, Insert insertStatement)
    {
        Analysis.Insert insert = analysis.getInsert().get();

        TableMetadata tableMetadata = metadata.getTableMetadata(session, insert.getTarget());

        List<ColumnMetadata> visibleTableColumns = tableMetadata.getColumns().stream()
                .filter(column -> !column.isHidden())
                .collect(toImmutableList());
        List<String> visibleTableColumnNames = visibleTableColumns.stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());

        RelationPlan plan = createRelationPlan(analysis, insertStatement.getQuery());

        Map<String, ColumnHandle> columns = metadata.getColumnHandles(session, insert.getTarget());
        Assignments.Builder assignments = Assignments.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            if (column.isHidden()) {
                continue;
            }
            Symbol output = planSymbolAllocator.newSymbol(column.getName(), column.getType());
            int index = insert.getColumns().indexOf(columns.get(column.getName()));
            if (index < 0) {
                Expression cast = new Cast(new NullLiteral(), column.getType().getTypeSignature().toString());
                assignments.put(output, castToRowExpression(cast));
            }
            else {
                Symbol input = plan.getSymbol(index);
                Type tableType = column.getType();
                Type queryType = planSymbolAllocator.getTypes().get(input);

                if (queryType.equals(tableType) || typeCoercion.isTypeOnlyCoercion(queryType, tableType)) {
                    assignments.put(output, castToRowExpression(toSymbolReference(input)));
                }
                else {
                    Expression cast = noTruncationCast(toSymbolReference(input), queryType, tableType);
                    assignments.put(output, castToRowExpression(cast));
                }
            }
        }
        ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), plan.getRoot(), assignments.build());

        List<Field> fields = visibleTableColumns.stream()
                .map(column -> Field.newUnqualified(column.getName(), column.getType()))
                .collect(toImmutableList());
        Scope scope = Scope.builder().withRelationType(RelationId.anonymous(), new RelationType(fields)).build();

        plan = new RelationPlan(projectNode, scope, projectNode.getOutputSymbols());

        Optional<NewTableLayout> newTableLayout = metadata.getInsertLayout(session, insert.getTarget());
        String catalogName = insert.getTarget().getCatalogName().getCatalogName();
        TableStatisticsMetadata statisticsMetadata = metadata.getStatisticsCollectionMetadataForWrite(session, catalogName, tableMetadata.getMetadata());

        return createTableWriterPlan(
                analysis,
                plan,
                new InsertReference(insert.getTarget(), analysis.isInsertOverwrite()),
                visibleTableColumnNames,
                newTableLayout,
                statisticsMetadata);
    }

    private RelationPlan createInsertCubePlan(Analysis analysis, InsertCube insertCubeStatement)
    {
        Analysis.CubeInsert insert = analysis.getCubeInsert().get();
        TableMetadata tableMetadata = metadata.getTableMetadata(session, insert.getTarget());
        List<ColumnMetadata> visibleTableColumns = tableMetadata.getColumns().stream()
                .filter(column -> !column.isHidden())
                .collect(toImmutableList());
        List<String> visibleTableColumnNames = visibleTableColumns.stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());
        RelationPlan plan = createRelationPlan(analysis, insertCubeStatement.getQuery());
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(session, insert.getTarget());
        Assignments.Builder assignments = Assignments.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            if (column.isHidden()) {
                continue;
            }
            Symbol output = planSymbolAllocator.newSymbol(column.getName(), column.getType());
            int index = insert.getColumns().indexOf(columns.get(column.getName()));
            if (index < 0) {
                Expression cast = new Cast(new NullLiteral(), column.getType().getTypeSignature().toString());
                assignments.put(output, castToRowExpression(cast));
            }
            else {
                Symbol input = plan.getSymbol(index);
                Type tableType = column.getType();
                Type queryType = planSymbolAllocator.getTypes().get(input);
                if (queryType.equals(tableType) || typeCoercion.isTypeOnlyCoercion(queryType, tableType)) {
                    assignments.put(output, castToRowExpression(toSymbolReference(input)));
                }
                else {
                    Expression cast = noTruncationCast(toSymbolReference(input), queryType, tableType);
                    assignments.put(output, castToRowExpression(cast));
                }
            }
        }
        ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), plan.getRoot(), assignments.build());
        List<Field> fields = visibleTableColumns.stream()
                .map(column -> Field.newUnqualified(column.getName(), column.getType()))
                .collect(toImmutableList());
        Scope scope = Scope.builder().withRelationType(RelationId.anonymous(), new RelationType(fields)).build();
        plan = new RelationPlan(projectNode, scope, projectNode.getOutputSymbols());
        Optional<NewTableLayout> newTableLayout = metadata.getInsertLayout(session, insert.getTarget());
        String catalogName = insert.getTarget().getCatalogName().getCatalogName();
        TableStatisticsMetadata statisticsMetadata = metadata.getStatisticsCollectionMetadataForWrite(session, catalogName, tableMetadata.getMetadata());
        RelationPlan tableWriterPlan = createTableWriterPlan(
                analysis,
                plan,
                new InsertReference(insert.getTarget(), analysis.isCubeOverwrite()),
                visibleTableColumnNames,
                newTableLayout,
                statisticsMetadata);
        Expression cubeWhere = analysis.getWhere((QuerySpecification) (insertCubeStatement.getQuery().getQueryBody()));
        Expression rewritten = null;
        if (cubeWhere != null) {
            rewritten = new QueryPlanner(analysis, planSymbolAllocator, idAllocator, buildLambdaDeclarationToSymbolMap(analysis, planSymbolAllocator), metadata, session, namedSubPlan, uniqueIdAllocator)
                    .rewriteExpression(tableWriterPlan, cubeWhere, analysis, buildLambdaDeclarationToSymbolMap(analysis, planSymbolAllocator));
        }
        CubeMetadata cubeMetadata = insert.getMetadata();
        if (!insertCubeStatement.isOverwrite() && !insertCubeStatement.getWhere().isPresent() && cubeMetadata.getCubeStatus() != CubeStatus.INACTIVE) {
            //Means data some data was inserted before, but trying to insert entire dataset
            throw new PrestoException(QUERY_REJECTED, "Cannot allow insert. Inserting entire dataset but cube already has partial data");
        }
        else if (!insertCubeStatement.isOverwrite() && insertCubeStatement.getWhere().isPresent() && arePredicatesOverlapping(rewritten, cubeMetadata)) {
            throw new PrestoException(QUERY_REJECTED, String.format("Cannot allow insert. Cube already contains data for the given predicate '%s'", ExpressionFormatter.formatExpression(insertCubeStatement.getWhere().get(), Optional.empty())));
        }
        TableHandle sourceTableHandle = insert.getSourceTable();
        //At this point it has been verified that source table has not been updated
        //so insert into cube should be allowed
        LongSupplier tableLastModifiedTimeSupplier = metadata.getTableLastModifiedTimeSupplier(session, sourceTableHandle);
        checkState(tableLastModifiedTimeSupplier != null, "Table last modified time is null");
        CubeFinishNode cubeFinishNode = new CubeFinishNode(
                idAllocator.getNextId(),
                tableWriterPlan.getRoot(),
                planSymbolAllocator.newSymbol("rows", BIGINT),
                new CubeUpdateMetadata(
                        tableMetadata.getQualifiedName().toString(),
                        tableLastModifiedTimeSupplier.getAsLong(),
                        cubeWhere != null ? ExpressionFormatter.formatExpression(rewritten, Optional.empty()) : null,
                        insertCubeStatement.isOverwrite()));
        return new RelationPlan(cubeFinishNode, analysis.getScope(insertCubeStatement), cubeFinishNode.getOutputSymbols());
    }

    private boolean arePredicatesOverlapping(Expression newDataPredicate, CubeMetadata cubeMetadata)
    {
        //Cannot do this check inside StatementAnalyzer because predicate expressions have not been rewritten by then.
        TypeProvider types = planSymbolAllocator.getTypes();
        newDataPredicate = ExpressionUtils.rewriteIdentifiersToSymbolReferences(newDataPredicate);
        ExpressionDomainTranslator.ExtractionResult decomposedNewDataPredicate = ExpressionDomainTranslator.fromPredicate(metadata, session, newDataPredicate, types);
        if (!BooleanLiteral.TRUE_LITERAL.equals(decomposedNewDataPredicate.getRemainingExpression())) {
            throw new RuntimeException(String.format("Cannot support predicate '%s'", ExpressionFormatter.formatExpression(newDataPredicate, Optional.empty())));
        }
        if (cubeMetadata.getCubeStatus() == CubeStatus.INACTIVE) {
            //Inactive cubes are empty. So inserts should be allowed.
            return false;
        }

        if (cubeMetadata.getPredicateString() == null) {
            //Means Cube was created for entire dataset.
            return true;
        }
        SqlParser sqlParser = new SqlParser();
        Expression cubePredicateAsExpr = sqlParser.createExpression(cubeMetadata.getPredicateString(), createParsingOptions(session));
        cubePredicateAsExpr = ExpressionUtils.rewriteIdentifiersToSymbolReferences(cubePredicateAsExpr);
        ExpressionDomainTranslator.ExtractionResult decomposedCubePredicate = ExpressionDomainTranslator.fromPredicate(metadata, session, cubePredicateAsExpr, types);
        return decomposedCubePredicate.getTupleDomain().overlaps(decomposedNewDataPredicate.getTupleDomain());
    }

    private Expression noTruncationCast(Expression expression, Type fromType, Type toType)
    {
        // cast larger size varchar to small size varchar
        if ((fromType instanceof VarcharType || fromType instanceof CharType) && (toType instanceof VarcharType || toType instanceof CharType)) {
            int targetLength;
            if (toType instanceof VarcharType) {
                if (((VarcharType) toType).isUnbounded()) {
                    return new Cast(expression, toType.getTypeSignature().toString());
                }
                targetLength = ((VarcharType) toType).getBoundedLength();
            }
            else {
                targetLength = ((CharType) toType).getLength();
            }

            Signature spaceTrimmedLength = metadata.getFunctionAndTypeManager().resolveBuiltInFunction(QualifiedName.of("$space_trimmed_length"), fromTypes(VARCHAR));
            Signature fail = metadata.getFunctionAndTypeManager().resolveBuiltInFunction(QualifiedName.of("fail"), fromTypes(VARCHAR));

            return new IfExpression(
                    // check if the trimmed value fits in the target type
                    new ComparisonExpression(
                            GREATER_THAN_OR_EQUAL,
                            new GenericLiteral("BIGINT", Integer.toString(targetLength)),
                            new FunctionCall(
                                    QualifiedName.of("$space_trimmed_length"),
                                    ImmutableList.of(new Cast(expression, VARCHAR.getTypeSignature().toString())))),
                    new Cast(expression, toType.getTypeSignature().toString()),
                    new Cast(
                            new FunctionCall(
                                    QualifiedName.of("fail"),
                                    ImmutableList.of(new Cast(new StringLiteral(format("Out of range for insert query type: Table: %s, Query: %s", toType.toString(), fromType.toString())),
                                            VARCHAR.getTypeSignature().toString()))),
                            toType.getTypeSignature().toString()));
        }

        return new Cast(expression, toType.getTypeSignature().toString());
    }

    private RelationPlan createTableWriterPlan(
            Analysis analysis,
            RelationPlan plan,
            WriterTarget target,
            List<String> columnNames,
            Optional<NewTableLayout> writeTableLayout,
            TableStatisticsMetadata statisticsMetadata)
    {
        PlanNode source = plan.getRoot();

        if (!analysis.isCreateTableAsSelectWithData()) {
            source = new LimitNode(idAllocator.getNextId(), source, 0L, false);
        }

        // todo this should be checked in analysis
        writeTableLayout.ifPresent(layout -> {
            if (!ImmutableSet.copyOf(columnNames).containsAll(layout.getPartitionColumns())) {
                throw new PrestoException(NOT_SUPPORTED, "INSERT must write all distribution columns: " + layout.getPartitionColumns());
            }
        });

        List<Symbol> symbols = plan.getFieldMappings();

        Optional<PartitioningScheme> partitioningScheme = Optional.empty();
        if (writeTableLayout.isPresent()) {
            List<Symbol> partitionFunctionArguments = new ArrayList<>();
            writeTableLayout.get().getPartitionColumns().stream()
                    .mapToInt(columnNames::indexOf)
                    .mapToObj(symbols::get)
                    .forEach(partitionFunctionArguments::add);

            List<Symbol> outputLayout = new ArrayList<>(symbols);

            PartitioningHandle partitioningHandle = writeTableLayout.get().getPartitioning()
                    .orElse(FIXED_HASH_DISTRIBUTION);

            partitioningScheme = Optional.of(new PartitioningScheme(
                    Partitioning.create(partitioningHandle, partitionFunctionArguments),
                    outputLayout));
        }

        if (!statisticsMetadata.isEmpty()) {
            verify(columnNames.size() == symbols.size(), "columnNames.size() != symbols.size(): %s and %s", columnNames, symbols);
            Map<String, Symbol> columnToSymbolMap = zip(columnNames.stream(), symbols.stream(), SimpleImmutableEntry::new)
                    .collect(toImmutableMap(Entry::getKey, Entry::getValue));

            TableStatisticAggregation result = statisticsAggregationPlanner.createStatisticsAggregation(statisticsMetadata, columnToSymbolMap);

            StatisticAggregations.Parts aggregations = result.getAggregations().createPartialAggregations(planSymbolAllocator, metadata);

            // partial aggregation is run within the TableWriteOperator to calculate the statistics for
            // the data consumed by the TableWriteOperator
            // final aggregation is run within the TableFinishOperator to summarize collected statistics
            // by the partial aggregation from all of the writer nodes
            StatisticAggregations partialAggregation = aggregations.getPartialAggregation();

            PlanNode writerNode = new TableWriterNode(
                    idAllocator.getNextId(),
                    source,
                    target,
                    planSymbolAllocator.newSymbol("partialrows", BIGINT),
                    planSymbolAllocator.newSymbol("fragment", VARBINARY),
                    symbols,
                    columnNames,
                    partitioningScheme,
                    Optional.of(partialAggregation),
                    Optional.of(result.getDescriptor().map(aggregations.getMappings()::get)));

            TableFinishNode commitNode = new TableFinishNode(
                    idAllocator.getNextId(),
                    writerNode,
                    target,
                    planSymbolAllocator.newSymbol("rows", BIGINT),
                    Optional.of(aggregations.getFinalAggregation()),
                    Optional.of(result.getDescriptor()));

            return new RelationPlan(commitNode, analysis.getRootScope(), commitNode.getOutputSymbols());
        }

        TableFinishNode commitNode = new TableFinishNode(
                idAllocator.getNextId(),
                new TableWriterNode(
                        idAllocator.getNextId(),
                        source,
                        target,
                        planSymbolAllocator.newSymbol("partialrows", BIGINT),
                        planSymbolAllocator.newSymbol("fragment", VARBINARY),
                        symbols,
                        columnNames,
                        partitioningScheme,
                        Optional.empty(),
                        Optional.empty()),
                target,
                planSymbolAllocator.newSymbol("rows", BIGINT),
                Optional.empty(),
                Optional.empty());
        return new RelationPlan(commitNode, analysis.getRootScope(), commitNode.getOutputSymbols());
    }

    private RelationPlan createDeletePlan(Analysis analysis, Delete node)
    {
        TableHandle handle = analysis.getTableHandle(node.getTable());
        if (handle.getConnectorHandle().isDeleteAsInsertSupported()) {
            QueryPlanner.UpdateDeleteRelationPlan deletePlan = new QueryPlanner(analysis, planSymbolAllocator, idAllocator, buildLambdaDeclarationToSymbolMap(analysis, planSymbolAllocator), metadata, session, namedSubPlan, uniqueIdAllocator)
                    .planDeleteRowAsInsert(node);

            RelationPlan plan = deletePlan.getPlan();

            Optional<NewTableLayout> newTableLayout = metadata.getUpdateLayout(session, handle);
            TableMetadata tableMetadata = metadata.getTableMetadata(session, handle);
            String catalogName = handle.getCatalogName().getCatalogName();
            // TableStatisticsMetadata statisticsMetadata = metadata.getStatisticsCollectionMetadataForWrite(session,
            //        catalogName, tableMetadata.getMetadata());
            Optional<Expression> constraint = deletePlan.getPredicate().isPresent() ? Optional.of(OriginalExpressionUtils.castToExpression(deletePlan.getPredicate().get())) : Optional.empty();
            //Skip statistics collection for delete,
            // because stats collection for delete seems to corrupt existing statistics
            TableStatisticsMetadata statisticsMetadata = TableStatisticsMetadata.empty();
            return createTableWriterPlan(
                    analysis,
                    plan,
                    new TableWriterNode.DeleteAsInsertReference(handle, constraint, deletePlan.getColumnAssignments()),
                    deletePlan.getColumNames(),
                    newTableLayout,
                    statisticsMetadata);
        }
        else {
            DeleteNode deleteNode = new QueryPlanner(analysis, planSymbolAllocator, idAllocator, buildLambdaDeclarationToSymbolMap(analysis, planSymbolAllocator), metadata, session, namedSubPlan, uniqueIdAllocator)
                    .plan(node);
            TableFinishNode commitNode = new TableFinishNode(idAllocator.getNextId(), deleteNode, deleteNode.getTarget(), planSymbolAllocator.newSymbol("rows", BIGINT),
                    Optional.empty(), Optional.empty());
            return new RelationPlan(commitNode, analysis.getScope(node), commitNode.getOutputSymbols());
        }
    }

    private RelationPlan createUpdatePlan(Analysis analysis, Update updateStatement)
    {
        QueryPlanner.UpdateDeleteRelationPlan updatePlan = new QueryPlanner(analysis, planSymbolAllocator, idAllocator, buildLambdaDeclarationToSymbolMap(analysis, planSymbolAllocator), metadata, session, namedSubPlan, uniqueIdAllocator)
                .plan(updateStatement);
        RelationPlan plan = updatePlan.getPlan();
        Analysis.Update update = analysis.getUpdate().get();
        TableMetadata tableMetadata = metadata.getTableMetadata(session, update.getTarget());
        Optional<NewTableLayout> newTableLayout = metadata.getUpdateLayout(session, update.getTarget());
        String catalogName = update.getTarget().getCatalogName().getCatalogName();
        // TableStatisticsMetadata statisticsMetadata = metadata.getStatisticsCollectionMetadataForWrite(session, catalogName, tableMetadata.getMetadata());
        Optional<Expression> constraint = updatePlan.getPredicate().isPresent() ? Optional.of(OriginalExpressionUtils.castToExpression(updatePlan.getPredicate().get())) : Optional.empty();
        TableStatisticsMetadata statisticsMetadata = TableStatisticsMetadata.empty();

        return createTableWriterPlan(
                analysis,
                plan,
                new TableWriterNode.UpdateReference(update.getTarget(), constraint, updatePlan.getColumnAssignments()),
                updatePlan.getColumNames(),
                newTableLayout,
                statisticsMetadata);
    }

    private RelationPlan createVacuumTablePlan(Analysis analysis, VacuumTable vacuumTable)
    {
        TableHandle handle = analysis.getTableHandle(vacuumTable.getTable());
        TableMetadata tableMetadata = metadata.getTableMetadata(session, handle);
        List<ColumnMetadata> columns = tableMetadata.getColumns();
        List<String> columnNames = columns.stream()
                .filter(column -> !column.isHidden())
                .map(ColumnMetadata::getName)
                .collect(Collectors.toList());

        List<Symbol> symbols = columns.stream()
                .filter(column -> !column.isHidden())
                .map(c -> planSymbolAllocator.newSymbol(c.getName(), c.getType()))
                .collect(Collectors.toList());

        ColumnHandle rowIdHandle = metadata.getUpdateRowIdColumnHandle(session, handle);
        ColumnMetadata rowIdColumnMetadata = metadata.getColumnMetadata(session, handle, rowIdHandle);

        Type rowIdType = rowIdColumnMetadata.getType();
        Symbol rowIdSymbol = planSymbolAllocator.newSymbol("$rowId", rowIdType);
        symbols.add(rowIdSymbol);
        columnNames.add(rowIdHandle.getColumnName());

        String catalogName = handle.getCatalogName().getCatalogName();
        TableStatisticsMetadata statisticsMetadata = TableStatisticsMetadata.empty();

        return createVacuumWriterPlan(analysis,
                handle,
                vacuumTable,
                new VacuumTargetReference(handle, vacuumTable.isFull(), vacuumTable.isUnify(), vacuumTable.getPartition()),
                symbols,
                columnNames,
                statisticsMetadata);
    }

    private RelationPlan createVacuumWriterPlan(
            Analysis analysis,
            TableHandle handle,
            VacuumTable node,
            WriterTarget target,
            List<Symbol> symbols,
            List<String> columnNames,
            TableStatisticsMetadata statisticsMetadata)
    {
        Optional<StatisticAggregations> statisticsAggregation = Optional.empty();
        Optional<StatisticAggregationsDescriptor<Symbol>> statisticsAggregationDescriptor = Optional.empty();
        Optional<StatisticAggregations> finalStatisticsAggregation = Optional.empty();
        Optional<StatisticAggregationsDescriptor<Symbol>> finalStatisticsAggregationDescriptor = Optional.empty();
        if (!statisticsMetadata.isEmpty()) {
            verify(columnNames.size() == symbols.size(), "columnNames.size() != symbols.size(): %s and %s", columnNames, symbols);
            Map<String, Symbol> columnToSymbolMap = zip(columnNames.stream(), symbols.stream(), SimpleImmutableEntry::new)
                    .collect(toImmutableMap(Entry::getKey, Entry::getValue));

            TableStatisticAggregation result = statisticsAggregationPlanner.createStatisticsAggregation(statisticsMetadata, columnToSymbolMap);

            StatisticAggregations.Parts aggregations = result.getAggregations().createPartialAggregations(planSymbolAllocator, metadata);

            // partial aggregation is run within the VacuumTableOperator to calculate the statistics for
            // the data consumed by the VacuumTableOperator
            // final aggregation is run within the TableFinishOperator to summarize collected statistics
            // by the partial aggregation from all of the writer nodes
            statisticsAggregation = Optional.of(aggregations.getPartialAggregation());
            statisticsAggregationDescriptor = Optional.of(result.getDescriptor().map(aggregations.getMappings()::get));
            finalStatisticsAggregation = Optional.of(aggregations.getFinalAggregation());
            finalStatisticsAggregationDescriptor = Optional.of(result.getDescriptor());
        }

        VacuumTableNode vacuumTableNode = new VacuumTableNode(idAllocator.getNextId(),
                handle,
                target,
                planSymbolAllocator.newSymbol("partialrows", BIGINT),
                planSymbolAllocator.newSymbol("fragment", VARBINARY),
                node.getPartition().orElse(""),
                node.isFull(),
                symbols,
                statisticsAggregation,
                statisticsAggregationDescriptor);

        TableFinishNode commitNode = new TableFinishNode(
                idAllocator.getNextId(),
                vacuumTableNode,
                target,
                planSymbolAllocator.newSymbol("rows", BIGINT),
                finalStatisticsAggregation,
                finalStatisticsAggregationDescriptor);

        return new RelationPlan(commitNode, analysis.getRootScope(), commitNode.getOutputSymbols());
    }

    private PlanNode createOutputPlan(RelationPlan plan, Analysis analysis)
    {
        ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
        ImmutableList.Builder<String> names = ImmutableList.builder();

        int columnNumber = 0;
        RelationType outputDescriptor = analysis.getOutputDescriptor();
        for (Field field : outputDescriptor.getVisibleFields()) {
            String name = field.getName().orElse("_col" + columnNumber);
            names.add(name);

            int fieldIndex = outputDescriptor.indexOf(field);
            Symbol symbol = plan.getSymbol(fieldIndex);
            outputs.add(symbol);

            columnNumber++;
        }

        return new OutputNode(idAllocator.getNextId(), plan.getRoot(), names.build(), outputs.build());
    }

    private RelationPlan createRelationPlan(Analysis analysis, Node node)
    {
        return new RelationPlanner(analysis, planSymbolAllocator, idAllocator, buildLambdaDeclarationToSymbolMap(analysis, planSymbolAllocator), metadata, session, namedSubPlan, uniqueIdAllocator)
                .process(node, null);
    }

    private ConnectorTableMetadata createTableMetadata(QualifiedObjectName table, List<ColumnMetadata> columns, Map<String, Expression> propertyExpressions, List<Expression> parameters, Optional<String> comment)
    {
        CatalogName catalogName = metadata.getCatalogHandle(session, table.getCatalogName())
                .orElseThrow(() -> new PrestoException(NOT_FOUND, "Catalog does not exist: " + table.getCatalogName()));

        Map<String, Object> properties = metadata.getTablePropertyManager().getProperties(
                catalogName,
                table.getCatalogName(),
                propertyExpressions,
                session,
                metadata,
                parameters);

        return new ConnectorTableMetadata(toSchemaTableName(table), columns, properties, comment);
    }

    private static List<ColumnMetadata> getOutputTableColumns(RelationPlan plan, Optional<List<Identifier>> columnAliases)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        int aliasPosition = 0;
        for (Field field : plan.getDescriptor().getVisibleFields()) {
            String columnName = columnAliases.isPresent() ? columnAliases.get().get(aliasPosition).getValue() : field.getName().get();
            columns.add(new ColumnMetadata(columnName, field.getType()));
            aliasPosition++;
        }
        return columns.build();
    }

    private static Map<NodeRef<LambdaArgumentDeclaration>, Symbol> buildLambdaDeclarationToSymbolMap(Analysis analysis, PlanSymbolAllocator planSymbolAllocator)
    {
        Map<NodeRef<LambdaArgumentDeclaration>, Symbol> resultMap = new LinkedHashMap<>();
        for (Entry<NodeRef<Expression>, Type> entry : analysis.getTypes().entrySet()) {
            if (!(entry.getKey().getNode() instanceof LambdaArgumentDeclaration)) {
                continue;
            }
            NodeRef<LambdaArgumentDeclaration> lambdaArgumentDeclaration = NodeRef.of((LambdaArgumentDeclaration) entry.getKey().getNode());
            if (resultMap.containsKey(lambdaArgumentDeclaration)) {
                continue;
            }
            resultMap.put(lambdaArgumentDeclaration, planSymbolAllocator.newSymbol(lambdaArgumentDeclaration.getNode(), entry.getValue()));
        }
        return resultMap;
    }
}
