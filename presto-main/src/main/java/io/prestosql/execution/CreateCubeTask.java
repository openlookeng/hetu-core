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

package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.hetu.core.spi.cube.CubeAggregateFunction;
import io.hetu.core.spi.cube.CubeFilter;
import io.hetu.core.spi.cube.CubeMetadataBuilder;
import io.hetu.core.spi.cube.CubeStatus;
import io.hetu.core.spi.cube.aggregator.AggregationSignature;
import io.hetu.core.spi.cube.io.CubeMetaStore;
import io.prestosql.Session;
import io.prestosql.cube.CubeManager;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableMetadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.sql.ExpressionFormatter;
import io.prestosql.sql.analyzer.Analysis;
import io.prestosql.sql.analyzer.Analyzer;
import io.prestosql.sql.analyzer.Field;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.Coercer;
import io.prestosql.sql.tree.CreateCube;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.Statement;
import io.prestosql.transaction.TransactionManager;
import org.assertj.core.util.VisibleForTesting;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.cube.CubeManager.STAR_TREE;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.sql.NodeUtils.mapFromProperties;
import static io.prestosql.sql.analyzer.SemanticErrorCode.CUBE_ALREADY_EXISTS;
import static io.prestosql.sql.analyzer.SemanticErrorCode.CUBE_OR_TABLE_ALREADY_EXISTS;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_COLUMN;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class CreateCubeTask
        implements DataDefinitionTask<CreateCube>
{
    private final CubeManager cubeManager;
    private final SqlParser sqlParser;

    @Inject
    public CreateCubeTask(CubeManager cubeManager, SqlParser sqlParser)
    {
        this.cubeManager = cubeManager;
        this.sqlParser = sqlParser;
    }

    @Override
    public String getName()
    {
        return "CREATE CUBE";
    }

    @Override
    public ListenableFuture<?> execute(CreateCube statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters, HeuristicIndexerManager heuristicIndexerManager)
    {
        return internalExecute(statement, metadata, accessControl, stateMachine.getSession(), stateMachine, parameters);
    }

    @VisibleForTesting
    public ListenableFuture<?> internalExecute(CreateCube statement, Metadata metadata,
            AccessControl accessControl, Session session, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Optional<CubeMetaStore> optionalCubeMetaStore = cubeManager.getMetaStore(STAR_TREE);
        if (!optionalCubeMetaStore.isPresent()) {
            throw new RuntimeException("HetuMetaStore is not initialized");
        }
        QualifiedObjectName cubeName = createQualifiedObjectName(session, statement, statement.getCubeName());
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getSourceTableName());
        Optional<TableHandle> cubeHandle = metadata.getTableHandle(session, cubeName);
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);

        if (optionalCubeMetaStore.get().getMetadataFromCubeName(cubeName.toString()).isPresent()) {
            if (!statement.isNotExists()) {
                throw new SemanticException(CUBE_ALREADY_EXISTS, statement, "Cube '%s' already exists", cubeName);
            }
            return immediateFuture(null);
        }
        if (cubeHandle.isPresent()) {
            if (!statement.isNotExists()) {
                throw new SemanticException(CUBE_OR_TABLE_ALREADY_EXISTS, statement, "Cube or Table '%s' already exists", cubeName);
            }
            return immediateFuture(null);
        }

        CatalogName catalogName = metadata.getCatalogHandle(session, cubeName.getCatalogName())
                .orElseThrow(() -> new PrestoException(NOT_FOUND, "Catalog not found: " + cubeName.getCatalogName()));

        if (!metadata.isPreAggregationSupported(session, catalogName)) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, String.format("Cube cannot be created on catalog '%s'", catalogName.toString()));
        }

        if (!tableHandle.isPresent()) {
            throw new SemanticException(MISSING_TABLE, statement, "Table '%s' does not exist", tableName);
        }

        TableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle.get());
        List<String> groupingSet = statement.getGroupingSet().stream().map(s -> s.getValue().toLowerCase(ENGLISH)).collect(Collectors.toList());
        Map<String, ColumnMetadata> sourceTableColumns = tableMetadata.getColumns().stream().collect(Collectors.toMap(ColumnMetadata::getName, col -> col));
        List<ColumnMetadata> cubeColumns = new ArrayList<>();
        Map<String, AggregationSignature> aggregations = new HashMap<>();
        Analysis analysis = analyzeStatement(statement, session, metadata, accessControl, parameters, stateMachine.getWarningCollector());
        Map<String, Field> fields = analysis.getOutputDescriptor().getAllFields().stream().collect(Collectors.toMap(col -> col.getName().map(String::toLowerCase).get(), col -> col));

        for (FunctionCall aggFunction : statement.getAggregations()) {
            String aggFunctionName = aggFunction.getName().toString().toLowerCase(ENGLISH);
            String argument = aggFunction.getArguments().isEmpty() || aggFunction.getArguments().get(0) instanceof LongLiteral ? null : ((Identifier) aggFunction.getArguments().get(0)).getValue().toLowerCase(ENGLISH);
            boolean distinct = aggFunction.isDistinct();
            String cubeColumnName = aggFunctionName + "_" + (argument == null ? "all" : argument) + (aggFunction.isDistinct() ? "_distinct" : "");
            CubeAggregateFunction cubeAggregateFunction = CubeAggregateFunction.valueOf(aggFunctionName.toUpperCase(ENGLISH));
            switch (cubeAggregateFunction) {
                case SUM:
                    aggregations.put(cubeColumnName, AggregationSignature.sum(argument, distinct));
                    break;
                case COUNT:
                    AggregationSignature aggregationSignature = argument == null ? AggregationSignature.count() : AggregationSignature.count(argument, distinct);
                    aggregations.put(cubeColumnName, aggregationSignature);
                    break;
                case AVG:
                    aggregations.put(cubeColumnName, AggregationSignature.avg(argument, distinct));
                    break;
                case MAX:
                    aggregations.put(cubeColumnName, AggregationSignature.max(argument, distinct));
                    break;
                case MIN:
                    aggregations.put(cubeColumnName, AggregationSignature.min(argument, distinct));
                    break;
                default:
                    throw new PrestoException(NOT_SUPPORTED, format("Unsupported aggregation function : %s", aggFunctionName));
            }

            Field tableField = fields.get(cubeColumnName);
            ColumnMetadata cubeCol = new ColumnMetadata(
                    cubeColumnName,
                    tableField.getType(),
                    true,
                    null,
                    null,
                    false,
                    Collections.emptyMap());
            cubeColumns.add(cubeCol);
        }

        accessControl.checkCanCreateTable(session.getRequiredTransactionId(), session.getIdentity(), tableName);
        Map<String, Expression> sqlProperties = mapFromProperties(statement.getProperties());
        Map<String, Object> properties = metadata.getTablePropertyManager().getProperties(
                catalogName,
                cubeName.getCatalogName(),
                sqlProperties,
                session,
                metadata,
                parameters);

        if (properties.containsKey("partitioned_by")) {
            List<String> partitionCols = new ArrayList<>(((List<String>) properties.get("partitioned_by")));
            // put all partition columns at the end of the list
            groupingSet.removeAll(partitionCols);
            groupingSet.addAll(partitionCols);
        }

        for (String dimension : groupingSet) {
            if (!sourceTableColumns.containsKey(dimension)) {
                throw new SemanticException(MISSING_COLUMN, statement, "Column %s does not exist", dimension);
            }
            ColumnMetadata tableCol = sourceTableColumns.get(dimension);
            ColumnMetadata cubeCol = new ColumnMetadata(
                    dimension,
                    tableCol.getType(),
                    tableCol.isNullable(),
                    null,
                    null,
                    false,
                    tableCol.getProperties());
            cubeColumns.add(cubeCol);
        }

        ConnectorTableMetadata cubeTableMetadata = new ConnectorTableMetadata(cubeName.asSchemaTableName(), ImmutableList.copyOf(cubeColumns), properties);
        try {
            metadata.createTable(session, cubeName.getCatalogName(), cubeTableMetadata, statement.isNotExists());
        }
        catch (PrestoException e) {
            // connectors are not required to handle the ignoreExisting flag
            if (!e.getErrorCode().equals(ALREADY_EXISTS.toErrorCode()) || !statement.isNotExists()) {
                throw e;
            }
        }

        CubeMetadataBuilder builder = optionalCubeMetaStore.get().getBuilder(cubeName.toString(), tableName.toString());
        groupingSet.forEach(dimension -> builder.addDimensionColumn(dimension, dimension));
        aggregations.forEach((column, aggregationSignature) -> builder.addAggregationColumn(column, aggregationSignature.getFunction(), aggregationSignature.getDimension(), aggregationSignature.isDistinct()));
        builder.addGroup(new HashSet<>(groupingSet));
        //Status and Table modified time will be updated on the first insert into the cube
        builder.setCubeStatus(CubeStatus.INACTIVE);
        builder.setTableLastUpdatedTime(-1L);
        statement.getSourceFilter().ifPresent(sourceTablePredicate -> {
            sourceTablePredicate = Coercer.addCoercions(sourceTablePredicate, analysis);
            builder.withCubeFilter(new CubeFilter(ExpressionFormatter.formatExpression(sourceTablePredicate, Optional.empty())));
        });
        builder.setCubeLastUpdatedTime(System.currentTimeMillis());
        optionalCubeMetaStore.get().persist(builder.build());
        return immediateFuture(null);
    }

    private Analysis analyzeStatement(Statement statement, Session session, Metadata metadata, AccessControl accessControl, List<Expression> parameters, WarningCollector warningCollector)
    {
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, accessControl, Optional.empty(), parameters, warningCollector, cubeManager);
        return analyzer.analyze(statement);
    }
}
