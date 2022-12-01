/*
 * Copyright (c) 2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.cache.CachedDataStorageProvider;
import io.prestosql.cache.elements.CachedDataKey;
import io.prestosql.cache.elements.CachedDataStorage;
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
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.sql.planner.Partitioning;
import io.prestosql.sql.planner.PartitioningHandle;
import io.prestosql.sql.planner.PartitioningScheme;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.CacheTableFinishNode;
import io.prestosql.sql.planner.plan.CacheTableWriterNode;
import io.prestosql.sql.planner.plan.OutputNode;
import io.prestosql.sql.planner.plan.SimplePlanRewriter;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.sql.tree.QualifiedName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.SystemSessionProperties.isCTEResultCacheEnabled;
import static io.prestosql.metadata.MetadataUtil.toSchemaTableName;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static java.util.Objects.requireNonNull;

public class AddCacheTableWriterAboveCTEOptimizer
    implements PlanOptimizer
{
    private final Metadata metadata;

    public AddCacheTableWriterAboveCTEOptimizer(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanSymbolAllocator planSymbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        return plan;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanSymbolAllocator planSymbolAllocator,
                             PlanNodeIdAllocator idAllocator, WarningCollector warningCollector,
                             CachedDataStorageProvider cdsProvider)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(planSymbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(cdsProvider, "cachedDataStorage is null");
        if (isCTEResultCacheEnabled(session)) {
            return SimplePlanRewriter.rewriteWith(new Rewriter(session, idAllocator, planSymbolAllocator, types, cdsProvider), plan, null);
        }
        return plan;
    }

    private class Rewriter
            extends SimplePlanRewriter<Void>
    {

        private final Session session;
        private final PlanNodeIdAllocator planNodeIdAllocator;
        private final PlanSymbolAllocator planSymbolAllocator;
        private final TypeProvider typeProvider;
        private final CachedDataStorageProvider cachedDataStorageProvider;

        public Rewriter(Session session, PlanNodeIdAllocator planNodeIdAllocator, PlanSymbolAllocator planSymbolAllocator, TypeProvider typeProvider, CachedDataStorageProvider cachedDataStorageProvider)
        {
            this.session = session;
            this.planNodeIdAllocator = planNodeIdAllocator;
            this.planSymbolAllocator = planSymbolAllocator;
            this.typeProvider = typeProvider;
            this.cachedDataStorageProvider = cachedDataStorageProvider;
        }

        @Override
        public PlanNode visitCTEScan(CTEScanNode node, RewriteContext<Void> context)
        {
            if (!isCTEResultCacheEnabled(session)) {
                return node;
            }

            CachedDataKey.Builder keyBuilder = cachedDataStorageProvider.getCachedDataKeyBuilder(node.getCteRefName());

            /* use Cache provider for cache entry lookup */
            CachedDataStorage cds = cachedDataStorageProvider.getOrCreateCachedDataKey(keyBuilder.build());
            if (cds == null)
            {
                return node;
            }

            QualifiedObjectName destination = QualifiedObjectName.valueOf(cds.getDataTable());
            if (cds.isCommitted()) {
                Optional<TableScanNode> tableScanNode = isTableExists(node, destination);

                //todo(Surya): add check for eligibilty of the table.
                if (tableScanNode.isPresent()) {
                    return tableScanNode.get();
                }

                /* Incase cache store got eliminated; re-cache to same storage */
                cds.reset();
            }

            CatalogName catalogName = metadata.getCatalogHandle(session, destination.getCatalogName())
                    .orElseThrow(() -> new PrestoException(NOT_FOUND, "Catalog does not exist: " + "hive"));

            Map<String, Object> properties = metadata.getTablePropertyManager().getProperties(
                    catalogName,
                    destination.getCatalogName(),
                    ImmutableMap.of(),
                    session,
                    metadata,
                    ImmutableList.of());
            ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
            for (Symbol symbol : node.getSource().getOutputSymbols()) {
                columns.add(new ColumnMetadata(symbol.getName(), typeProvider.get(symbol)));
            }
            ConnectorTableMetadata tableMetadata =  new ConnectorTableMetadata(toSchemaTableName(destination), columns.build(), properties, Optional.empty());
            Optional<NewTableLayout> newTableLayout = metadata.getNewTableLayout(session, destination.getCatalogName(), tableMetadata);
            Optional<PartitioningScheme> partitioningScheme = Optional.empty();
            if (newTableLayout.isPresent()) {
                List<Symbol> partitionFunctionArguments = new ArrayList<>();
                List<Symbol> outputLayout = new ArrayList<>(node.getSource().getOutputSymbols());

                PartitioningHandle partitioningHandle = newTableLayout.get().getPartitioning()
                        .orElse(FIXED_HASH_DISTRIBUTION);

                partitioningScheme = Optional.of(new PartitioningScheme(
                        Partitioning.create(partitioningHandle, partitionFunctionArguments),
                        outputLayout));
            }

            TableWriterNode.WriterTarget target = new TableWriterNode.CreateReference(destination.getCatalogName(), tableMetadata, newTableLayout);

            CacheTableWriterNode cacheTableWriterNode = new CacheTableWriterNode(
                    planNodeIdAllocator.getNextId(),
                    node.getSource(),
                    target,
                    planSymbolAllocator.newSymbol("partialrows", BIGINT),
                    planSymbolAllocator.newSymbol("fragment", VARBINARY),
                    node.getSource().getOutputSymbols(),
                    tableMetadata.getColumns().stream().map(columnMetadata -> columnMetadata.getName()).collect(Collectors.toList()),
                    partitioningScheme);

            CacheTableFinishNode commitNode = new CacheTableFinishNode(
                    planNodeIdAllocator.getNextId(),
                    cacheTableWriterNode,
                    target,
                    planSymbolAllocator.newSymbol("rows", BIGINT),
                    Optional.empty(),
                    Optional.of(cds));

            return new CTEScanNode(planNodeIdAllocator.getNextId(), commitNode, node.getOutputSymbols(), node.getPredicate(), node.getCteRefName(), node.getConsumerPlans(), node.getCommonCTERefNum());
        }

        private Optional<TableScanNode> isTableExists(CTEScanNode node, QualifiedObjectName cacheStore)
        {
            Optional<TableHandle> targetTable = metadata.getTableHandle(session, cacheStore);
            if (targetTable.isPresent()) {
                Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, targetTable.get());
                ImmutableList.Builder<Symbol> tableScanOutputs = ImmutableList.builder();
                ImmutableMap.Builder<Symbol, ColumnHandle> symbolToColumnHandle = ImmutableMap.builder();
                ImmutableMap.Builder<String, Symbol> columnNameToSymbol = ImmutableMap.builder();
                TableMetadata tableMetadata = metadata.getTableMetadata(session, targetTable.get());
                List<ColumnMetadata> columns = tableMetadata.getColumns().stream().filter(columnMetadata -> !columnMetadata.isHidden()).collect(Collectors.toList());
                List<Symbol> outputSymbols = node.getOutputSymbols();
                checkArgument(outputSymbols.size() == columns.size(), "output symbols not matching with columns");
                for (int count = 0; count < columns.size(); count++) {
                    ColumnMetadata column = columns.get(count);
                    if (column.isHidden()) {
                        continue;
                    }

                    Symbol symbol = outputSymbols.get(count);
                    tableScanOutputs.add(symbol);
                    symbolToColumnHandle.put(symbol, columnHandles.get(column.getName()));
                    columnNameToSymbol.put(column.getName(), symbol);
                }
                TableScanNode tableScanNode = TableScanNode.newInstance(planNodeIdAllocator.getNextId(), targetTable.get(), tableScanOutputs.build(),
                        symbolToColumnHandle.build(), ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT,
                        new UUID(0, 0), 0, false);
                return Optional.of(tableScanNode);
            }
            else {
                return Optional.empty();
            }
        }
    }
}
