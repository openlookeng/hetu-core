/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.cache.CachedDataStorageProvider;
import io.prestosql.cache.elements.CachedDataKey;
import io.prestosql.cache.elements.CachedDataStorage;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableMetadata;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.CacheTableFinishNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.OutputNode;
import io.prestosql.sql.planner.plan.SimplePlanRewriter;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.SystemSessionProperties.isCTEResultCacheEnabled;

public class ResultCacheTableRead
        implements PlanOptimizer
{
    private final Metadata metadata;

    public ResultCacheTableRead(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanSymbolAllocator planSymbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        return plan;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanSymbolAllocator planSymbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector, CachedDataStorageProvider provider)
    {
        return SimplePlanRewriter.rewriteWith(new ResultCacheTableRead.Rewriter(session, planSymbolAllocator, idAllocator, provider), plan, null);
    }

    private class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final Session session;
        private final PlanNodeIdAllocator planNodeIdAllocator;
        private final PlanSymbolAllocator planSymbolAllocator;
        private final CachedDataStorageProvider cachedDataStorageProvider;

        public Rewriter(Session session, PlanSymbolAllocator planSymbolAllocator, PlanNodeIdAllocator planNodeIdAllocator, CachedDataStorageProvider provider)
        {
            this.session = session;
            this.planSymbolAllocator = planSymbolAllocator;
            this.planNodeIdAllocator = planNodeIdAllocator;
            this.cachedDataStorageProvider = provider;
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
            if (cds == null || !cds.isCommitted()) {
                return node;
            }
            QualifiedObjectName destination = QualifiedObjectName.valueOf(cds.getDataTable());
            Optional<TableHandle> targetTable = metadata.getTableHandle(session, destination);
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
                return tableScanNode;
            }
            else {
                throw new NoSuchElementException();
            }
        }

        private boolean isSourceCacheTableFinishNode(PlanNode node)
        {
            if (node.getSources().stream().anyMatch(planNode -> planNode instanceof CacheTableFinishNode)) {
                return true;
            }
            if (node instanceof OutputNode) {
                return isSourceCacheTableFinishNode(((OutputNode) node).getSource());
            }
            if (node instanceof ExchangeNode) {
                return isSourceCacheTableFinishNode(node.getSources().get(0));
            }
            if (node instanceof ProjectNode) {
                return isSourceCacheTableFinishNode(((ProjectNode) node).getSource());
            }
            return false;
        }
    }
}
