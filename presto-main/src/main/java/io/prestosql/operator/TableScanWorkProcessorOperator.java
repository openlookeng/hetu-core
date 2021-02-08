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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.connector.DataCenterUtility;
import io.prestosql.dynamicfilter.DynamicFilterCacheManager;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Split;
import io.prestosql.operator.WorkProcessor.ProcessState;
import io.prestosql.operator.WorkProcessor.TransformationState;
import io.prestosql.spi.Page;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.dynamicfilter.DynamicFilterSupplier;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.util.BloomFilter;
import io.prestosql.split.EmptySplit;
import io.prestosql.split.EmptySplitPageSource;
import io.prestosql.split.PageSourceProvider;
import io.prestosql.statestore.StateStoreProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.operator.PageUtils.recordMaterializedBytes;
import static io.prestosql.operator.project.MergePages.mergePages;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class TableScanWorkProcessorOperator
        implements WorkProcessorSourceOperator
{
    private final WorkProcessor<Page> pages;
    private final SplitToPages splitToPages;

    public TableScanWorkProcessorOperator(
            Session session,
            MemoryTrackingContext memoryTrackingContext,
            WorkProcessor<Split> splits,
            PageSourceProvider pageSourceProvider,
            TableHandle table,
            Iterable<ColumnHandle> columns,
            Iterable<Type> types,
            DataSize minOutputPageSize,
            int minOutputPageRowCount,
            Optional<TableScanNode> tableScanNodeOptional,
            Optional<StateStoreProvider> stateStoreProviderOptional,
            Optional<Metadata> metadataOptional,
            Optional<DynamicFilterCacheManager> dynamicFilterCacheManagerOptional,
            Optional<QueryId> queryIdOptional)
    {
        this.splitToPages = new SplitToPages(
                session,
                pageSourceProvider,
                table,
                columns,
                types,
                memoryTrackingContext.aggregateSystemMemoryContext(),
                minOutputPageSize,
                minOutputPageRowCount,
                tableScanNodeOptional,
                stateStoreProviderOptional,
                metadataOptional,
                dynamicFilterCacheManagerOptional,
                queryIdOptional);
        this.pages = splits.flatTransform(splitToPages);
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return pages;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> getUpdatablePageSourceSupplier()
    {
        return splitToPages.getUpdatablePageSourceSupplier();
    }

    @Override
    public DataSize getPhysicalInputDataSize()
    {
        return splitToPages.getPhysicalInputDataSize();
    }

    @Override
    public long getPhysicalInputPositions()
    {
        return splitToPages.getPhysicalInputPositions();
    }

    @Override
    public DataSize getInputDataSize()
    {
        return splitToPages.getInputDataSize();
    }

    @Override
    public long getInputPositions()
    {
        return splitToPages.getInputPositions();
    }

    @Override
    public Duration getReadTime()
    {
        return splitToPages.getReadTime();
    }

    @Override
    public void close()
            throws Exception
    {
        splitToPages.close();
    }

    private static class SplitToPages
            implements WorkProcessor.Transformation<Split, WorkProcessor<Page>>
    {
        final Session session;
        final PageSourceProvider pageSourceProvider;
        final TableHandle table;
        final List<ColumnHandle> columns;
        final List<Type> types;
        final AggregatedMemoryContext aggregatedMemoryContext;
        final DataSize minOutputPageSize;
        final int minOutputPageRowCount;
        private final AggregatedMemoryContext localAggregatedMemoryContext;
        private final LocalMemoryContext memoryContext;
        final Optional<TableScanNode> tableScanNodeOptional;
        final Optional<StateStoreProvider> stateStoreProviderOptional;
        final Optional<QueryId> queryIdOptional;
        final Optional<DynamicFilterCacheManager> dynamicFilterCacheManagerOptional;
        boolean isDcTable;

        long processedBytes;
        long processedPositions;

        ConnectorPageSource source;

        SplitToPages(
                Session session,
                PageSourceProvider pageSourceProvider,
                TableHandle table,
                Iterable<ColumnHandle> columns,
                Iterable<Type> types,
                AggregatedMemoryContext aggregatedMemoryContext,
                DataSize minOutputPageSize,
                int minOutputPageRowCount,
                Optional<TableScanNode> tableScanNodeOptional,
                Optional<StateStoreProvider> stateStoreProviderOptional,
                Optional<Metadata> metadataOptional,
                Optional<DynamicFilterCacheManager> dynamicFilterCacheManagerOptional,
                Optional<QueryId> queryIdOptional)
        {
            this.session = requireNonNull(session, "session is null");
            this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
            this.table = requireNonNull(table, "table is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.aggregatedMemoryContext = requireNonNull(aggregatedMemoryContext, "aggregatedMemoryContext is null");
            this.minOutputPageSize = requireNonNull(minOutputPageSize, "minOutputPageSize is null");
            this.minOutputPageRowCount = minOutputPageRowCount;
            this.memoryContext = aggregatedMemoryContext.newLocalMemoryContext(TableScanWorkProcessorOperator.class.getSimpleName());
            this.localAggregatedMemoryContext = newSimpleAggregatedMemoryContext();
            this.tableScanNodeOptional = tableScanNodeOptional;
            this.stateStoreProviderOptional = stateStoreProviderOptional;
            this.queryIdOptional = queryIdOptional;
            this.dynamicFilterCacheManagerOptional = dynamicFilterCacheManagerOptional;

            if (metadataOptional.isPresent() && tableScanNodeOptional.isPresent()) {
                if (DataCenterUtility.isDCCatalog(metadataOptional.get(), tableScanNodeOptional.get().getTable().getCatalogName().getCatalogName())) {
                    isDcTable = true;
                }
            }
        }

        @Override
        public TransformationState<WorkProcessor<Page>> process(Split split)
        {
            if (split == null) {
                memoryContext.close();
                return TransformationState.finished();
            }

            checkState(source == null, "Table scan split already set");
            if (split.getConnectorSplit() instanceof EmptySplit) {
                source = new EmptySplitPageSource();
            }
            else {
                if (isDcTable) {
                    source = pageSourceProvider.createPageSource(session,
                            split,
                            table,
                            columns,
                            Optional.of(new DynamicFilterSupplier(BloomFilterUtils.getCrossRegionDynamicFilterSupplier(dynamicFilterCacheManagerOptional.get(), queryIdOptional.get().getId(), tableScanNodeOptional.get()), System.currentTimeMillis(), 0L)));
                }
                else {
                    source = pageSourceProvider.createPageSource(session, split, table, columns, Optional.empty());
                }
            }
            if (source.needMergingForPages()) {
                return TransformationState.ofResult(
                        WorkProcessor.create(new ConnectorPageSourceToPages(aggregatedMemoryContext, source, tableScanNodeOptional, stateStoreProviderOptional, dynamicFilterCacheManagerOptional, queryIdOptional, isDcTable))
                                .map(page -> {
                                    processedPositions += page.getPositionCount();
                                    return recordMaterializedBytes(page, sizeInBytes -> processedBytes += sizeInBytes);
                                })
                                .transformProcessor(processor -> mergePages(types, minOutputPageSize.toBytes(), minOutputPageRowCount, processor, localAggregatedMemoryContext))
                                .withProcessStateMonitor(state -> memoryContext.setBytes(localAggregatedMemoryContext.getBytes())));
            }

            return TransformationState.ofResult(
                    WorkProcessor.create(new ConnectorPageSourceToPages(aggregatedMemoryContext, source, tableScanNodeOptional, stateStoreProviderOptional, dynamicFilterCacheManagerOptional, queryIdOptional, isDcTable))
                            .map(page -> {
                                processedPositions += page.getPositionCount();
                                return recordMaterializedBytes(page, sizeInBytes -> processedBytes += sizeInBytes);
                            }));
        }

        Supplier<Optional<UpdatablePageSource>> getUpdatablePageSourceSupplier()
        {
            return () -> {
                if (source instanceof UpdatablePageSource) {
                    return Optional.of((UpdatablePageSource) source);
                }
                return Optional.empty();
            };
        }

        DataSize getPhysicalInputDataSize()
        {
            if (source == null) {
                return new DataSize(0, BYTE);
            }

            return new DataSize(source.getCompletedBytes(), BYTE);
        }

        long getPhysicalInputPositions()
        {
            return processedPositions;
        }

        DataSize getInputDataSize()
        {
            return new DataSize(processedBytes, BYTE);
        }

        long getInputPositions()
        {
            return processedPositions;
        }

        Duration getReadTime()
        {
            if (source == null) {
                return new Duration(0, NANOSECONDS);
            }

            return new Duration(source.getReadTimeNanos(), NANOSECONDS);
        }

        void close()
        {
            if (source != null) {
                try {
                    source.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    private static class ConnectorPageSourceToPages
            implements WorkProcessor.Process<Page>
    {
        final ConnectorPageSource pageSource;
        final LocalMemoryContext pageSourceMemoryContext;
        final Optional<TableScanNode> tableScanNodeOptional;
        final Optional<StateStoreProvider> stateStoreProviderOptional;
        final Optional<QueryId> queryIdOptional;
        final Optional<DynamicFilterCacheManager> dynamicFilterCacheManagerOptional;
        Map<String, byte[]> bloomFiltersBackup = new HashMap<>();
        Map<Integer, BloomFilter> bloomFilters = new ConcurrentHashMap<>();
        boolean existsCrossFilter;
        boolean isDcTable;

        ConnectorPageSourceToPages(AggregatedMemoryContext aggregatedMemoryContext, ConnectorPageSource pageSource, Optional<TableScanNode> tableScanNodeOptional, Optional<StateStoreProvider> stateStoreProviderOptional, Optional<DynamicFilterCacheManager> dynamicFilterCacheManagerOptional, Optional<QueryId> queryIdOptional, boolean isDcTable)
        {
            this.pageSource = pageSource;
            this.pageSourceMemoryContext = aggregatedMemoryContext
                    .newLocalMemoryContext(TableScanWorkProcessorOperator.class.getSimpleName());
            this.tableScanNodeOptional = tableScanNodeOptional;
            this.stateStoreProviderOptional = stateStoreProviderOptional;
            this.queryIdOptional = queryIdOptional;
            this.isDcTable = isDcTable;
            this.dynamicFilterCacheManagerOptional = dynamicFilterCacheManagerOptional;
            if (queryIdOptional.isPresent() && dynamicFilterCacheManagerOptional.isPresent()) {
                existsCrossFilter = true;
            }
        }

        @Override
        public ProcessState<Page> process()
        {
            if (pageSource.isFinished()) {
                pageSourceMemoryContext.close();
                return ProcessState.finished();
            }

            CompletableFuture<?> isBlocked = pageSource.isBlocked();
            if (!isBlocked.isDone()) {
                return ProcessState.blocked(toListenableFuture(isBlocked));
            }

            Page page = pageSource.getNextPage();
            pageSourceMemoryContext.setBytes(pageSource.getSystemMemoryUsage());

            if (page == null) {
                if (pageSource.isFinished()) {
                    pageSourceMemoryContext.close();
                    return ProcessState.finished();
                }
                else {
                    return ProcessState.yield();
                }
            }

            // pull bloomFilter from stateStore and filter page
            if (existsCrossFilter) {
                try {
                    page = filter(page);
                }
                catch (Throwable e) {
                    // ignore
                }
            }

            // TODO: report operator stats
            return ProcessState.ofResult(page);
        }

        private Page filter(Page page)
        {
            if (bloomFilters.isEmpty()) {
                BloomFilterUtils.updateBloomFilter(queryIdOptional, isDcTable, stateStoreProviderOptional, tableScanNodeOptional, dynamicFilterCacheManagerOptional, bloomFiltersBackup, bloomFilters);
            }
            if (!bloomFilters.isEmpty()) {
                page = BloomFilterUtils.filter(page, bloomFilters);
            }
            return page;
        }
    }
}
