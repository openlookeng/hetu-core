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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.prestosql.Session;
import io.prestosql.connector.DataCenterUtility;
import io.prestosql.dynamicfilter.DynamicFilterCacheManager;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Split;
import io.prestosql.metadata.TableHandle;
import io.prestosql.spi.Page;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.dynamicfilter.DynamicFilterSupplier;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.util.BloomFilter;
import io.prestosql.split.EmptySplit;
import io.prestosql.split.EmptySplitPageSource;
import io.prestosql.split.PageSourceProvider;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.statestore.StateStoreProvider;

import java.io.Closeable;
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
import static io.prestosql.SystemSessionProperties.isCrossRegionDynamicFilterEnabled;
import static java.util.Objects.requireNonNull;

public class TableScanOperator
        implements SourceOperator, Closeable
{
    public static class TableScanOperatorFactory
            implements SourceOperatorFactory, WorkProcessorSourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final PageSourceProvider pageSourceProvider;
        private final TableHandle table;
        private final List<ColumnHandle> columns;
        private final List<Type> types;
        private final DataSize minOutputPageSize;
        private final int minOutputPageRowCount;
        private boolean closed;
        private Optional<TableScanNode> tableScanNodeOptional = Optional.empty();
        private Optional<StateStoreProvider> stateStoreProviderOptional = Optional.empty();
        private Optional<QueryId> queryIdOptional = Optional.empty();
        private Optional<Metadata> metadataOptional = Optional.empty();
        private Optional<DynamicFilterCacheManager> dynamicFilterCacheManagerOptional = Optional.empty();

        public TableScanOperatorFactory(
                Session session,
                int operatorId,
                PlanNode sourceNode,
                PageSourceProvider pageSourceProvider,
                TableHandle table,
                Iterable<ColumnHandle> columns,
                List<Type> types,
                StateStoreProvider stateStoreProvider,
                Metadata metadata,
                DynamicFilterCacheManager dynamicFilterCacheManager,
                DataSize minOutputPageSize,
                int minOutputPageRowCount)
        {
            this(operatorId, sourceNode.getId(), pageSourceProvider, table, columns, types, minOutputPageSize, minOutputPageRowCount);
            if (isCrossRegionDynamicFilterEnabled(session)) {
                if (sourceNode instanceof TableScanNode) {
                    tableScanNodeOptional = Optional.of((TableScanNode) sourceNode);
                }
                if (stateStoreProvider != null) {
                    stateStoreProviderOptional = Optional.of(stateStoreProvider);
                }
                this.queryIdOptional = Optional.of(session.getQueryId());
                this.metadataOptional = Optional.of(metadata);
                this.dynamicFilterCacheManagerOptional = Optional.of(dynamicFilterCacheManager);
            }
        }

        public TableScanOperatorFactory(
                int operatorId,
                PlanNodeId sourceId,
                PageSourceProvider pageSourceProvider,
                TableHandle table,
                Iterable<ColumnHandle> columns,
                List<Type> types,
                DataSize minOutputPageSize,
                int minOutputPageRowCount)
        {
            this.operatorId = operatorId;
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
            this.table = requireNonNull(table, "table is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.types = requireNonNull(types, "types is null");
            this.minOutputPageSize = requireNonNull(minOutputPageSize, "minOutputPageSize is null");
            this.minOutputPageRowCount = minOutputPageRowCount;
        }

        @Override
        public int getOperatorId()
        {
            return operatorId;
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public String getOperatorType()
        {
            return TableScanOperator.class.getSimpleName();
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, sourceId, getOperatorType());
            if (table.getConnectorHandle().isSuitableForPushdown()) {
                return new WorkProcessorSourceOperatorAdapter(operatorContext, this);
            }

            return new TableScanOperator(
                    operatorContext,
                    sourceId,
                    pageSourceProvider,
                    table,
                    columns,
                    tableScanNodeOptional,
                    stateStoreProviderOptional,
                    queryIdOptional,
                    metadataOptional,
                    dynamicFilterCacheManagerOptional);
        }

        @Override
        public WorkProcessorSourceOperator create(
                Session session,
                MemoryTrackingContext memoryTrackingContext,
                DriverYieldSignal yieldSignal,
                WorkProcessor<Split> splits)
        {
            return new TableScanWorkProcessorOperator(
                    session,
                    memoryTrackingContext,
                    splits,
                    pageSourceProvider,
                    table,
                    columns,
                    types,
                    minOutputPageSize,
                    minOutputPageRowCount,
                    tableScanNodeOptional,
                    stateStoreProviderOptional,
                    metadataOptional,
                    dynamicFilterCacheManagerOptional,
                    queryIdOptional);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId planNodeId;
    private final PageSourceProvider pageSourceProvider;
    private final TableHandle table;
    private final List<ColumnHandle> columns;
    private final LocalMemoryContext systemMemoryContext;
    private final SettableFuture<?> blocked = SettableFuture.create();

    private Split split;
    private ConnectorPageSource source;

    private boolean finished;

    private long completedBytes;
    private long readTimeNanos;
    Optional<TableScanNode> tableScanNodeOptional;
    Optional<StateStoreProvider> stateStoreProviderOptional;
    Optional<QueryId> queryIdOptional;
    Map<String, byte[]> bloomFiltersBackup = new HashMap<>();
    Map<Integer, BloomFilter> bloomFilters = new ConcurrentHashMap<>();
    Optional<Metadata> metadataOptional;
    Optional<DynamicFilterCacheManager> dynamicFilterCacheManagerOptional;
    boolean existsCrossFilter;
    boolean isDcTable;

    public TableScanOperator(
            OperatorContext operatorContext,
            PlanNodeId planNodeId,
            PageSourceProvider pageSourceProvider,
            TableHandle table,
            Iterable<ColumnHandle> columns,
            Optional<TableScanNode> tableScanNodeOptional,
            Optional<StateStoreProvider> stateStoreProviderOptional,
            Optional<QueryId> queryIdOptional,
            Optional<Metadata> metadataOptional,
            Optional<DynamicFilterCacheManager> dynamicFilterCacheManagerOptional)
    {
        this(operatorContext, planNodeId, pageSourceProvider, table, columns);
        this.tableScanNodeOptional = tableScanNodeOptional;
        this.stateStoreProviderOptional = stateStoreProviderOptional;
        this.queryIdOptional = queryIdOptional;
        this.metadataOptional = metadataOptional;
        this.dynamicFilterCacheManagerOptional = dynamicFilterCacheManagerOptional;

        if (queryIdOptional.isPresent() && stateStoreProviderOptional.isPresent() && stateStoreProviderOptional.get().getStateStore() != null) {
            existsCrossFilter = true;

            if (metadataOptional.isPresent() && tableScanNodeOptional.isPresent()) {
                if (DataCenterUtility.isDCCatalog(metadataOptional.get(), tableScanNodeOptional.get().getTable().getCatalogName().getCatalogName())) {
                    isDcTable = true;
                }
            }
        }
    }

    public TableScanOperator(
            OperatorContext operatorContext,
            PlanNodeId planNodeId,
            PageSourceProvider pageSourceProvider,
            TableHandle table,
            Iterable<ColumnHandle> columns)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.systemMemoryContext = operatorContext.newLocalSystemMemoryContext(TableScanOperator.class.getSimpleName());
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return planNodeId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        requireNonNull(split, "split is null");
        checkState(this.split == null, "Table scan split already set");

        if (finished) {
            return Optional::empty;
        }

        this.split = split;

        Object splitInfo = split.getInfo();
        if (splitInfo != null) {
            operatorContext.setInfoSupplier(() -> new SplitOperatorInfo(splitInfo));
        }

        blocked.set(null);

        if (split.getConnectorSplit() instanceof EmptySplit) {
            source = new EmptySplitPageSource();
        }

        return () -> {
            if (source instanceof UpdatablePageSource) {
                return Optional.of((UpdatablePageSource) source);
            }
            return Optional.empty();
        };
    }

    @Override
    public void noMoreSplits()
    {
        if (split == null) {
            finished = true;
        }
        blocked.set(null);
    }

    @Override
    public void close()
    {
        finish();
    }

    @Override
    public void finish()
    {
        finished = true;
        blocked.set(null);

        if (source != null) {
            try {
                source.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            systemMemoryContext.setBytes(source.getSystemMemoryUsage());
        }
    }

    @Override
    public boolean isFinished()
    {
        if (!finished) {
            finished = (source != null) && source.isFinished();
            if (source != null) {
                systemMemoryContext.setBytes(source.getSystemMemoryUsage());
            }
        }

        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (!blocked.isDone()) {
            return blocked;
        }
        if (source != null) {
            CompletableFuture<?> pageSourceBlocked = source.isBlocked();
            return pageSourceBlocked.isDone() ? NOT_BLOCKED : toListenableFuture(pageSourceBlocked);
        }
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " can not take input");
    }

    @Override
    public Page getOutput()
    {
        if (split == null) {
            return null;
        }
        if (source == null) {
            if (isDcTable) {
                source = pageSourceProvider.createPageSource(operatorContext.getSession(),
                        split,
                        table,
                        columns,
                        Optional.of(new DynamicFilterSupplier(BloomFilterUtils.getCrossRegionDynamicFilterSupplier(dynamicFilterCacheManagerOptional.get(), queryIdOptional.get().getId(), tableScanNodeOptional.get()), System.currentTimeMillis(), 0L)));
            }
            else {
                source = pageSourceProvider.createPageSource(operatorContext.getSession(), split, table, columns, Optional.empty());
            }
        }

        Page page = source.getNextPage();
        if (page != null) {
            // assure the page is in memory before handing to another operator
            page = page.getLoadedPage();

            // update operator stats
            long endCompletedBytes = source.getCompletedBytes();
            long endReadTimeNanos = source.getReadTimeNanos();
            operatorContext.recordPhysicalInputWithTiming(endCompletedBytes - completedBytes, page.getPositionCount(), endReadTimeNanos - readTimeNanos);
            operatorContext.recordProcessedInput(page.getSizeInBytes(), page.getPositionCount());
            completedBytes = endCompletedBytes;
            readTimeNanos = endReadTimeNanos;

            // pull bloomFilter from stateStore and filter page
            if (existsCrossFilter) {
                try {
                    page = filter(page);
                }
                catch (Throwable e) {
                    // ignore
                }
            }
        }

        // updating system memory usage should happen after page is loaded.
        systemMemoryContext.setBytes(source.getSystemMemoryUsage());

        return page;
    }

    private Page filter(Page page)
    {
        BloomFilterUtils.updateBloomFilter(queryIdOptional, isDcTable, stateStoreProviderOptional, tableScanNodeOptional, dynamicFilterCacheManagerOptional, bloomFiltersBackup, bloomFilters);

        if (!bloomFilters.isEmpty()) {
            page = BloomFilterUtils.filter(page, bloomFilters);
        }
        return page;
    }
}
