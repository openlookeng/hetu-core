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
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.prestosql.Session;
import io.prestosql.connector.DataCenterUtility;
import io.prestosql.dynamicfilter.DynamicFilterCacheManager;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Split;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.dynamicfilter.DynamicFilterSupplier;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.util.BloomFilter;
import io.prestosql.spiller.Spiller;
import io.prestosql.spiller.SpillerFactory;
import io.prestosql.split.EmptySplit;
import io.prestosql.split.EmptySplitPageSource;
import io.prestosql.split.PageSourceProvider;
import io.prestosql.statestore.StateStoreProvider;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.prestosql.SystemSessionProperties.isCrossRegionDynamicFilterEnabled;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_CONSUMER;
import static io.prestosql.spi.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT;
import static io.prestosql.spi.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_PRODUCER;
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
        private ReuseExchangeOperator.STRATEGY strategy;
        private UUID reuseTableScanMappingId;
        private boolean spillEnabled;
        private final Optional<SpillerFactory> spillerFactory;
        private Integer spillerThreshold;
        private Integer consumerTableScanNodeCount;

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
                int minOutputPageRowCount,
                ReuseExchangeOperator.STRATEGY strategy,
                UUID reuseTableScanMappingId,
                boolean spillEnabled,
                Optional<SpillerFactory> spillerFactory,
                Integer spillerThreshold,
                Integer consumerTableScanNodeCount)
        {
            this(operatorId, sourceNode.getId(), pageSourceProvider, table, columns, types, minOutputPageSize, minOutputPageRowCount, strategy, reuseTableScanMappingId, spillEnabled, spillerFactory, spillerThreshold, consumerTableScanNodeCount);
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
                int minOutputPageRowCount,
                ReuseExchangeOperator.STRATEGY strategy,
                UUID reuseTableScanMappingId,
                boolean spillEnabled,
                Optional<SpillerFactory> spillerFactory,
                Integer spillerThreshold,
                Integer consumerTableScanNodeCount)
        {
            this.operatorId = operatorId;
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
            this.table = requireNonNull(table, "table is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.types = requireNonNull(types, "types is null");
            this.minOutputPageSize = requireNonNull(minOutputPageSize, "minOutputPageSize is null");
            this.minOutputPageRowCount = minOutputPageRowCount;
            this.strategy = strategy;
            this.reuseTableScanMappingId = reuseTableScanMappingId;
            this.spillEnabled = spillEnabled;
            this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
            this.spillerThreshold = spillerThreshold;
            this.consumerTableScanNodeCount = consumerTableScanNodeCount;
        }

        public ReuseExchangeOperator.STRATEGY getStrategy()
        {
            return strategy;
        }

//        public void setStrategy(ReuseExchangeOperator.STRATEGY strategy)
//        {
//            //todo: find usage
//            this.strategy = strategy;
//        }

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
                return new WorkProcessorSourceOperatorAdapter(operatorContext, this, REUSE_STRATEGY_DEFAULT, new UUID(0, 0), spillEnabled, types, spillerFactory, spillerThreshold, consumerTableScanNodeCount);
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
                    dynamicFilterCacheManagerOptional,
                    strategy,
                    reuseTableScanMappingId,
                    types,
                    spillEnabled,
                    spillerFactory,
                    spillerThreshold,
                    consumerTableScanNodeCount);
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

    private static final Logger LOG = Logger.get(TableScanOperator.class);
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

    private ReuseExchangeOperator.STRATEGY strategy;
    private UUID reuseTableScanMappingId;
    private static ConcurrentMap<String, Integer> sourceReuseTableScanMappingIdPositionIndexMap;
    private String sourceIdString;
    private final Optional<SpillerFactory> spillerFactory;
    private final List<Type> types;
    private boolean spillEnabled;
    private final long spillThreshold;
    private static ConcurrentMap<UUID, ReuseExchangeTableScanMappingIdState> reuseExchangeTableScanMappingIdUtilsMap = new ConcurrentHashMap<>();
    private ReuseExchangeTableScanMappingIdState reuseExchangeTableScanMappingIdState;
    private ListenableFuture<?> spillInProgress = immediateFuture(null);

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
            Optional<DynamicFilterCacheManager> dynamicFilterCacheManagerOptional,
            ReuseExchangeOperator.STRATEGY strategy,
            UUID reuseTableScanMappingId,
            List<Type> types,
            boolean spillEnabled,
            Optional<SpillerFactory> spillerFactory,
            Integer spillerThreshold,
            Integer consumerTableScanNodeCount)
    {
        this(operatorContext, planNodeId, pageSourceProvider, table, columns, strategy, reuseTableScanMappingId, types, spillEnabled, spillerFactory, spillerThreshold, consumerTableScanNodeCount);
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
            Iterable<ColumnHandle> columns,
            ReuseExchangeOperator.STRATEGY strategy,
            UUID reuseTableScanMappingId,
            List<Type> types,
            boolean spillEnabled,
            Optional<SpillerFactory> spillerFactory,
            Integer spillerThreshold,
            Integer consumerTableScanNodeCount)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.systemMemoryContext = operatorContext.newLocalSystemMemoryContext(TableScanOperator.class.getSimpleName());
        this.strategy = strategy;
        this.reuseTableScanMappingId = reuseTableScanMappingId;
        this.spillEnabled = spillEnabled;
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.spillThreshold = spillerThreshold;
        this.types = requireNonNull(types, "types is null");

        if (!strategy.equals(REUSE_STRATEGY_DEFAULT)) {
            if (strategy.equals(REUSE_STRATEGY_PRODUCER)) {
                reuseExchangeTableScanMappingIdUtilsMap.putIfAbsent(reuseTableScanMappingId, new ReuseExchangeTableScanMappingIdState(strategy, reuseTableScanMappingId, operatorContext, consumerTableScanNodeCount));
            }

            this.reuseExchangeTableScanMappingIdState = reuseExchangeTableScanMappingIdUtilsMap.get(reuseTableScanMappingId);

            if (strategy.equals(REUSE_STRATEGY_CONSUMER)) {
                sourceIdString = planNodeId.toString().concat(reuseTableScanMappingId.toString());
                reuseExchangeTableScanMappingIdUtilsMap.get(reuseTableScanMappingId).addToSourceNodeModifiedIdList(sourceIdString);
            }

            if (sourceReuseTableScanMappingIdPositionIndexMap == null) {
                sourceReuseTableScanMappingIdPositionIndexMap = new ConcurrentHashMap<>();
            }
        }
    }

    public ReuseExchangeOperator.STRATEGY getStrategy()
    {
        return this.strategy;
    }

    public boolean isNotSpilled()
    {
        int pagesWrittenCount = reuseExchangeTableScanMappingIdState.getPagesWrittenCount();

        if (pagesWrittenCount == 0) {
            // there was no spilling of data- either spilling is not used, or not enough data to spill
            return true;
        }

        return false;
    }

    public static void deleteSpilledFiles(UUID reuseTableScanMappingId)
    {
        if (reuseExchangeTableScanMappingIdUtilsMap.containsKey(reuseTableScanMappingId)) {
            if (reuseExchangeTableScanMappingIdUtilsMap.get(reuseTableScanMappingId).getSpiller().isPresent()) {
                reuseExchangeTableScanMappingIdUtilsMap.get(reuseTableScanMappingId).getSpiller().get().close();
            }
        }
    }

    private List<Iterator<Page>> getSpilledPages()
    {
        if (!reuseExchangeTableScanMappingIdState.getSpiller().isPresent()) {
            return ImmutableList.of();
        }
        return reuseExchangeTableScanMappingIdState.getSpiller().get().getSpills().stream().collect(toImmutableList());
    }

    private long totalPageSize(List<Page> pageList)
    {
        if (pageList != null && pageList.size() > 0) {
            long totalSize = 0;
            for (Page page : pageList) {
                totalSize += page.getSizeInBytes();
            }
            return totalSize;
        }
        return 0;
    }

    private Page getPage()
    {
        Page newPage;
        initIndex();

        synchronized (reuseExchangeTableScanMappingIdState) {
            int offset = sourceReuseTableScanMappingIdPositionIndexMap.get(sourceIdString);

            if (reuseExchangeTableScanMappingIdState.getPageCaches().isEmpty()
                    || offset >= reuseExchangeTableScanMappingIdState.getPageCaches().size()) {
                return null;
            }
            sourceReuseTableScanMappingIdPositionIndexMap.put(sourceIdString, offset + 1);
            newPage = reuseExchangeTableScanMappingIdState.getPageCaches().get(offset);

            if (offset + 1 == reuseExchangeTableScanMappingIdState.getPageCaches().size()) {
                int consumerTableScanNodeCount = reuseExchangeTableScanMappingIdState.getCurConsumerScanNodeRefCount() - 1;
                reuseExchangeTableScanMappingIdState.setCurConsumerScanNodeRefCount(consumerTableScanNodeCount);
                if (consumerTableScanNodeCount == 0) {
                    reuseExchangeTableScanMappingIdState.getPageCaches().clear();
                    unSpillData();
                }
            }
        }

        return newPage;
    }

    private void unSpillData()
    {
        boolean isUnSpill = false;
        if (reuseExchangeTableScanMappingIdState.getPagesWrittenCount() != 0) {
            // no page available in memory, unspill from disk and read
            List<Iterator<Page>> spilledPages = getSpilledPages();
            Iterator<Page> readPages;
            List<Page> pagesRead = new ArrayList<>();
            if (!spilledPages.isEmpty()) {
                for (int i = 0; i < spilledPages.size(); ++i) {
                    readPages = spilledPages.get(i);
                    readPages.forEachRemaining(pagesRead::add);
                }

                if (0 == pagesRead.size()) {
                    cleanupInErrorCase();
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "unSpill have no pages");
                }

                reuseExchangeTableScanMappingIdState.setPageCaches(pagesRead);
                reuseExchangeTableScanMappingIdState.setPagesWritten(reuseExchangeTableScanMappingIdState.getPagesWrittenCount() - pagesRead.size());
                if (reuseExchangeTableScanMappingIdState.getPagesWrittenCount() < 0) {
                    cleanupInErrorCase();
                    throw new ArrayIndexOutOfBoundsException("MORE PAGES READ THAN WRITTEN");
                }

                if (reuseExchangeTableScanMappingIdState.getPagesWrittenCount() == 0) {
                    deleteSpilledFiles(reuseTableScanMappingId);
                }
                isUnSpill = true;
            }
            LOG.debug("un spilled from disk %s sourceIdString:", sourceIdString);
        }
        if (!reuseExchangeTableScanMappingIdState.getPagesToSpill().isEmpty()) {
            reuseExchangeTableScanMappingIdState.getPageCaches().addAll(reuseExchangeTableScanMappingIdState.getPagesToSpill());
            reuseExchangeTableScanMappingIdState.setPagesToSpill(new ArrayList<>());
            LOG.debug("move from Spill cache to Page cache %s sourceIdString:", sourceIdString);
            isUnSpill = true;
        }

        if (isUnSpill) {
            for (String sourceId : reuseExchangeTableScanMappingIdState.getSourceNodeModifiedIdList()) {
                //reinitialize indexes for all consumers in this reuseTableScanMappingId here
                sourceReuseTableScanMappingIdPositionIndexMap.put(sourceId, 0);
            }

            //restore original count so that all consumers are now active again
            reuseExchangeTableScanMappingIdState.setCurConsumerScanNodeRefCount(reuseExchangeTableScanMappingIdState.getTotalConsumerScanNodeCount());
        }

        if (null != reuseExchangeTableScanMappingIdState.getOperatorContext() &&
                reuseExchangeTableScanMappingIdState.getPagesWrittenCount() == 0) {
            //destroy context for producer from here.
            reuseExchangeTableScanMappingIdState.getOperatorContext().destroy();
            reuseExchangeTableScanMappingIdState.setOperatorContext(null);
        }
    }

    private void setPage(Page page)
    {
        synchronized (reuseExchangeTableScanMappingIdUtilsMap) {
            if (!spillEnabled) {
                //spilling is not enabled so keep adding pages to cache in-memory
                List<Page> pageCachesList = reuseExchangeTableScanMappingIdState.getPageCaches();
                pageCachesList.add(page);
                reuseExchangeTableScanMappingIdState.setPageCaches(pageCachesList);
            }
            else {
                if (totalPageSize(reuseExchangeTableScanMappingIdState.getPageCaches()) < (spillThreshold / 2)) {
                    // if pageCaches hasn't reached spillThreshold/2, keep adding pages to it.
                    List<Page> pageCachesList = reuseExchangeTableScanMappingIdState.getPageCaches();
                    pageCachesList.add(page);
                    reuseExchangeTableScanMappingIdState.setPageCaches(pageCachesList);
                }
                else {
                    // no more space available in memory to store pages. pages will be spilled now
                    List<Page> pageSpilledList = reuseExchangeTableScanMappingIdState.getPagesToSpill();
                    pageSpilledList.add(page);
                    reuseExchangeTableScanMappingIdState.setPagesToSpill(pageSpilledList);

                    if (totalPageSize(pageSpilledList) >= (spillThreshold / 2)) {
                        if (!reuseExchangeTableScanMappingIdState.getSpiller().isPresent()) {
                            Optional<Spiller> spillObject = Optional.of(spillerFactory.get().create(types, operatorContext.getSpillContext(),
                                    operatorContext.newAggregateSystemMemoryContext()));
                            reuseExchangeTableScanMappingIdState.setSpiller(spillObject);
                        }

                        spillInProgress = reuseExchangeTableScanMappingIdState.getSpiller().get().spill(pageSpilledList.iterator());

                        try {
                            // blocking call to ensure spilling completes before we move forward
                            spillInProgress.get();
                        }
                        catch (InterruptedException | ExecutionException e) {
                            cleanupInErrorCase();
                            throw new PrestoException(GENERIC_INTERNAL_ERROR, e.getMessage(), e);
                        }

                        reuseExchangeTableScanMappingIdState.setPagesWritten(reuseExchangeTableScanMappingIdState.getPagesWrittenCount() + pageSpilledList.size());
                        reuseExchangeTableScanMappingIdState.clearPagesToSpill(); //clear the memory pressure once the data is spilled to disk
                    }
                }
            }
        }
    }

    private synchronized Boolean checkFinished()
    {
        synchronized (reuseExchangeTableScanMappingIdState) {
            boolean finishStatus = reuseExchangeTableScanMappingIdState.getPageCaches().isEmpty()
                    || (sourceReuseTableScanMappingIdPositionIndexMap.get(sourceIdString) != null
                    && sourceReuseTableScanMappingIdPositionIndexMap.get(sourceIdString) >= reuseExchangeTableScanMappingIdState.getPageCaches().size()
                    && reuseExchangeTableScanMappingIdState.getPagesWrittenCount() == 0
                    && reuseExchangeTableScanMappingIdState.getPagesToSpill().size() == 0);

            if (finishStatus && reuseExchangeTableScanMappingIdState.getCurConsumerScanNodeRefCount() <= 0) {
                // if it is last consumer remove ReuseExchangeTableScanMappingIdState from ConcurrentMap
                LOG.debug("checkFinished remove %s", reuseTableScanMappingId.toString());

                for (String sourceId : reuseExchangeTableScanMappingIdState.getSourceNodeModifiedIdList()) {
                    //reinitialize indexes for all consumers in this reuseTableScanMappingId here
                    sourceReuseTableScanMappingIdPositionIndexMap.remove(sourceId);
                }
                reuseExchangeTableScanMappingIdUtilsMap.remove(reuseTableScanMappingId);
            }
            return finishStatus;
        }
    }

    private void initIndex()
    {
        sourceReuseTableScanMappingIdPositionIndexMap.putIfAbsent(sourceIdString, 0);
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
            if (strategy.equals(REUSE_STRATEGY_CONSUMER)) {
                finished = checkFinished();
            }
            else {
                finished = (source != null) && source.isFinished();
                if (source != null) {
                    systemMemoryContext.setBytes(source.getSystemMemoryUsage());
                }
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
        if (strategy.equals(REUSE_STRATEGY_CONSUMER)) {
            return getPage();
        }
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

        if (strategy.equals(REUSE_STRATEGY_PRODUCER) && page != null) {
            setPage(page);
        }

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

    private void cleanupInErrorCase()
    {
        deleteSpilledFiles(reuseTableScanMappingId);
        if (null != reuseExchangeTableScanMappingIdState.getOperatorContext()) {
            //destroy context for producer from here.
            reuseExchangeTableScanMappingIdState.getOperatorContext().destroy();
            reuseExchangeTableScanMappingIdState.setOperatorContext(null);
        }
        reuseExchangeTableScanMappingIdUtilsMap.remove(reuseTableScanMappingId);
    }
}
