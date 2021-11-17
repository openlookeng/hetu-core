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
package io.prestosql.plugin.memory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.plugin.memory.data.MemoryTableManager;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.plugin.memory.MemoryErrorCode.MISSING_DATA;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class MemoryPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final MemoryTableManager pagesStore;
    private final HostAddress currentHostAddress;
    private final ConcurrentHashMap<Long, AtomicInteger> tableSinkCount;

    @Inject
    public MemoryPageSinkProvider(MemoryTableManager pagesStore, NodeManager nodeManager)
    {
        this(pagesStore, requireNonNull(nodeManager, "nodeManager is null").getCurrentNode().getHostAndPort());
    }

    @VisibleForTesting
    public MemoryPageSinkProvider(MemoryTableManager pagesStore, HostAddress currentHostAddress)
    {
        this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
        this.currentHostAddress = requireNonNull(currentHostAddress, "currentHostAddress is null");
        this.tableSinkCount = new ConcurrentHashMap<>();
    }

    /**
     * This method is used for CTAS (CREATE TABLE AS)
     */
    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        // MemoryWriteTableHandle is used for both CTAS and inserts
        MemoryWriteTableHandle memoryOutputTableHandle = (MemoryWriteTableHandle) outputTableHandle;
        long tableId = memoryOutputTableHandle.getTable();
        checkState(memoryOutputTableHandle.getActiveTableIds().contains(tableId));

        AtomicInteger sinkCount = tableSinkCount.computeIfAbsent(tableId, (k) -> new AtomicInteger(0));
        sinkCount.incrementAndGet();

        pagesStore.refreshTables(memoryOutputTableHandle.getActiveTableIds());
        pagesStore.initialize(tableId,
                memoryOutputTableHandle.isCompressionEnabled(),
                memoryOutputTableHandle.isAsyncProcessingEnabled(),
                memoryOutputTableHandle.getColumns(),
                memoryOutputTableHandle.getSortedBy(),
                memoryOutputTableHandle.getPartitionedBy(),
                memoryOutputTableHandle.getIndexColumns());

        try {
            pagesStore.validateSpillRoot();
        }
        catch (Exception e) {
            throw new PrestoException(GENERIC_USER_ERROR, "Failed writing data to memory.spill-path, ensure directory has correct permissions and free space is available.", e);
        }

        return new MemoryPageSink(pagesStore, currentHostAddress, tableId, sinkCount);
    }

    /**
     * This method is used when inserting data after table has already been created
     */
    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        // MemoryWriteTableHandle is used for both CTAS and inserts
        MemoryWriteTableHandle memoryOutputTableHandle = (MemoryWriteTableHandle) insertTableHandle;
        long tableId = memoryOutputTableHandle.getTable();
        checkState(memoryOutputTableHandle.getActiveTableIds().contains(tableId));

        AtomicInteger sinkCount = tableSinkCount.computeIfAbsent(tableId, (k) -> new AtomicInteger(0));
        sinkCount.incrementAndGet();

        pagesStore.refreshTables(memoryOutputTableHandle.getActiveTableIds());

        // Try restore since table was created and may have data
        // if restore fails, table was never initialized or data was lost from spill
        // initialize anyways since coordinator sends how many rows to expect and so an error will be reported in the data loss case
        try {
            pagesStore.restoreTable(tableId);
        }
        catch (PrestoException pe) {
            throw pe;
        }
        catch (FileNotFoundException e) {
            //
            pagesStore.initialize(tableId,
                    memoryOutputTableHandle.isCompressionEnabled(),
                    memoryOutputTableHandle.isAsyncProcessingEnabled(),
                    memoryOutputTableHandle.getColumns(),
                    memoryOutputTableHandle.getSortedBy(),
                    memoryOutputTableHandle.getPartitionedBy(),
                    memoryOutputTableHandle.getIndexColumns());
        }
        catch (Exception e) {
            throw new PrestoException(MISSING_DATA, "Failed to find/restore table on a worker", e);
        }

        try {
            pagesStore.validateSpillRoot();
        }
        catch (Exception e) {
            throw new PrestoException(GENERIC_USER_ERROR, "Failed writing data to memory.spill-path, ensure directory has correct permissions and free space is available.", e);
        }

        return new MemoryPageSink(pagesStore, currentHostAddress, tableId, sinkCount);
    }

    private static class MemoryPageSink
            implements ConnectorPageSink
    {
        private final MemoryTableManager tablesManager;
        private final HostAddress currentHostAddress;
        private final long tableId;
        private long addedRows;
        private AtomicInteger sinkCount;

        public MemoryPageSink(MemoryTableManager tablesManager, HostAddress currentHostAddress, long tableId, AtomicInteger sinkCount)
        {
            this.tablesManager = requireNonNull(tablesManager, "pagesStore is null");
            this.currentHostAddress = requireNonNull(currentHostAddress, "currentHostAddress is null");
            this.tableId = tableId;
            this.sinkCount = sinkCount;
        }

        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            tablesManager.add(tableId, page);
            addedRows += page.getPositionCount();
            return NOT_BLOCKED;
        }

        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            // there could be multiple writers in parallel, wait until the last one
            if (sinkCount.decrementAndGet() == 0) {
                tablesManager.finishUpdatingTable(tableId);
                // the JSON parser can't handle null keys in a map, so they must be skipped
                // see LogicalPart#partitionPage and MemorySplitManager#getSplits for details
                Map<String, List<Integer>> logicalPartPartitionMap =
                        tablesManager.getTableLogicalPartPartitionMap(tableId).entrySet().stream()
                        .filter(e -> e.getKey() != null)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                return completedFuture(ImmutableList.of(new MemoryDataFragment(currentHostAddress, addedRows, tablesManager.getTableLogicalPartCount(tableId), logicalPartPartitionMap).toSlice()));
            }
            return completedFuture(ImmutableList.of(new MemoryDataFragment(currentHostAddress, addedRows, 0, Collections.emptyMap()).toSlice()));
        }

        @Override
        public void abort()
        {
            sinkCount.decrementAndGet();
            tablesManager.cleanTable(tableId);
        }
    }
}
