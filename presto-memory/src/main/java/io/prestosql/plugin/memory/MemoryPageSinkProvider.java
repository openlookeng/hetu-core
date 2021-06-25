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
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class MemoryPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final MemoryTableManager pagesStore;
    private final HostAddress currentHostAddress;

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

        pagesStore.refreshTables(memoryOutputTableHandle.getActiveTableIds());
        pagesStore.initialize(tableId,
                memoryOutputTableHandle.isCompressionEnabled(),
                memoryOutputTableHandle.getSplitsPerNode(),
                memoryOutputTableHandle.getColumns(),
                memoryOutputTableHandle.getSortedBy(),
                memoryOutputTableHandle.getIndexColumns());
        return new MemoryPageSink(pagesStore, currentHostAddress, tableId);
    }

    /**
     * This method is used when inserting data after table has already been created
     */
    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        // MemoryWriteTableHandle is used for both CTAS and inserts
        return createPageSink(transactionHandle, session, (ConnectorOutputTableHandle) insertTableHandle);
    }

    private static class MemoryPageSink
            implements ConnectorPageSink
    {
        private final MemoryTableManager tablesManager;
        private final HostAddress currentHostAddress;
        private final long tableId;
        private long addedRows;

        public MemoryPageSink(MemoryTableManager tablesManager, HostAddress currentHostAddress, long tableId)
        {
            this.tablesManager = requireNonNull(tablesManager, "pagesStore is null");
            this.currentHostAddress = requireNonNull(currentHostAddress, "currentHostAddress is null");
            this.tableId = tableId;
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
            tablesManager.finishUpdatingTable(tableId);
            return completedFuture(ImmutableList.of(new MemoryDataFragment(currentHostAddress, addedRows).toSlice()));
        }

        @Override
        public void abort()
        {
            tablesManager.cleanTable(tableId);
        }
    }
}
