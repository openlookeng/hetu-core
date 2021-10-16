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
package io.prestosql.plugin.memory.data;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.prestosql.plugin.memory.MemoryColumnHandle;
import io.prestosql.plugin.memory.MemoryConfig;
import io.prestosql.plugin.memory.MemoryThreadManager;
import io.prestosql.plugin.memory.SortingColumn;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.util.BloomFilter;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class Table
        implements Serializable
{
    public long getByteSize()
    {
        return byteSize;
    }

    enum TableState
    {
        MODIFIED, COMMITTED, SPILLED
    }

    private static final long serialVersionUID = 588783549296983464L;
    private static final Logger LOG = Logger.get(Table.class);
    private static final Long PROCESSING_DELAY = 5000L; // 5s
    private static final ScheduledExecutorService executor = MemoryThreadManager.getSharedThreadPool();

    /* NOTE: MemoryTableManager uses Object serialization on this class
    but for security it requires a whitelist of accepted classes during de-serialization.

    Below is a list of all classes that are serialized as part of Table's class tree.

    In the future, if a new class is added to this class as a field, de-serialization will fail with an error like:
    Caused by: java.io.InvalidClassException: io.prestosql.plugin.memory.data.NewClass not supported.
    the new class must be added to the whitelist below.
    */
    public static final String[] TYPES_WHITELIST = ImmutableList.of(
                    Number.class.getCanonicalName(),
                    Integer.class.getCanonicalName(),
                    Long.class.getCanonicalName(),
                    Table.class.getName(),
                    AtomicInteger.class.getName(),
                    List.class.getName(),
                    ArrayList.class.getName(),
                    TableState.class.getName(),
                    MemoryColumnHandle.class.getName(),
                    SortingColumn.class.getName(),
                    SortOrder.class.getName(),
                    Enum.class.getName(),
                    HashMap.class.getName(),
                    HashSet.class.getName(),
                    AbstractMap.SimpleEntry.class.getName(),
                    BloomFilter.class.getName(),
                    BloomFilter.BitSet.class.getName(),
                    LogicalPart.class.getName(),
                    LogicalPart.LogicalPartState.class.getName(),
                    TreeMap.class.getName(),
                    LogicalPart.SparseValue.class.getName(),
                    AtomicReference.class.getName(),
                    long[].class.getName())
            .toArray(new String[0]);

    private final List<MemoryColumnHandle> columns;
    private final List<SortingColumn> sortedBy;
    private final List<String> partitionedBy;
    private final List<String> indexColumns;
    private final long maxLogicalPartBytes;
    private final int maxPageSizeBytes;
    private final List<LogicalPart> logicalParts; // actual data structure that stores the LPs
    private final Map<String, List<Integer>> logicalPartPartitionedMap;  // data structure to store the mapping between that partition value and LP index
    private final boolean compressionEnabled;
    private TableState tableState;
    private long byteSize;
    private final long id;
    private final boolean asyncEnabled;

    private transient Path tableDataRoot;
    private transient PagesSerde pagesSerde;

    private transient PageSorter pageSorter;
    private transient TypeManager typeManager;

    public Table(long id, boolean compressionEnabled, boolean asyncEnabled, Path tableDataRoot, List<MemoryColumnHandle> columns, List<SortingColumn> sortedBy,
                 List<String> partitionedBy, List<String> indexColumns, PageSorter pageSorter, MemoryConfig config, TypeManager typeManager, PagesSerde pagesSerde)
    {
        this.id = id;
        this.tableDataRoot = tableDataRoot;
        this.maxLogicalPartBytes = config.getMaxLogicalPartSize().toBytes();
        this.maxPageSizeBytes = Long.valueOf(config.getMaxPageSize().toBytes()).intValue();
        this.compressionEnabled = compressionEnabled;
        this.columns = requireNonNull(columns, "columns is null");
        this.sortedBy = requireNonNull(sortedBy, "sortedBy is null");
        this.partitionedBy = requireNonNull(partitionedBy, "partitionedBy is null"); //only support one partition column
        this.indexColumns = requireNonNull(indexColumns, "indexColumns is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
        this.asyncEnabled = asyncEnabled;
        this.logicalParts = new ArrayList<>();
        this.logicalPartPartitionedMap = new HashMap<>();
    }

    /**
     * used for deserialization. these objects are per-runtime so must be restored separately after loading from disk.
     */
    public void restoreTransientObjects(PageSorter pageSorter, TypeManager typeManager, PagesSerde pagesSerde, Path tableDataRoot)
    {
        this.pageSorter = pageSorter;
        this.typeManager = typeManager;
        this.pagesSerde = pagesSerde;
        this.tableDataRoot = tableDataRoot;

        for (LogicalPart lp : logicalParts) {
            lp.restoreTransientObjects(pageSorter, typeManager, pagesSerde, tableDataRoot);
        }
    }

    /**
     * Add page to splits in a robin-robin way.
     *
     * @param page page to add
     */
    public void add(Page page)
    {
        // if there is no partition statement, just create one LP with the empty partition key
        if (partitionedBy.isEmpty()) {
            if (logicalParts.isEmpty() || !logicalParts.get(logicalParts.size() - 1).canAdd()) {
                this.logicalParts.add(new LogicalPart(columns, sortedBy, indexColumns, tableDataRoot, pageSorter, maxLogicalPartBytes, maxPageSizeBytes, typeManager, pagesSerde, logicalParts.size() + 1, compressionEnabled));
            }
            logicalParts.get(logicalParts.size() - 1).add(page);
        }

        // if there is partitioned_by string,
        // 1. get the partition values from the partitioned column;
        // 2. partition the page to multiple subpages according to the values and add their indices to the logicalPartPartitionedMap accordingly
        else {
            Map<String, Page> partitions = LogicalPart.partitionPage(page, partitionedBy, columns, typeManager);
            for (Map.Entry<String, Page> entry : partitions.entrySet()) {
                String curPartitionKey = entry.getKey();
                Page curSubPage = entry.getValue();
                logicalPartPartitionedMap.putIfAbsent(curPartitionKey, new ArrayList<>());

                List<Integer> logicalPartIndices = logicalPartPartitionedMap.get(curPartitionKey);
                LogicalPart lastLogicalPart = logicalPartIndices.isEmpty() ? null : this.logicalParts.get(logicalPartIndices.get(logicalPartIndices.size() - 1) - 1);
                if (lastLogicalPart == null || !lastLogicalPart.canAdd()) {
                    int logicalPartNum = logicalParts.size() + 1;
                    lastLogicalPart = new LogicalPart(columns, sortedBy, indexColumns, tableDataRoot, pageSorter, maxLogicalPartBytes, maxPageSizeBytes, typeManager, pagesSerde, logicalPartNum, compressionEnabled);
                    logicalParts.add(lastLogicalPart);
                    logicalPartIndices.add(logicalPartNum);
                }
                lastLogicalPart.add(curSubPage);
            }
        }

        byteSize += page.getSizeInBytes();
        tableState = TableState.MODIFIED;
    }

    public boolean allProcessed()
    {
        for (LogicalPart lp : logicalParts) {
            if (lp.getProcessingState().get() != LogicalPart.LogicalPartState.COMPLETED) {
                return false;
            }
        }
        return true;
    }

    /**
     * Removed all uncommitted LogicalParts, and return their total size in bytes.
     */
    public long rollBackUncommitted()
    {
        int size = 0;

        Iterator<LogicalPart> iterator = logicalParts.iterator();
        while (iterator.hasNext()) {
            LogicalPart lp = iterator.next();
            if (lp.getProcessingState().get() == LogicalPart.LogicalPartState.ACCEPTING_PAGES) {
                size += lp.getByteSize();
                iterator.remove();
            }
        }
        byteSize -= size;

        return size;
    }

    public boolean isSpilled()
    {
        return tableState == TableState.SPILLED;
    }

    public void finishCreation(Runnable cleanup)
    {
        tableState = TableState.COMMITTED;

        List<Future<?>> futuresList = new ArrayList<>(logicalParts.size());
        for (int i = 0; i < logicalParts.size(); i++) {
            // for all new logical parts, set state to finished adding pages
            if (logicalParts.get(i).getProcessingState().get() == LogicalPart.LogicalPartState.ACCEPTING_PAGES) {
                logicalParts.get(i).finishAdding();
            }

            int finalI = i;
            Runnable runnable = () -> {
                LOG.info("Processing Table %d :: logicalPart %d", id, finalI + 1);
                try {
                    logicalParts.get(finalI).process();

                    // run manager's cleanup, this does two things
                    // 1. spills the table to disk, the manager handles this because the Table itself doesn't know how/where to spill
                    // 2. manager handles memory release
                    // only once all LPs are processed this cleanup will be called by the last LP
                    if (allProcessed()) {
                        cleanup.run();
                    }
                }
                catch (Exception e) {
                    LOG.warn("Failed to process Table %d :: logicalPart %d", id, finalI + 1);
                }
                LOG.info("Processed Table %d :: logicalPart %d", id, finalI + 1);
            };

            if (asyncEnabled) {
                MemoryThreadManager.getSharedThreadPool().schedule(runnable, 5, TimeUnit.SECONDS);
            }
            else {
                futuresList.add(MemoryThreadManager.getSharedThreadPool().submit(runnable));
            }
        }

        // Used to synchronize processing, it will only run for sync processing since for async the list is empty
        for (Future<?> future : futuresList) {
            try {
                future.get();
            }
            catch (ExecutionException | InterruptedException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to process table", e);
            }
        }
    }

    public void offLoadPages()
    {
        logicalParts.forEach(LogicalPart::unloadPages);
    }

    public void setState(TableState state)
    {
        tableState = state;
    }

    protected List<Page> getPages(int logicalPartNum)
    {
        List<Page> list = new ArrayList<>();
        if (!logicalParts.isEmpty()) {
            for (LogicalPart lp : logicalParts) {
                if (lp.getLogicalPartNum() == logicalPartNum) {
                    list.addAll(lp.getPages());
                }
            }
        }
        return list;
    }

    protected List<Page> getPages(int logicalPartNum, TupleDomain<ColumnHandle> predicate)
    {
        if (predicate.isAll()) {
            return getPages(logicalPartNum);
        }

        List<Page> list = new ArrayList<>();
        for (LogicalPart lp : logicalParts) {
            if (lp.getLogicalPartNum() == logicalPartNum) {
                list.addAll(lp.getPages(predicate));
            }
        }
        return list;
    }

    protected long getRows()
    {
        long total = 0;
        for (LogicalPart lp : logicalParts) {
            total += lp.getRows();
        }
        return total;
    }

    protected Map<String, List<Integer>> getLogicalPartPartitionMap()
    {
        return logicalPartPartitionedMap;
    }

    protected int getLogicalPartCount()
    {
        return logicalParts.size();
    }
}
