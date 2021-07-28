/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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

    private final long processingDelay;
    private final List<MemoryColumnHandle> columns;
    private final List<SortingColumn> sortedBy;
    private final List<String> indexColumns;
    private final long maxLogicalPartBytes;
    private final int maxPageSizeBytes;
    private final List<LogicalPart> logicalParts; // actual data (pages) stored here
    private final boolean compressionEnabled;
    private TableState tableState;
    private long lastModified = System.currentTimeMillis();
    private long byteSize;

    private transient Path tableDataRoot;
    private transient PagesSerde pagesSerde;
    private transient PageSorter pageSorter;
    private transient TypeManager typeManager;

    public Table(long id, boolean compressionEnabled, Path tableDataRoot, List<MemoryColumnHandle> columns, List<SortingColumn> sortedBy,
            List<String> indexColumns, PageSorter pageSorter, MemoryConfig config, TypeManager typeManager, PagesSerde pagesSerde)
    {
        this.tableDataRoot = tableDataRoot;
        this.maxLogicalPartBytes = config.getMaxLogicalPartSize().toBytes();
        this.maxPageSizeBytes = Long.valueOf(config.getMaxPageSize().toBytes()).intValue();
        this.processingDelay = config.getProcessingDelay().toMillis();
        this.compressionEnabled = compressionEnabled;
        this.columns = requireNonNull(columns, "columns is null");
        this.sortedBy = requireNonNull(sortedBy, "sortedBy is null");
        this.indexColumns = requireNonNull(indexColumns, "indexColumns is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");

        this.logicalParts = new ArrayList<>();

        MemoryThreadManager.getSharedThreadPool().scheduleWithFixedDelay(() -> {
            if ((System.currentTimeMillis() - lastModified) > processingDelay) {
                for (int i = 0; i < logicalParts.size(); i++) {
                    LogicalPart logicalPart = logicalParts.get(i);
                    if (logicalPart.getProcessingState().get() == LogicalPart.LogicalPartState.FINISHED_ADDING) {
                        int finalI = i;
                        MemoryThreadManager.getSharedThreadPool().execute(() -> {
                            LOG.info("Processing Table %d :: logicalPart %d", id, finalI + 1);
                            try {
                                logicalPart.process();
                            }
                            catch (Exception e) {
                                LOG.warn("Failed to process Table %d :: logicalPart %d", id, finalI + 1);
                            }
                            LOG.info("Processed Table %d :: logicalPart %d", id, finalI + 1);
                        });
                    }
                }
            }
        }, 5, 2, TimeUnit.SECONDS);
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
        if (logicalParts.isEmpty() || !logicalParts.get(logicalParts.size() - 1).canAdd()) {
            this.logicalParts.add(new LogicalPart(columns, sortedBy, indexColumns, tableDataRoot, pageSorter, maxLogicalPartBytes, maxPageSizeBytes, typeManager, pagesSerde, logicalParts.size(), compressionEnabled));
        }
        logicalParts.get(logicalParts.size() - 1).add(page);
        byteSize += page.getSizeInBytes();
        lastModified = System.currentTimeMillis();
        tableState = TableState.MODIFIED;
    }

    public boolean allProcessed()
    {
        for (LogicalPart logicalPart : logicalParts) {
            if (logicalPart.getProcessingState().get() != LogicalPart.LogicalPartState.COMPLETED) {
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

    public void finishCreation()
    {
        tableState = TableState.COMMITTED;
        for (LogicalPart logicalPart : logicalParts) {
            // for all new logical parts, set state to finished adding pages
            if (logicalPart.getProcessingState().get() == LogicalPart.LogicalPartState.ACCEPTING_PAGES) {
                logicalPart.finishAdding();
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

    protected List<Page> getPages(int lpNum)
    {
        List<Page> list = new ArrayList<>();
        if (!logicalParts.isEmpty()) {
            list.addAll(logicalParts.get(lpNum).getPages());
        }
        return list;
    }

    protected List<Page> getPages(int lpNum, TupleDomain<ColumnHandle> predicate)
    {
        if (predicate.isAll()) {
            return getPages(lpNum);
        }

        List<Page> list = new ArrayList<>();
        if (!logicalParts.isEmpty()) {
            list.addAll(logicalParts.get(lpNum).getPages(predicate));
        }
        return list;
    }

    protected long getRows()
    {
        int total = 0;
        for (LogicalPart logicalPart : logicalParts) {
            total += logicalPart.getRows();
        }
        return total;
    }

    protected int getLogicalPartCount()
    {
        return logicalParts.size() - 1;
    }
}
