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

import io.airlift.log.Logger;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.prestosql.plugin.memory.ColumnInfo;
import io.prestosql.plugin.memory.MemoryConfig;
import io.prestosql.plugin.memory.MemoryThreadManager;
import io.prestosql.plugin.memory.SortingColumn;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

    private final long processingDelay;
    private final int totalSplits;
    private final AtomicInteger nextSplit;
    private final List<ColumnInfo> columns;
    private final List<SortingColumn> sortedBy;
    private final List<String> indexColumns;
    private final long maxLogicalPartBytes;
    private final int maxPageSizeBytes;
    private final List<List<LogicalPart>> splits;
    private final boolean compressionEnabled;
    private TableState tableState;
    private long lastModified = System.currentTimeMillis();
    private long byteSize;

    private transient Path tableDataRoot;
    private transient PagesSerde pagesSerde;
    private transient PageSorter pageSorter;
    private transient TypeManager typeManager;

    public Table(long id, boolean compressionEnabled, int splitsPerNode, Path tableDataRoot, List<ColumnInfo> columns, List<SortingColumn> sortedBy,
            List<String> indexColumns, PageSorter pageSorter, MemoryConfig config, TypeManager typeManager, PagesSerde pagesSerde)
    {
        this.tableDataRoot = tableDataRoot;
        this.totalSplits = splitsPerNode;
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

        this.splits = new ArrayList<>(splitsPerNode);
        for (int i = 0; i < splitsPerNode; i++) {
            this.splits.add(new ArrayList<>());
        }
        this.nextSplit = new AtomicInteger(0);

        MemoryThreadManager.getSharedThreadPool().scheduleWithFixedDelay(() -> {
            if ((System.currentTimeMillis() - lastModified) > processingDelay) {
                for (int i = 0; i < splits.size(); i++) {
                    List<LogicalPart> split = splits.get(i);
                    for (int j = 0; j < split.size(); j++) {
                        LogicalPart logicalPart = split.get(j);
                        if (logicalPart.getProcessingState().get() == LogicalPart.LogicalPartState.FINISHED_ADDING) {
                            int finalI = i;
                            int finalJ = j;
                            MemoryThreadManager.getSharedThreadPool().execute(() -> {
                                LOG.info("Processing Table %d :: Split %d/%d :: LogicalPart %d/%d", id, finalI + 1, splits.size(), finalJ + 1, split.size());
                                try {
                                    logicalPart.process();
                                }
                                catch (Exception e) {
                                    LOG.warn("Failed to process Table %d :: Split %d/%d :: LogicalPart %d/%d", id, finalI + 1, splits.size(), finalJ + 1, split.size());
                                }
                                LOG.info("Processed Table %d :: Split %d/%d :: LogicalPart %d/%d", id, finalI + 1, splits.size(), finalJ + 1, split.size());
                            });
                        }
                    }
                }
            }
        }, 5, 2, TimeUnit.SECONDS);
    }

    // used for deserialization
    public void restoreTransientObjects(PageSorter pageSorter, TypeManager typeManager, PagesSerde pagesSerde, Path tableDataRoot)
    {
        this.pageSorter = pageSorter;
        this.typeManager = typeManager;
        this.pagesSerde = pagesSerde;
        this.tableDataRoot = tableDataRoot;
        for (List<LogicalPart> split : splits) {
            for (LogicalPart lp : split) {
                lp.restoreTransientObjects(pageSorter, typeManager, pagesSerde, tableDataRoot);
            }
        }
    }

    public void add(Page page)
    {
        int splitNum = nextSplit.getAndIncrement() % totalSplits;
        List<LogicalPart> splitParts = splits.get(splitNum);
        if (splitParts.isEmpty() || !splitParts.get(splitParts.size() - 1).canAdd()) {
            int logicalPartNum = splitParts.size();
            splitParts.add(new LogicalPart(columns, sortedBy, indexColumns, tableDataRoot, pageSorter, maxLogicalPartBytes, maxPageSizeBytes, typeManager, pagesSerde, splitNum, logicalPartNum, compressionEnabled));
        }

        LogicalPart currentSplitPart = splitParts.get(splitParts.size() - 1);
        currentSplitPart.add(page);

        byteSize += page.getSizeInBytes();
        lastModified = System.currentTimeMillis();
        tableState = TableState.MODIFIED;
    }

    public boolean allProcessed()
    {
        for (List<LogicalPart> split : splits) {
            for (LogicalPart logicalPart : split) {
                if (logicalPart.getProcessingState().get() != LogicalPart.LogicalPartState.COMPLETED) {
                    return false;
                }
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
        for (List<LogicalPart> split : splits) {
            Iterator<LogicalPart> iterator = split.iterator();
            while (iterator.hasNext()) {
                LogicalPart lp = iterator.next();
                if (lp.getProcessingState().get() == LogicalPart.LogicalPartState.ACCEPTING_PAGES) {
                    size += lp.getByteSize();
                    iterator.remove();
                }
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
        for (List<LogicalPart> spilt : splits) {
            for (LogicalPart logicalPart : spilt) {
                // for all new logical parts, set state to finished adding pages
                if (logicalPart.getProcessingState().get() == LogicalPart.LogicalPartState.ACCEPTING_PAGES) {
                    logicalPart.finishAdding();
                }
            }
        }
    }

    public void setState(TableState state)
    {
        tableState = state;
    }

    protected List<Page> getPages(int split)
    {
        // use for-loop instead of stream to improve performance
        List<Page> list = new ArrayList<>();
        for (LogicalPart lp : splits.get(split)) {
            list.addAll(lp.getPages());
        }
        return list;
    }

    protected List<Page> getPages(int split, TupleDomain<ColumnHandle> predicate)
    {
        if (predicate.isAll()) {
            return getPages(split);
        }

        // use for-loop instead of stream to improve performance
        List<Page> list = new ArrayList<>();
        for (LogicalPart lp : splits.get(split)) {
            list.addAll(lp.getPages(predicate));
        }
        return list;
    }

    protected long getRows()
    {
        int total = 0;
        for (List<LogicalPart> split : splits) {
            for (LogicalPart logiPart : split) {
                total += logiPart.getRows();
            }
        }
        return total;
    }
}
