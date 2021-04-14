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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.log.Logger;
import io.prestosql.plugin.memory.ColumnInfo;
import io.prestosql.plugin.memory.MemoryConfig;
import io.prestosql.plugin.memory.SortingColumn;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class TableData
{
    private static final Logger LOG = Logger.get(TableData.class);

    private static final Long PROCESSING_DELAY = 5000L; // 5s
    private static final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("MemoryConnector-pool-%d").setDaemon(true).build();
    private static final ScheduledExecutorService executor = Executors.newScheduledThreadPool(Math.max((Runtime.getRuntime().availableProcessors() / 2), 2), threadFactory);

    private final long id;
    private final int totalSplits;
    private final List<LogicalPart>[] splits;
    private final AtomicInteger nextSplit;
    private final List<ColumnInfo> columns;
    private final List<SortingColumn> sortedBy;
    private final List<String> indexColumns;
    private final PageSorter pageSorter;
    private final long maxLogicalPartBytes;
    private final MemoryConfig config;
    private long lastModified = System.currentTimeMillis();

    public TableData(long id, List<ColumnInfo> columns, List<SortingColumn> sortedBy,
                     List<String> indexColumns, PageSorter pageSorter, MemoryConfig config)
    {
        this.id = requireNonNull(id, "id is null");
        this.config = requireNonNull(config, "config is null");
        this.totalSplits = config.getSplitsPerNode();
        this.maxLogicalPartBytes = config.getMaxLogicalPartSize().toBytes();
        this.columns = requireNonNull(columns, "columns is null");
        this.sortedBy = requireNonNull(sortedBy, "sortedBy is null");
        this.indexColumns = requireNonNull(indexColumns, "indexColumns is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");

        this.splits = new ArrayList[totalSplits];
        for (int i = 0; i < totalSplits; i++) {
            this.splits[i] = new ArrayList<>();
        }
        this.nextSplit = new AtomicInteger(0);

        Timer timer = new Timer(true);
        timer.scheduleAtFixedRate(new TimerTask()
        {
            @Override
            public void run()
            {
                if ((System.currentTimeMillis() - lastModified) > PROCESSING_DELAY) {
                    for (int i = 0; i < splits.length; i++) {
                        List<LogicalPart> split = splits[i];
                        for (int j = 0; j < split.size(); j++) {
                            LogicalPart logicalPart = split.get(j);
                            if (logicalPart.getProcessingState().get() == LogicalPart.ProcessingState.NOT_STARTED) {
                                int finalI = i;
                                int finalJ = j;
                                executor.execute(() -> {
                                    LOG.info("Processing Table %d :: Split %d/%d :: LogicalPart %d/%d", id, finalI + 1, splits.length, finalJ + 1, split.size());
                                    try {
                                        logicalPart.process();
                                    }
                                    catch (Exception e) {
                                        LOG.warn("Failed to process Table %d :: Split %d/%d :: LogicalPart %d/%d", id, finalI + 1, splits.length, finalJ + 1, split.size());
                                    }
                                    LOG.info("Processed Table %d :: Split %d/%d :: LogicalPart %d/%d", id, finalI + 1, splits.length, finalJ + 1, split.size());
                                });
                            }
                        }
                    }
                }
            }
        }, 1000, 5000);
    }

    public void add(Page page)
    {
        List<LogicalPart> splitParts = splits[nextSplit.getAndIncrement() % totalSplits];
        if (splitParts.isEmpty() || !splitParts.get(splitParts.size() - 1).canAdd()) {
            splitParts.add(new LogicalPart(columns, sortedBy, indexColumns, pageSorter, maxLogicalPartBytes));
        }

        LogicalPart currentSplitPart = splitParts.get(splitParts.size() - 1);
        currentSplitPart.add(page);

        lastModified = System.currentTimeMillis();
    }

    protected List<Page> getPages()
    {
        return Arrays.stream(splits).flatMap(l -> l.stream()).flatMap(lp -> lp.getPages().stream()).collect(Collectors.toList());
    }

    protected List<Page> getPages(int split)
    {
        return splits[split].parallelStream().flatMap(lp -> lp.getPages().stream()).collect(Collectors.toList());
    }

    protected List<Page> getPages(int split, TupleDomain<ColumnHandle> predicate)
    {
        if (predicate.isAll()) {
            return getPages(split);
        }

        return splits[split].parallelStream().flatMap(lp -> lp.getPages(predicate).stream()).collect(Collectors.toList());
    }

    protected long getRows()
    {
        int total = 0;
        for (int i = 0; i < splits.length; i++) {
            for (LogicalPart logiPart : splits[i]) {
                total += logiPart.getRows();
            }
        }
        return total;
    }
}
