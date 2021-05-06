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
package io.prestosql.plugin.memory.data;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.prestosql.plugin.memory.ColumnInfo;
import io.prestosql.plugin.memory.MemoryConfig;
import io.prestosql.plugin.memory.SortingColumn;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.prestosql.plugin.memory.MemoryErrorCode.MEMORY_LIMIT_EXCEEDED;
import static io.prestosql.plugin.memory.MemoryErrorCode.MISSING_DATA;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class MemoryTableManager
{
    private static final Logger LOG = Logger.get(MemoryTableManager.class);
    private static final String TABLE_METADATA_SUFFIX = "_tabledata";
    private static final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private final Path spillRoot;
    private final long maxBytes;
    private final int splitsPerNode;
    private final PageSorter pageSorter;
    private final MemoryConfig config;
    private final TypeManager typeManager;
    private final PagesSerde pagesSerde;

    @GuardedBy("this")
    private long currentBytes;
    private final Map<Long, TableData> tables = new HashMap<>();

    @Inject
    public MemoryTableManager(MemoryConfig config, PageSorter pageSorter, TypeManager typeManager, PagesSerde pagesSerde)
    {
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.config = requireNonNull(config, "config is null");
        this.splitsPerNode = config.getSplitsPerNode();
        this.maxBytes = config.getMaxDataPerNode().toBytes();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.spillRoot = config.getSpillRoot();
        this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
    }

    public void finishCreateTable(long id)
    {
        Timer timer = new Timer(true);
        timer.scheduleAtFixedRate(new TimerTask()
        {
            @Override
            public void run()
            {
                if (tables.get(id).allProcessed()) {
                    try {
                        if (tables.get(id).isSpilled()) {
                            timer.cancel();
                        }
                        spillTable(id);
                    }
                    catch (Exception e) {
                        LOG.error("Failed to serialize table " + id, e);
                    }
                }
            }
        }, 0, 3000);
    }

    public synchronized void initialize(long tableId, boolean compressionEnabled, List<ColumnInfo> columns, List<SortingColumn> sortedBy, List<String> indexColumns)
    {
        if (!tables.containsKey(tableId)) {
            tables.put(tableId, new TableData(tableId, compressionEnabled, spillRoot.resolve(String.valueOf(tableId)), columns, sortedBy, indexColumns, pageSorter, config, typeManager, pagesSerde));
        }
    }

    public synchronized void add(Long tableId, Page page)
    {
        if (!contains(tableId)) {
            throw new PrestoException(MISSING_DATA, "Failed to find table on a worker.");
        }

        page.compact();

        long newSize = currentBytes + page.getRetainedSizeInBytes();
        if (maxBytes < newSize) {
            throw new PrestoException(MEMORY_LIMIT_EXCEEDED, format("Memory limit [%d] for memory connector exceeded", maxBytes));
        }
        currentBytes = newSize;

        TableData tableData = tables.get(tableId);
        tableData.add(page);
    }

    public List<Page> getPages(
            Long tableId,
            int partNumber,
            int totalParts,
            List<Integer> columnIndexes,
            long expectedRows,
            OptionalLong limit,
            OptionalDouble sampleRatio)
    {
        return getPages(tableId, partNumber, totalParts, columnIndexes, expectedRows, limit, sampleRatio, TupleDomain.all());
    }

    public List<Page> getPages(
            Long tableId,
            int partNumber,
            int totalParts,
            List<Integer> columnIndexes,
            long expectedRows,
            OptionalLong limit,
            OptionalDouble sampleRatio,
            TupleDomain<ColumnHandle> predicate)
    {
        if (!contains(tableId)) {
            try {
                restoreTable(tableId);
            }
            catch (Exception e) {
                throw new PrestoException(MISSING_DATA, "Failed to find/restore table on a worker.");
            }
        }
        TableData tableData = tables.get(tableId);
        if (tableData.getRows() < expectedRows) {
            throw new PrestoException(MISSING_DATA,
                    format("Expected to find [%s] rows on a worker, but found [%s].", expectedRows, tableData.getRows()));
        }

        ImmutableList.Builder<Page> projectedPages = ImmutableList.builder();

        int splitNumber = partNumber % splitsPerNode;

        List<Page> pages;
        if (predicate.isAll()) {
            pages = tableData.getPages(splitNumber);
        }
        else {
            pages = tableData.getPages(splitNumber, predicate);
        }

        for (int i = 0; i < pages.size(); i++) {
            projectedPages.add(getColumns(pages.get(i), columnIndexes));
        }

        // TODO: disabling limit and sample pushdown for now
//        boolean done = false;
//        long totalRows = 0;
//        for (int i = partNumber; i < pages.size() && !done; i += totalParts) {
//            if (sampleRatio.isPresent() && ThreadLocalRandom.current().nextDouble() >= sampleRatio.getAsDouble()) {
//                continue;
//            }
//
//            Page page = pages.get(i);
//            totalRows += page.getPositionCount();
//            if (limit.isPresent() && totalRows > limit.getAsLong()) {
//                page = page.getRegion(0, (int) (page.getPositionCount() - (totalRows - limit.getAsLong())));
//                done = true;
//            }
//            partitionedPages.add(getColumns(page, columnIndexes));
//        }
        return projectedPages.build();
    }

    public boolean contains(Long tableId)
    {
        return tables.containsKey(tableId);
    }

    public synchronized void cleanUp(Set<Long> activeTableIds)
    {
        // We have to remember that there might be some race conditions when there are two tables created at once.
        // That can lead to a situation when MemoryPagesStore already knows about a newer second table on some worker
        // but cleanUp is triggered by insert from older first table, which MemoryTableHandle was created before
        // second table creation. Thus activeTableIds can have missing latest ids and we can only clean up tables
        // that:
        // - have smaller value then max(activeTableIds).
        // - are missing from activeTableIds set

        if (activeTableIds.isEmpty()) {
            // if activeTableIds is empty, we can not determine latestTableId...
            return;
        }
        long latestTableId = Collections.max(activeTableIds);

        for (Iterator<Map.Entry<Long, TableData>> tableDataIterator = tables.entrySet().iterator(); tableDataIterator.hasNext(); ) {
            Map.Entry<Long, TableData> tablePagesEntry = tableDataIterator.next();
            Long tableId = tablePagesEntry.getKey();
            if (tableId < latestTableId && !activeTableIds.contains(tableId)) {
                for (Page removedPage : tablePagesEntry.getValue().getPages()) {
                    currentBytes -= removedPage.getRetainedSizeInBytes();
                }
                tableDataIterator.remove();
            }
        }
    }

    private static Page getColumns(Page page, List<Integer> columnIndexes)
    {
        Block[] outputBlocks = new Block[columnIndexes.size()];

        for (int i = 0; i < columnIndexes.size(); i++) {
            outputBlocks[i] = page.getBlock(columnIndexes.get(i));
        }

        return new Page(page.getPositionCount(), outputBlocks);
    }

    private synchronized void spillTable(long id)
            throws IOException
    {
        TableData tableData = tables.get(id);
        if (tableData.isSpilled()) {
            return;
        }
        LOG.debug("[Spill] serializing metadata of table " + id);
        long start = System.currentTimeMillis();
        Path tablePath = spillRoot.resolve(String.valueOf(id));
        if (!Files.exists(tablePath)) {
            Files.createDirectories(tablePath);
        }
        try (OutputStream outputStream = Files.newOutputStream(tablePath.resolve(TABLE_METADATA_SUFFIX))) {
            ObjectOutputStream oos = new ObjectOutputStream(outputStream);
            oos.writeObject(tableData);
        }
        tableData.finishSpilling();
        long dur = System.currentTimeMillis() - start;
        LOG.debug("[Spill] Table " + id + " has been serialized to disk . Time elapsed: " + dur + "ms");
    }

    private synchronized void restoreTable(long id)
            throws IOException, ClassNotFoundException
    {
        if (tables.containsKey(id)) {
            return;
        }
        LOG.debug("[Load] Loading metadata of table " + id);
        long start = System.currentTimeMillis();
        Path tablePath = spillRoot.resolve(String.valueOf(id));
        if (!Files.exists(tablePath)) {
            throw new FileNotFoundException("No spill data found for table id " + id);
        }
        try (InputStream inputStream = Files.newInputStream(tablePath.resolve(TABLE_METADATA_SUFFIX))) {
            ObjectInputStream ois = new ObjectInputStream(inputStream);
            TableData tableData = (TableData) ois.readObject();
            tableData.restoreTransientObjects(pageSorter, typeManager, pagesSerde, tablePath);
            tables.put(id, tableData);
        }
        long dur = System.currentTimeMillis() - start;
        LOG.debug("[Load] Table " + id + " has been restored. Time elapsed: " + dur + "ms");
    }
}
