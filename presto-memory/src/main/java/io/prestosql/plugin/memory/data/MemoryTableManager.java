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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

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
    private final AtomicLong currentBytes = new AtomicLong();
    private final Map<Long, Table> tables = new LinkedHashMap<>(16, 0.75f, true);

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

    public void finishUpdatingTable(long id)
    {
        tables.get(id).finishCreation();

        Timer timer = new Timer(true);
        timer.scheduleAtFixedRate(new TimerTask()
        {
            @Override
            public void run()
            {
                if (tables.containsKey(id) && tables.get(id).allProcessed()) {
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
            tables.put(tableId, new Table(tableId, compressionEnabled, spillRoot.resolve(String.valueOf(tableId)), columns, sortedBy, indexColumns, pageSorter, config, typeManager, pagesSerde));
        }
    }

    public synchronized void add(Long tableId, Page page)
    {
        if (!contains(tableId)) {
            throw new PrestoException(MISSING_DATA, "Failed to find table on a worker.");
        }

        page.compact();

        Table table = tables.get(tableId); // get current table to make sure it's not LRU
        applyForMemory(page.getSizeInBytes(), tableId, () -> {}, () -> releaseMemory(table.rollBackUncommitted(), "Rolling back."));
        table.add(page);
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
            catch (PrestoException pe) {
                throw pe;
            }
            catch (Exception e) {
                throw new PrestoException(MISSING_DATA, "Failed to find/restore table on a worker", e);
            }
        }
        Table table = tables.get(tableId);
        if (table.getRows() < expectedRows) {
            throw new PrestoException(MISSING_DATA,
                    format("Expected to find [%s] rows on a worker, but found [%s].", expectedRows, table.getRows()));
        }

        ImmutableList.Builder<Page> projectedPages = ImmutableList.builder();

        int splitNumber = partNumber % splitsPerNode;

        List<Page> pages;
        if (predicate.isAll()) {
            pages = table.getPages(splitNumber);
        }
        else {
            pages = table.getPages(splitNumber, predicate);
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

    public synchronized void cleanTable(Long tableId)
    {
        if (tables.containsKey(tableId)) {
            try {
                releaseMemory(tables.get(tableId).getByteSize(), "Cleaning table");
                tables.remove(tableId);
            }
            catch (Exception e) {
                LOG.error(e, "Unable to clean table " + tableId);
            }
        }
    }

    public synchronized void refreshTables(Set<Long> activeTableIds)
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

        Iterator<Map.Entry<Long, Table>> tableDataIterator = tables.entrySet().iterator();
        while (tableDataIterator.hasNext()) {
            Map.Entry<Long, Table> tablePagesEntry = tableDataIterator.next();
            Long tableId = tablePagesEntry.getKey();
            if (tableId < latestTableId && !activeTableIds.contains(tableId)) {
                releaseMemory(tablePagesEntry.getValue().getByteSize(), "Table refresh");
                tableDataIterator.remove();
                LOG.info("[TableRefresh] Dropped table %s from memory", tableId);
            }
        }

        if (config.getSpillRoot() != null && Files.exists(config.getSpillRoot())) {
            try {
                Files.list(config.getSpillRoot())
                        .filter(Files::isDirectory)
                        .filter(path -> path.getFileName().toString().matches("\\d+"))
                        .filter(path -> !activeTableIds.contains(Long.valueOf(path.getFileName().toString())))
                        .peek(path -> LOG.info("[TableRefresh] Cleaning table " + Long.valueOf(path.getFileName().toString()) + " from disk."))
                        .forEach(this::deleteRecursively);
            }
            catch (Exception e) {
                LOG.error(e, "[TableRefresh] Unable to update spilled tables");
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
        Table table = tables.get(id);
        if (table.isSpilled()) {
            return;
        }
        LOG.debug("[Spill] serializing metadata of table " + id);
        long start = System.currentTimeMillis();
        Path tablePath = spillRoot.resolve(String.valueOf(id));
        if (!Files.exists(tablePath)) {
            Files.createDirectories(tablePath);
        }
        table.setState(Table.TableState.SPILLED);
        try (OutputStream outputStream = Files.newOutputStream(tablePath.resolve(TABLE_METADATA_SUFFIX))) {
            ObjectOutputStream oos = new ObjectOutputStream(outputStream);
            oos.writeObject(table);
        }
        catch (Exception e) {
            // if spilling failed mark it back to spilled
            table.setState(Table.TableState.COMMITTED);
        }
        long dur = System.currentTimeMillis() - start;
        LOG.debug("[Spill] Table " + id + " has been serialized to disk. Time elapsed: " + dur + "ms");
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
            Table table = (Table) ois.readObject();
            table.restoreTransientObjects(pageSorter, typeManager, pagesSerde, tablePath);
            applyForMemory(table.getByteSize(), -1, () -> logNumFormat("Loaded table %s with %s bytes.", id, table.getByteSize()), () -> {});
            tables.put(id, table);
        }
        long dur = System.currentTimeMillis() - start;
        LOG.debug("[Load] Table " + id + " has been restored. Time elapsed: " + dur + "ms");
    }

    private void deleteRecursively(Path path)
    {
        try {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Apply for given size of memory in memory connector. If it can't be fulfilled the method tries to release tables from memory according to LRU order.
     *
     * @param bytes memory size requested
     * @param reserved reserved tableId so it will not be considered cleanable from LRU
     * @param onSuccess operation after successful memory allocation
     * @param rollBack operation when memory request cannot be fulfilled
     * @throws PrestoException if the request still can't be fulfilled after dropping all non-reserved tables
     */
    private synchronized void applyForMemory(long bytes, long reserved, Runnable onSuccess, Runnable rollBack)
            throws PrestoException
    {
        long newSize = bytes + currentBytes.get();
        while (maxBytes < newSize) {
            logNumFormat("Not enough memory for the request. Current bytes: %s. Limit: %s. Requested size: %s", currentBytes.get(), maxBytes, bytes);
            tables.get(reserved); // perform a get and make sure reserved table is not LRU
            Map.Entry<Long, Table> lru = tables.entrySet().iterator().next();
            if (lru != null && lru.getKey() != reserved && lru.getValue().isSpilled()) {
                // if there is any LRU table available to be dropped, evict it to make space for current
                releaseMemory(lru.getValue().getByteSize(), "Offloading LRU");
                tables.remove(lru.getKey());
                logNumFormat("Released %s bytes by offloading LRU table %s. Current bytes after offloading: %s", lru.getValue().getByteSize(), lru.getKey(), currentBytes.get());
                newSize = currentBytes.get() + bytes;
            }
            else {
                long current = currentBytes.get();
                rollBack.run();
                throw new PrestoException(MEMORY_LIMIT_EXCEEDED, format("Memory limit [%s] for memory connector exceeded. Current: [%s]. Requested: [%s]",
                        NumberFormat.getIntegerInstance(Locale.US).format(maxBytes),
                        NumberFormat.getIntegerInstance(Locale.US).format(current),
                        NumberFormat.getIntegerInstance(Locale.US).format(bytes)));
            }
        }
        currentBytes.set(newSize);
        onSuccess.run();
        logNumFormat("Fulfilled %s bytes. Current: %s", bytes, currentBytes.get());
    }

    private synchronized void releaseMemory(long bytes, String reason)
    {
        currentBytes.addAndGet(-bytes);
        logNumFormat("Released %s bytes. Current: %s. Caused by: " + reason, bytes, currentBytes.get());
    }

    /**
     * Format long numbers by thousands so it's easier to read.
     */
    private void logNumFormat(String format, long... numbers)
    {
        Object[] formatted = new String[numbers.length];
        for (int i = 0; i < numbers.length; i++) {
            formatted[i] = NumberFormat.getIntegerInstance(Locale.US).format(numbers[i]);
        }
        LOG.debug(format, formatted);
    }
}
