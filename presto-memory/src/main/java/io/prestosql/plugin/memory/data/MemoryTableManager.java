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
import io.hetu.core.common.util.SecureObjectInputStream;
import io.hetu.core.common.util.SecurePathWhiteList;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.prestosql.plugin.memory.MemoryColumnHandle;
import io.prestosql.plugin.memory.MemoryConfig;
import io.prestosql.plugin.memory.SortingColumn;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
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
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.memory.MemoryErrorCode.MEMORY_LIMIT_EXCEEDED;
import static io.prestosql.plugin.memory.MemoryErrorCode.MISSING_DATA;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class MemoryTableManager
{
    private static final Logger LOG = Logger.get(MemoryTableManager.class);
    private static final String TABLE_METADATA_SUFFIX = "_tabledata";
    private static final int CREATION_SCALE_FACTOR = 4;
    private static final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private final Path spillRoot;
    private final long maxBytes;
    private final PageSorter pageSorter;
    private final MemoryConfig config;
    private final TypeManager typeManager;
    private final PagesSerde pagesSerde;

    @GuardedBy("this")
    private final AtomicLong currentBytes = new AtomicLong(); // Keep track of current byte usage in memory

    // The following maps store the usage byte sizes with tableID as key and are used to update currentBytes.

    // in-memory LRU map of tableId -> table
    private final Map<Long, Table> tables = new LinkedHashMap<>(16, 0.75f, true);

    // Processed bytes are spilled and updated current table memory usage sizes.
    private final Map<Long, AtomicLong> tableProcessedBytes = new LinkedHashMap<>(16, 0.75f, true);

    // Non-processed bytes are additional page sizes newly added to the table.
    // The new bytes added are applied for memory and waiting to be spilled, updated and processed.
    // Once processed, the corresponding usage size for the tableID in this map will be cleared to 0 or removed if the table is removed.
    private final Map<Long, AtomicLong> tableNotProcessedBytes = new LinkedHashMap<>(16, 0.75f, true);

    // For example:
    // Table with ID 1 is created, then tableNotProcessedBytes stores [1, 0] and tableProcessedBytes also stores [1, 0] as size is 0.
    // Consider that data is added to table 1, taking 23 bytes.
    // The tableProcessedBytes map will have updated value of 23 bytes corresponding to table 1.
    // There will be 23 * CREATION_SCALE_FACTOR = 23 * 4 = 92 bytes of memory to be applied. So, currentBytes will be 92 bytes.
    // The tableNotProcessedBytes will then update table 1's byte value as 23 bytes once the memory is applied. The newly added bytes will wait to be processed.
    // After spilling and updating, since the actual memory usage is 23 bytes and the CREATION_SCALE_FACTOR is 4,
    // there will be 23 * (CREATION_SCALE_FACTOR - 1) = 23 * 3 = 69 bytes being released. Leaving currentBytes to be 23 bytes.
    // The tableNotProcessedBytes will then update table 1's value to be 0 once memory is released, as bytes are now processed.
    // When more data is inserted into table 1, considering an additional 23 bytes, the process repeats.
    // There will be 23 * CREATION_SCALE_FACTOR = 23 * 4 = 92 bytes of additional memory to be applied.
    // So, currentBytes will be 23 + 92 = 115 bytes. 23 bytes from previous operations and 92 bytes newly applied.
    // The tableProcessedBytes will update table 1's value to be 23 + 23 = 46 bytes.
    // The tableNotProcessedBytes will update table 1's value to be 23 bytes, since the newly inserted data is 23 bytes.
    // After spills and updates, similar from previous calculations, the currentBytes will be 23 + 23 = 46 bytes, since two data insertions both using 23 bytes.
    // Once bytes are processed, the tableNotProcessedBytes update table 1's value to be 0 bytes again.
    // This process repeats until the table is removed or fully released from memory.
    // If the table is removed, the two maps will simply not contain the key with tableID and no corresponding value.
    // If the table is released from memory, then the memory released is the corresponding value found using tableID in tableProcessedBytes.
    // Once releases are complete, currentBytes will be 0 bytes indicating no memory usage.

    @Inject
    public MemoryTableManager(MemoryConfig config, PageSorter pageSorter, TypeManager typeManager, PagesSerde pagesSerde)
    {
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.config = requireNonNull(config, "config is null");
        this.maxBytes = config.getMaxDataPerNode().toBytes();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.spillRoot = config.getSpillRoot();
        this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
    }

    public void validateSpillRoot()
            throws IOException
    {
        // make sure spillRoot folder exist first.
        // if not, make specified directory first
        if (!Files.exists(spillRoot)) {
            Files.createDirectories(spillRoot);
        }

        String name = UUID.randomUUID().toString();
        Path testPath = spillRoot.resolve(name);
        // added for security scan since we are constructing a path using input
        checkArgument(!testPath.toString().contains("../"),
                testPath + " must be absolute and under one of the following whitelisted directories:  " + SecurePathWhiteList.getSecurePathWhiteList().toString());
        checkArgument(SecurePathWhiteList.isSecurePath(testPath),
                testPath + " must be under one of the following whitelisted directories: " + SecurePathWhiteList.getSecurePathWhiteList().toString());

        try (RandomAccessFile testFile = new RandomAccessFile(testPath.toFile(), "rw")) {
            // create a new file in the spillRoot that can be written and read from
            // set its length to 1MB
            testFile.setLength(1024 * 1024);
        }
        finally {
            // delete the test file
            Files.delete(testPath);
        }
    }

    public void finishUpdatingTable(long id)
    {
        tables.get(id).finishCreation(() -> {
            // this should only be called once entire table has been processed
            if (tables.containsKey(id) && tables.get(id).allProcessed() && !tables.get(id).isSpilled()) {
                try {
                    // first spill the table to disk
                    spillTable(id);
                    // release memory overhead used during processing
                    // Release amount of bytes not yet processed times (CREATION_SCALE_FACTOR - 1)
                    // For example, 23 bytes are not yet processed, store in tableNotProcessedBytes corresponding to tableID 1.
                    // This means that 23 * CREATION_SCALE_FACTOR = 23 * 4 = 92 bytes was applied earlier.
                    // Then, 23 * (CREATION_SCALE_FACTOR - 1) = 23 * 3 = 69 bytes need to be released for spilling.
                    releaseMemory(tableNotProcessedBytes.get(id).get() * (CREATION_SCALE_FACTOR - 1), id, "Finish processing table " + id);
                }
                catch (Exception e) {
                    LOG.error("Failed to serialize table " + id, e);
                }
            }
        });
    }

    /**
     * Initialize a table and store it in memory
     */
    public synchronized void initialize(long tableId, boolean compressionEnabled, boolean asyncProcessingEnabled, List<MemoryColumnHandle> columns, List<SortingColumn> sortedBy, List<String> partitionedBy, List<String> indexColumns)
    {
        if (!tables.containsKey(tableId)) {
            tables.put(tableId, new Table(tableId,
                    compressionEnabled,
                    asyncProcessingEnabled,
                    spillRoot.resolve(String.valueOf(tableId)),
                    columns,
                    sortedBy,
                    partitionedBy,
                    indexColumns,
                    pageSorter,
                    config,
                    typeManager,
                    pagesSerde));
            AtomicLong newUsageLong = new AtomicLong(0L);
            AtomicLong newAddedLong = new AtomicLong(0L);
            tableProcessedBytes.put(tableId, newUsageLong);
            tableNotProcessedBytes.put(tableId, newAddedLong);
        }
    }

    public synchronized void add(Long tableId, Page page)
    {
        if (!contains(tableId)) {
            throw new PrestoException(MISSING_DATA, "Failed to find table on a worker.");
        }
        page.compact();
        Table table = tables.get(tableId); // get current table to make sure it's not LRU
        applyForMemory(page.getSizeInBytes() * CREATION_SCALE_FACTOR, tableId, () -> {}, () -> releaseMemory(table.rollBackUncommitted() * CREATION_SCALE_FACTOR, tableId, "Rolling back."));
        tableNotProcessedBytes.get(tableId).set(page.getSizeInBytes()); // Additional byte size is recorded.
        table.add(page);
    }

    public Map<String, List<Integer>> getTableLogicalPartPartitionMap(long tableId)
    {
        return tables.get(tableId).getLogicalPartPartitionMap();
    }

    public int getTableLogicalPartCount(long tableId)
    {
        return tables.get(tableId).getLogicalPartCount();
    }

    public List<Page> getPages(
            Long tableId,
            int logicalPartNum,
            List<Integer> columnIndexes,
            long expectedRows,
            OptionalLong limit,
            OptionalDouble sampleRatio)
    {
        return getPages(tableId, logicalPartNum, columnIndexes, expectedRows, limit, sampleRatio, TupleDomain.all());
    }

    public List<Page> getPages(
            Long tableId,
            int logicalPartNum,
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

        List<Page> pages;
        if (predicate.isAll()) {
            pages = table.getPages(logicalPartNum);
        }
        else {
            pages = table.getPages(logicalPartNum, predicate);
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

    /**
     * Rollback table creation/insertion
     *
     * @param tableId the id of table to be cleaned
     */
    public synchronized void cleanTable(Long tableId)
    {
        if (tables.containsKey(tableId)) {
            try {
                releaseMemory(tableProcessedBytes.get(tableId).get(), tableId, "Cleaning table");
                // Remove tableID from corresponding maps
                tables.remove(tableId);
                tableProcessedBytes.remove(tableId);
                tableNotProcessedBytes.remove(tableId);
            }
            catch (Exception e) {
                LOG.error(e, "Unable to clean table " + tableId);
            }
        }
    }

    /**
     * Clean local tables. All non-active tables stored locally in tables map, and the data serialized on disk will be cleaned.
     *
     * @param activeTableIds the ids of active tables. all tables not in this set will be cleaned.
     */
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
                if (tableProcessedBytes.keySet().contains(tableId)) { // Ensure tableID is valid in map
                    releaseMemory(tableProcessedBytes.get(tableId).get(), tableId, "Table refresh");
                }
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

    /**
     * Spill table to disk.
     * <p>
     * Table object (metadata) is serialized into one file. Pages are serialized separately in logical part.
     *
     * @param id table id to spill
     */
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

    /**
     * Restore the table from disk to load into tables map.
     * <p>
     * Only the skeleton of the table and the logical parts in it will be restored at this time.
     * (pages won't be loaded until used)
     *
     * @param id table to be restored
     */
    public synchronized void restoreTable(long id)
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
            SecureObjectInputStream ois = new SecureObjectInputStream(inputStream, Table.TYPES_WHITELIST);
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
                releaseMemory(tableProcessedBytes.get(lru.getKey()).get(), lru.getKey(), "Offloading LRU");
                tables.get(lru.getKey()).offLoadPages(); // clean reference and help GC
                // Remove tableID from corresponding maps
                tables.remove(lru.getKey());
                tableProcessedBytes.remove(lru.getKey());
                tableNotProcessedBytes.remove(lru.getKey());
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
        if (tables.keySet().contains(reserved)) { // Ensure valid tableID
            currentBytes.set(newSize);
            tableProcessedBytes.get(reserved).set(newSize); // New table byte size
            onSuccess.run();
            logNumFormat("Fulfilled %s bytes for Table %s. Current: %s", bytes, reserved, currentBytes.get());
        }
    }

    private synchronized void releaseMemory(long bytes, long tableId, String reason)
    {
        currentBytes.addAndGet(-bytes); // CurrentBytes reduce by bytes variable
        tableProcessedBytes.get(tableId).addAndGet(-bytes); // Reduce bytes usage correspondingly
        tableNotProcessedBytes.get(tableId).set(0L); // Newly added bytes has been freed
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

    @Managed
    public long getCurrentBytes()
    {
        return currentBytes.get();
    }

    @Managed
    public long getAllTablesMemoryByteUsage()
    {
        long totalBytes = 0L;
        for (Table table : tables.values()) {
            totalBytes += table.getByteSize();
        }
        return totalBytes;
    }

    @Managed
    public long getAllTablesDiskByteUsage() throws IOException
    {
        long totalBytes = 0L;
        for (long id : tables.keySet()) {
            long diskUsageSize = Files.walk(spillRoot.resolve(String.valueOf(id)))
                    .filter(p -> p.toFile().isFile())
                    .mapToLong(p -> p.toFile().length())
                    .sum();
            totalBytes += diskUsageSize;
        }
        return totalBytes;
    }
}
