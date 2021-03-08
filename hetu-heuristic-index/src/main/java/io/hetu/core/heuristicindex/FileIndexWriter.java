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
package io.hetu.core.heuristicindex;

import io.airlift.log.Logger;
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.hetu.core.heuristicindex.util.IndexConstants;
import io.hetu.core.heuristicindex.util.IndexServiceUtils;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.connector.CreateIndexMetadata;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.IndexWriter;
import io.prestosql.spi.heuristicindex.Pair;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.prestosql.spi.HetuConstant.DATASOURCE_PAGE_NUMBER;
import static io.prestosql.spi.HetuConstant.DATASOURCE_STRIPE_OFFSET;
import static io.prestosql.spi.HetuConstant.DATASOURCE_TOTAL_PAGES;
import static java.util.Objects.requireNonNull;

/**
 * All of the values added to this index writer should belong to one single data source file (e.g. one ORC file on Hive)
 * <p>
 * This writer cache the result in an internal map in (offset -> index) format and write them to disk whenever finish() is called
 *
 * @since 2019-10-11
 */
public class FileIndexWriter
        implements IndexWriter
{
    private static final HetuFileSystemClient LOCAL_FS_CLIENT = new HetuLocalFileSystemClient(new LocalConfig(new Properties()), Paths.get("/"));
    private static final Logger LOG = Logger.get(FileIndexWriter.class);

    private final String dataSourceFileName;
    private final String dataSourceFileLastModifiedTime;
    // "stripe offset" -> (column name -> list<entry<page values, page number>>)
    private final Map<Long, Map<String, List<Map.Entry<List<Object>, Integer>>>> indexPages;
    private final Map<Long, AtomicInteger> pageCountExpected;
    private final CreateIndexMetadata createIndexMetadata;
    private final HetuFileSystemClient fs;
    private final Path root;
    private Path tmpPath;

    /**
     * Constructor
     *
     * @param createIndexMetadata metadata of create index, includes indexName, tableName, indexType, indexColumns and partitions
     * @param fs                  filesystem client to access filesystem where the indexes are persisted/stored
     */
    public FileIndexWriter(CreateIndexMetadata createIndexMetadata, Properties connectorMetadata, HetuFileSystemClient fs, Path root)
    {
        this.createIndexMetadata = createIndexMetadata;
        this.dataSourceFileName = Paths.get(connectorMetadata.getProperty(HetuConstant.DATASOURCE_FILE_PATH)).toString();
        this.dataSourceFileLastModifiedTime = connectorMetadata.getProperty(HetuConstant.DATASOURCE_FILE_MODIFICATION);
        this.fs = requireNonNull(fs);
        this.root = root;
        this.indexPages = new ConcurrentHashMap<>();
        this.pageCountExpected = new ConcurrentHashMap<>();
    }

    /**
     * This method IS thread-safe. Multiple operators can add data to one writer in parallel.
     *
     * @param values            values to be indexed
     * @param connectorMetadata metadata for the index
     */
    @Override
    public void addData(Map<String, List<Object>> values, Properties connectorMetadata)
            throws IOException
    {
        long stripeOffset = Long.parseLong(connectorMetadata.getProperty(DATASOURCE_STRIPE_OFFSET));

        // Add values first
        indexPages.computeIfAbsent(stripeOffset, k -> new ConcurrentHashMap<>());
        for (Map.Entry<String, List<Object>> e : values.entrySet()) {
            indexPages.get(stripeOffset).computeIfAbsent(e.getKey(),
                    k -> Collections.synchronizedList(new LinkedList<>()))
                    .add(new AbstractMap.SimpleEntry(e.getValue(), Integer.parseInt(connectorMetadata.getProperty(DATASOURCE_PAGE_NUMBER))));
        }

        // Update page count
        int current = pageCountExpected.computeIfAbsent(stripeOffset, k -> new AtomicInteger()).decrementAndGet();
        if (connectorMetadata.getProperty(DATASOURCE_TOTAL_PAGES) != null) {
            int expected = Integer.parseInt(connectorMetadata.getProperty(DATASOURCE_TOTAL_PAGES));
            int updatedCurrent = pageCountExpected.get(stripeOffset).addAndGet(expected);
            LOG.debug("offset %d finishing page received, expected page count: %d, actual received: %d, remaining: %d",
                    stripeOffset, expected, -current, updatedCurrent);
        }

        // Check page count to know if all pages have been received for a stripe. Persist and delete values if true to save memory
        if (pageCountExpected.get(stripeOffset).get() == 0) {
            synchronized (pageCountExpected.get(stripeOffset)) {
                if (indexPages.containsKey(stripeOffset)) {
                    LOG.debug("All pages for offset %d have been received. Persisting.", stripeOffset);
                    // sort the stripe's pages and collect the values into a single list
                    List<Pair<String, List<Object>>> columnValuesMap = new ArrayList<>();
                    // each entry represents a mapping from column name -> list<entry<page values, page number>>
                    for (Map.Entry<String, List<Map.Entry<List<Object>, Integer>>> entry : indexPages.get(stripeOffset).entrySet()) {
                        // sort the page values lists based on page numbers
                        entry.getValue().sort(Comparator.comparingInt(Map.Entry::getValue));
                        // collect all page values lists into a single list
                        List<Object> columnValues = entry.getValue().stream()
                                .map(Map.Entry::getKey).flatMap(Collection::stream).collect(Collectors.toList());
                        columnValuesMap.add(new Pair(entry.getKey(), columnValues));
                    }

                    persistStripe(stripeOffset, columnValuesMap);
                    indexPages.remove(stripeOffset);
                }
                else {
                    LOG.debug("All pages for offset %d have been received, but the values are missing. " +
                            "This stripe should have already been persisted by another thread.", stripeOffset);
                }
            }
        }
    }

    /**
     * This method is NOT thread-safe. Should never be called in parallel.
     * <p>
     * Persist index files with following file structure:
     *
     * <pre>
     * /--- {this.root}
     *   |--- INDEX_RECORDS
     *   |--- table1
     *     |--- column
     *       |--- indexType1
     *         |--- [ORC FilePath] (e.g. /user/hive/warehouse/.../OrcFileName/)
     *           ...
     *             |--- [tarFile] (e.g. lastModified=123456.tar)
     *       |--- indexType2
     *   |--- table2
     *   |--- ...
     * </pre>
     *
     * @throws IOException when exceptions occur during persisting
     */
    @Override
    public void persist()
            throws IOException
    {
        for (Long offset : indexPages.keySet()) {
            LOG.error("Offset %d data is NOT PERSISTED. Current page count: %d. Check debug log.", offset, pageCountExpected.get(offset).get());
        }
        // Package index files for one File and write to remote filesystem
        String table = createIndexMetadata.getTableName();
        String column = createIndexMetadata.getIndexColumns().iterator().next().getFirst(); // Support indexing on only one column for now
        String type = createIndexMetadata.getIndexType();
        String lastModifiedFileName = IndexConstants.LAST_MODIFIED_FILE_PREFIX + dataSourceFileLastModifiedTime + ".tar";

        Path tarPath = Paths.get(root.toString(), table, column, type, dataSourceFileName, lastModifiedFileName);

        try {
            IndexServiceUtils.writeToHdfs(LOCAL_FS_CLIENT, fs, tmpPath, tarPath);
        }
        catch (IOException e) {
            LOG.debug("Error copying index files to remote filesystem: ", e);
            // roll back creation
            fs.delete(tarPath);
        }
        finally {
            LOCAL_FS_CLIENT.deleteRecursively(tmpPath);
        }
    }

    private void persistStripe(Long offset, List<Pair<String, List<Object>>> stripeData)
            throws IOException
    {
        synchronized (this) {
            if (tmpPath == null) {
                tmpPath = Files.createTempDirectory("tmp-indexwriter-");
            }
        }

        // Get sum of expected entries
        int expectedNumEntries = 0;
        for (Pair<String, List<Object>> l : stripeData) {
            expectedNumEntries += l.getSecond().size();
        }

        // Create index and put values
        try (Index index = HeuristicIndexFactory.createIndex(createIndexMetadata.getIndexType())) {
            index.setProperties(createIndexMetadata.getProperties());
            index.setExpectedNumOfEntries(expectedNumEntries);
            index.addValues(stripeData);

            // Persist one index (e.g. 3.bloom)
            String indexFileName = offset + "." + index.getId();
            try (OutputStream os = LOCAL_FS_CLIENT.newOutputStream(tmpPath.resolve(indexFileName))) {
                index.serialize(os);
            }
        }
    }
}
