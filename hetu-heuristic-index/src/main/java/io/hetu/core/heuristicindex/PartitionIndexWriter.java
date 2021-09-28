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
package io.hetu.core.heuristicindex;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.airlift.log.Logger;
import io.hetu.core.common.util.SecurePathWhiteList;
import io.hetu.core.heuristicindex.util.IndexConstants;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.connector.CreateIndexMetadata;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.IndexWriter;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.type.Type;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.heuristicindex.SerializationUtils.serializeMap;
import static io.prestosql.spi.heuristicindex.SerializationUtils.serializeStripeSymbol;

/**
 * Indexes which needs to be created at table or partition level
 * needs to use this writer. E.g. BTREE index.
 */
public class PartitionIndexWriter
        implements IndexWriter
{
    public static final String SYMBOL_TABLE_KEY_NAME = "__hetu__symboltable";
    public static final String PREFIX_KEY_NAME = "__hetu__pathprefix";
    public static final String MAX_MODIFIED_TIME = "__hetu__maxmodifiedtime";

    private static final Logger LOG = Logger.get(PartitionIndexWriter.class);

    private final CreateIndexMetadata createIndexMetadata;
    private final Lock persistLock = new ReentrantLock();
    private final Properties properties;
    private final HetuFileSystemClient fs;
    private final Path root;

    /*
    Each stripe from pages is mapped to a auto-incremental integer, starting from 0.
    This mapping is stored in symbolToIdMap like 0 -> "<ORCFileName>:<stripeStart>:<stripeEnd>"
    See more about serialization at {@link io.prestosql.spi.heuristicindex.SerializationUtils}

    The inverted index is stored in data map like {10 -> "1,2", 2 -> "2,3"}, meaning that
    value 10 occurs in stripe 1 and 2, and value 2 occurs in stripe 2 and 3. The majority of memory
    is used by this map.
     */
    private final AtomicInteger counter = new AtomicInteger(0); // symbol table counter
    private final Map<String, String> symbolToIdMap;
    private final Map<Comparable<? extends Comparable<?>>, String> dataMap;

    private Index partitionIndex;
    private String partition;
    private Long maxLastModifiedTime = 0L;

    public PartitionIndexWriter(CreateIndexMetadata createIndexMetadata, HetuFileSystemClient fs, Path root)
    {
        this.createIndexMetadata = createIndexMetadata;
        this.fs = fs;
        this.root = root;
        properties = new Properties();
        symbolToIdMap = new ConcurrentHashMap<>();
        dataMap = new ConcurrentHashMap<>();
    }

    @Override
    public void addData(Map<String, List<Object>> values, Properties connectorMetadata)
            throws IOException
    {
        Path path = Paths.get(connectorMetadata.getProperty(HetuConstant.DATASOURCE_FILE_PATH));

        if (Strings.isNullOrEmpty(partition)) {
            if (createIndexMetadata.getCreateLevel() == CreateIndexMetadata.Level.PARTITION) {
                partition = path.getName(path.getNameCount() - 2).toString();
            }
        }

        long lastModified = Long.parseLong(connectorMetadata.getProperty(HetuConstant.DATASOURCE_FILE_MODIFICATION));
        long offset = Long.parseLong(connectorMetadata.getProperty(HetuConstant.DATASOURCE_STRIPE_OFFSET));
        long stripeLength = Long.parseLong(connectorMetadata.getProperty(HetuConstant.DATASOURCE_STRIPE_LENGTH));

        if (lastModified > maxLastModifiedTime) {
            maxLastModifiedTime = lastModified;
        }
        fillDataMap(values, serializeStripeSymbol(path.toString(), offset, offset + stripeLength));
        LOG.debug("Symbol Table: " + symbolToIdMap);
    }

    private void fillDataMap(Map<String, List<Object>> values, String symbol)
    {
        Map.Entry<String, List<Object>> valueEntry = values.entrySet().iterator().next();

        String code;
        if (this.symbolToIdMap.containsKey(symbol)) {
            code = symbolToIdMap.get(symbol);
        }
        else {
            code = String.valueOf(counter.incrementAndGet());
            this.symbolToIdMap.put(symbol, code);
        }
        for (Object key : valueEntry.getValue()) {
            if (key != null) {
                // key must be a Comparable<T extends Comparable<T>> to be inserted into btree
                Comparable<? extends Comparable<?>> comparableKey = (Comparable<? extends Comparable<?>>) key;
                String existing = dataMap.putIfAbsent(comparableKey, code);
                if (existing != null) {
                    // replace old string with new values added. e.g. "1,2,2" -> "1,2,2,3"
                    // while loop used to allow concurrent modification.
                    // THIS IS UGLY BUT IS THE WORKING SOLUTION TO SAVE MEMORY.
                    // Tried to use collections like List[1,2,2,3] or Set[1,2,3] but both
                    // crash node with too high memory usage.
                    // TODO: Resolved memory issue in a more decent way
                    String newData = getNewData(key, code);
                    boolean done = dataMap.replace(comparableKey, existing, newData);
                    while (!done) {
                        existing = dataMap.get(key);
                        newData = getNewData(key, code);
                        done = dataMap.replace(comparableKey, existing, newData);
                    }
                }
            }
        }
    }

    private String getNewData(Object key, String splitData)
    {
        String output = dataMap.get(key);
        return output + "," + splitData;
    }

    /**
     * Persists the data into an Index object and serialize it to disk.
     */
    @Override
    public long persist()
            throws IOException
    {
        persistLock.lock();
        try {
            // inverse map from symbol -> id to id -> symbol for better lookup performance
            Map<String, String> idToSymbolMap = symbolToIdMap.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

            String serializedSymbolTable = serializeMap(idToSymbolMap);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Symbol table size: " + idToSymbolMap.size());
                LOG.debug("Output map size: " + dataMap.size());
                LOG.debug("symbol table: " + serializedSymbolTable);
            }

            String dbPath = "";
            for (Pair<String, Type> entry : createIndexMetadata.getIndexColumns()) {
                if (partition != null) {
                    dbPath = this.root + "/" + createIndexMetadata.getTableName() + "/" + entry.getFirst().toLowerCase(Locale.ENGLISH) + "/" + createIndexMetadata.getIndexType().toUpperCase() + "/" + partition;
                }
                else {
                    dbPath = this.root + "/" + createIndexMetadata.getTableName() + "/" + entry.getFirst().toLowerCase(Locale.ENGLISH) + "/" + createIndexMetadata.getIndexType().toUpperCase();
                }
                partitionIndex = HeuristicIndexFactory.createIndex(createIndexMetadata.getIndexType());
            }

            // check required for security scan since we are constructing a path using input
            checkArgument(!dbPath.toString().contains("../"),
                    dbPath + " must be absolute and under one of the following whitelisted directories:  " + SecurePathWhiteList.getSecurePathWhiteList().toString());
            checkArgument(SecurePathWhiteList.isSecurePath(dbPath),
                    dbPath + " must be under one of the following whitelisted directories: " + SecurePathWhiteList.getSecurePathWhiteList().toString());

            List<Pair<Comparable<? extends Comparable<?>>, String>> values = new ArrayList<>(dataMap.size());
            for (Map.Entry<Comparable<? extends Comparable<?>>, String> entry : dataMap.entrySet()) {
                values.add(new Pair<>(entry.getKey(), entry.getValue()));
            }
            String columnName = createIndexMetadata.getIndexColumns().get(0).getFirst().toLowerCase(Locale.ENGLISH);
            partitionIndex.addKeyValues(Collections.singletonList(new Pair<>(columnName, values)));

            properties.put(SYMBOL_TABLE_KEY_NAME, serializedSymbolTable);
            properties.put(MAX_MODIFIED_TIME, String.valueOf(maxLastModifiedTime));
            partitionIndex.setProperties(properties);
            Path filePath = Paths.get(dbPath + "/" + IndexConstants.LAST_MODIFIED_FILE_PREFIX + maxLastModifiedTime);

            // check required for security scan since we are constructing a path using input
            checkArgument(!filePath.toString().contains("../"),
                    filePath + " must be absolute and under one of the following whitelisted directories:  " + SecurePathWhiteList.getSecurePathWhiteList().toString());
            checkArgument(SecurePathWhiteList.isSecurePath(dbPath),
                    filePath + " must be under one of the following whitelisted directories: " + SecurePathWhiteList.getSecurePathWhiteList().toString());

            List<Path> oldFiles = Collections.emptyList();
            if (fs.exists(Paths.get(dbPath))) {
                oldFiles = fs.walk(Paths.get(dbPath)).filter(p -> !fs.isDirectory(p)).collect(Collectors.toList());
            }
            for (Path oldFile : oldFiles) {
                fs.deleteIfExists(oldFile);
            }
            fs.createDirectories(filePath.getParent());
            try (OutputStream os = fs.newOutputStream(filePath)) {
                partitionIndex.serialize(os);
            }
            catch (IOException e) {
                // roll back creation
                fs.delete(filePath);
                throw e;
            }
            return (long) fs.getAttribute(filePath, "size");
        }
        finally {
            if (partitionIndex != null) {
                partitionIndex.close();
            }
            persistLock.unlock();
        }
    }

    @VisibleForTesting
    protected Map<Comparable<? extends Comparable<?>>, String> getDataMap()
    {
        return this.dataMap;
    }

    @VisibleForTesting
    protected Map<String, String> getSymbolTable()
    {
        return this.symbolToIdMap;
    }
}
