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

import com.google.common.collect.ImmutableList;
import io.hetu.core.common.util.SecurePathWhiteList;
import io.prestosql.spi.filesystem.FileBasedLock;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.filesystem.SupportedFileAttributes;
import io.prestosql.spi.heuristicindex.IndexRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class IndexRecordManager
{
    private static final String RECORD_FILE_NAME = "INDEX_RECORDS";
    private final HetuFileSystemClient fs;
    private final Path root;

    private List<IndexRecord> cache;
    private long cacheLastModifiedTime;

    public IndexRecordManager(HetuFileSystemClient fs, Path root)
    {
        this.fs = fs;
        this.root = root;
        try {
            checkArgument(!root.toString().contains("../"), "Index store directory path must be absolute");
            checkArgument(SecurePathWhiteList.isSecurePath(root.toString()),
                    "Index store directory path must be at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Failed to get secure path list.", e);
        }
    }

    public List<IndexRecord> getIndexRecords()
            throws IOException
    {
        Path recordFile = root.resolve(RECORD_FILE_NAME);
        ImmutableList.Builder<IndexRecord> records = ImmutableList.builder();

        if (!fs.exists(recordFile)) {
            synchronized (this) {
                // invalidate cache
                cache = records.build();
                cacheLastModifiedTime = 0;
            }
            return cache;
        }

        long modifiedTime = (long) fs.getAttribute(recordFile, SupportedFileAttributes.LAST_MODIFIED_TIME);
        if (modifiedTime != cacheLastModifiedTime) {
            synchronized (this) {
                if (modifiedTime == cacheLastModifiedTime) {
                    // already updated by another call
                    return cache;
                }
                // invalidate cache
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.newInputStream(recordFile)))) {
                    reader.readLine(); // skip header
                    String line;
                    while ((line = reader.readLine()) != null) {
                        records.add(new IndexRecord(line));
                    }
                }
                catch (Exception e) {
                    throw new IllegalArgumentException(
                            "Error reading index record. If you have recently updated server, please delete old index directory and recreate the indices.", e);
                }
                cache = records.build();
                cacheLastModifiedTime = modifiedTime;
            }
        }

        return cache;
    }

    public IndexRecord lookUpIndexRecord(String name)
            throws IOException
    {
        List<IndexRecord> records = getIndexRecords();

        for (IndexRecord record : records) {
            if (record.name.equals(name)) {
                return record;
            }
        }

        return null;
    }

    public IndexRecord lookUpIndexRecord(String table, String[] columns, String indexType)
            throws IOException
    {
        List<IndexRecord> records = getIndexRecords();

        for (IndexRecord record : records) {
            if (record.table.equals(table) && Arrays.equals(record.columns, columns) && record.indexType.equals(indexType)) {
                return record;
            }
        }

        return null;
    }

    /**
     * Add IndexRecord into record file. If the method is called with a name that already exists,
     * it will OVERWRITE the existing entry but combine the partition column
     */
    public synchronized void addIndexRecord(String name, String user, String table, String[] columns, String indexType, List<String> indexProperties, List<String> partitions)
            throws IOException
    {
        // Protect root directory
        FileBasedLock lock = new FileBasedLock(fs, root);
        try {
            lock.lock();
            List<IndexRecord> records = new ArrayList<>(getIndexRecords()); // read from records and make a copy
            Iterator<IndexRecord> iterator = records.iterator();
            while (iterator.hasNext()) {
                IndexRecord record = iterator.next();
                if (name.equals(record.name)) {
                    partitions.addAll(0, record.partitions);
                    iterator.remove();
                }
            }
            records.add(new IndexRecord(name, user, table, columns, indexType, indexProperties, partitions));
            writeIndexRecords(records);
        }
        finally {
            lock.unlock();
        }
    }

    public synchronized void deleteIndexRecord(String name, List<String> partitionsToRemove)
            throws IOException
    {
        // Protect root directory
        FileBasedLock lock = new FileBasedLock(fs, root);
        try {
            lock.lock();
            List<IndexRecord> records = new ArrayList<>(getIndexRecords()); // read from records and make a copy
            if (partitionsToRemove.isEmpty()) {
                // remove record
                records.removeIf(record -> record.name.equals(name));
            }
            else {
                // only remove partitions
                Iterator<IndexRecord> iterator = records.iterator();
                IndexRecord newRecord = null;
                while (iterator.hasNext()) {
                    IndexRecord record = iterator.next();
                    if (record.name.equals(name)) {
                        record.partitions.removeAll(partitionsToRemove);
                        newRecord = new IndexRecord(record.name, record.user, record.table,
                                record.columns, record.indexType, record.properties, record.partitions);
                        iterator.remove();
                    }
                }
                // If this is a partial remove, and there are still partitions remaining, put the updated record back
                if (newRecord != null && !newRecord.partitions.isEmpty()) {
                    records.add(newRecord);
                }
            }
            writeIndexRecords(records);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Write the given records into the record file. This operation OVERWRITES the existing file and is NOT atomoc.
     * Therefore it should only be called from lock-protected block to avoid overwriting data.
     */
    private void writeIndexRecords(List<IndexRecord> records)
            throws IOException
    {
        Path recordFile = root.resolve(RECORD_FILE_NAME);

        try (OutputStream os = fs.newOutputStream(recordFile)) {
            // Use IndexRecord to generate a special "entry" as table head so it's easier to maintain when csv format changes
            String head = IndexRecord.getHeader();
            os.write(head.getBytes());
            for (IndexRecord record : records) {
                os.write(record.toCsvRecord().getBytes());
            }
        }
    }
}
