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

import io.hetu.core.common.util.SecurePathWhiteList;
import io.prestosql.spi.filesystem.FileBasedLock;
import io.prestosql.spi.filesystem.HetuFileSystemClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static io.hetu.core.heuristicindex.util.IndexConstants.COLUMN_DELIMITER;

public class IndexRecordManager
{
    private static final String RECORD_FILE_NAME = "INDEX_RECORDS";

    private IndexRecordManager() {}

    public static List<IndexRecord> readAllIndexRecords(HetuFileSystemClient fs, Path root)
            throws IOException
    {
        validatePath(root);
        Path recordFile = root.resolve(RECORD_FILE_NAME);
        List<IndexRecord> records = new ArrayList<>();

        if (!fs.exists(recordFile)) {
            return records;
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.newInputStream(recordFile)))) {
            reader.readLine(); // skip header
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                records.add(new IndexRecord(line));
            }
        }

        return records;
    }

    public static IndexRecord lookUpIndexRecord(HetuFileSystemClient fs, Path root, String name)
            throws IOException
    {
        validatePath(root);
        List<IndexRecord> records = readAllIndexRecords(fs, root);

        for (IndexRecord record : records) {
            if (record.name.equals(name)) {
                return record;
            }
        }

        return null;
    }

    public static IndexRecord lookUpIndexRecord(HetuFileSystemClient fs, Path root, String table, String[] columns, String indexType)
            throws IOException
    {
        validatePath(root);
        List<IndexRecord> records = readAllIndexRecords(fs, root);

        for (IndexRecord record : records) {
            if (record.table.equals(table) && Arrays.equals(record.columns, columns) && record.indexType.equals(indexType)) {
                return record;
            }
        }

        return null;
    }

    /**
     * Add IndexRecord into record file. If the method is called with a name that already exists,
     * it will OVERWRITE the existing entry but combine the note part
     */
    public static synchronized void addIndexRecord(HetuFileSystemClient fs, Path root, String name, String user, String table, String[] columns, String indexType, String... note)
            throws IOException
    {
        validatePath(root);
        // Protect root directory
        FileBasedLock lock = new FileBasedLock(fs, root);
        try {
            lock.lock();
            List<IndexRecord> records = readAllIndexRecords(fs, root);
            String noteToWrite = String.join(",", note);
            Iterator<IndexRecord> iterator = records.iterator();
            while (iterator.hasNext()) {
                IndexRecord record = iterator.next();
                if (name.equals(record.name)) {
                    noteToWrite = record.note.equals("") ? noteToWrite : record.note + "," + noteToWrite;
                    iterator.remove();
                }
            }
            records.add(new IndexRecord(name, user, table, columns, indexType, noteToWrite));
            writeIndexRecords(fs, root, records);
        }
        finally {
            lock.unlock();
        }
    }

    public static synchronized void deleteIndexRecord(HetuFileSystemClient fs, Path root, String name)
            throws IOException
    {
        validatePath(root);
        // Protect root directory
        FileBasedLock lock = new FileBasedLock(fs, root);
        try {
            lock.lock();
            List<IndexRecord> records = readAllIndexRecords(fs, root);
            records.removeIf(record -> record.name.equals(name));
            writeIndexRecords(fs, root, records);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Write the given records into the record file. This operation OVERWRITES the existing file and is NOT atomoc.
     * Therefore it should only be called from lock-protected block to avoid overwriting data.
     */
    private static void writeIndexRecords(HetuFileSystemClient fs, Path root, List<IndexRecord> records)
            throws IOException
    {
        validatePath(root);
        Path recordFile = root.resolve(RECORD_FILE_NAME);

        boolean writeHead = false;
        try (OutputStream os = fs.newOutputStream(recordFile)) {
            // Use IndexRecord to generate a special "entry" as table head so it's easier to maintain when csv format changes
            String head = new IndexRecord("Name", "User", "Table", new String[] {"Columns"}, "IndexType", "Notes").toCsvRecord();
            os.write(head.getBytes());
            for (IndexRecord record : records) {
                os.write(record.toCsvRecord().getBytes());
            }
        }
    }

    private static void validatePath(Path root)
    {
        try {
            checkArgument(!root.toString().contains("../"), "Index store directory path must be absolute");
            checkArgument(SecurePathWhiteList.isSecurePath(root.toString()),
                    "Index store directory path must be at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Failed to get secure path list.", e);
        }
    }

    public static class IndexRecord
    {
        public final String name;
        public final String user;
        public final String table;
        public final String[] columns;
        public final String indexType;
        public final String note;

        public IndexRecord(String name, String user, String table, String[] columns, String indexType, String note)
        {
            this.name = name;
            this.user = user == null ? "" : user;
            this.table = table;
            this.columns = columns;
            this.indexType = indexType;
            this.note = note;
        }

        public IndexRecord(String csvRecord)
        {
            String[] records = csvRecord.split("\\t");
            this.name = records[0];
            this.user = records[1];
            this.table = records[2];
            this.columns = records[3].split(COLUMN_DELIMITER);
            this.indexType = records[4];
            this.note = records.length > 5 ? records[5] : "";
        }

        public String toCsvRecord()
        {
            return String.format("%s\t%s\t%s\t%s\t%s\t%s\n", name, user, table, String.join(COLUMN_DELIMITER, columns), indexType, note);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof IndexRecord)) {
                return false;
            }
            IndexRecord that = (IndexRecord) o;
            return Objects.equals(name, that.name) &&
                    Objects.equals(user, that.user) &&
                    Objects.equals(table, that.table) &&
                    Arrays.equals(columns, that.columns) &&
                    Objects.equals(indexType, that.indexType);
        }

        @Override
        public int hashCode()
        {
            int result = Objects.hash(name, user, table, indexType);
            result = 31 * result + Arrays.hashCode(columns);
            return result;
        }

        @Override
        public String toString()
        {
            return name + ","
                    + user + ","
                    + table + ","
                    + "[" + String.join(",", columns) + "],"
                    + indexType;
        }
    }
}
