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
package io.prestosql.spi.heuristicindex;

import io.prestosql.spi.connector.CreateIndexMetadata;

import java.io.IOException;
import java.util.List;

public interface IndexClient
{
    List<IndexMetadata> readSplitIndex(String path)
            throws IOException;

    /**
     * Searches the path for lastModified file and returns the value as a long.
     * The filename is expected to be in the form: lastModified=123456.
     * <p>
     * If the path is a directory, only the direct children will be searched
     * (i.e. not recursively).
     * <p>
     * If multiple lastModified files are present, only the first one will be read
     *
     * @param path URI to the file
     * @return last modified time or 0 if not found.
     * @throws IOException thrown by filesystem client
     */
    long getLastModified(String path)
            throws IOException;

    /**
     * Reads the partition index file from the specified path.
     * @param path
     */
    List<IndexMetadata> readPartitionIndex(String path)
            throws IOException;

    /**
     * Delete the indexes for the table, according to the index name.
     *
     * @param indexName index name
     * @param partitionsToDelete partitionsToDelete. Delete all partitions if empty list provided
     * @throws IOException any IOException thrown by filesystem client during file deletion
     */
    void deleteIndex(String indexName, List<String> partitionsToDelete)
            throws IOException;

    /**
     * Add the indexes record for the table
     *
     * @param createIndexMetadata metadata of the index
     * @throws IOException any IOException thrown by filesystem client during file creation
     */
    void addIndexRecord(CreateIndexMetadata createIndexMetadata)
            throws IOException;

    /**
     * Check indexes record exist before create new index
     *
     * @param createIndexMetadata metadata of the index
     * @throws IOException any IOException thrown by filesystem client during file read
     */
    boolean indexRecordExists(CreateIndexMetadata createIndexMetadata)
            throws IOException;

    /**
     * Load all indexes records
     *
     * @throws IOException any IOException thrown by filesystem client during file read
     */
    List<IndexRecord> getAllIndexRecords()
            throws IOException;

    /**
     * look up index record
     *
     * @param name index name
     * @throws IOException any IOException thrown by filesystem client during file read
     */
    IndexRecord getIndexRecord(String name)
            throws IOException;
}
