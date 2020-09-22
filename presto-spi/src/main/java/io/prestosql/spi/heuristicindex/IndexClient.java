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

import java.io.IOException;
import java.util.List;

public interface IndexClient
{
    /**
     * <pre>
     * Reads all split indexes at the specified path recursively.
     * It is assumed that the provided path is relative to the filesystem client's root uri, if one was set
     *
     * All split indexes are loaded into Index objects based on their file extensions.
     * For example, if a file name is filename.bloom, then the file will be loaded
     * as a BloomIndex.
     *
     * The split file paths must match the following pattern:
     * <b>root uri</b>/<b>table</b>/<b>column</b>/<b>source file path</b>/<b>index file</b>
     *
     * where <i>index file</i> is of format:
     *
     * <i>source file name</i>#<i>splitStart</i>.<i>indexType</i>
     *
     * for example:
     * /tmp/hetu/indices/hive.farhan.index_test_1k/g5/user/hive/warehouse/farhan.db/index_test_1k/20190904_221627_0
     * 0003_ygvh9_1e9b8c7c-cbe8-4ff2-a1d5-73e3fecd1138/20190904_221627_00003_ygvh9_1e9b8c7c-cbe8-4ff2-
     * a1d5-73e3fecd1138#0.bloom
     *
     * where:
     * root uri = /tmp/hetu/indices
     * table = hive.farhan.index_test_1k
     * column = g5
     * source file path = /user/hive/warehouse/farhan.db/index_test_1k/20190904_221627_00003_ygvh9_1e9b8c7c-cbe8-4ff2-
     * a1d5-73e3fecd1138
     * index file = 20190904_221627_00003_ygvh9_1e9b8c7c-cbe8-4ff2-a1d5-73e3fecd1138#0.bloom
     *
     * </pre>
     *
     * @param path relative path to the split index file or dir, if dir, it will be searched recursively (relative to \
     * the root uri, if one was set)
     * @return all split indexes that were read, with the split metadata set based on the split path
     * @throws IOException thrown by doing IO operations using filesystem client
     */
    public List<IndexMetadata> readSplitIndex(String path)
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
    public long getLastModified(String path) throws IOException;

    /**
     * Delete the indexes for the table, if columns are specified, only indexes
     * for the specified columns will be deleted.
     *
     * @param table table of the index
     * @param columns columns of the index
     * @param indexType the index type
     * @throws IOException any IOException thrown by filesystem client during file deletion
     */
    public void deleteIndex(String table, String[] columns, String indexType)
            throws IOException;
}
