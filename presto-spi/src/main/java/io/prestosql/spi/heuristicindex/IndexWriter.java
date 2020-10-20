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

public interface IndexWriter
{
    /**
     * <pre>
     * Creates the index for the specified columns. Filters on partitions if specified.
     *
     * The configured DataSource is responsible for reading the column values.
     * Indexes are created for each specified type and populated with columns
     * and the Index is written to the filesystem.
     *
     * The index files will be stored at:
     * [root uri]/[table]/[column]/[source file path]/[index file]
     *
     * where [index file] is of format:
     *
     * [source file name]#[splitStart].[indexType]
     *
     * for example:
     * /tmp/hetu/indices/hive.farhan.index_test_1k/g5/user/hive/warehouse/farhan.db/index_test_1k/20190904_221627_00003_ygvh9_1e9b8c7c-cbe8-4ff2-a1d5-73e3fecd1138/20190904_221627_00003_ygvh9_1e9b8c7c-cbe8-4ff2-a1d5-73e3fecd1138#0.bloom
     *
     * where:
     *
     * root uri = /tmp/hetu/indices
     * table = hive.farhan.index_test_1k
     * column = g5
     * source file path = <!--
     * -->/user/hive/warehouse/farhan.db/index_test_1k/20190904_221627_00003_ygvh9_1e9b8c7c-cbe8-4ff2-\a1d5-73e3fecd1138
     * index file = 20190904_221627_00003_ygvh9_1e9b8c7c-cbe8-4ff2-a1d5-73e3fecd1138#0.bloom
     *
     * the index file is of type bloom and the splt has a start of 0
     * </pre>
     *
     * @param table fully qualified table name
     * @param columns columns to index
     * @param partitions only index specified partitions, if null, index all partitions
     * @param indexType type of the index to be created (its string ID returned from {@link Index#getId()})
     * @throws IOException thrown during index creation
     */
    void createIndex(String table, String[] columns, String[] partitions, String indexType)
            throws IOException;
}
