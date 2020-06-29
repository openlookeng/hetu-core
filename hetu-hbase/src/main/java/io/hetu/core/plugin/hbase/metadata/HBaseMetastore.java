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
package io.hetu.core.plugin.hbase.metadata;

import java.util.Map;

/**
 * HBaseMetastore
 *
 * @since 2020-03-30
 */
public interface HBaseMetastore
{
    /**
     * init
     */
    void init();

    /**
     * getId
     *
     * @return String
     */
    String getId();

    /**
     * addHBaseTable
     *
     * @param hBaseTable HBaseTable
     */
    void addHBaseTable(HBaseTable hBaseTable);

    /**
     * renameHBaseTable
     *
     * @param newTable newTable
     * @param oldTable oldTable
     */
    void renameHBaseTable(HBaseTable newTable, String oldTable);

    /**
     * dropHBaseTable
     *
     * @param hBaseTable hbaseTable
     */
    void dropHBaseTable(HBaseTable hBaseTable);

    /**
     * getAllHBaseTables
     *
     * @return table map
     */
    Map<String, HBaseTable> getAllHBaseTables();

    /**
     * getHBaseTable
     *
     * @param tableName tableName
     * @return HBaseTable
     */
    HBaseTable getHBaseTable(String tableName);
}
