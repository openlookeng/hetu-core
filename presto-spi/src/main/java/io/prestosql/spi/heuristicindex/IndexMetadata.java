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

import java.util.Arrays;
import java.util.Properties;

/**
 * Metadata for each individual index of a split
 *
 * @since 2019-10-18
 */
public class IndexMetadata
{
    private final Index index;
    private final String table;
    private final String[] columns;
    private final String rootUri;
    private final String uri;
    private final Properties connectorMetadata;
    private final long splitStart;
    private final long lastModifiedTime;
    private final String checksum = "";
    private final long valueCount = 0L;
    private final long valueCardinality = 0L;

    /**
     * Constructor
     *
     * @param index Type of index being applied
     * @param table Table name of split
     * @param columns Column of split
     * @param rootUri Base location where all indexes are stored
     * @param uri Absolute location from where this split is stored
     * @param splitStart The byte offset that the split starts at
     * @param lastModifiedTime Creation time of the index for this split.
     */
    public IndexMetadata(Index index, String table, String[] columns, String rootUri, String uri, long splitStart, long lastModifiedTime)
    {
        this.index = index;
        this.table = table;
        this.columns = columns;
        this.rootUri = rootUri;
        this.uri = uri;
        this.splitStart = splitStart;
        this.lastModifiedTime = lastModifiedTime;
        this.connectorMetadata = new Properties();
    }

    public String getTable()
    {
        return table;
    }

    public String[] getColumns()
    {
        return columns;
    }

    public String getRootUri()
    {
        return rootUri;
    }

    public String getUri()
    {
        return uri;
    }

    public long getSplitStart()
    {
        return splitStart;
    }

    public Index getIndex()
    {
        return index;
    }

    public long getLastModifiedTime()
    {
        return lastModifiedTime;
    }

    public String getConnectorMetadata(String key)
    {
        return connectorMetadata.getProperty(key);
    }

    public void setConnectorMetadata(String key, String value)
    {
        connectorMetadata.setProperty(key, value);
    }

    @Override
    public String toString()
    {
        return "SplitIndexMetadata{" +
                "index=" + index +
                ", table='" + table + '\'' +
                ", column='" + Arrays.toString(columns) + '\'' +
                ", rootUri='" + rootUri + '\'' +
                ", uri='" + uri + '\'' +
                ", splitStart=" + splitStart +
                ", lastUpdated=" + lastModifiedTime +
                '}';
    }
}
