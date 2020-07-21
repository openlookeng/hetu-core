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

/**
 * Metadata for each individual index of a split
 *
 * @since 2019-10-18
 */
public class IndexMetadata
{
    private final Index index;
    private final String table;
    private final String column;
    private final String rootUri;
    private final String uri;
    private final long splitStart;
    private final long lastUpdated;

    /**
     * Constructor
     *
     * @param index         Type of index being applied
     * @param table         Table name of split
     * @param column        Column of split
     * @param rootUri       Base location where all indexes are stored
     * @param uri           Absolute location from where this split is stored
     * @param splitStart    The byte offset that the split starts at
     * @param lastUpdated   Creation time of the index for this split.
     */
    public IndexMetadata(Index index, String table, String column, String rootUri, String uri, long splitStart, long lastUpdated)
    {
        this.index = index;
        this.table = table;
        this.column = column;
        this.rootUri = rootUri;
        this.uri = uri;
        this.splitStart = splitStart;
        this.lastUpdated = lastUpdated;
    }

    public String getTable()
    {
        return table;
    }

    public String getColumn()
    {
        return column;
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

    public long getLastUpdated()
    {
        return lastUpdated;
    }

    @Override
    public String toString()
    {
        return "SplitIndexMetadata{" +
                "index=" + index +
                ", table='" + table + '\'' +
                ", column='" + column + '\'' +
                ", rootUri='" + rootUri + '\'' +
                ", uri='" + uri + '\'' +
                ", splitStart=" + splitStart +
                ", lastUpdated=" + lastUpdated +
                '}';
    }
}
