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

package io.hetu.core.spi.heuristicindex;

/**
 * Metadata for each individual index of a split
 *
 * @since 2019-10-18
 */
public class SplitIndexMetadata
{
    private final Index index;
    private final SplitMetadata splitMetadata;
    private final long lastUpdated;

    /**
     * Constructor
     *
     * @param index         Type of index being applied
     * @param splitMetadata Metadata of the split
     * @param lastUpdated   Creation time of the index for this split.
     */
    public SplitIndexMetadata(Index index, SplitMetadata splitMetadata,
                              long lastUpdated)
    {
        this.index = index;
        this.splitMetadata = splitMetadata;
        this.lastUpdated = lastUpdated;
    }

    public String getRootUri()
    {
        return splitMetadata.getRootUri();
    }

    public String getTable()
    {
        return splitMetadata.getTable();
    }

    public String getColumn()
    {
        return splitMetadata.getColumn();
    }

    public Index getIndex()
    {
        return index;
    }

    public String getUri()
    {
        return splitMetadata.getUri();
    }

    public long getSplitStart()
    {
        return splitMetadata.getSplitStart();
    }

    public long getLastUpdated()
    {
        return lastUpdated;
    }

    @Override
    public String toString()
    {
        return "{"
                + "table=" + getTable()
                + ", column=" + getColumn()
                + ", index=" + index.getId()
                + ", uri='" + getUri()
                + ", splitStart='" + getSplitStart()
                + ", lastUpdated=" + lastUpdated
                + '}';
    }
}
