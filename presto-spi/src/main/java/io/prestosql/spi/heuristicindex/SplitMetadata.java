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
public class SplitMetadata
{
    private final String splitIdentity;
    private final long lastModifiedTime;

    /**
     * Constructor
     *
     * @param splitIdentity Identity of split
     * @param lastModifiedTime Creation time of the split.
     */
    public SplitMetadata(String splitIdentity, long lastModifiedTime)
    {
        this.splitIdentity = splitIdentity;
        this.lastModifiedTime = lastModifiedTime;
    }

    public String getSplitIdentity()
    {
        return splitIdentity;
    }

    public long getLastModifiedTime()
    {
        return lastModifiedTime;
    }

    @Override
    public String toString()
    {
        return "SplitMetadata{" +
                "splitIdentity=" + splitIdentity +
                ", lastUpdated=" + lastModifiedTime +
                '}';
    }
}
