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
package io.prestosql.heuristicindex;

import io.prestosql.spi.heuristicindex.Index;

import java.util.Map;

public class PartitionIndexHolder
{
    private final Index index;
    private final Map<String, String> symbolTable;
    private final String partition;
    private final long maxLastUpdate;
    private final Map<String, Long> lastUpdated;
    private final Map<String, String> resultMap;

    public PartitionIndexHolder(Index index, String partition, Map<String, String> symbolTable, Map<String, Long> lastUpdated, Map<String, String> resultMap, long maxLastUpdate)
    {
        this.index = index;
        this.symbolTable = symbolTable;
        this.lastUpdated = lastUpdated;
        this.maxLastUpdate = maxLastUpdate;
        this.partition = partition;
        this.resultMap = resultMap;
    }

    public Index getIndex()
    {
        return index;
    }

    public Map<String, String> getSymbolTable()
    {
        return symbolTable;
    }

    public long getMaxLastUpdate()
    {
        return maxLastUpdate;
    }

    public Map<String, Long> getLastUpdated()
    {
        return lastUpdated;
    }

    public String getPartition()
    {
        return this.partition;
    }

    public Map<String, String> getResultMap()
    {
        return this.resultMap;
    }
}
