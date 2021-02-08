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
package io.hetu.core.plugin.hbase.split;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.hetu.core.plugin.hbase.connector.HBaseTableHandle;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.predicate.Range;
import org.apache.hadoop.hbase.HRegionInfo;

import java.util.List;
import java.util.Map;

/**
 * HBaseSplit
 *
 * @since 2020-03-30
 */
public class HBaseSplit
        implements ConnectorSplit
{
    private final List<HostAddress> addresses;

    private final String rowKeyName;

    private final String startRow;

    private final String endRow;

    private final Map<Integer, List<Range>> ranges;

    private final HRegionInfo regionInfo;

    private final HBaseTableHandle tableHandle;

    private final boolean randomSplit;

    /**
     * constructor
     *
     * @param rowKeyName rowKeyName
     * @param tableHandle table
     * @param addresses addresses
     * @param startRow startRow
     * @param endRow endRow
     * @param ranges search ranges
     * @param regionInfo regionInfo
     * @param randomSplit randomSplit
     */
    @JsonCreator
    public HBaseSplit(
            @JsonProperty("rowKeyName") String rowKeyName,
            @JsonProperty("table") HBaseTableHandle tableHandle,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("startRow") String startRow,
            @JsonProperty("endRow") String endRow,
            @JsonProperty("ranges") Map<Integer, List<Range>> ranges,
            @JsonProperty("regionInfo") HRegionInfo regionInfo,
            @JsonProperty("randomSplit") boolean randomSplit)
    {
        this.rowKeyName = rowKeyName;
        this.tableHandle = tableHandle;
        this.addresses = addresses;
        this.startRow = startRow;
        this.endRow = endRow;
        this.ranges = ranges;
        this.regionInfo = regionInfo;
        this.randomSplit = randomSplit;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @JsonProperty
    public String getRowKeyName()
    {
        return rowKeyName;
    }

    @JsonProperty
    public String getStartRow()
    {
        return startRow;
    }

    @JsonProperty
    public String getEndRow()
    {
        return endRow;
    }

    @JsonProperty
    public Map<Integer, List<Range>> getRanges()
    {
        return ranges;
    }

    @JsonProperty
    public HRegionInfo getRegionInfo()
    {
        return regionInfo;
    }

    @JsonProperty
    public HBaseTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public boolean isRandomSplit()
    {
        return randomSplit;
    }
}
