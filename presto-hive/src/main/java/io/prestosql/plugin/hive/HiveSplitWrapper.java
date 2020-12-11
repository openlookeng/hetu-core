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
package io.prestosql.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class HiveSplitWrapper
        implements ConnectorSplit
{
    private final List<HiveSplit> splits;
    private final OptionalInt bucketNumber;

    @JsonCreator
    public HiveSplitWrapper(
            @JsonProperty("splits") List<HiveSplit> splits,
            @JsonProperty("bucketNumber") OptionalInt bucketNumber)
    {
        this.splits = requireNonNull(splits, "split lists is null");
        this.bucketNumber = bucketNumber;
    }

    @Override
    public String getFilePath()
    {
        return splits.stream().findFirst().orElseThrow(IllegalArgumentException::new).getFilePath();
    }

    @Override
    public long getStartIndex()
    {
        return splits.stream().findFirst().orElseThrow(IllegalArgumentException::new).getStartIndex();
    }

    @Override
    public long getEndIndex()
    {
        return splits.stream().findFirst().orElseThrow(IllegalArgumentException::new).getEndIndex();
    }

    @Override
    public long getLastModifiedTime()
    {
        return splits.stream().findFirst().orElseThrow(IllegalArgumentException::new).getLastModifiedTime();
    }

    @Override
    public boolean isCacheable()
    {
        return splits.stream().findFirst().orElseThrow(IllegalAccessError::new).isCacheable();
    }

    @JsonProperty
    public List<HiveSplit> getSplits()
    {
        return splits;
    }

    @Override
    public List<ConnectorSplit> getUnwrappedSplits()
    {
        /*
        * Splits are unwrapped here when called by Split#getSplits()
        * for Split Assignment processing in Reuse Exchange, where the Consumer splits are assigned
        * to same node as Producer. Split by Split comparison is done for the same
        * and therefore, wrapped splits cannot be used.
        * */
        return splits.stream().map(x -> wrap(x)).collect(Collectors.toList());
    }

    @JsonProperty
    public OptionalInt getBucketNumber()
    {
        return bucketNumber;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return splits.stream()
                .flatMap(s -> s.getAddresses().stream())
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public Object getInfo()
    {
        if (splits.isEmpty()) {
            return ImmutableMap.of();
        }
        HiveSplit split = splits.get(0);
        return ImmutableMap.builder()
                .put("hosts", getAddresses())
                .put("database", split.getDatabase())
                .put("table", split.getTable())
                .put("partitionName", split.getPartitionName())
                .build();
    }

    public static HiveSplitWrapper wrap(HiveSplit hiveSplit)
    {
        return new HiveSplitWrapper(ImmutableList.of(hiveSplit), hiveSplit.getBucketNumber());
    }

    public static HiveSplitWrapper wrap(List<HiveSplit> hiveSplitList, OptionalInt bucketNumber)
    {
        return new HiveSplitWrapper(hiveSplitList, bucketNumber);
    }

    public static HiveSplit getOnlyHiveSplit(ConnectorSplit connectorSplit)
    {
        return getOnlyElement(((HiveSplitWrapper) connectorSplit).getSplits());
    }

    @Override
    public int getSplitCount()
    {
        return splits.size();
    }
}
