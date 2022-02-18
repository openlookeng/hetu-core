/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class MockSplit
        implements ConnectorSplit
{
    private static final CatalogName CONNECTOR_ID = new CatalogName("connector_id");
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";

    String schema;
    String table;
    String filepath;
    long startIndex;
    long endIndex;
    long lastModifiedTime;
    boolean cacheable;
    private Optional<Set<TupleDomain<ColumnMetadata>>> cachePredicates = Optional.empty();

    @JsonCreator
    public MockSplit(
            @JsonProperty("filepath") String filepath,
            @JsonProperty("startIndex") long startIndex,
            @JsonProperty("endIndex") long endIndex,
            @JsonProperty("lastModifiedTime") long lastModifiedTime)
    {
        this(filepath, startIndex, endIndex, lastModifiedTime, false);
    }

    @JsonCreator
    public MockSplit(
            @JsonProperty("filepath") String filepath,
            @JsonProperty("startIndex") long startIndex,
            @JsonProperty("endIndex") long endIndex,
            @JsonProperty("lastModifiedTime") long lastModifiedTime,
            @JsonProperty("cacheable") boolean cacheable)
    {
        this.filepath = filepath;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.lastModifiedTime = lastModifiedTime;
        this.schema = TEST_SCHEMA;
        this.table = TEST_TABLE;
        this.cacheable = cacheable;
    }

    @JsonCreator
    public MockSplit(
            @JsonProperty("filepath") String filepath,
            @JsonProperty("startIndex") int startIndex,
            @JsonProperty("endIndex") int endIndex)
    {
        this.filepath = filepath;
        this.startIndex = Long.valueOf(startIndex);
        this.endIndex = Long.valueOf(endIndex);
        this.lastModifiedTime = System.currentTimeMillis();
        this.schema = TEST_SCHEMA;
        this.table = TEST_TABLE;
    }

    @JsonProperty
    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @JsonProperty
    @Override
    public String getFilePath()
    {
        return filepath;
    }

    @JsonProperty
    @Override
    public long getStartIndex()
    {
        return startIndex;
    }

    @JsonProperty
    @Override
    public long getEndIndex()
    {
        return endIndex;
    }

    @JsonProperty
    @Override
    public long getLastModifiedTime()
    {
        return lastModifiedTime;
    }

    @JsonProperty
    @Override
    public boolean isCacheable()
    {
        return cacheable;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("path", filepath)
                .put("start", startIndex)
                .put("end", endIndex)
                .put("lastModifiedTime", lastModifiedTime)
                .put("database", schema)
                .put("table", table)
                .build();
    }
}
