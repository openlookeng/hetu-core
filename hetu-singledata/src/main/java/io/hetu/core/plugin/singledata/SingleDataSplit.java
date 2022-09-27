/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.hetu.core.plugin.singledata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.spi.HostAddress;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SingleDataSplit
        extends JdbcSplit
{
    private final String dataSourceName;
    private final String sql;

    @JsonCreator
    public SingleDataSplit(
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("splitField") String splitField,
            @JsonProperty("beginIndex") String rangeStart,
            @JsonProperty("endIndex") String rangEnd,
            @JsonProperty("timeStamp") long timeStamp,
            @JsonProperty("scanNodes") int scanNodes,
            @JsonProperty("additionalPredicate") Optional<String> additionalPredicate,
            @JsonProperty("dataSourceName") @Nullable String dataSourceName,
            @JsonProperty("sql") String sql)
    {
        super(catalogName, schemaName, tableName, splitField, rangeStart, rangEnd, timeStamp, scanNodes, additionalPredicate);
        this.dataSourceName = dataSourceName;
        this.sql = requireNonNull(sql, "sql is null");
    }

    @JsonProperty
    @Nullable
    public String getDataSourceName()
    {
        return dataSourceName;
    }

    @JsonProperty
    public String getSql()
    {
        return sql;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
