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
import com.google.common.base.Joiner;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.optimization.JdbcQueryGeneratorResult;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public class SingleDataTableHandle
        extends JdbcTableHandle
{
    private final List<SingleDataSplit> splits;

    public SingleDataTableHandle(JdbcTableHandle tableHandle, List<SingleDataSplit> splits)
    {
        this(
                tableHandle.getSchemaTableName(),
                tableHandle.getCatalogName(),
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                tableHandle.getConstraint(),
                tableHandle.getLimit(),
                tableHandle.getGeneratedSql(),
                tableHandle.getDeleteOrUpdate(),
                tableHandle.getUpdatedColumnTypes(),
                splits);
    }

    @JsonCreator
    public SingleDataTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("sql") Optional<JdbcQueryGeneratorResult.GeneratedSql> generatedSql,
            @JsonProperty("deleteOrUpdate") boolean deleteOrUpdate,
            @JsonProperty("updatedColumnTypes") @Nullable List<Type> updatedColumnTypes,
            @JsonProperty("splits") List<SingleDataSplit> splits)
    {
        super(schemaTableName, catalogName, schemaName, tableName, constraint, limit, generatedSql, deleteOrUpdate, updatedColumnTypes);
        this.splits = requireNonNull(splits, "splits is null");
    }

    @JsonProperty
    public List<SingleDataSplit> getSplits()
    {
        return splits;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("SingleDataTableHandle:");
        Joiner.on(".").skipNulls().appendTo(builder, getCatalogName(), getSchemaName(), getTableName());
        return builder.toString();
    }
}
