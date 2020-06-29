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

package io.hetu.core.plugin.datacenter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ColumnMetadata;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * Data center table information.
 *
 * @since 2020-02-11
 */
public class DataCenterTable
{
    private final String name;

    private final List<DataCenterColumn> columns;

    private final List<ColumnMetadata> columnsMetadata;

    /**
     * Constructor of data center table.
     *
     * @param name table name.
     * @param columns columns of table.
     */
    @JsonCreator
    public DataCenterTable(@JsonProperty("name") String name, @JsonProperty("columns") List<DataCenterColumn> columns)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = requireNonNull(name, "name is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));

        ImmutableList.Builder<ColumnMetadata> columnsMetadataBuilder = ImmutableList.builder();
        for (DataCenterColumn column : this.columns) {
            columnsMetadataBuilder.add(new ColumnMetadata(column.getName(), column.getType()));
        }
        this.columnsMetadata = columnsMetadataBuilder.build();
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<DataCenterColumn> getColumns()
    {
        return columns;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }
}
