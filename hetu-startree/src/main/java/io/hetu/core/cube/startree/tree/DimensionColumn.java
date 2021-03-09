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

package io.hetu.core.cube.startree.tree;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class DimensionColumn
        extends StarTreeColumn
{
    private final String originalColumn;

    @JsonCreator
    public DimensionColumn(
            @JsonProperty("name") String name,
            @JsonProperty("originalColumn") String originalColumn)
    {
        super(name);
        this.originalColumn = requireNonNull(originalColumn, "originalColumn is null");
    }

    @JsonProperty
    public String getOriginalColumn()
    {
        return originalColumn;
    }

    @JsonIgnore
    public String getUserFriendlyName()
    {
        return "(" + originalColumn + ")";
    }

    @JsonIgnore
    @Override
    public ColumnType getType()
    {
        return ColumnType.DIMENSION;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DimensionColumn that = (DimensionColumn) o;
        return Objects.equals(originalColumn, that.originalColumn) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, originalColumn);
    }

    @Override
    public String toString()
    {
        return "DimensionColumn{" +
                "originalColumn='" + originalColumn + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
