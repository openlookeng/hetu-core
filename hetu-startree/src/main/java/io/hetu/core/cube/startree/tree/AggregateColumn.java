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

public class AggregateColumn
        extends StarTreeColumn
{
    private final String aggregateFunction;
    private final String originalColumn;
    private boolean distinct;

    @JsonCreator
    public AggregateColumn(
            @JsonProperty("name") String name,
            @JsonProperty("aggregateFunction") String aggregateFunction,
            @JsonProperty("originalColumn") String originalColumn,
            @JsonProperty("distinct") boolean distinct)
    {
        super(name);
        this.aggregateFunction = requireNonNull(aggregateFunction, "aggregateFunction is null");
        this.originalColumn = requireNonNull(originalColumn, "originalColumn is null");
        this.distinct = distinct;
    }

    @JsonProperty
    public String getAggregateFunction()
    {
        return aggregateFunction;
    }

    @JsonProperty
    public String getOriginalColumn()
    {
        return originalColumn;
    }

    @JsonIgnore
    public String getUserFriendlyName()
    {
        return aggregateFunction + "(" + originalColumn + ")";
    }

    @JsonIgnore
    @Override
    public ColumnType getType()
    {
        return ColumnType.AGGREGATE;
    }

    @JsonProperty
    public boolean isDistinct()
    {
        return distinct;
    }

    public void setDistinct(boolean distinct)
    {
        this.distinct = distinct;
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
        AggregateColumn that = (AggregateColumn) o;
        return Objects.equals(aggregateFunction, that.aggregateFunction) &&
                Objects.equals(name, that.name) &&
                Objects.equals(originalColumn, that.originalColumn) &&
                Objects.equals(distinct, that.distinct);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, aggregateFunction, originalColumn, distinct);
    }

    @Override
    public String toString()
    {
        return "AggregateColumn{" +
                "aggregateFunction='" + aggregateFunction + '\'' +
                ", originalColumn='" + originalColumn + '\'' +
                ", name='" + name + '\'' +
                ", distinct='" + distinct + '\'' +
                '}';
    }
}
