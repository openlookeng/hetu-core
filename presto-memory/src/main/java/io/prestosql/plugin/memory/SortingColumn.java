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

package io.prestosql.plugin.memory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.block.SortOrder;

import javax.annotation.concurrent.Immutable;

import java.io.Serializable;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class SortingColumn
        implements Serializable
{
    private static final long serialVersionUID = -5788899318458356130L;
    private final String columnName;
    private final SortOrder order;

    @JsonCreator
    public SortingColumn(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("order") SortOrder order)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.order = requireNonNull(order, "order is null");
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public SortOrder getOrder()
    {
        return order;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("order", order)
                .toString();
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

        SortingColumn that = (SortingColumn) o;
        return Objects.equals(columnName, that.columnName) &&
                order == that.order;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, order);
    }
}
