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
package io.prestosql.spi.dynamicfilter;

import io.prestosql.spi.connector.ColumnHandle;

import java.util.Objects;

/**
 * DynamicFilter contains dynamic filter information and
 * one of value set, bloom filter, min/max values for filtering
 *
 * @since 2020-04-01
 */
public abstract class DynamicFilter
{
    public enum Type
    {
        LOCAL,
        GLOBAL
    }

    public enum DataType
    {
        HASHSET,
        BLOOM_FILTER
    }

    protected String filterId;
    protected ColumnHandle columnHandle;
    protected Object min;
    protected Object max;
    protected Type type;

    public DynamicFilter()
    {
    }

    /**
     * Get id of current dynamic filter
     *
     * @return dynamic filter id
     */
    public String getFilterId()
    {
        return filterId;
    }

    /**
     * Get column information of current dynamic filter
     *
     * @return ColumnHandle contains column information
     */
    public ColumnHandle getColumnHandle()
    {
        return columnHandle;
    }

    /**
     * Set column information of current dynamic filter
     */
    public void setColumnHandle(ColumnHandle columnHandle)
    {
        this.columnHandle = columnHandle;
    }

    /**
     * Contains for the current dynamic filter
     *
     * @return boolean whether or not the object is in the DynamicFilter
     */
    public abstract boolean contains(Object value);

    /**
     * Get the size of the current DynamicFilter
     *
     * @return DynamicFilter size in long
     */
    public abstract long getSize();

    /**
     * Clone the current DynamicFilter
     *
     * @return A new DynamicFilter cloned from current one
     */
    public abstract DynamicFilter clone();

    /**
     * Determine whether the current DynamicFilter is empty
     *
     * @return true if it is empty, otherwise return false
     */
    public abstract boolean isEmpty();

    /**
     * Get minimum value of current dynamic filter
     *
     * @return minimum value, or null if not set
     */
    public Object getMin()
    {
        return min;
    }

    public void setMin(Object min)
    {
        this.min = min;
    }

    /**
     * Get maximum value of current dynamic filter
     *
     * @return maximum value, or null if not set
     */
    public Object getMax()
    {
        return max;
    }

    public Type getType()
    {
        return type;
    }

    public void setType(Type type)
    {
        this.type = type;
    }

    public void setMax(Object max)
    {
        this.max = max;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(filterId, columnHandle);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof DynamicFilter)) {
            return false;
        }
        DynamicFilter other = (DynamicFilter) obj;
        return other.getColumnHandle().equals(this.columnHandle) &&
                other.getFilterId().equals(this.filterId);
    }
}
