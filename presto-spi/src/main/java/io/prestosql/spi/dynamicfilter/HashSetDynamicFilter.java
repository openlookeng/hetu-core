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
package io.prestosql.spi.dynamicfilter;

import io.prestosql.spi.connector.ColumnHandle;

import java.util.Set;

public class HashSetDynamicFilter
        extends DynamicFilter
{
    protected Set valueSet;

    public HashSetDynamicFilter(String filterId, ColumnHandle columnHandle, Set valueSet, Type type)
    {
        super();
        this.valueSet = valueSet;
        this.columnHandle = columnHandle;
        this.filterId = filterId;
        this.type = type;
    }

    public Set getSetValues()
    {
        return valueSet;
    }

    @Override
    public boolean contains(Object value)
    {
        return valueSet.contains(value);
    }

    @Override
    public long getSize()
    {
        return valueSet.size();
    }

    @Override
    public DynamicFilter clone()
    {
        DynamicFilter clone = new HashSetDynamicFilter(filterId, columnHandle, valueSet, type);
        clone.setMax(max);
        clone.setMax(min);
        return clone;
    }

    @Override
    public boolean isEmpty()
    {
        return valueSet.size() == 0;
    }
}
