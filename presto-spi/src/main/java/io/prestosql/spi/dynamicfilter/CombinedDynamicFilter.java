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

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ColumnHandle;

import java.util.List;

import static com.google.common.base.Verify.verify;

/**
 * This is a DynamicFilter to combine multiple filters on same column handle.
 */
public class CombinedDynamicFilter
        extends DynamicFilter
{
    private DynamicFilter filter1;
    private DynamicFilter filter2;

    public CombinedDynamicFilter(ColumnHandle columnHandle, DynamicFilter filter1, DynamicFilter filter2)
    {
        super();
        verify(columnHandle.equals(filter1.columnHandle) && columnHandle.equals(filter2.columnHandle),
                "Mismatched column handles; Expected: "
                        + columnHandle + ", f1: " + filter1.columnHandle + ", f2:" + filter2.columnHandle);
        this.columnHandle = columnHandle;
        this.filter1 = filter1;
        this.filter2 = filter2;
    }

    @Override
    public boolean contains(Object value)
    {
        return filter1.contains(value) && filter2.contains(value);
    }

    @Override
    public long getSize()
    {
        return filter1.getSize() + filter2.getSize();
    }

    @Override
    public DynamicFilter clone()
    {
        return new CombinedDynamicFilter(getColumnHandle(), filter1.clone(), filter2.clone());
    }

    @Override
    public boolean isEmpty()
    {
        return filter1.isEmpty() && filter2.isEmpty();
    }

    public List<DynamicFilter> getFilters()
    {
        ImmutableList.Builder<DynamicFilter> builder = ImmutableList.builder();

        if (filter1 instanceof CombinedDynamicFilter) {
            builder.addAll(((CombinedDynamicFilter) filter1).getFilters());
        }
        else {
            builder.add(filter1);
        }

        if (filter2 instanceof CombinedDynamicFilter) {
            builder.addAll(((CombinedDynamicFilter) filter2).getFilters());
        }
        else {
            builder.add(filter2);
        }

        return builder.build();
    }
}
