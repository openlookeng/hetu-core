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

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.ColumnHandle;

import java.util.Map;
import java.util.function.Supplier;

public class DynamicFilterSupplier
{
    private final Supplier<Map<ColumnHandle, DynamicFilter>> supplier;
    private final long createTime;
    private final long waitTime;
    private boolean block = true;

    public DynamicFilterSupplier(Supplier<Map<ColumnHandle, DynamicFilter>> supplier,
            long createTime,
            long waitTime)
    {
        this.supplier = supplier;
        this.createTime = createTime;
        this.waitTime = waitTime;
    }

    public Supplier<Map<ColumnHandle, DynamicFilter>> getSupplier()
    {
        return supplier;
    }

    public Map<ColumnHandle, DynamicFilter> getDynamicFilters()
    {
        return supplier == null ? ImmutableMap.of() : supplier.get();
    }

    public boolean isBlocked()
    {
        if (block) {
            block = System.currentTimeMillis() < (createTime + waitTime);
        }
        return block;
    }
}
