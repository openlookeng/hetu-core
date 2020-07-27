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

import java.util.Set;

public class DynamicFilterFactory
{
    private DynamicFilterFactory()
    {
    }

    public static BloomFilterDynamicFilter create(String filterId, ColumnHandle columnHandle, byte[] serializedBloomFilter, DynamicFilter.Type type)
    {
        return new BloomFilterDynamicFilter(filterId, columnHandle, serializedBloomFilter, type);
    }

    public static HashSetDynamicFilter create(String filterId, ColumnHandle columnHandle, Set values, DynamicFilter.Type type)
    {
        return new HashSetDynamicFilter(filterId, columnHandle, values, type);
    }
}
