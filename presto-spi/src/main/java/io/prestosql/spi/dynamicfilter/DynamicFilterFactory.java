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
import io.prestosql.spi.relation.RowExpression;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

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

    public static HashSetDynamicFilter create(String filterId, ColumnHandle columnHandle, Set values, DynamicFilter.Type type, Optional<Predicate<List>> filter, Optional<RowExpression> filterExpression)
    {
        if (filter.isPresent()) {
            return new FilteredDynamicFilter(filterId, columnHandle, values, type, filter, filterExpression);
        }
        else {
            return create(filterId, columnHandle, values, type);
        }
    }

    public static CombinedDynamicFilter combine(ColumnHandle columnHandle, DynamicFilter filter1, DynamicFilter filter2)
    {
        return new CombinedDynamicFilter(columnHandle, filter1, filter2);
    }
}
