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
import io.prestosql.spi.relation.RowExpression;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class FilteredDynamicFilter
        extends HashSetDynamicFilter
{
    private final Optional<Predicate<List>> filter;
    private final Optional<RowExpression> filterExpression;

    public FilteredDynamicFilter(String filterId, ColumnHandle columnHandle, Set valueSet, Type type, Optional<Predicate<List>> filter, Optional<RowExpression> filterExpression)
    {
        super(filterId, columnHandle, valueSet, type);
        this.filter = requireNonNull(filter, "filter is null");
        this.filterExpression = requireNonNull(filterExpression, "filterExpression is null");
    }

    public Optional<RowExpression> getFilterExpression()
    {
        return filterExpression;
    }

    @Override
    public boolean contains(Object value)
    {
        if (!filter.isPresent()) {
            return super.contains(value);
        }
        for (Object dynamicFilterValue : valueSet) {
            if (value != null && filter.get().test(ImmutableList.of(value, dynamicFilterValue))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public DynamicFilter clone()
    {
        FilteredDynamicFilter filteredDynamicFilter = new FilteredDynamicFilter(filterId, columnHandle, valueSet, type, filter, filterExpression);
        filteredDynamicFilter.setMin(min);
        filteredDynamicFilter.setMax(max);
        return filteredDynamicFilter;
    }
}
