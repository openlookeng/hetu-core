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
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static org.testng.Assert.assertEquals;

public class DynamicFilterFactoryTest
{
    @Test
    public void testCreate1()
    {
        // Setup
        final ColumnHandle columnHandle = null;
        final BloomFilterDynamicFilter expectedResult = new BloomFilterDynamicFilter("filterId", null,
                "content".getBytes(), DynamicFilter.Type.LOCAL);

        // Run the test
        final BloomFilterDynamicFilter result = DynamicFilterFactory.create("filterId", columnHandle,
                "content".getBytes(), DynamicFilter.Type.LOCAL);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCreate2() throws Exception
    {
        // Setup
        final ColumnHandle columnHandle = null;
        final Set values = new HashSet<>();
        final HashSetDynamicFilter expectedResult = new HashSetDynamicFilter("filterId", null, new HashSet<>(),
                DynamicFilter.Type.LOCAL);

        // Run the test
        final HashSetDynamicFilter result = DynamicFilterFactory.create("filterId", columnHandle, values,
                DynamicFilter.Type.LOCAL);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCreate3() throws Exception
    {
        // Setup
        final ColumnHandle columnHandle = null;
        final Set values = new HashSet<>();
        final Optional<Predicate<List>> filter = Optional.of(val -> {
            return false;
        });
        final Optional<RowExpression> filterExpression = Optional.empty();
        final HashSetDynamicFilter expectedResult = new HashSetDynamicFilter("filterId", null, new HashSet<>(),
                DynamicFilter.Type.LOCAL);

        // Run the test
        final HashSetDynamicFilter result = DynamicFilterFactory.create("filterId", columnHandle, values,
                DynamicFilter.Type.LOCAL, filter, filterExpression);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCombine() throws Exception
    {
        // Setup
        final ColumnHandle columnHandle = null;
        final DynamicFilter filter1 = null;
        final DynamicFilter filter2 = null;
        final CombinedDynamicFilter expectedResult = new CombinedDynamicFilter(null, null, null);

        // Run the test
        final CombinedDynamicFilter result = DynamicFilterFactory.combine(columnHandle, filter1, filter2);

        // Verify the results
        assertEquals(expectedResult, result);
    }
}
