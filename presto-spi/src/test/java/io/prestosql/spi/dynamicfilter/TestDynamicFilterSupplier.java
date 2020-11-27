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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.TestingColumnHandle;
import io.prestosql.spi.util.BloomFilter;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDynamicFilterSupplier
{
    @Test(description = "get dynamic-filter when supplier is null")
    void testNullDynamicFilter()
    {
        Supplier<Map<ColumnHandle, DynamicFilter>> supplier = null;
        DynamicFilterSupplier theSupplier = new DynamicFilterSupplier(supplier, System.currentTimeMillis(), 10000);
        assertEquals(theSupplier.getDynamicFilters(), ImmutableMap.of());
    }

    @Test(description = "get dynamic-filter when supplier is not null")
    void testNotNullDynamicFilter()
            throws IOException
    {
        // construct a supplier
        List<Long> filterValues = ImmutableList.of(1L, 50L, 100L);
        ColumnHandle testColumnHandle = new TestingColumnHandle("test");
        BloomFilter filter = new BloomFilter(filterValues.size(), 0.01);
        for (Long value : filterValues) {
            filter.add(value);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        filter.writeTo(out);
        DynamicFilter dynamicFilter = DynamicFilterFactory.create("testFilter", testColumnHandle, out.toByteArray(), DynamicFilter.Type.GLOBAL);

        Supplier<Map<ColumnHandle, DynamicFilter>> supplier = () -> ImmutableMap.of(testColumnHandle, dynamicFilter);
        DynamicFilterSupplier theSupplier = new DynamicFilterSupplier(supplier, System.currentTimeMillis(), 10000);
        assertEquals(theSupplier.getDynamicFilters(), supplier.get());
    }

    @Test(description = "is blocked")
    void testIsBlocked()
    {
        Supplier<Map<ColumnHandle, DynamicFilter>> supplier = null;
        DynamicFilterSupplier theSupplier = new DynamicFilterSupplier(supplier, System.currentTimeMillis(), 10000);
        assertTrue(theSupplier.isBlocked());
    }

    @Test(description = "is not blocked")
    void testIsNotBlocked()
    {
        Supplier<Map<ColumnHandle, DynamicFilter>> supplier = null;
        DynamicFilterSupplier theSupplier = new DynamicFilterSupplier(supplier, System.currentTimeMillis(), 0);
        assertFalse(theSupplier.isBlocked());
    }
}
