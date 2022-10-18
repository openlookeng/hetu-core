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
package io.prestosql.spi.connector;

import io.prestosql.spi.block.SortOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SortingPropertyTest
{
    private SortingProperty<Object> sortingPropertyUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        sortingPropertyUnderTest = new SortingProperty<>(null, SortOrder.ASC_NULLS_FIRST);
    }

    @Test
    public void testIsOrderSensitive() throws Exception
    {
        assertTrue(sortingPropertyUnderTest.isOrderSensitive());
    }

    @Test
    public void testGetColumns() throws Exception
    {
        // Setup
        // Run the test
        final Set<Object> result = sortingPropertyUnderTest.getColumns();

        // Verify the results
    }

    @Test
    public void testTranslate() throws Exception
    {
        // Setup
        final Function<Object, Optional<Object>> translator = val -> {
            return null;
        };

        // Run the test
        final Optional<LocalProperty<Object>> result = sortingPropertyUnderTest.translate(translator);

        // Verify the results
    }

    @Test
    public void testIsSimplifiedBy() throws Exception
    {
        // Setup
        final LocalProperty<Object> known = null;

        // Run the test
        final boolean result = sortingPropertyUnderTest.isSimplifiedBy(known);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", sortingPropertyUnderTest.toString());
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(sortingPropertyUnderTest.equals("o"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, sortingPropertyUnderTest.hashCode());
    }
}
