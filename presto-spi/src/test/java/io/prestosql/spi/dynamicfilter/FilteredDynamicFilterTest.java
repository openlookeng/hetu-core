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
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class FilteredDynamicFilterTest
{
    @Mock
    private ColumnHandle mockColumnHandle;

    private FilteredDynamicFilter filteredDynamicFilterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        filteredDynamicFilterUnderTest = new FilteredDynamicFilter("filterId", mockColumnHandle, new HashSet<>(),
                DynamicFilter.Type.LOCAL,
                Optional.of(val -> {
                    return false;
                }), Optional.empty());
    }

    @Test
    public void testContains() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = filteredDynamicFilterUnderTest.contains("value");

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testClone() throws Exception
    {
        // Setup
        final DynamicFilter expectedResult = null;

        // Run the test
        final DynamicFilter result = filteredDynamicFilterUnderTest.clone();

        // Verify the results
        assertEquals(expectedResult, result);
    }
}
