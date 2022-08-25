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
import io.prestosql.spi.predicate.TupleDomain;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class CombinedDynamicFilterTest
{
    @Mock
    private ColumnHandle mockColumnHandle;
    @Mock
    private DynamicFilter mockFilter1;
    @Mock
    private DynamicFilter mockFilter2;

    private CombinedDynamicFilter combinedDynamicFilterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        combinedDynamicFilterUnderTest = new CombinedDynamicFilter(mockColumnHandle, mockFilter1, mockFilter2);
    }

    @Test
    public void testContains() throws Exception
    {
        // Setup
        when(mockFilter1.contains(any(Object.class))).thenReturn(false);
        when(mockFilter2.contains(any(Object.class))).thenReturn(false);

        // Run the test
        final boolean result = combinedDynamicFilterUnderTest.contains("value");

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testGetSize() throws Exception
    {
        // Setup
        when(mockFilter1.getSize()).thenReturn(0L);
        when(mockFilter2.getSize()).thenReturn(0L);

        // Run the test
        final long result = combinedDynamicFilterUnderTest.getSize();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testClone() throws Exception
    {
        // Setup
        final DynamicFilter expectedResult = null;
        when(mockFilter1.clone()).thenReturn(null);
        when(mockFilter2.clone()).thenReturn(null);

        // Run the test
        final DynamicFilter result = combinedDynamicFilterUnderTest.clone();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testIsEmpty() throws Exception
    {
        // Setup
        when(mockFilter1.isEmpty()).thenReturn(false);
        when(mockFilter2.isEmpty()).thenReturn(false);

        // Run the test
        final boolean result = combinedDynamicFilterUnderTest.isEmpty();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testGetFilters() throws Exception
    {
        // Setup
        final List<DynamicFilter> expectedResult = Arrays.asList();

        // Run the test
        final List<DynamicFilter> result = combinedDynamicFilterUnderTest.getFilters();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testIsAwaitable() throws Exception
    {
        assertTrue(combinedDynamicFilterUnderTest.isAwaitable());
    }

    @Test
    public void testIsBlocked() throws Exception
    {
        // Setup
        // Run the test
        final CompletableFuture<?> result = combinedDynamicFilterUnderTest.isBlocked();

        // Verify the results
    }

    @Test
    public void testGetCurrentPredicate() throws Exception
    {
        // Setup
        // Run the test
        final TupleDomain<ColumnHandle> result = combinedDynamicFilterUnderTest.getCurrentPredicate();

        // Verify the results
    }
}
