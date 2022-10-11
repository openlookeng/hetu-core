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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class HashSetDynamicFilterTest
{
    @Mock
    private ColumnHandle mockColumnHandle;

    private HashSetDynamicFilter hashSetDynamicFilterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        hashSetDynamicFilterUnderTest = new HashSetDynamicFilter("filterId", mockColumnHandle, new HashSet<>(),
                DynamicFilter.Type.LOCAL);
    }

    @Test
    public void testGetSetValues() throws Exception
    {
        // Setup
        // Run the test
        final Set result = hashSetDynamicFilterUnderTest.getSetValues();

        // Verify the results
    }

    @Test
    public void testContains() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = hashSetDynamicFilterUnderTest.contains("value");

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testGetSize() throws Exception
    {
        // Setup
        // Run the test
        final long result = hashSetDynamicFilterUnderTest.getSize();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testClone() throws Exception
    {
        // Setup
        final DynamicFilter expectedResult = null;

        // Run the test
        final DynamicFilter result = hashSetDynamicFilterUnderTest.clone();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testIsEmpty() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = hashSetDynamicFilterUnderTest.isEmpty();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testIsAwaitable() throws Exception
    {
        assertTrue(hashSetDynamicFilterUnderTest.isAwaitable());
    }

    @Test
    public void testIsBlocked() throws Exception
    {
        // Setup
        // Run the test
        final CompletableFuture<?> result = hashSetDynamicFilterUnderTest.isBlocked();

        // Verify the results
    }

    @Test
    public void testGetCurrentPredicate() throws Exception
    {
        // Setup
        // Run the test
        final TupleDomain<ColumnHandle> result = hashSetDynamicFilterUnderTest.getCurrentPredicate();

        // Verify the results
    }
}
