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
package io.prestosql.spi.metastore.model;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class CatalogEntityTest
{
    private CatalogEntity catalogEntityUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        catalogEntityUnderTest = new CatalogEntity("name", "owner", 0L, "comment", new HashMap<>());
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(catalogEntityUnderTest.equals("o"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, catalogEntityUnderTest.hashCode());
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", catalogEntityUnderTest.toString());
    }

    @Test
    public void testBuilder1() throws Exception
    {
        // Setup
        // Run the test
        final CatalogEntity.Builder result = CatalogEntity.builder();

        // Verify the results
    }

    @Test
    public void testBuilder2() throws Exception
    {
        // Setup
        final CatalogEntity catalog = new CatalogEntity("name", "owner", 0L, "comment", new HashMap<>());

        // Run the test
        final CatalogEntity.Builder result = CatalogEntity.builder(catalog);

        // Verify the results
    }
}
