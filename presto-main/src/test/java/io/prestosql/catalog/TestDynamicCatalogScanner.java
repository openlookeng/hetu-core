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

package io.prestosql.catalog;

import com.google.common.collect.ImmutableList;
import com.google.inject.Key;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDynamicCatalogScanner
        extends TestDynamicCatalogRunner
{
    public TestDynamicCatalogScanner()
            throws Exception
    {
    }

    @Test
    public void testLoadCatalog()
            throws Exception
    {
        String catalogName = "tpch101";
        assertTrue(executeAddCatalogCall(catalogName, "tpch", tpchProperties, ImmutableList.of(), ImmutableList.of()));
        server.getInstance(Key.get(DynamicCatalogStore.class)).unloadCatalog(catalogName);
        assertFalse(server.getCatalogManager().getCatalog(catalogName).isPresent());
        server.getInstance(Key.get(DynamicCatalogScanner.class)).scan();
        assertTrue(server.getCatalogManager().getCatalog(catalogName).isPresent());
    }

    @Test
    public void testDeleteCatalog()
            throws Exception
    {
        String catalogName = "tpch102";
        assertTrue(executeAddCatalogCall(catalogName, "tpch", tpchProperties, ImmutableList.of(), ImmutableList.of()));
        assertTrue(server.getCatalogManager().getCatalog(catalogName).isPresent());
        assertTrue(executeDeleteCatalogCall(catalogName));
        server.getInstance(Key.get(DynamicCatalogScanner.class)).scan();
        assertFalse(server.getCatalogManager().getCatalog(catalogName).isPresent());
    }

    @Test
    public void testLoadExistCatalog()
            throws Exception
    {
        String catalogName = "tpch103";
        assertTrue(executeAddCatalogCall(catalogName, "tpch", tpchProperties, ImmutableList.of(), ImmutableList.of()));
        server.getInstance(Key.get(DynamicCatalogStore.class)).loadCatalog(catalogName);
    }
}
