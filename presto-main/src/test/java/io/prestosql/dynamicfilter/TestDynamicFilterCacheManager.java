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
package io.prestosql.dynamicfilter;

import io.prestosql.execution.TaskId;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import org.testng.annotations.Test;

import static io.prestosql.dynamicfilter.DynamicFilterCacheManager.createCacheKey;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestDynamicFilterCacheManager
{
    @Test
    public void testCacheAndRemoveDynamicFilters()
    {
        DynamicFilterCacheManager cacheManager = new DynamicFilterCacheManager();
        String filterKey1 = createCacheKey("filter1", "query");
        DynamicFilter filter1 = mock(DynamicFilter.class);
        TaskId taskId = new TaskId("task1");

        cacheManager.registerTask(filterKey1, taskId);
        cacheManager.cacheDynamicFilter(filterKey1, filter1);
        assertEquals(cacheManager.getDynamicFilter(filterKey1), filter1, "filter1 should be cached");

        cacheManager.removeDynamicFilter(filterKey1, taskId);
        assertNull(cacheManager.getDynamicFilter(filterKey1), "filter1 should be removed");
    }

    @Test
    public void testRegisterTask()
    {
        DynamicFilterCacheManager cacheManager = new DynamicFilterCacheManager();
        String filterKey1 = createCacheKey("filter1", "query");
        DynamicFilter filter1 = mock(DynamicFilter.class);
        TaskId task1 = new TaskId("task1");
        TaskId task2 = new TaskId("task2");

        cacheManager.registerTask(filterKey1, task1);
        cacheManager.registerTask(filterKey1, task2);

        cacheManager.cacheDynamicFilter(filterKey1, filter1);
        assertEquals(cacheManager.getDynamicFilter(filterKey1), filter1, "filter1 should be cached");

        cacheManager.removeDynamicFilter(filterKey1, task1);
        assertEquals(cacheManager.getDynamicFilter(filterKey1), filter1, "filter1 shouldn't be removed, it's still used by task2");

        cacheManager.removeDynamicFilter(filterKey1, task2);
        assertNull(cacheManager.getDynamicFilter(filterKey1), "filter1 should be removed");
    }

    @Test
    public void testInvalidateCache()
    {
        DynamicFilterCacheManager cacheManager = new DynamicFilterCacheManager();
        String filterKey1 = createCacheKey("filter1", "query");
        DynamicFilter filter1 = mock(DynamicFilter.class);
        TaskId taskId = new TaskId("task1");

        cacheManager.registerTask(filterKey1, taskId);
        cacheManager.cacheDynamicFilter(filterKey1, filter1);
        assertEquals(cacheManager.getDynamicFilter(filterKey1), filter1, "filter1 should be cached");

        cacheManager.invalidateCache();
        assertNull(cacheManager.getDynamicFilter(filterKey1), "filter1 should be removed");
    }

    @Test
    public void testCreateCacheKey()
    {
        assertEquals(createCacheKey("filterId", "queryId"), "filterId-queryId");
    }
}
