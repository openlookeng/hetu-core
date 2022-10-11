/*
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
package io.prestosql.plugin.hive.metastore.cache;

import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

public class CachingHiveMetastoreConfigTest
{
    private CachingHiveMetastoreConfig cachingHiveMetastoreConfigUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        cachingHiveMetastoreConfigUnderTest = new CachingHiveMetastoreConfig();
    }

    @Test
    public void testGetMetastoreCacheTtl()
    {
        Duration metastoreCacheTtl = cachingHiveMetastoreConfigUnderTest.getMetastoreCacheTtl();
        cachingHiveMetastoreConfigUnderTest.setMetastoreCacheTtl(metastoreCacheTtl);
        cachingHiveMetastoreConfigUnderTest.setMetastoreRefreshInterval(metastoreCacheTtl);
    }

    @Test
    public void testGetMetastoreRefreshInterval()
    {
        Optional<Duration> metastoreRefreshInterval = cachingHiveMetastoreConfigUnderTest.getMetastoreRefreshInterval();
    }

    @Test
    public void testGetMetastoreCacheMaximumSize()
    {
        long metastoreCacheMaximumSize = cachingHiveMetastoreConfigUnderTest.getMetastoreCacheMaximumSize();
        cachingHiveMetastoreConfigUnderTest.setMetastoreCacheMaximumSize(metastoreCacheMaximumSize);
    }

    @Test
    public void testGetMaxMetastoreRefreshThreads()
    {
        int maxMetastoreRefreshThreads = cachingHiveMetastoreConfigUnderTest.getMaxMetastoreRefreshThreads();
        cachingHiveMetastoreConfigUnderTest.setMaxMetastoreRefreshThreads(maxMetastoreRefreshThreads);
    }
}
