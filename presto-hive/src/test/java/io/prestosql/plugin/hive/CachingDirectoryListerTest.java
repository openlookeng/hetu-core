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
package io.prestosql.plugin.hive;

import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class CachingDirectoryListerTest
{
    private CachingDirectoryLister cachingDirectoryListerUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        cachingDirectoryListerUnderTest = new CachingDirectoryLister(new Duration(0.0, TimeUnit.MILLISECONDS), 0L, Arrays.asList("v\\.alue"));
    }

    @Test
    public void testFlushCache()
    {
        cachingDirectoryListerUnderTest.flushCache();
    }

    @Test
    public void testGetHitRate()
    {
        final Double result = cachingDirectoryListerUnderTest.getHitRate();
    }

    @Test
    public void testGetMissRate()
    {
        final Double result = cachingDirectoryListerUnderTest.getMissRate();
    }
}
