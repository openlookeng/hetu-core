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

import com.google.common.util.concurrent.MoreExecutors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ReentrantBoundedExecutorTest
{
    private ReentrantBoundedExecutor reentrantBoundedExecutorUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        reentrantBoundedExecutorUnderTest = new ReentrantBoundedExecutor(MoreExecutors.directExecutor(), 1);
    }

    @Test
    public void testExecute() throws Exception
    {
        // Setup
        final Runnable task = null;

        // Run the test
        reentrantBoundedExecutorUnderTest.execute(task);

        // Verify the results
    }
}
