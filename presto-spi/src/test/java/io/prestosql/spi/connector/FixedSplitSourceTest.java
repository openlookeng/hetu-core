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
package io.prestosql.spi.connector;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static org.testng.Assert.assertTrue;

public class FixedSplitSourceTest
{
    private FixedSplitSource fixedSplitSourceUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        fixedSplitSourceUnderTest = new FixedSplitSource(Arrays.asList(), Arrays.asList("value"));
    }

    @Test
    public void testGetNextBatch() throws Exception
    {
        // Setup
        final ConnectorPartitionHandle partitionHandle = null;

        // Run the test
        final CompletableFuture<ConnectorSplitSource.ConnectorSplitBatch> result = fixedSplitSourceUnderTest.getNextBatch(
                partitionHandle, 0);

        // Verify the results
    }

    @Test
    public void testIsFinished() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = fixedSplitSourceUnderTest.isFinished();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        fixedSplitSourceUnderTest.close();

        // Verify the results
    }
}
