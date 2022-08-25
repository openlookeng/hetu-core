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
package io.hetu.core.plugin.iceberg;

import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.metrics.Metrics;
import io.prestosql.spi.snapshot.MarkerPage;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static org.testng.Assert.assertEquals;

public class ConstantPopulatingPageSourceTest
{
    private ConstantPopulatingPageSource constantPopulatingPageSourceUnderTest;
    private ConnectorPageSource connectorPageSource;

    @BeforeMethod
    public void testBuilder()
    {
        ConstantPopulatingPageSource.Builder builder = ConstantPopulatingPageSource.builder();
        ConnectorPageSource connectorPageSource = new ConnectorPageSource()
        {
            @Override
            public long getCompletedBytes()
            {
                return 0;
            }

            @Override
            public long getReadTimeNanos()
            {
                return 0;
            }

            @Override
            public boolean isFinished()
            {
                return false;
            }

            @Override
            public Page getNextPage()
            {
                return MarkerPage.resumePage(1);
            }

            @Override
            public long getSystemMemoryUsage()
            {
                return 0;
            }

            @Override
            public void close() throws IOException
            {
            }
        };
        ConstantPopulatingPageSource.Builder builde = builder.addDelegateColumn(10);
        this.constantPopulatingPageSourceUnderTest = (ConstantPopulatingPageSource) builde.build(connectorPageSource);
        ConnectorPageSource build = builder.build(connectorPageSource);
    }

    @Test
    public void testGetCompletedBytes()
    {
        // Setup
        // Run the test
        final long result = constantPopulatingPageSourceUnderTest.getCompletedBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetCompletedPositions()
    {
        // Run the test
        final OptionalLong result = constantPopulatingPageSourceUnderTest.getCompletedPositions();
    }

    @Test
    public void testGetReadTimeNanos()
    {
        // Setup
        // Run the test
        final long result = constantPopulatingPageSourceUnderTest.getReadTimeNanos();
    }

    @Test
    public void testIsFinished()
    {
        // Setup
        // Run the test
        final boolean result = constantPopulatingPageSourceUnderTest.isFinished();
    }

    @Test
    public void testGetNextPage()
    {
        // Setup
        // Run the test
        final Page result = constantPopulatingPageSourceUnderTest.getNextPage();

        // Verify the results
    }

    @Test
    public void testGetMemoryUsage()
    {
        // Setup
        // Run the test
        final long result = constantPopulatingPageSourceUnderTest.getMemoryUsage();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        constantPopulatingPageSourceUnderTest.close();

        // Verify the results
    }

    @Test
    public void testIsBlocked()
    {
        // Setup
        // Run the test
        final CompletableFuture<?> result = constantPopulatingPageSourceUnderTest.isBlocked();

        // Verify the results
    }

    @Test
    public void testGetMetrics()
    {
        // Setup
        // Run the test
        final Metrics result = constantPopulatingPageSourceUnderTest.getMetrics();
    }

    @Test
    public void testGetSystemMemoryUsage()
    {
        // Setup
        // Run the test
        final long result = constantPopulatingPageSourceUnderTest.getSystemMemoryUsage();
    }
}
