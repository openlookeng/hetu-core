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
package io.prestosql.orc;

import io.airlift.units.DataSize;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OrcReaderOptionsTest
{
    private OrcReaderOptions orcReaderOptionsUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        orcReaderOptionsUnderTest = new OrcReaderOptions();
    }

    @Test
    public void testWithBloomFiltersEnabled()
    {
        // Setup
        // Run the test
        final OrcReaderOptions result = orcReaderOptionsUnderTest.withBloomFiltersEnabled(false);

        // Verify the results
    }

    @Test
    public void testWithMaxMergeDistance()
    {
        // Setup
        final DataSize maxMergeDistance = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final OrcReaderOptions result = orcReaderOptionsUnderTest.withMaxMergeDistance(maxMergeDistance);

        // Verify the results
    }

    @Test
    public void testWithMaxBufferSize()
    {
        // Setup
        final DataSize maxBufferSize = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final OrcReaderOptions result = orcReaderOptionsUnderTest.withMaxBufferSize(maxBufferSize);

        // Verify the results
    }

    @Test
    public void testWithTinyStripeThreshold()
    {
        // Setup
        final DataSize tinyStripeThreshold = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final OrcReaderOptions result = orcReaderOptionsUnderTest.withTinyStripeThreshold(tinyStripeThreshold);

        // Verify the results
    }

    @Test
    public void testWithStreamBufferSize()
    {
        // Setup
        final DataSize streamBufferSize = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final OrcReaderOptions result = orcReaderOptionsUnderTest.withStreamBufferSize(streamBufferSize);

        // Verify the results
    }

    @Test
    public void testWithMaxReadBlockSize()
    {
        // Setup
        final DataSize maxBlockSize = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final OrcReaderOptions result = orcReaderOptionsUnderTest.withMaxReadBlockSize(maxBlockSize);

        // Verify the results
    }

    @Test
    public void testWithLazyReadSmallRanges()
    {
        // Setup
        // Run the test
        final OrcReaderOptions result = orcReaderOptionsUnderTest.withLazyReadSmallRanges(false);

        // Verify the results
    }

    @Test
    public void testWithNestedLazy()
    {
        // Setup
        // Run the test
        final OrcReaderOptions result = orcReaderOptionsUnderTest.withNestedLazy(false);

        // Verify the results
    }
}
