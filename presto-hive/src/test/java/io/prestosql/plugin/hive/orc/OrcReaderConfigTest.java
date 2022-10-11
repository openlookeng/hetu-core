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
package io.prestosql.plugin.hive.orc;

import io.airlift.units.DataSize;
import io.prestosql.orc.OrcReaderOptions;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OrcReaderConfigTest
{
    private OrcReaderConfig orcReaderConfigUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        orcReaderConfigUnderTest = new OrcReaderConfig();
    }

    @Test
    public void testToOrcReaderOptions()
    {
        // Setup
        // Run the test
        final OrcReaderOptions result = orcReaderConfigUnderTest.toOrcReaderOptions();

        // Verify the results
    }

    @Test
    public void testIsBloomFiltersEnabled()
    {
        // Setup
        // Run the test
        final boolean result = orcReaderConfigUnderTest.isBloomFiltersEnabled();
    }

    @Test
    public void testSetBloomFiltersEnabled()
    {
        // Setup
        // Run the test
        final OrcReaderConfig result = orcReaderConfigUnderTest.setBloomFiltersEnabled(false);

        // Verify the results
    }

    @Test
    public void testGetMaxMergeDistance()
    {
        // Setup
        final DataSize expectedResult = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final DataSize result = orcReaderConfigUnderTest.getMaxMergeDistance();
    }

    @Test
    public void testSetMaxMergeDistance()
    {
        // Setup
        final DataSize maxMergeDistance = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final OrcReaderConfig result = orcReaderConfigUnderTest.setMaxMergeDistance(maxMergeDistance);
    }

    @Test
    public void testGetMaxBufferSize()
    {
        // Setup
        final DataSize expectedResult = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final DataSize result = orcReaderConfigUnderTest.getMaxBufferSize();
    }

    @Test
    public void testSetMaxBufferSize()
    {
        // Setup
        final DataSize maxBufferSize = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final OrcReaderConfig result = orcReaderConfigUnderTest.setMaxBufferSize(maxBufferSize);
    }

    @Test
    public void testGetTinyStripeThreshold()
    {
        // Setup
        final DataSize expectedResult = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final DataSize result = orcReaderConfigUnderTest.getTinyStripeThreshold();
    }

    @Test
    public void testSetTinyStripeThreshold()
    {
        // Setup
        final DataSize tinyStripeThreshold = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final OrcReaderConfig result = orcReaderConfigUnderTest.setTinyStripeThreshold(tinyStripeThreshold);
    }

    @Test
    public void testGetStreamBufferSize()
    {
        // Setup
        final DataSize expectedResult = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final DataSize result = orcReaderConfigUnderTest.getStreamBufferSize();
    }

    @Test
    public void testSetStreamBufferSize()
    {
        // Setup
        final DataSize streamBufferSize = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final OrcReaderConfig result = orcReaderConfigUnderTest.setStreamBufferSize(streamBufferSize);
    }

    @Test
    public void testGetMaxBlockSize()
    {
        // Setup
        final DataSize expectedResult = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final DataSize result = orcReaderConfigUnderTest.getMaxBlockSize();
    }

    @Test
    public void testSetMaxBlockSize()
    {
        // Setup
        final DataSize maxBlockSize = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final OrcReaderConfig result = orcReaderConfigUnderTest.setMaxBlockSize(maxBlockSize);
    }

    @Test
    public void testIsLazyReadSmallRanges()
    {
        // Setup
        // Run the test
        final boolean result = orcReaderConfigUnderTest.isLazyReadSmallRanges();
    }

    @Test
    public void testSetLazyReadSmallRanges()
    {
        // Setup
        // Run the test
        final OrcReaderConfig result = orcReaderConfigUnderTest.setLazyReadSmallRanges(false);
    }

    @Test
    public void testIsNestedLazy()
    {
        // Setup
        // Run the test
        final boolean result = orcReaderConfigUnderTest.isNestedLazy();
    }

    @Test
    public void testSetNestedLazy()
    {
        // Setup
        // Run the test
        final OrcReaderConfig result = orcReaderConfigUnderTest.setNestedLazy(false);

        // Verify the results
    }
}
