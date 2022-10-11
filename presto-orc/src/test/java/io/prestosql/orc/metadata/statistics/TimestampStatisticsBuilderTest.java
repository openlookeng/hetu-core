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
package io.prestosql.orc.metadata.statistics;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class TimestampStatisticsBuilderTest
{
    @Mock
    private BloomFilterBuilder mockBloomFilterBuilder;

    @Mock
    private TimestampStatisticsBuilder.MillisFunction millisFunction;

    private TimestampStatisticsBuilder timestampStatisticsBuilderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        timestampStatisticsBuilderUnderTest = new TimestampStatisticsBuilder(mockBloomFilterBuilder, millisFunction);
    }

    @Test
    public void testGetValueFromBlock()
    {
        // Setup
        final Type type = null;
        final Block block = null;

        // Run the test
        timestampStatisticsBuilderUnderTest.getValueFromBlock(type, block, 0);
    }

    @Test
    public void testAddValue()
    {
        // Setup
        when(mockBloomFilterBuilder.addLong(0L)).thenReturn(null);

        // Run the test
        timestampStatisticsBuilderUnderTest.addValue(0L);

        // Verify the results
        verify(mockBloomFilterBuilder).addLong(0L);
    }

    @Test
    public void testBuildColumnStatistics()
    {
        timestampStatisticsBuilderUnderTest.buildColumnStatistics();
    }
}
