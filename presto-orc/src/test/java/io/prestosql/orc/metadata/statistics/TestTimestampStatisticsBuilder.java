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

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import io.airlift.log.Logger;
import org.testng.annotations.Test;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static io.prestosql.orc.metadata.statistics.TimestampStatistics.TIMESTAMP_VALUE_BYTES;
import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTimestampStatisticsBuilder
        extends AbstractStatisticsBuilderTest<TimestampStatisticsBuilder, Long>
{
    private static final Logger LOG = Logger.get(TestTimestampStatisticsBuilder.class);

    public TestTimestampStatisticsBuilder(StatisticsType statisticsType, Supplier<TimestampStatisticsBuilder> statisticsBuilderSupplier, BiConsumer<TimestampStatisticsBuilder, Long> adder)
    {
        super(statisticsType, statisticsBuilderSupplier, adder);
    }

    @Test
    public void testMinMaxValues()
    {
        assertMinMaxValues(0L, 0L);
        assertMinMaxValues(100L, 100L);
        assertMinMaxValues(MIN_VALUE, MIN_VALUE);
        assertMinMaxValues(MAX_VALUE, MAX_VALUE);

        assertMinMaxValues(0L, 100L);
        assertMinMaxValues(100L, 100L);
        assertMinMaxValues(MIN_VALUE, 100L);
        assertMinMaxValues(100L, MAX_VALUE);
        assertMinMaxValues(MIN_VALUE, MAX_VALUE);

        assertValues(-100L, 0L, ContiguousSet.create(Range.closed(-100L, 0L), DiscreteDomain.longs()).asList());
        assertValues(-100L, 100L, ContiguousSet.create(Range.closed(-100L, 100L), DiscreteDomain.longs()).asList());
        assertValues(0L, 100L, ContiguousSet.create(Range.closed(0L, 100L), DiscreteDomain.longs()).asList());
        assertValues(MIN_VALUE, MIN_VALUE + 100L, ContiguousSet.create(Range.closed(MIN_VALUE, MIN_VALUE + 100L), DiscreteDomain.longs()).asList());
        assertValues(MAX_VALUE - 100L, MAX_VALUE, ContiguousSet.create(Range.closed(MAX_VALUE - 100L, MAX_VALUE), DiscreteDomain.longs()).asList());
    }

    @Test
    public void testValueOutOfRange()
    {
        try {
            new TimestampStatisticsBuilder(new NoOpBloomFilterBuilder()).addValue(MAX_VALUE + 1L);
            fail("Expected ArithmeticException");
        }
        catch (ArithmeticException expected) {
            LOG.info("Error message: " + expected.getMessage());
        }

        try {
            new TimestampStatisticsBuilder(new NoOpBloomFilterBuilder()).addValue(MIN_VALUE - 1L);
            fail("Expected ArithmeticException");
        }
        catch (ArithmeticException expected) {
            LOG.info("Error message: " + expected.getMessage());
        }
    }

    @Test
    public void testMinAverageValueBytes()
    {
        assertMinAverageValueBytes(0L, ImmutableList.of());
        assertMinAverageValueBytes(TIMESTAMP_VALUE_BYTES, ImmutableList.of(100L));
        assertMinAverageValueBytes(TIMESTAMP_VALUE_BYTES, ImmutableList.of(0L));
        assertMinAverageValueBytes(TIMESTAMP_VALUE_BYTES, ImmutableList.of(0L, 100L, 100L, 101L));
    }

    @Test
    public void testUtf8BloomFilter()
    {
        TimestampStatisticsBuilder statisticsBuilder = new TimestampStatisticsBuilder(new Utf8BloomFilterBuilder(3, 0.001));
        statisticsBuilder.addValue(1234);
        statisticsBuilder.addValue(5678);
        statisticsBuilder.addValue(9012);
        HashableBloomFilter hashableBloomFilter = statisticsBuilder.buildColumnStatistics().getBloomFilter();
        assertNotNull(hashableBloomFilter);
        assertTrue(hashableBloomFilter.test(1234));
        assertTrue(hashableBloomFilter.test(5678));
        assertTrue(hashableBloomFilter.test(9012));
        assertFalse(hashableBloomFilter.test(3456));
    }
}
