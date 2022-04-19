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

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.orc.metadata.statistics.AbstractStatisticsBuilderTest.StatisticsType.DOUBLE;
import static io.prestosql.orc.metadata.statistics.DoubleStatistics.DOUBLE_VALUE_BYTES;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestDoubleStatisticsBuilder
        extends AbstractStatisticsBuilderTest<DoubleStatisticsBuilder, Double>
{
    private static final List<Long> ZERO_TO_42 = LongStream.rangeClosed(0, 42).boxed().collect(toImmutableList());

    public TestDoubleStatisticsBuilder()
    {
        super(DOUBLE, () -> new DoubleStatisticsBuilder(new NoOpBloomFilterBuilder()), DoubleStatisticsBuilder::addValue);
    }

    @Test
    public void testMinMaxValues()
    {
        assertMinMaxValues(0.0, 0.0);
        assertMinMaxValues(42.42, 42.42);
        assertMinMaxValues(NEGATIVE_INFINITY, NEGATIVE_INFINITY);
        assertMinMaxValues(POSITIVE_INFINITY, POSITIVE_INFINITY);

        assertMinMaxValues(0.0, 42.42);
        assertMinMaxValues(42.42, 42.42);
        assertMinMaxValues(NEGATIVE_INFINITY, 42.42);
        assertMinMaxValues(42.42, POSITIVE_INFINITY);
        assertMinMaxValues(NEGATIVE_INFINITY, POSITIVE_INFINITY);

        assertValues(0.0, 88.88, toDoubleList(0.0, 88.88, ZERO_TO_42));
        assertValues(-88.88, 0.0, toDoubleList(-88.88, 0.0, ZERO_TO_42));
        assertValues(-44.44, 44.44, toDoubleList(-44.44, 44.44, ZERO_TO_42));
    }

    private static List<Double> toDoubleList(Double minValue, Double maxValue, List<Long> values)
    {
        return values.stream()
                .flatMap(value -> Stream.of(maxValue - value, minValue + value))
                .collect(toImmutableList());
    }

    @Test
    public void testNanValue()
    {
        DoubleStatisticsBuilder statisticsBuilder = new DoubleStatisticsBuilder(new NoOpBloomFilterBuilder());
        statisticsBuilder.addValue(NaN);
        assertNoColumnStatistics(statisticsBuilder.buildColumnStatistics(), 1);
        statisticsBuilder.addValue(NaN);
        assertNoColumnStatistics(statisticsBuilder.buildColumnStatistics(), 2);
        statisticsBuilder.addValue(42.42);
        assertNoColumnStatistics(statisticsBuilder.buildColumnStatistics(), 3);

        statisticsBuilder = new DoubleStatisticsBuilder(new NoOpBloomFilterBuilder());
        statisticsBuilder.addValue(42.42);
        assertColumnStatistics(statisticsBuilder.buildColumnStatistics(), 1, 42.42, 42.42);
        statisticsBuilder.addValue(NaN);
        assertNoColumnStatistics(statisticsBuilder.buildColumnStatistics(), 2);
        statisticsBuilder.addValue(42.42);
        assertNoColumnStatistics(statisticsBuilder.buildColumnStatistics(), 3);
    }

    @Test
    public void testMinAverageValueBytes()
    {
        assertMinAverageValueBytes(0L, ImmutableList.of());
        assertMinAverageValueBytes(DOUBLE_VALUE_BYTES, ImmutableList.of(42D));
        assertMinAverageValueBytes(DOUBLE_VALUE_BYTES, ImmutableList.of(0D));
        assertMinAverageValueBytes(DOUBLE_VALUE_BYTES, ImmutableList.of(0D, 42D, 42D, 43D));
    }

    @Test
    public void testUtf8BloomFilter()
    {
        DoubleStatisticsBuilder statisticsBuilder = new DoubleStatisticsBuilder(new Utf8BloomFilterBuilder(3, 0.001));
        statisticsBuilder.addValue(1.23);
        statisticsBuilder.addValue(45.67);
        statisticsBuilder.addValue(89.01);
        HashableBloomFilter hashableBloomFilter = statisticsBuilder.buildColumnStatistics().getBloomFilter();
        assertNotNull(hashableBloomFilter);
        assertTrue(hashableBloomFilter.test(1.23));
        assertTrue(hashableBloomFilter.test(45.67));
        assertTrue(hashableBloomFilter.test(89.01));
        assertFalse(hashableBloomFilter.test(100));
    }
}
