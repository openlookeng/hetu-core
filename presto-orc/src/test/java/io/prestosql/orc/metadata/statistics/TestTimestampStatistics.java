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

import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;

public class TestTimestampStatistics
        extends AbstractRangeStatisticsTest<TimestampStatistics, Long>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TimestampStatistics.class).instanceSize();

    @Override
    protected TimestampStatistics getCreateStatistics(Long min, Long max)
    {
        return new TimestampStatistics(min, max);
    }

    @Test
    public void test()
    {
        assertMinMax(0L, 100L);
        assertMinMax(100L, 100L);
        assertMinMax(MIN_VALUE, 100L);
        assertMinMax(100L, MAX_VALUE);
        assertMinMax(MIN_VALUE, MAX_VALUE);
    }

    @Test
    public void testRetainedSize()
    {
        assertRetainedSize(0L, 100L, INSTANCE_SIZE);
        assertRetainedSize(100L, 100L, INSTANCE_SIZE);
        assertRetainedSize(MIN_VALUE, 100L, INSTANCE_SIZE);
        assertRetainedSize(100L, MAX_VALUE, INSTANCE_SIZE);
        assertRetainedSize(MIN_VALUE, MAX_VALUE, INSTANCE_SIZE);
    }
}
