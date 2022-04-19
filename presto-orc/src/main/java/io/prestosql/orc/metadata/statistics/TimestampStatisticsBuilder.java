/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

import java.util.List;
import java.util.Optional;

import static io.prestosql.orc.metadata.statistics.TimestampStatistics.TIMESTAMP_VALUE_BYTES;
import static java.util.Objects.requireNonNull;

public class TimestampStatisticsBuilder
        implements LongValueStatisticsBuilder
{
    private long nonNullValueCount;
    private long minimum = Integer.MAX_VALUE;
    private long maximum = Integer.MIN_VALUE;
    private final BloomFilterBuilder bloomFilterBuilder;

    public TimestampStatisticsBuilder(BloomFilterBuilder bloomFilterBuilder)
    {
        this.bloomFilterBuilder = requireNonNull(bloomFilterBuilder, "bloomFilterBuilder is null");
    }

    @Override
    public void addValue(long value)
    {
        nonNullValueCount++;

        minimum = Math.min(value, minimum);
        maximum = Math.max(value, maximum);
        bloomFilterBuilder.addLong(value);
    }

    private void addTimestampStatistics(long valueCount, TimestampStatistics value)
    {
        requireNonNull(value, "value is null");
        requireNonNull(value.getMin(), "value.getMin() is null");
        requireNonNull(value.getMax(), "value.getMax() is null");

        nonNullValueCount += valueCount;
        minimum = Math.min(value.getMin(), minimum);
        maximum = Math.max(value.getMax(), maximum);
    }

    private Optional<TimestampStatistics> buildTimestampStatistics()
    {
        if (nonNullValueCount == 0) {
            return Optional.empty();
        }
        return Optional.of(new TimestampStatistics(minimum, maximum));
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        Optional<TimestampStatistics> timestampStatistics = buildTimestampStatistics();
        return new ColumnStatistics(
                nonNullValueCount,
                timestampStatistics.map(s -> TIMESTAMP_VALUE_BYTES).orElse(0L),
                null,
                null,
                null,
                null,
                null,
                timestampStatistics.orElse(null),
                null,
                null,
                bloomFilterBuilder.buildBloomFilter());
    }

    public static Optional<TimestampStatistics> mergeTimestampStatistics(List<ColumnStatistics> stats)
    {
        TimestampStatisticsBuilder timestampStatisticsBuilder = new TimestampStatisticsBuilder(new NoOpBloomFilterBuilder());
        for (ColumnStatistics columnStatistics : stats) {
            TimestampStatistics partialStatistics = columnStatistics.getTimestampStatistics();
            if (columnStatistics.getNumberOfValues() > 0) {
                if (partialStatistics == null) {
                    // there are non-null values but no statistics, so we can not say anything about the data
                    return Optional.empty();
                }
                timestampStatisticsBuilder.addTimestampStatistics(columnStatistics.getNumberOfValues(), partialStatistics);
            }
        }
        return timestampStatisticsBuilder.buildTimestampStatistics();
    }
}
