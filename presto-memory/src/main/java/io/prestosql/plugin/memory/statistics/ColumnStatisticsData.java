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

package io.prestosql.plugin.memory.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.statistics.ColumnStatistics;
import io.prestosql.spi.statistics.DoubleRange;
import io.prestosql.spi.statistics.Estimate;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ColumnStatisticsData
{
    private final long distinctValuesCount;
    private final long nullsCount;
    private final Optional<Double> min;
    private final Optional<Double> max;
    private final Optional<Long> dataSize;

    @JsonCreator
    public ColumnStatisticsData(
            @JsonProperty("distinctValuesCount") long distinctValuesCount,
            @JsonProperty("nullsCount") long nullsCount,
            @JsonProperty("min") Optional<Double> min,
            @JsonProperty("max") Optional<Double> max,
            @JsonProperty("dataSize") Optional<Long> dataSize)
    {
        this.distinctValuesCount = distinctValuesCount;
        this.nullsCount = nullsCount;
        this.min = requireNonNull(min);
        this.max = requireNonNull(max);
        this.dataSize = requireNonNull(dataSize, "dataSize is null");
    }

    @JsonProperty
    public long getDistinctValuesCount()
    {
        return distinctValuesCount;
    }

    @JsonProperty
    public long getNullsCount()
    {
        return nullsCount;
    }

    @JsonProperty
    public Optional<Double> getMin()
    {
        return min;
    }

    @JsonProperty
    public Optional<Double> getMax()
    {
        return max;
    }

    @JsonProperty
    public Optional<Long> getDataSize()
    {
        return dataSize;
    }

    public static ColumnStatisticsData.Builder builder()
    {
        return new ColumnStatisticsData.Builder();
    }

    public ColumnStatistics toColumnStatistics(long rowCount)
    {
        ColumnStatistics.Builder builder = ColumnStatistics.builder();
        builder.setDataSize(Estimate.of((double) nullsCount / (double) rowCount));
        builder.setDistinctValuesCount(Estimate.of(distinctValuesCount));
        builder.setDataSize(dataSize.map(Estimate::of).orElse(Estimate.unknown()));
        if (min.isPresent() && max.isPresent()) {
            builder.setRange(new DoubleRange((double) min.get(), (double) max.get()));
        }
        return builder.build();
    }

    /**
     * If one of the estimates below is unspecified (i.e. left as "unknown"),
     * the optimizer may not be able to derive the statistics needed for
     * performing cost-based query plan optimizations.
     */
    public static final class Builder
    {
        private long distinctValuesCount;
        private long nullsCount;
        private Optional<Double> min = Optional.empty();
        private Optional<Double> max = Optional.empty();
        private Optional<Long> dataSize = Optional.empty();

        public ColumnStatisticsData.Builder setDistinctValuesCount(long distinctValuesCount)
        {
            this.distinctValuesCount = distinctValuesCount;
            return this;
        }

        public ColumnStatisticsData.Builder setNullsCount(long nullsCount)
        {
            this.nullsCount = nullsCount;
            return this;
        }

        public ColumnStatisticsData.Builder setMin(double min)
        {
            this.min = Optional.of(min);
            return this;
        }

        public ColumnStatisticsData.Builder setMax(double max)
        {
            this.max = Optional.of(max);
            return this;
        }

        public ColumnStatisticsData.Builder setDataSize(long dataSize)
        {
            this.dataSize = Optional.of(dataSize);
            return this;
        }

        public ColumnStatisticsData build()
        {
            return new ColumnStatisticsData(distinctValuesCount, nullsCount, min, max, dataSize);
        }
    }
}
