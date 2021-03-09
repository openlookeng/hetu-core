/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.plugin.hive.metastore.HivePageSinkMetadata;
import io.prestosql.spi.connector.ConnectorVacuumTableHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HiveVacuumTableHandle
        extends HiveWritableTableHandle
        implements ConnectorVacuumTableHandle
{
    private boolean full;
    private boolean unify;
    Map<String, List<Range>> ranges;

    @JsonCreator
    public HiveVacuumTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("inputColumns") List<HiveColumnHandle> inputColumns,
            @JsonProperty("pageSinkMetadata") HivePageSinkMetadata pageSinkMetadata,
            @JsonProperty("locationHandle") LocationHandle locationHandle,
            @JsonProperty("bucketProperty") Optional<HiveBucketProperty> bucketProperty,
            @JsonProperty("tableStorageFormat") HiveStorageFormat tableStorageFormat,
            @JsonProperty("partitionStorageFormat") HiveStorageFormat partitionStorageFormat,
            @JsonProperty("full") boolean full,
            @JsonProperty("unify") boolean unify,
            @JsonProperty("ranges") Map<String, List<Range>> ranges)
    {
        super(
                schemaName,
                tableName,
                inputColumns,
                pageSinkMetadata,
                locationHandle,
                bucketProperty,
                tableStorageFormat,
                partitionStorageFormat,
                false);
        this.full = full;
        this.unify = unify;
        this.ranges = ranges;
    }

    /**
     * This will add the specified range of compacted files to be written.
     * There is no way to identify the range of delta files involved in compaction for all batches in worker side.
     * Especially useful, when bucketed files are not present in all bucketed batches
     * Usually there will be only one range per partition. When multiple partitions are involved,
     * ranges has to be corrected involving all together.
     */
    synchronized void addRange(String partitionName, Range range)
    {
        requireNonNull(partitionName, "Partition name is null");
        if (ranges == null) {
            ranges = new HashMap<>();
        }
        List<Range> partitionRanges = ranges.get(partitionName);
        if (partitionRanges == null) {
            partitionRanges = new ArrayList<>();
            ranges.put(partitionName, partitionRanges);
        }
        addRange(range, partitionRanges);
    }

    static void addRange(Range range, List<Range> ranges)
    {
        List<Range> suitableRange = getSuitableRange(range, ranges);
        if (!suitableRange.isEmpty()) {
            //Already there is an entry to cover this.
            return;
        }
        /*
         * Filter out the entries which are overalaps with (part of) current entry and expand such entry to cover both
         * ranges, and later remove all other entries which are covered by expanded entry.
         */
        List<Range> expandableEntries = ranges.stream()
                .filter(r -> ((range.getMin() >= r.getMin()
                        && range.getMax() > r.getMax()
                        && range.getMin() <= r.getMax())
                        || (range.getMax() <= r.getMax()
                        && range.getMin() < r.getMin()
                        && range.getMax() >= r.getMin())))
                .collect(toList());
        if (expandableEntries.isEmpty()) {
            ranges.add(range);
        }
        else {
            long min = range.getMin();
            long max = range.getMax();
            for (Range expandableRange : expandableEntries) {
                min = Math.min(expandableRange.getMin(), min);
                max = Math.max(expandableRange.getMax(), max);
                ranges.remove(expandableRange);
            }
            Range expandedRange = new Range(min, max);
            ranges.add(expandedRange);
        }
        //Remove duplicate ranges.
        Collections.sort(ranges);
        long current = 0;
        for (Iterator<Range> it = ranges.iterator(); it.hasNext(); ) {
            Range next = it.next();
            if (next.getMax() > current) {
                current = next.getMax();
            }
            else {
                it.remove();
            }
        }
    }

    List<Range> getSuitableRange(String partitionName, Range range)
    {
        List<Range> partitionRanges = this.ranges.get(partitionName);
        if (partitionRanges == null || partitionRanges.isEmpty()) {
            return partitionRanges;
        }
        return getSuitableRange(range, partitionRanges);
    }

    static List<Range> getSuitableRange(Range range, List<Range> ranges)
    {
        return ranges.stream()
                .filter(r -> (range.getMin() >= r.getMin()
                        && range.getMax() <= r.getMax()))
                .collect(toList());
    }

    @JsonProperty("full")
    public boolean isFullVacuum()
    {
        return full;
    }

    @JsonProperty("unify")
    public boolean isUnifyVacuum()
    {
        return unify;
    }

    @JsonProperty("ranges")
    public synchronized Map<String, List<Range>> getRanges()
    {
        return ranges;
    }

    public static class Range
            implements Comparable
    {
        private final long min;
        private final long max;

        @JsonCreator
        public Range(
                @JsonProperty("min") long min,
                @JsonProperty("max") long max)
        {
            this.min = min;
            this.max = max;
        }

        @JsonProperty("min")
        public long getMin()
        {
            return min;
        }

        @JsonProperty("max")
        public long getMax()
        {
            return max;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Range range = (Range) o;
            return min == range.min &&
                    max == range.max;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(min, max);
        }

        /**
         * Sorted in Ascending order of min and Descending order of Max to find out the overalapping ranges.
         */
        @Override
        public int compareTo(Object o)
        {
            Range other = (Range) o;
            return min < other.getMin() ? -1 : min > other.getMin() ? 1 :
                    max > other.getMax() ? -1 : 1;
        }

        @Override
        public String toString()
        {
            return "Range{" +
                    "min=" + min +
                    ", max=" + max +
                    '}';
        }
    }
}
