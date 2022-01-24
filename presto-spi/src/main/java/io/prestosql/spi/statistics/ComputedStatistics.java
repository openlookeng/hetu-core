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
package io.prestosql.spi.statistics;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class ComputedStatistics
        implements Restorable
{
    private final List<String> groupingColumns;
    private final List<Block> groupingValues;
    private final Map<TableStatisticType, Block> tableStatistics;
    private final Map<ColumnStatisticMetadata, Block> columnStatistics;

    private ComputedStatistics(
            List<String> groupingColumns,
            List<Block> groupingValues,
            Map<TableStatisticType, Block> tableStatistics,
            Map<ColumnStatisticMetadata, Block> columnStatistics)
    {
        this.groupingColumns = unmodifiableList(new ArrayList<>(requireNonNull(groupingColumns, "groupingColumns is null")));
        this.groupingValues = unmodifiableList(new ArrayList<>(requireNonNull(groupingValues, "groupingValues is null")));
        if (!groupingValues.stream().allMatch(ComputedStatistics::isSingleValueBlock)) {
            throw new IllegalArgumentException("grouping value blocks are expected to be single value blocks");
        }
        this.tableStatistics = unmodifiableMap(new HashMap<>(requireNonNull(tableStatistics, "tableStatistics is null")));
        if (!tableStatistics.values().stream().allMatch(ComputedStatistics::isSingleValueBlock)) {
            throw new IllegalArgumentException("computed table statistics blocks are expected to be single value blocks");
        }
        this.columnStatistics = unmodifiableMap(new HashMap<>(requireNonNull(columnStatistics, "columnStatistics is null")));
        if (!columnStatistics.values().stream().allMatch(ComputedStatistics::isSingleValueBlock)) {
            throw new IllegalArgumentException("computed column statistics blocks are expected to be single value blocks");
        }
    }

    private static boolean isSingleValueBlock(Block block)
    {
        return block.getPositionCount() == 1;
    }

    public List<String> getGroupingColumns()
    {
        return groupingColumns;
    }

    public List<Block> getGroupingValues()
    {
        return groupingValues;
    }

    public Map<TableStatisticType, Block> getTableStatistics()
    {
        return tableStatistics;
    }

    public Map<ColumnStatisticMetadata, Block> getColumnStatistics()
    {
        return columnStatistics;
    }

    public static Builder builder(List<String> groupingColumns, List<Block> groupingValues)
    {
        return new Builder(groupingColumns, groupingValues);
    }

    public static class Builder
    {
        private final List<String> groupingColumns;
        private final List<Block> groupingValues;
        private final Map<TableStatisticType, Block> tableStatistics = new HashMap<>();
        private final Map<ColumnStatisticMetadata, Block> columnStatistics = new HashMap<>();

        private Builder(List<String> groupingColumns, List<Block> groupingValues)
        {
            this.groupingColumns = requireNonNull(groupingColumns, "groupingColumns is null");
            this.groupingValues = requireNonNull(groupingValues, "groupingValues is null");
        }

        public Builder addTableStatistic(TableStatisticType type, Block value)
        {
            tableStatistics.put(type, value);
            return this;
        }

        public Builder addColumnStatistic(ColumnStatisticMetadata columnStatisticMetadata, Block value)
        {
            columnStatistics.put(columnStatisticMetadata, value);
            return this;
        }

        public ComputedStatistics build()
        {
            return new ComputedStatistics(groupingColumns, groupingValues, tableStatistics, columnStatistics);
        }
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        ComputedStatisticsState myState = new ComputedStatisticsState();
        myState.groupingColumns = groupingColumns;
        myState.groupingValues = groupingValues.stream().map(block -> serializeBlock(block, serdeProvider)).toArray(byte[][]::new);
        myState.tableStatistics = new HashMap<>();
        for (Map.Entry<TableStatisticType, Block> entry : tableStatistics.entrySet()) {
            myState.tableStatistics.put(entry.getKey(), serializeBlock(entry.getValue(), serdeProvider));
        }
        myState.columnStatistics = new HashMap<>();
        for (Map.Entry<ColumnStatisticMetadata, Block> entry : columnStatistics.entrySet()) {
            myState.columnStatistics.put(entry.getKey(), serializeBlock(entry.getValue(), serdeProvider));
        }
        return myState;
    }

    public static ComputedStatistics restoreComputedStatistics(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        ComputedStatisticsState myState = (ComputedStatisticsState) state;
        List<Block> groupingValuesList = Arrays.stream(myState.groupingValues).map(array -> restoreBlock(array, serdeProvider)).collect(Collectors.toList());
        Map<TableStatisticType, Block> tableStatisticTypeMap = new HashMap<>();
        for (Map.Entry<TableStatisticType, byte[]> entry : myState.tableStatistics.entrySet()) {
            tableStatisticTypeMap.put(entry.getKey(), restoreBlock(entry.getValue(), serdeProvider));
        }
        Map<ColumnStatisticMetadata, Block> columnStatisticMetadataMap = new HashMap<>();
        for (Map.Entry<ColumnStatisticMetadata, byte[]> entry : myState.columnStatistics.entrySet()) {
            columnStatisticMetadataMap.put(entry.getKey(), restoreBlock(entry.getValue(), serdeProvider));
        }
        return new ComputedStatistics(myState.groupingColumns, groupingValuesList, tableStatisticTypeMap, columnStatisticMetadataMap);
    }

    private static byte[] serializeBlock(Block block, BlockEncodingSerdeProvider serdeProvider)
    {
        SliceOutput sliceOutput = new DynamicSliceOutput(0);
        serdeProvider.getBlockEncodingSerde().writeBlock(sliceOutput, block);
        return sliceOutput.getUnderlyingSlice().getBytes();
    }

    private static Block restoreBlock(byte[] array, BlockEncodingSerdeProvider serdeProvider)
    {
        Slice slice = Slices.wrappedBuffer(array);
        return serdeProvider.getBlockEncodingSerde().readBlock(slice.getInput());
    }

    private static class ComputedStatisticsState
            implements Serializable
    {
        private List<String> groupingColumns;
        private byte[][] groupingValues;
        private Map<TableStatisticType, byte[]> tableStatistics;
        private Map<ColumnStatisticMetadata, byte[]> columnStatistics;
    }
}
