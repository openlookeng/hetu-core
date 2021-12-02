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
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.statistics.Estimate;
import io.prestosql.spi.statistics.TableStatistics;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TableStatisticsData
{
    private final long rowCount;
    private final Map<String, ColumnStatisticsData> columns;

    @JsonCreator
    public TableStatisticsData(
            @JsonProperty("rowCount") long rowCount,
            @JsonProperty("columns") Map<String, ColumnStatisticsData> columns)
    {
        this.rowCount = rowCount;
        this.columns = ImmutableMap.copyOf(columns);
    }

    @JsonProperty
    public long getRowCount()
    {
        return rowCount;
    }

    @JsonProperty
    public Map<String, ColumnStatisticsData> getColumns()
    {
        return columns;
    }

    public TableStatistics toTableStatistics(Map<String, ColumnHandle> columnHandleMap)
    {
        TableStatistics.Builder builder = TableStatistics.builder();
        builder.setRowCount(Estimate.of(rowCount));
        for (Map.Entry<String, ColumnStatisticsData> entry : columns.entrySet()) {
            builder.setColumnStatistics(columnHandleMap.get(entry.getKey()), entry.getValue().toColumnStatistics(rowCount));
        }
        return builder.build();
    }

    public static TableStatisticsData.Builder builder()
    {
        return new TableStatisticsData.Builder();
    }

    public static final class Builder
    {
        private long rowCount;
        private final Map<String, ColumnStatisticsData> columnStatisticsMap = new LinkedHashMap<>();

        public TableStatisticsData.Builder setRowCount(long rowCount)
        {
            this.rowCount = rowCount;
            return this;
        }

        public TableStatisticsData.Builder setColumnStatistics(String columnName, ColumnStatisticsData columnStatistics)
        {
            requireNonNull(columnName, "columnName can not be null");
            requireNonNull(columnStatistics, "columnStatistics can not be null");
            this.columnStatisticsMap.put(columnName, columnStatistics);
            return this;
        }

        public TableStatisticsData build()
        {
            return new TableStatisticsData(rowCount, columnStatisticsMap);
        }
    }
}
