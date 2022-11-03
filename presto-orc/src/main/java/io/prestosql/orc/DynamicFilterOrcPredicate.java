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

package io.prestosql.orc;

import io.airlift.log.Logger;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.metadata.OrcColumnId;
import io.prestosql.orc.metadata.statistics.ColumnStatistics;
import io.prestosql.orc.metadata.statistics.IntegerStatistics;
import io.prestosql.spi.dynamicfilter.DynamicFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * DynamicFilterOrcPredicate
 *
 * @author Huawei
 * @since 29-Sep-2022
 */
public class DynamicFilterOrcPredicate
        implements OrcPredicate
{
    private final List<ColumnDynamicFilter> columnDynamicFilters;

    private static final Logger log = Logger.get(DynamicFilterOrcPredicate.class);

    public DynamicFilterOrcPredicate(List<ColumnDynamicFilter> dynamicFilters)
    {
        this.columnDynamicFilters = dynamicFilters;
    }

    @Override
    public boolean matches(long numberOfRows, ColumnMetadata<ColumnStatistics> allColumnStatistics)
    {
        if (null != columnDynamicFilters) {
            for (ColumnDynamicFilter columnDynamicFilter : columnDynamicFilters) {
                ColumnStatistics columnStatistics = allColumnStatistics.get(columnDynamicFilter.getColumnId());
                if (columnStatistics == null) {
                    // no statistics for this column, so we can't exclude this section
                    continue;
                }

                if (!columnOverlaps(columnDynamicFilter.getDynamicFilters(), numberOfRows, columnStatistics)) {
                    return false;
                }
            }
        }

        // this section was not excluded
        return true;
    }

    private boolean columnOverlaps(List<DynamicFilter> dynamicFilters, long numberOfRows, ColumnStatistics columnStatistics)
    {
        boolean filterResult = true;
        for (int j = 0; j < dynamicFilters.size(); j++) {
            String typeName = dynamicFilters.get(j).getColumnHandle().getTypeName();
            Optional<ColumnBasicStats> stats = getColumnBasicStats(typeName, numberOfRows, columnStatistics);
            if (stats.isPresent()) {
                Long min = stats.get().getMinimum();
                Long max = stats.get().getMaximum();
                if (dynamicFilters.get(j).hasMinMaxStats()) {
                    filterResult = dynamicFilters.get(j).isRangeOverlaps(min, max);
                    if (filterResult) {
                        break;
                    }
                }
                else if (dynamicFilters.get(j).contains(min) || dynamicFilters.get(j).contains(max) || rangeMatch(min, max, dynamicFilters.get(j))) {
                    filterResult = true;
                    break;
                }
                else {
                    filterResult = false;
                }
            }
        }
        return filterResult;
    }

    private boolean rangeMatch(Long min, Long max, DynamicFilter dynamicFilter)
    {
        for (long value = min + 1; value < max; value++) {
            if (dynamicFilter.contains(value)) {
                return true;
            }
        }
        return false;
    }

    public List<DynamicFilter> getColumnDynamicFilters(int columnId)
    {
        for (ColumnDynamicFilter columnDynamicFilter : columnDynamicFilters) {
            if (columnDynamicFilter.columnId.getId() == columnId + 1) {
                return columnDynamicFilter.getDynamicFilters();
            }
        }
        return null;
    }

    private Optional<ColumnBasicStats> getColumnBasicStats(String typeName, long numberOfRows, ColumnStatistics columnStatistics)
    {
        if (numberOfRows == 0) {
            return Optional.empty();
        }

        if (columnStatistics == null) {
            return Optional.empty();
        }

        if (columnStatistics.hasNumberOfValues() && columnStatistics.getNumberOfValues() == 0) {
            return Optional.empty();
        }
        switch (typeName) {
            case "bigint":
            case "tinyint":
            case "smallint":
            case "integer": {
                IntegerStatistics integerStatistics = columnStatistics.getIntegerStatistics();
                if (integerStatistics != null) {
                    Long min = integerStatistics.getMin();
                    Long max = integerStatistics.getMax();
                    if (null != min && null != max) {
                        ColumnBasicStats stats = new ColumnBasicStats(min, max);
                        return Optional.of(stats);
                    }
                }
                break;
            }
            default:
                break;
        }

        return Optional.empty();
    }

    private static class ColumnBasicStats
    {
        private final Long minimum;
        private final Long maximum;

        private ColumnBasicStats(long minimum, long maximum)
        {
            this.minimum = minimum;
            this.maximum = maximum;
        }

        public long getMinimum()
        {
            return minimum;
        }

        public long getMaximum()
        {
            return maximum;
        }
    }

    private static class ColumnDynamicFilter
    {
        private final OrcColumnId columnId;
        private final List<DynamicFilter> dynamicFilters;

        private ColumnDynamicFilter(OrcColumnId columnId, List<DynamicFilter> dynamicFilter)
        {
            this.columnId = requireNonNull(columnId, "columnId is null");
            this.dynamicFilters = requireNonNull(dynamicFilter, "dynamicFilter is null");
        }

        public OrcColumnId getColumnId()
        {
            return columnId;
        }

        public List<DynamicFilter> getDynamicFilters()
        {
            return dynamicFilters;
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
            ColumnDynamicFilter that = (ColumnDynamicFilter) o;
            return columnId.equals(that.columnId) && dynamicFilters.equals(that.dynamicFilters);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(columnId, dynamicFilters);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("columnId", columnId)
                    .add("DynamicFilter", dynamicFilters)
                    .toString();
        }
    }

    public static class DynamicFilterOrcPredicateBuilder
    {
        private final List<DynamicFilterOrcPredicate.ColumnDynamicFilter> filterColumns = new ArrayList<>();

        public static DynamicFilterOrcPredicateBuilder builder()
        {
            return new DynamicFilterOrcPredicateBuilder();
        }

        public DynamicFilterOrcPredicate.DynamicFilterOrcPredicateBuilder addColumn(OrcColumnId columnId, List<DynamicFilter> dynamicFilters)
        {
            requireNonNull(dynamicFilters, "dynamicFilters is null");
            filterColumns.add(new DynamicFilterOrcPredicate.ColumnDynamicFilter(columnId, dynamicFilters));
            return this;
        }

        public Optional<OrcPredicate> build()
        {
            if (filterColumns.isEmpty()) {
                return Optional.empty();
            }
            else {
                return Optional.of(new DynamicFilterOrcPredicate(filterColumns));
            }
        }
    }
}
