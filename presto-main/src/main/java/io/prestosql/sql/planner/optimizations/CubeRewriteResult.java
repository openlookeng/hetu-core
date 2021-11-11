/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.prestosql.sql.planner.optimizations;

import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class CubeRewriteResult
{
    private final TableScanNode tableScanNode;
    private final Map<Symbol, ColumnMetadata> symbolMetadataMap;
    private final Set<DimensionSource> dimensionColumns;
    private final Set<AggregatorSource> aggregationColumns;
    private final Set<AverageAggregatorSource> avgAggregationColumns;
    private final boolean computeAvgDividingSumByCount;

    public CubeRewriteResult(TableScanNode tableScanNode, Map<Symbol, ColumnMetadata> symbolMetadataMap, Set<DimensionSource> dimensionColumns, Set<AggregatorSource> aggregationColumns, Set<AverageAggregatorSource> avgAggregationColumns, boolean useAvgAggregationColumns)
    {
        this.tableScanNode = tableScanNode;
        this.symbolMetadataMap = symbolMetadataMap;
        this.dimensionColumns = dimensionColumns;
        this.aggregationColumns = aggregationColumns;
        this.avgAggregationColumns = avgAggregationColumns;
        this.computeAvgDividingSumByCount = useAvgAggregationColumns;
    }

    public TableScanNode getTableScanNode()
    {
        return tableScanNode;
    }

    public Map<Symbol, ColumnMetadata> getSymbolMetadataMap()
    {
        return symbolMetadataMap;
    }

    public Set<DimensionSource> getDimensionColumns()
    {
        return dimensionColumns;
    }

    public Set<AggregatorSource> getAggregationColumns()
    {
        return aggregationColumns;
    }

    public Set<AverageAggregatorSource> getAvgAggregationColumns()
    {
        return avgAggregationColumns;
    }

    public boolean getComputeAvgDividingSumByCount()
    {
        return computeAvgDividingSumByCount;
    }

    public static class DimensionSource
    {
        private final Symbol originalScanSymbol;
        private final Symbol scanSymbol;

        public DimensionSource(Symbol originalScanSymbol, Symbol scanSymbol)
        {
            this.originalScanSymbol = originalScanSymbol;
            this.scanSymbol = scanSymbol;
        }

        public Symbol getOriginalScanSymbol()
        {
            return originalScanSymbol;
        }

        public Symbol getScanSymbol()
        {
            return scanSymbol;
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
            DimensionSource that = (DimensionSource) o;
            return Objects.equals(originalScanSymbol, that.originalScanSymbol) &&
                    Objects.equals(scanSymbol, that.scanSymbol);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(originalScanSymbol, scanSymbol);
        }
    }

    public static class AggregatorSource
    {
        private final Symbol originalAggSymbol;
        private final Symbol scanSymbol;

        public AggregatorSource(Symbol originalAggSymbol, Symbol scanSymbol)
        {
            this.originalAggSymbol = originalAggSymbol;
            this.scanSymbol = scanSymbol;
        }

        public Symbol getOriginalAggSymbol()
        {
            return originalAggSymbol;
        }

        public Symbol getScanSymbol()
        {
            return scanSymbol;
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
            AggregatorSource that = (AggregatorSource) o;
            return Objects.equals(originalAggSymbol, that.originalAggSymbol) &&
                    Objects.equals(scanSymbol, that.scanSymbol);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(originalAggSymbol, scanSymbol);
        }
    }

    public static class AverageAggregatorSource
    {
        private final Symbol originalAggSymbol;
        private final Symbol sum;
        private final Symbol count;

        public AverageAggregatorSource(Symbol originalAggSymbol, Symbol sum, Symbol count)
        {
            this.originalAggSymbol = originalAggSymbol;
            this.sum = sum;
            this.count = count;
        }

        public Symbol getOriginalAggSymbol()
        {
            return originalAggSymbol;
        }

        public Symbol getSum()
        {
            return sum;
        }

        public Symbol getCount()
        {
            return count;
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
            AverageAggregatorSource that = (AverageAggregatorSource) o;
            return Objects.equals(originalAggSymbol, that.originalAggSymbol) &&
                    Objects.equals(sum, that.sum) &&
                    Objects.equals(count, that.count);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(originalAggSymbol, sum, count);
        }
    }
}
