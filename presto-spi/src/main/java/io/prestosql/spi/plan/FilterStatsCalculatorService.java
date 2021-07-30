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

package io.prestosql.spi.plan;

import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.Type;

import java.util.Map;

public interface FilterStatsCalculatorService
{
    /**
     * @param tableStatistics Table-level and column-level statistics.
     * @param predicate Filter expression referring to columns by name
     * @param session Connector session
     * @param columnNames Mapping from ColumnHandles used in tableStatistics to column names used in the predicate
     * @param columnTypes Mapping from column names used in the predicate to column types
     * @param types symbol name and data type
     * @param layout Mapping from output symbols of plan node
     */
    TableStatistics filterStats(
            TableStatistics tableStatistics,
            RowExpression predicate,
            ConnectorSession session,
            Map<ColumnHandle, String> columnNames,
            Map<String, Type> columnTypes,
            Map<Symbol, Type> types,
            Map<Integer, Symbol> layout);
}
