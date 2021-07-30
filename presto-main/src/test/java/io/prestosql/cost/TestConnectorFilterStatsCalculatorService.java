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

package io.prestosql.cost;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.TestingColumnHandle;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.statistics.ColumnStatistics;
import io.prestosql.spi.statistics.DoubleRange;
import io.prestosql.spi.statistics.Estimate;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.TestingRowExpressionTranslator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.tree.Expression;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestConnectorFilterStatsCalculatorService
{
    private Session session;
    private ConnectorFilterStatsCalculatorService statsCalculatorService;
    private ColumnHandle xColumn = new TestingColumnHandle("x");
    private ColumnStatistics xStats;
    private TableStatistics originalTableStatistics;
    private TableStatistics originalTableStatisticsWithoutTotalSize;
    private TableStatistics zeroTableStatistics;
    private TableStatistics unknownTableStatistics;
    private TypeProvider standardTypes;
    private TestingRowExpressionTranslator translator;

    @BeforeClass
    public void setUp()
    {
        session = testSessionBuilder().build();
        MetadataManager metadata = MetadataManager.createTestMetadataManager();
        FilterStatsCalculator statsCalculator = new FilterStatsCalculator(metadata, new ScalarStatsCalculator(metadata), new StatsNormalizer());
        statsCalculatorService = new ConnectorFilterStatsCalculatorService(statsCalculator);
        xStats = ColumnStatistics.builder()
                .setDistinctValuesCount(Estimate.of(40))
                .setRange(new DoubleRange(-10, 10))
                .setNullsFraction(Estimate.of(0.25))
                .build();
        zeroTableStatistics = TableStatistics.builder()
                .setRowCount(Estimate.zero())
                .build();
        unknownTableStatistics = TableStatistics.builder()
                .setRowCount(Estimate.unknown())
                .build();
        originalTableStatistics = TableStatistics.builder()
                .setRowCount(Estimate.of(100))
                .setColumnStatistics(xColumn, xStats)
                .build();
        originalTableStatisticsWithoutTotalSize = TableStatistics.builder()
                .setRowCount(Estimate.of(100))
                .setColumnStatistics(xColumn, xStats)
                .build();
        standardTypes = TypeProvider.viewOf(ImmutableMap.<Symbol, Type>builder()
                .put(new Symbol("x"), DOUBLE)
                .build());
        translator = new TestingRowExpressionTranslator(MetadataManager.createTestMetadataManager());
    }

    @Test
    public void testTableStatisticsAfterFilter()
    {
        // totalSize always be zero
        assertPredicate("true", zeroTableStatistics, zeroTableStatistics);
        assertPredicate("x < 3e0", zeroTableStatistics, unknownTableStatistics);
        assertPredicate("false", zeroTableStatistics, zeroTableStatistics);

        // rowCount and totalSize all NaN
        assertPredicate("true", TableStatistics.empty(), TableStatistics.empty());
        // rowCount and totalSize from NaN to 0.0
        assertPredicate("false", TableStatistics.empty(), TableStatistics.builder().setRowCount(Estimate.zero()).build());

        TableStatistics filteredToZeroStatistics = TableStatistics.builder()
                .setRowCount(Estimate.zero())
                .setColumnStatistics(xColumn, new ColumnStatistics(Estimate.of(1.0), Estimate.zero(), Estimate.zero(), Optional.empty()))
                .build();
        assertPredicate("false", originalTableStatistics, filteredToZeroStatistics);

        TableStatistics filteredStatistics = TableStatistics.builder()
                .setRowCount(Estimate.of(37.5))
                .setColumnStatistics(xColumn, new ColumnStatistics(Estimate.zero(), Estimate.of(20), Estimate.unknown(), Optional.of(new DoubleRange(-10, 0))))
                .build();
        assertPredicate("x < 0", originalTableStatistics, filteredStatistics);

        TableStatistics filteredStatisticsWithoutTotalSize = TableStatistics.builder()
                .setRowCount(Estimate.of(37.5))
                .setColumnStatistics(xColumn, new ColumnStatistics(Estimate.zero(), Estimate.of(20), Estimate.unknown(), Optional.of(new DoubleRange(-10, 0))))
                .build();
        assertPredicate("x < 0", originalTableStatisticsWithoutTotalSize, filteredStatisticsWithoutTotalSize);
    }

    private void assertPredicate(String filterExpression, TableStatistics tableStatistics, TableStatistics expectedStatistics)
    {
        assertPredicate(expression(filterExpression), tableStatistics, expectedStatistics);
    }

    private void assertPredicate(Expression filterExpression, TableStatistics tableStatistics, TableStatistics expectedStatistics)
    {
        RowExpression predicate = translator.translateAndOptimize(filterExpression, standardTypes);
        TableStatistics filteredStatistics = statsCalculatorService.filterStats(tableStatistics, predicate, session.toConnectorSession(),
                ImmutableMap.of(xColumn, "x"), ImmutableMap.of("x", DOUBLE), standardTypes.allTypes(), ImmutableMap.of(0, new Symbol("x")));
        assertEquals(filteredStatistics, expectedStatistics);
    }
}
