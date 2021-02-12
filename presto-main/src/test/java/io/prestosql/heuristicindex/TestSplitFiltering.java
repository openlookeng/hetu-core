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
package io.prestosql.heuristicindex;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.prestosql.connector.CatalogName;
import io.prestosql.execution.Lifespan;
import io.prestosql.execution.SqlStageExecution;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.metadata.Split;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.split.SplitSource;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.NotExpression;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.utils.MockSplit;
import io.prestosql.utils.TestUtil;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.prestosql.heuristicindex.SplitFiltering.getAllColumns;
import static io.prestosql.heuristicindex.SplitFiltering.rangeSearch;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestSplitFiltering
{
    /**
     * This test will not actually filter any splits since the indexes will not be found,
     * instead it's just testing the flow. The actual filtering is tested in other classes.
     */
    @Test
    public void testGetFilteredSplit()
    {
        PropertyService.setProperty(HetuConstant.FILTER_ENABLED, true);
        PropertyService.setProperty(HetuConstant.INDEXSTORE_URI, "/tmp/hetu/indices");
        PropertyService.setProperty(HetuConstant.INDEXSTORE_FILESYSTEM_PROFILE, "local-config-default");
        PropertyService.setProperty(HetuConstant.FILTER_CACHE_TTL, new Duration(10, TimeUnit.MINUTES));
        PropertyService.setProperty(HetuConstant.FILTER_CACHE_LOADING_DELAY, new Duration(5000, TimeUnit.MILLISECONDS));
        PropertyService.setProperty(HetuConstant.FILTER_CACHE_LOADING_THREADS, 2L);

        ComparisonExpression expr = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("a"), new StringLiteral("test_value"));

        SqlStageExecution stage = TestUtil.getTestStage(expr);

        List<Split> mockSplits = new ArrayList<>();
        MockSplit mock = new MockSplit("hdfs://hacluster/AppData/BIProd/DWD/EVT/bogus_table/000000_0", 0, 10, 0);
        MockSplit mock1 = new MockSplit("hdfs://hacluster/AppData/BIProd/DWD/EVT/bogus_table/000000_1", 0, 10, 0);
        MockSplit mock2 = new MockSplit("hdfs://hacluster/AppData/BIProd/DWD/EVT/bogus_table/000001_0", 0, 10, 0);
        MockSplit mock3 = new MockSplit("hdfs://hacluster/AppData/BIProd/DWD/EVT/bogus_table/000000_4", 0, 10, 0);

        mockSplits.add(new Split(new CatalogName("bogus_catalog"), mock, Lifespan.taskWide()));
        mockSplits.add(new Split(new CatalogName("bogus_catalog"), mock1, Lifespan.taskWide()));
        mockSplits.add(new Split(new CatalogName("bogus_catalog"), mock2, Lifespan.taskWide()));
        mockSplits.add(new Split(new CatalogName("bogus_catalog"), mock3, Lifespan.taskWide()));

        SplitSource.SplitBatch nextSplits = new SplitSource.SplitBatch(mockSplits, true);
        HeuristicIndexerManager indexerManager = new HeuristicIndexerManager(new FileSystemClientManager());
        Pair<Optional<Expression>, Map<Symbol, ColumnHandle>> pair = SplitFiltering.getExpression(stage);
        List<Split> filteredSplits = SplitFiltering.getFilteredSplit(pair.getFirst(),
                SplitFiltering.getFullyQualifiedName(stage), pair.getSecond(), nextSplits, indexerManager);
        assertNotNull(filteredSplits);
        assertEquals(filteredSplits.size(), 4);
    }

    /**
     * Test that split filter is applicable for different operators
     */
    @Test
    public void testIsSplitFilterApplicableForOperators()
    {
        // EQUAL is supported
        testIsSplitFilterApplicableForOperator(
                new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("a"), new StringLiteral("hello")),
                true);

        // GREATER_THAN is supported
        testIsSplitFilterApplicableForOperator(
                new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN, new SymbolReference("a"), new StringLiteral("hello")),
                true);

        // LESS_THAN is supported
        testIsSplitFilterApplicableForOperator(
                new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN, new SymbolReference("a"), new StringLiteral("hello")),
                true);

        // GREATER_THAN_OR_EQUAL is supported
        testIsSplitFilterApplicableForOperator(
                new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new StringLiteral("hello")),
                true);

        // LESS_THAN_OR_EQUAL is supported
        testIsSplitFilterApplicableForOperator(
                new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, new SymbolReference("a"), new StringLiteral("hello")),
                true);

        // NOT is not supported
        testIsSplitFilterApplicableForOperator(
                new NotExpression(new SymbolReference("a")),
                true);

        // Test multiple supported predicates
        // AND is supported
        ComparisonExpression expr1 = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("a"), new Cast(new StringLiteral("a"), "A"));
        ComparisonExpression expr2 = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("b"), new Cast(new StringLiteral("b"), "B"));
        LogicalBinaryExpression andLbExpression = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, expr1, expr2);
        testIsSplitFilterApplicableForOperator(
                andLbExpression,
                true);

        // OR is supported
        LogicalBinaryExpression orLbExpression = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.OR, expr1, expr2);
        testIsSplitFilterApplicableForOperator(
                orLbExpression,
                true);

        // IN is supported
        InPredicate inExpression = new InPredicate(new SymbolReference("a"), new InListExpression(ImmutableList.of(new StringLiteral("hello"), new StringLiteral("hello2"))));
        testIsSplitFilterApplicableForOperator(
                inExpression,
                true);
    }

    private void testIsSplitFilterApplicableForOperator(Expression expression, boolean expected)
    {
        SqlStageExecution stage = TestUtil.getTestStage(expression);
        assertEquals(SplitFiltering.isSplitFilterApplicable(stage), expected);
    }

    @Test
    public void testGetColumns()
    {
        Expression expression1 = new LogicalBinaryExpression(
                LogicalBinaryExpression.Operator.AND,
                new ComparisonExpression(EQUAL, new SymbolReference("col_a"), new LongLiteral("8")),
                new InPredicate(new SymbolReference("col_b"),
                        new InListExpression(ImmutableList.of(new LongLiteral("20"), new LongLiteral("80")))));

        Expression expression2 = new LogicalBinaryExpression(
                LogicalBinaryExpression.Operator.AND,
                new ComparisonExpression(EQUAL, new SymbolReference("c1"), new StringLiteral("d")),
                new LogicalBinaryExpression(LogicalBinaryExpression.Operator.OR,
                        new ComparisonExpression(EQUAL, new SymbolReference("c2"), new StringLiteral("e")),
                        new InPredicate(new SymbolReference("c2"),
                                new InListExpression(ImmutableList.of(new StringLiteral("a"), new StringLiteral("f"))))));

        parseExpressionGetColumns(expression1, ImmutableSet.of("col_a", "col_b"));
        parseExpressionGetColumns(expression2, ImmutableSet.of("c1", "c2"));
    }

    private void parseExpressionGetColumns(Expression expression, Set<String> expected)
    {
        Set<String> columns = new HashSet<>();
        getAllColumns(expression, columns, new HashMap<>());
        assertEquals(columns, expected);
    }

    @Test
    public void testRangeSearch()
    {
        List<Pair<Long, Long>> input = new ArrayList<>(20);
        input.add(new Pair<>(1L, 5L));
        input.add(new Pair<>(5L, 18L));
        input.add(new Pair<>(30L, 50L));
        input.add(new Pair<>(70L, 80L));
        input.add(new Pair<>(100L, 200L));
        input.add(new Pair<>(201L, 250L));
        input.add(new Pair<>(29999L, 30000L));
        input.add(new Pair<>(32000L, 50000L));
        input.add(new Pair<>(50000L, 50001L));
        input.add(new Pair<>(600000L, 700000L));
        input.add(new Pair<>(100000000L, 200000000L));
        input.add(new Pair<>(250000000L, 300000000L));
        input.add(new Pair<>(400000000L, 410000000L));
        input.add(new Pair<>(412000000L, 413000000L));
        input.add(new Pair<>(1000000000000L, 2000000000000L));

        assertTrue(rangeSearch(input, new Pair<>(1L, 5L)));
        assertTrue(rangeSearch(input, new Pair<>(18L, 20L)));
        assertFalse(rangeSearch(input, new Pair<>(19L, 25L)));
        assertTrue(rangeSearch(input, new Pair<>(200L, 201L)));
        assertTrue(rangeSearch(input, new Pair<>(0L, 50000000L)));
        assertTrue(rangeSearch(input, new Pair<>(29999L, 30000L)));
        assertFalse(rangeSearch(input, new Pair<>(50002L, 599999L)));
        assertTrue(rangeSearch(input, new Pair<>(700000L, 200000000L)));
        assertFalse(rangeSearch(input, new Pair<>(2000000000001L, 3000000000000L)));
        assertFalse(rangeSearch(input, new Pair<>(410000002L, 411900000L)));
        assertTrue(rangeSearch(input, new Pair<>(50000L, 50000000L)));
        assertTrue(rangeSearch(input, new Pair<>(1500000000000L, 1800000000000L)));
    }
}
