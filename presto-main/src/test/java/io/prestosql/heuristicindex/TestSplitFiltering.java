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
import io.prestosql.execution.Lifespan;
import io.prestosql.execution.SqlStageExecution;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.metadata.Split;
import io.prestosql.metastore.HetuMetaStoreManager;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.split.SplitSource;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.relational.Expressions;
import io.prestosql.sql.relational.Signatures;
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

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.heuristicindex.SplitFiltering.getAllColumns;
import static io.prestosql.heuristicindex.SplitFiltering.rangeSearch;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
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

        //ComparisonExpression expr = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("a"), new StringLiteral("test_value"));
        RowExpression expression = PlanBuilder.comparison(OperatorType.EQUAL, new VariableReferenceExpression("a", VarcharType.VARCHAR), new ConstantExpression(utf8Slice("test_value"), VarcharType.VARCHAR));

        SqlStageExecution stage = TestUtil.getTestStage(expression);

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
        HeuristicIndexerManager indexerManager = new HeuristicIndexerManager(new FileSystemClientManager(), new HetuMetaStoreManager());
        Pair<Optional<RowExpression>, Map<Symbol, ColumnHandle>> pair = SplitFiltering.getExpression(stage);
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
        testIsSplitFilterApplicableForOperator(PlanBuilder.comparison(OperatorType.EQUAL, new VariableReferenceExpression("a", VarcharType.VARCHAR), new ConstantExpression(utf8Slice("hello"), VarcharType.VARCHAR)),
                true);

        // GREATER_THAN is supported
        testIsSplitFilterApplicableForOperator(
                PlanBuilder.comparison(OperatorType.GREATER_THAN, new VariableReferenceExpression("a", VarcharType.VARCHAR), new ConstantExpression(utf8Slice("hello"), VarcharType.VARCHAR)),
                true);

        // LESS_THAN is supported
        testIsSplitFilterApplicableForOperator(
                PlanBuilder.comparison(OperatorType.LESS_THAN, new VariableReferenceExpression("a", VarcharType.VARCHAR), new ConstantExpression(utf8Slice("hello"), VarcharType.VARCHAR)), true);

        // GREATER_THAN_OR_EQUAL is supported
        testIsSplitFilterApplicableForOperator(
                PlanBuilder.comparison(OperatorType.GREATER_THAN_OR_EQUAL, new VariableReferenceExpression("a", VarcharType.VARCHAR), new ConstantExpression(utf8Slice("hello"), VarcharType.VARCHAR)),
                true);

        // LESS_THAN_OR_EQUAL is supported
        testIsSplitFilterApplicableForOperator(
                PlanBuilder.comparison(OperatorType.LESS_THAN_OR_EQUAL, new VariableReferenceExpression("a", VarcharType.VARCHAR), new ConstantExpression(utf8Slice("hello"), VarcharType.VARCHAR)),
                true);

        // NOT is not supported
        testIsSplitFilterApplicableForOperator(
                Expressions.call(Signatures.notSignature(), BOOLEAN, new VariableReferenceExpression("a", VarcharType.VARCHAR)),
                true);

        // Test multiple supported predicates
        // AND is supported
        RowExpression castExpressionA = Expressions.call(Signatures.castSignature(BIGINT, BIGINT), BIGINT, new VariableReferenceExpression("a", BIGINT));
        RowExpression castExpressionB = Expressions.call(Signatures.castSignature(BIGINT, BIGINT), BIGINT, new VariableReferenceExpression("b", BIGINT));
        RowExpression expr1 = PlanBuilder.comparison(OperatorType.EQUAL, new VariableReferenceExpression("a", BIGINT), castExpressionA);
        RowExpression expr2 = PlanBuilder.comparison(OperatorType.EQUAL, new VariableReferenceExpression("b", BIGINT), castExpressionB);
        RowExpression andLbExpression = new SpecialForm(SpecialForm.Form.AND, BIGINT, expr1, expr2);
        testIsSplitFilterApplicableForOperator(
                andLbExpression,
                true);

        // OR is supported
        RowExpression orLbExpression = new SpecialForm(SpecialForm.Form.OR, BIGINT, expr1, expr2);
        testIsSplitFilterApplicableForOperator(
                orLbExpression,
                true);

        // IN is supported
        RowExpression item0 = new ConstantExpression(utf8Slice("a"), VarcharType.VARCHAR);
        RowExpression item1 = new ConstantExpression(utf8Slice("hello"), VarcharType.VARCHAR);
        RowExpression item2 = new ConstantExpression(utf8Slice("hello2"), VarcharType.VARCHAR);
        RowExpression inExpression = new SpecialForm(SpecialForm.Form.IN, BOOLEAN, ImmutableList.of(item0, item1, item2));
        testIsSplitFilterApplicableForOperator(
                inExpression,
                true);
    }

    private void testIsSplitFilterApplicableForOperator(RowExpression expression, boolean expected)
    {
        SqlStageExecution stage = TestUtil.getTestStage(expression);
        assertEquals(SplitFiltering.isSplitFilterApplicable(stage), expected);
    }

    @Test
    public void testGetColumns()
    {
        RowExpression rowExpression1 = PlanBuilder.comparison(OperatorType.EQUAL,
                new VariableReferenceExpression("col_a", BIGINT), new ConstantExpression(8L, BIGINT));
        RowExpression rowExpression2 = new SpecialForm(SpecialForm.Form.IN, BOOLEAN,
                new VariableReferenceExpression("col_b", BIGINT), new ConstantExpression(20L, BIGINT), new ConstantExpression(80L, BIGINT));
        RowExpression expression1 = new SpecialForm(SpecialForm.Form.AND, BOOLEAN, rowExpression1, rowExpression2);

        RowExpression rowExpression3 = PlanBuilder.comparison(OperatorType.EQUAL,
                new VariableReferenceExpression("c1", VarcharType.VARCHAR), new ConstantExpression("d", VarcharType.VARCHAR));
        RowExpression rowExpression4 = PlanBuilder.comparison(OperatorType.EQUAL,
                new VariableReferenceExpression("c2", VarcharType.VARCHAR), new ConstantExpression("e", VarcharType.VARCHAR));
        RowExpression rowExpression5 = new SpecialForm(SpecialForm.Form.IN, BOOLEAN,
                new VariableReferenceExpression("c2", VarcharType.VARCHAR), new ConstantExpression("a", VarcharType.VARCHAR), new ConstantExpression("f", VarcharType.VARCHAR));
        RowExpression expression6 = new SpecialForm(SpecialForm.Form.OR, BOOLEAN, rowExpression4, rowExpression5);
        RowExpression expression2 = new SpecialForm(SpecialForm.Form.AND, BOOLEAN, rowExpression3, expression6);

        parseExpressionGetColumns(expression1, ImmutableSet.of("col_a", "col_b"));
        parseExpressionGetColumns(expression2, ImmutableSet.of("c1", "c2"));
    }

    private void parseExpressionGetColumns(RowExpression expression, Set<String> expected)
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
