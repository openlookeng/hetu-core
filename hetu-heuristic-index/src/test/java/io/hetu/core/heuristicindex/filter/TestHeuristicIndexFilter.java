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

package io.hetu.core.heuristicindex.filter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.hetu.core.plugin.heuristicindex.index.bloom.BloomIndex;
import io.hetu.core.plugin.heuristicindex.index.minmax.MinMaxIndex;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SymbolReference;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHeuristicIndexFilter
{
    BloomIndex bloomIndex1;
    BloomIndex bloomIndex2;
    MinMaxIndex minMaxIndex1;
    MinMaxIndex minMaxIndex2;

    @BeforeClass
    public void setup()
    {
        bloomIndex1 = new BloomIndex();
        bloomIndex1.setExpectedNumOfEntries(2);
        bloomIndex1.addValues(ImmutableMap.of("testColumn", ImmutableList.of("a", "b")));

        bloomIndex2 = new BloomIndex();
        bloomIndex2.setExpectedNumOfEntries(2);
        bloomIndex2.addValues(ImmutableMap.of("testColumn", ImmutableList.of("c", "d")));

        minMaxIndex1 = new MinMaxIndex();
        minMaxIndex1.addValues(ImmutableMap.of("testColumn", ImmutableList.of(1L, 5L, 10L)));

        minMaxIndex2 = new MinMaxIndex();
        minMaxIndex2.addValues(ImmutableMap.of("testColumn", ImmutableList.of(50L, 80L, 100L)));
    }

    @Test
    public void testFilterWithBloomIndices()
    {
        Expression expression1 = new LogicalBinaryExpression(
                LogicalBinaryExpression.Operator.AND,
                new ComparisonExpression(EQUAL, new SymbolReference("testColumn"), new StringLiteral("a")),
                new ComparisonExpression(EQUAL, new SymbolReference("testColumn"), new StringLiteral("b")));
        Expression expression2 = new LogicalBinaryExpression(
                LogicalBinaryExpression.Operator.AND,
                new ComparisonExpression(EQUAL, new SymbolReference("testColumn"), new StringLiteral("a")),
                new ComparisonExpression(EQUAL, new SymbolReference("testColumn"), new StringLiteral("e")));
        Expression expression3 = new LogicalBinaryExpression(
                LogicalBinaryExpression.Operator.OR,
                new ComparisonExpression(EQUAL, new SymbolReference("testColumn"), new StringLiteral("e")),
                new ComparisonExpression(EQUAL, new SymbolReference("testColumn"), new StringLiteral("c")));
        Expression expression4 = new LogicalBinaryExpression(
                LogicalBinaryExpression.Operator.OR,
                new ComparisonExpression(EQUAL, new SymbolReference("testColumn"), new StringLiteral("e")),
                new ComparisonExpression(EQUAL, new SymbolReference("testColumn"), new StringLiteral("f")));
        Expression expression5 = new LogicalBinaryExpression(
                LogicalBinaryExpression.Operator.AND,
                new ComparisonExpression(EQUAL, new SymbolReference("testColumn"), new StringLiteral("d")),
                new LogicalBinaryExpression(LogicalBinaryExpression.Operator.OR,
                        new ComparisonExpression(EQUAL, new SymbolReference("testColumn"), new StringLiteral("e")),
                        new InPredicate(new SymbolReference("testColumn"),
                                new InListExpression(ImmutableList.of(new StringLiteral("a"), new StringLiteral("f"))))));

        HeuristicIndexFilter filter = new HeuristicIndexFilter(ImmutableMap.of("testColumn", ImmutableList.of(
                new IndexMetadata(bloomIndex1, "testTable", new String[] {"testColumn"}, null, null, 0, 0),
                new IndexMetadata(bloomIndex2, "testTable", new String[] {"testColumn"}, null, null, 10, 0))));

        assertTrue(filter.matches(expression1));
        assertFalse(filter.matches(expression2));
        assertTrue(filter.matches(expression3));
        assertFalse(filter.matches(expression4));
        assertTrue(filter.matches(expression5));
    }

    @Test
    public void testFilterWithMinMaxIndices()
    {
        Expression expression1 = new LogicalBinaryExpression(
                LogicalBinaryExpression.Operator.AND,
                new ComparisonExpression(EQUAL, new SymbolReference("testColumn"), new LongLiteral("8")),
                new InPredicate(new SymbolReference("testColumn"),
                        new InListExpression(ImmutableList.of(new LongLiteral("20"), new LongLiteral("80")))));
        Expression expression2 = new LogicalBinaryExpression(
                LogicalBinaryExpression.Operator.AND,
                new ComparisonExpression(EQUAL, new SymbolReference("testColumn"), new LongLiteral("5")),
                new ComparisonExpression(EQUAL, new SymbolReference("testColumn"), new LongLiteral("20")));
        Expression expression3 = new LogicalBinaryExpression(
                LogicalBinaryExpression.Operator.AND,
                new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("testColumn"), new LongLiteral("2")),
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("testColumn"), new LongLiteral("10")));
        Expression expression4 = new LogicalBinaryExpression(
                LogicalBinaryExpression.Operator.AND,
                new ComparisonExpression(GREATER_THAN, new SymbolReference("testColumn"), new LongLiteral("8")),
                new ComparisonExpression(LESS_THAN, new SymbolReference("testColumn"), new LongLiteral("20")));
        Expression expression5 = new LogicalBinaryExpression(
                LogicalBinaryExpression.Operator.OR,
                new ComparisonExpression(GREATER_THAN, new SymbolReference("testColumn"), new LongLiteral("200")),
                new ComparisonExpression(LESS_THAN, new SymbolReference("testColumn"), new LongLiteral("0")));
        Expression expression6 = new LogicalBinaryExpression(
                LogicalBinaryExpression.Operator.OR,
                new ComparisonExpression(LESS_THAN, new SymbolReference("testColumn"), new LongLiteral("0")),
                new BetweenPredicate(new SymbolReference("testColumn"), new LongLiteral("5"), new LongLiteral("15")));

        HeuristicIndexFilter filter = new HeuristicIndexFilter(ImmutableMap.of("testColumn", ImmutableList.of(
                new IndexMetadata(minMaxIndex1, "testTable", new String[] {"testColumn"}, null, null, 0, 0),
                new IndexMetadata(minMaxIndex2, "testTable", new String[] {"testColumn"}, null, null, 10, 0))));

        assertTrue(filter.matches(expression1));
        assertFalse(filter.matches(expression2));
        assertTrue(filter.matches(expression3));
        assertTrue(filter.matches(expression4));
        assertFalse(filter.matches(expression5));
        assertTrue(filter.matches(expression6));
    }
}
