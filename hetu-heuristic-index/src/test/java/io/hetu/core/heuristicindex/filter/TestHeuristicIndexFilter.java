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

package io.hetu.core.heuristicindex.filter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.hetu.core.common.filesystem.TempFolder;
import io.hetu.core.plugin.heuristicindex.index.bloom.BloomIndex;
import io.hetu.core.plugin.heuristicindex.index.minmax.MinMaxIndex;
import io.prestosql.expressions.LogicalRowExpressions;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;

import static io.hetu.core.HeuristicIndexTestUtils.simplePredicate;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
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
            throws IOException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File testFile = folder.newFile();

            bloomIndex1 = new BloomIndex();
            bloomIndex1.setExpectedNumOfEntries(2);
            bloomIndex1.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of("a", "b"))));

            try (FileOutputStream fo = new FileOutputStream(testFile)) {
                bloomIndex1.serialize(fo);
            }

            try (FileInputStream fi = new FileInputStream(testFile)) {
                bloomIndex1.deserialize(fi);
            }

            bloomIndex2 = new BloomIndex();
            bloomIndex2.setExpectedNumOfEntries(2);
            bloomIndex2.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of("c", "d"))));

            try (FileOutputStream fo = new FileOutputStream(testFile)) {
                bloomIndex2.serialize(fo);
            }

            try (FileInputStream fi = new FileInputStream(testFile)) {
                bloomIndex2.deserialize(fi);
            }

            minMaxIndex1 = new MinMaxIndex();
            minMaxIndex1.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of(1L, 5L, 10L))));

            minMaxIndex2 = new MinMaxIndex();
            minMaxIndex2.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of(50L, 80L, 100L))));
        }
    }

    @Test
    public void testFilterWithBloomIndices()
    {
        RowExpression expression1 = LogicalRowExpressions.and(
                simplePredicate(OperatorType.EQUAL, "testColumn", VARCHAR, "a"),
                simplePredicate(OperatorType.EQUAL, "testColumn", VARCHAR, "b"));
        RowExpression expression2 = LogicalRowExpressions.and(
                simplePredicate(OperatorType.EQUAL, "testColumn", VARCHAR, "a"),
                simplePredicate(OperatorType.EQUAL, "testColumn", VARCHAR, "e"));
        RowExpression expression3 = LogicalRowExpressions.or(
                simplePredicate(OperatorType.EQUAL, "testColumn", VARCHAR, "e"),
                simplePredicate(OperatorType.EQUAL, "testColumn", VARCHAR, "c"));
        RowExpression expression4 = LogicalRowExpressions.or(
                simplePredicate(OperatorType.EQUAL, "testColumn", VARCHAR, "e"),
                simplePredicate(OperatorType.EQUAL, "testColumn", VARCHAR, "f"));
        RowExpression expression5 = LogicalRowExpressions.and(
                simplePredicate(OperatorType.EQUAL, "testColumn", VARCHAR, "d"),
                LogicalRowExpressions.or(
                        simplePredicate(OperatorType.EQUAL, "testColumn", VARCHAR, "e"),
                        new SpecialForm(SpecialForm.Form.IN, BOOLEAN,
                                new VariableReferenceExpression("testColumn", VARCHAR),
                                new ConstantExpression("a", VARCHAR),
                                new ConstantExpression("f", VARCHAR))));

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
        RowExpression expression1 = LogicalRowExpressions.and(
                simplePredicate(OperatorType.EQUAL, "testColumn", BIGINT, 8L),
                new SpecialForm(SpecialForm.Form.IN, BOOLEAN,
                        new VariableReferenceExpression("testColumn", VARCHAR),
                        new ConstantExpression(20L, BIGINT),
                        new ConstantExpression(80L, BIGINT)));
        RowExpression expression2 = LogicalRowExpressions.and(
                simplePredicate(OperatorType.EQUAL, "testColumn", BIGINT, 5L),
                simplePredicate(OperatorType.EQUAL, "testColumn", BIGINT, 20L));
        RowExpression expression3 = LogicalRowExpressions.and(
                simplePredicate(OperatorType.GREATER_THAN_OR_EQUAL, "testColumn", BIGINT, 2L),
                simplePredicate(OperatorType.LESS_THAN_OR_EQUAL, "testColumn", BIGINT, 10L));
        RowExpression expression4 = LogicalRowExpressions.and(
                simplePredicate(OperatorType.GREATER_THAN, "testColumn", BIGINT, 8L),
                simplePredicate(OperatorType.LESS_THAN, "testColumn", BIGINT, 20L));
        RowExpression expression5 = LogicalRowExpressions.or(
                simplePredicate(OperatorType.GREATER_THAN, "testColumn", BIGINT, 200L),
                simplePredicate(OperatorType.LESS_THAN, "testColumn", BIGINT, 0L));
        RowExpression expression6 = LogicalRowExpressions.or(
                simplePredicate(OperatorType.LESS_THAN, "testColumn", BIGINT, 0L),
                new SpecialForm(SpecialForm.Form.BETWEEN, BOOLEAN,
                        new VariableReferenceExpression("testColumn", VARCHAR),
                        new ConstantExpression(5L, BIGINT),
                        new ConstantExpression(15L, BIGINT)));

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
