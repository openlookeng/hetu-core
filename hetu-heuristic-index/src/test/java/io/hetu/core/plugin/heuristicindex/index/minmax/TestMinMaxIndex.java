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
package io.hetu.core.plugin.heuristicindex.index.minmax;

import com.google.common.collect.ImmutableList;
import io.hetu.core.common.filesystem.TempFolder;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.relation.RowExpression;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

import static io.prestosql.spi.sql.RowExpressionUtils.simplePredicate;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMinMaxIndex
{
    @Test
    public void testMatches()
            throws IOException
    {
        MinMaxIndex minMaxIndex = new MinMaxIndex();
        List<Object> minmaxValues = ImmutableList.of(1L, 10L, 100L, 1000L);
        minMaxIndex.addValues(Collections.singletonList(new Pair<>("testColumn", minmaxValues)));

        RowExpression expression1 = simplePredicate(OperatorType.LESS_THAN, "testColumn", BIGINT, 0L);
        RowExpression expression2 = simplePredicate(OperatorType.EQUAL, "testColumn", BIGINT, 1L);
        RowExpression expression3 = simplePredicate(OperatorType.GREATER_THAN, "testColumn", BIGINT, 10L);
        RowExpression expression4 = simplePredicate(OperatorType.GREATER_THAN, "testColumn", BIGINT, 1000L);
        RowExpression expression5 = simplePredicate(OperatorType.LESS_THAN_OR_EQUAL, "testColumn", BIGINT, 1L);

        assertFalse(minMaxIndex.matches(expression1));
        assertTrue(minMaxIndex.matches(expression2));
        assertTrue(minMaxIndex.matches(expression3));
        assertFalse(minMaxIndex.matches(expression4));
        assertTrue(minMaxIndex.matches(expression5));
    }

    @Test
    public void testContains()
    {
        testHelper(OperatorType.EQUAL, 0L, 100L, 100L, 101L);
        testHelper(OperatorType.EQUAL, 0L, 100L, 50L, -50L);
        testHelper(OperatorType.EQUAL, -0.1, 10.9, -0.1, 11.0);
        testHelper(OperatorType.EQUAL, -0.1, 10.9, 2.11, -0.111);
        testHelper(OperatorType.EQUAL, "a", "y", "a", "z");
        testHelper(OperatorType.EQUAL, "a", "y", "h", "H");
    }

    void testHelper(OperatorType operator, Comparable min, Comparable max, Comparable trueVal, Comparable falseVal)
    {
        MinMaxIndex index = new MinMaxIndex(min, max);
        assertTrue(index.matches(simplePredicate(operator, "testColumn", DOUBLE, trueVal)));
        assertFalse(index.matches(simplePredicate(operator, "testColumn", DOUBLE, falseVal)));
    }

    @Test
    public void testGreaterThan()
    {
        testHelper(OperatorType.GREATER_THAN, 0L, 100L, 50L, 101L);
        testHelper(OperatorType.GREATER_THAN, 0L, 100L, 0L, 100L);
    }

    @Test
    public void testGreaterThanEqual()
    {
        testHelper(OperatorType.GREATER_THAN_OR_EQUAL, 0L, 100L, 50L, 101L);
        testHelper(OperatorType.GREATER_THAN_OR_EQUAL, 0L, 100L, 0L, 101L);
        testHelper(OperatorType.GREATER_THAN_OR_EQUAL, 0L, 100L, 100L, 101L);
    }

    @Test
    public void testLessThan()
    {
        testHelper(OperatorType.LESS_THAN, 20L, 1000L, 25L, 5L);
        testHelper(OperatorType.LESS_THAN, -10L, 1000L, -1L, -15L);
        testHelper(OperatorType.LESS_THAN, -10L, 1000L, -9L, -10L);
    }

    @Test
    public void testLessThanEqual()
    {
        testHelper(OperatorType.LESS_THAN_OR_EQUAL, 20L, 1000L, 25L, 5L);
        testHelper(OperatorType.LESS_THAN_OR_EQUAL, -10L, 1000L, -10L, -15L);
        testHelper(OperatorType.LESS_THAN_OR_EQUAL, -10L, 1000L, -9L, -11L);
    }

    @Test
    public void testPersist()
            throws IOException
    {
        testPersistHelper(0L, 100L);
        testPersistHelper("min", "max");
        testPersistHelper(0.1, 1.0);
    }

    void testPersistHelper(Comparable min, Comparable max)
            throws IOException
    {
        MinMaxIndex index = new MinMaxIndex(min, max);

        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File file = folder.newFile();
            String path = file.getCanonicalPath() + ".minmax";
            assertTrue(file.renameTo(new File(path)));
            file.deleteOnExit();

            if (!file.getParentFile().exists()) {
                assertTrue(file.getParentFile().mkdirs());
            }
            try (OutputStream os = new FileOutputStream(file)) {
                index.serialize(os);
            }

            assertTrue(file.isFile());

            MinMaxIndex loadedIndex = new MinMaxIndex();
            try (InputStream is = new FileInputStream(file)) {
                loadedIndex.deserialize(is);
            }

            assertEquals(index, loadedIndex);
        }
    }

    @Test
    public void testLoad()
            throws IOException
    {
        testLoadHelper("testLoadInteger.minmax", 0, 100);
        testLoadHelper("testLoadString.minmax", "min", "max");
        testLoadHelper("testLoadDouble.minmax", 0.1, 1.0);
    }

    void testLoadHelper(String fileName, Comparable expectedMin, Comparable expectedMax)
            throws IOException
    {
        File file = new File("src/test/resources/minmax/" + fileName).getCanonicalFile();
        MinMaxIndex loadedIndex = new MinMaxIndex();
        try (InputStream is = new FileInputStream(file)) {
            loadedIndex.deserialize(is);
        }

        assertEquals(new MinMaxIndex(expectedMin, expectedMax), loadedIndex);
    }

    @Test
    public void testIntersect()
    {
        MinMaxIndex index = new MinMaxIndex(-5, 5);
        MinMaxIndex index1 = new MinMaxIndex(0, 10);
        MinMaxIndex index2 = new MinMaxIndex(-10, 0);
        MinMaxIndex index3 = new MinMaxIndex(-1, 1);
        MinMaxIndex index4 = new MinMaxIndex(-10, 10);

        assertEquals(index.intersect(index1), new MinMaxIndex(0, 5));
        assertEquals(index.intersect(index2), new MinMaxIndex(-5, 0));
        assertEquals(index.intersect(index3), index3);
        assertEquals(index.intersect(index4), index);
    }

    @Test
    public void testUnion()
    {
        MinMaxIndex index = new MinMaxIndex(-5, 5);
        MinMaxIndex index1 = new MinMaxIndex(0, 10);
        MinMaxIndex index2 = new MinMaxIndex(-10, 0);
        MinMaxIndex index3 = new MinMaxIndex(-1, 1);
        MinMaxIndex index4 = new MinMaxIndex(-10, 10);

        assertEquals(index.union(index1), new MinMaxIndex(-5, 10));
        assertEquals(index.union(index2), new MinMaxIndex(-10, 5));
        assertEquals(index.union(index3), index);
        assertEquals(index.union(index4), index4);
    }
}
