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
import com.google.common.collect.ImmutableMap;
import io.hetu.core.common.filesystem.TempFolder;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Expression;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.List;

import static io.prestosql.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMinMaxIndex
{
    @Test
    public void testMatches()
    {
        MinMaxIndex minMaxIndex = new MinMaxIndex();
        List<Object> minmaxValues = ImmutableList.of(1L, 10L, 100L, 1000L);
        minMaxIndex.addValues(ImmutableMap.of("testColumn", minmaxValues));

        Expression expression1 = new SqlParser().createExpression("(testColumn < 0)", new ParsingOptions());
        Expression expression2 = new SqlParser().createExpression("(testColumn = 1)", new ParsingOptions());
        Expression expression3 = new SqlParser().createExpression("(testColumn > 10)", new ParsingOptions());
        Expression expression4 = new SqlParser().createExpression("(testColumn > 1000)", new ParsingOptions());
        Expression expression5 = new SqlParser().createExpression("(testColumn <= 1)", new ParsingOptions());

        assertFalse(minMaxIndex.matches(expression1));
        assertTrue(minMaxIndex.matches(expression2));
        assertTrue(minMaxIndex.matches(expression3));
        assertFalse(minMaxIndex.matches(expression4));
        assertTrue(minMaxIndex.matches(expression5));
    }

    @Test
    public void testContains()
    {
        testContainsHelper(0L, 100L, 100, 101);
        testContainsHelper(0L, 100L, 50, -50);
        testContainsHelper(BigDecimal.valueOf(-0.1), BigDecimal.valueOf(10.9), -0.1, 11.0);
        testContainsHelper(BigDecimal.valueOf(-0.1), BigDecimal.valueOf(10.9), 2.11, -0.111);
        testContainsHelper("a", "y", "'a'", "'z'");
        testContainsHelper("a", "y", "'h'", "'H'");
    }

    void testContainsHelper(Comparable min, Comparable max, Comparable containsValue, Comparable doesNotContainValue)
    {
        MinMaxIndex index = new MinMaxIndex(min, max);
        assertTrue(index.matches(new SqlParser().createExpression(String.format("(testColumn = %s)", containsValue.toString()), new ParsingOptions(AS_DECIMAL))));
        assertFalse(index.matches(new SqlParser().createExpression(String.format("(testColumn = %s)", doesNotContainValue.toString()), new ParsingOptions(AS_DECIMAL))));
    }

    @Test
    public void testGreaterThan()
    {
        testGreaterThanHelper(0L, 100L, 50, 101);
        testGreaterThanHelper(0L, 100L, 0, 100);
    }

    void testGreaterThanHelper(Comparable min, Comparable max, Comparable greaterThanValue, Comparable notGreaterThanValue)
    {
        MinMaxIndex index = new MinMaxIndex(min, max);
        assertTrue(index.matches(new SqlParser().createExpression(String.format("(testColumn > %s)", greaterThanValue.toString()), new ParsingOptions(AS_DECIMAL))));
        assertFalse(index.matches(new SqlParser().createExpression(String.format("(testColumn > %s)", notGreaterThanValue.toString()), new ParsingOptions(AS_DECIMAL))));
    }

    @Test
    public void testGreaterThanEqual()
    {
        testGreaterThanEqualHelper(0L, 100L, 50, 101);
        testGreaterThanEqualHelper(0L, 100L, 0, 101);
        testGreaterThanEqualHelper(0L, 100L, 100, 101);
    }

    void testGreaterThanEqualHelper(Comparable min, Comparable max, Comparable greaterThanEqualValue, Comparable notGreaterThanEqualValue)
    {
        MinMaxIndex index = new MinMaxIndex(min, max);
        assertTrue(index.matches(new SqlParser().createExpression(String.format("(testColumn >= %s)", greaterThanEqualValue.toString()), new ParsingOptions(AS_DECIMAL))));
        assertFalse(index.matches(new SqlParser().createExpression(String.format("(testColumn >= %s)", notGreaterThanEqualValue.toString()), new ParsingOptions(AS_DECIMAL))));
    }

    @Test
    public void testLessThan()
    {
        testLessThanHelper(20L, 1000L, 25, 5);
        testLessThanHelper(-10L, 1000L, -1, -15);
        testLessThanHelper(-10L, 1000L, -9, -10);
    }

    void testLessThanHelper(Comparable min, Comparable max, Comparable lessThanValue, Comparable notLessThanValue)
    {
        MinMaxIndex index = new MinMaxIndex(min, max);
        assertTrue(index.matches(new SqlParser().createExpression(String.format("(testColumn < %s)", lessThanValue.toString()), new ParsingOptions(AS_DECIMAL))));
        assertFalse(index.matches(new SqlParser().createExpression(String.format("(testColumn < %s)", notLessThanValue.toString()), new ParsingOptions(AS_DECIMAL))));
    }

    @Test
    public void testLessThanEqual()
    {
        testLessThanEqualHelper(20L, 1000L, 25, 5);
        testLessThanEqualHelper(-10L, 1000L, -10, -15);
        testLessThanEqualHelper(-10L, 1000L, -9, -11);
    }

    void testLessThanEqualHelper(Comparable min, Comparable max, Comparable lessThanEqualValue, Comparable notLessThanEqualValue)
    {
        MinMaxIndex index = new MinMaxIndex(min, max);
        assertTrue(index.matches(new SqlParser().createExpression(String.format("(testColumn <= %s)", lessThanEqualValue.toString()), new ParsingOptions(AS_DECIMAL))));
        assertFalse(index.matches(new SqlParser().createExpression(String.format("(testColumn <= %s)", notLessThanEqualValue.toString()), new ParsingOptions(AS_DECIMAL))));
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
