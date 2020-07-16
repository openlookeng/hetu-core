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
package io.hetu.core.heuristicindex.base;

import io.hetu.core.common.filesystem.TempFolder;
import io.hetu.core.spi.heuristicindex.Operator;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestMinMaxIndex
{
    @Test
    public void testContains()
    {
        testContainsHelper(0, 100, 100, 101);
        testContainsHelper(0, 100, 50, -50);
        testContainsHelper(-0.1, 10.9, -0.1, 11.0);
        testContainsHelper(-0.1, 10.9, 2.11, -0.111);
        testContainsHelper("a", "y", "a", "z");
        testContainsHelper("a", "y", "h", "H");
    }

    void testContainsHelper(Comparable min, Comparable max, Comparable containsValue, Comparable doesNotContainValue)
    {
        MinMaxIndex index = new MinMaxIndex(min, max);
        assertTrue(index.contains(containsValue));
        assertFalse(index.contains(doesNotContainValue));
    }

    @Test
    public void testGreaterThan()
    {
        testGreaterThanHelper(0, 100, 50, 101);
        testGreaterThanHelper(0, 100, 0, 100);
    }

    void testGreaterThanHelper(Comparable min, Comparable max, Comparable greaterThanValue, Comparable notGreaterThanValue)
    {
        MinMaxIndex index = new MinMaxIndex(min, max);
        assertTrue(index.greaterThan(greaterThanValue));
        assertFalse(index.greaterThan(notGreaterThanValue));
    }

    @Test
    public void testGreaterThanEqual()
    {
        testGreaterThanEqualHelper(0, 100, 50, 101);
        testGreaterThanEqualHelper(0, 100, 0, 101);
        testGreaterThanEqualHelper(0, 100, 100, 101);
    }

    void testGreaterThanEqualHelper(Comparable min, Comparable max, Comparable greaterThanEqualValue, Comparable notGreaterThanEqualValue)
    {
        MinMaxIndex index = new MinMaxIndex(min, max);
        assertTrue(index.greaterThanEqual(greaterThanEqualValue));
        assertFalse(index.greaterThanEqual(notGreaterThanEqualValue));
    }

    @Test
    public void testLessThan()
    {
        testLessThanHelper(20, 1000, 25, 5);
        testLessThanHelper(-10, 1000, -1, -15);
        testLessThanHelper(-10, 1000, -9, -10);
    }

    void testLessThanHelper(Comparable min, Comparable max, Comparable lessThanValue, Comparable notLessThanValue)
    {
        MinMaxIndex index = new MinMaxIndex(min, max);
        assertTrue(index.lessThan(lessThanValue));
        assertFalse(index.lessThan(notLessThanValue));
    }

    @Test
    public void testLessThanEqual()
    {
        testLessThanEqualHelper(20, 1000, 25, 5);
        testLessThanEqualHelper(-10, 1000, -10, -15);
        testLessThanEqualHelper(-10, 1000, -9, -11);
    }

    void testLessThanEqualHelper(Comparable min, Comparable max, Comparable lessThanEqualValue, Comparable notLessThanEqualValue)
    {
        MinMaxIndex index = new MinMaxIndex(min, max);
        assertTrue(index.lessThanEqual(lessThanEqualValue));
        assertFalse(index.lessThanEqual(notLessThanEqualValue));
    }

    @Test
    public void testPersist() throws IOException
    {
        testPersistHelper(0, 100);
        testPersistHelper("min", "max");
        testPersistHelper(0.1, 1.0);
    }

    void testPersistHelper(Comparable min, Comparable max) throws IOException
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
                index.persist(os);
            }

            assertTrue(file.isFile());

            MinMaxIndex loadedIndex = new MinMaxIndex();
            try (InputStream is = new FileInputStream(file)) {
                loadedIndex.load(is);
            }

            assertEquals(index, loadedIndex);
        }
    }

    @Test
    public void testLoad() throws IOException
    {
        testLoadHelper("testLoadInteger.minmax", 0, 100);
        testLoadHelper("testLoadString.minmax", "min", "max");
        testLoadHelper("testLoadDouble.minmax", 0.1, 1.0);
    }

    void testLoadHelper(String fileName, Comparable expectedMin, Comparable expectedMax) throws IOException
    {
        File file = new File("src/test/resources/minmax/" + fileName).getCanonicalFile();
        MinMaxIndex loadedIndex = new MinMaxIndex();
        try (InputStream is = new FileInputStream(file)) {
            loadedIndex.load(is);
        }

        assertEquals(new MinMaxIndex(expectedMin, expectedMax), loadedIndex);
    }

    @Test
    public void testSupports()
    {
        MinMaxIndex index = new MinMaxIndex();
        // has to add values to the min max index, otherwise it will return false for all operator supports
        Integer[] testValues = new Integer[]{0};
        index.addValues(testValues);
        assertTrue(index.supports(Operator.EQUAL));
        assertTrue(index.supports(Operator.LESS_THAN));
        assertTrue(index.supports(Operator.LESS_THAN_OR_EQUAL));
        assertTrue(index.supports(Operator.GREATER_THAN));
        assertTrue(index.supports(Operator.GREATER_THAN_OR_EQUAL));
        assertFalse(index.supports(Operator.NOT_EQUAL));
    }

    @Test
    public void testUnsupportedOperator()
    {
        MinMaxIndex index = new MinMaxIndex();
        Integer[] testValues = new Integer[]{1, 2, 3};
        index.addValues(testValues);
        assertThrows(IllegalArgumentException.class, () -> index.matches(1, Operator.NOT_EQUAL));
    }
}
