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
package io.hetu.core.plugin.heuristicindex.index.bloom;

import io.hetu.core.common.filesystem.TempFolder;
import io.prestosql.spi.heuristicindex.Operator;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestBloomIndex
{
    @Test
    public void testGetId()
    {
        assertEquals("BLOOM", new BloomIndex<>().getId());
    }

    @Test
    public void testFilterValues()
    {
        // Test String bloom indexer
        BloomIndex<String> stringBloomIndex = new BloomIndex<>();
        String[] testValues = new String[]{"a", "ab", "测试", "\n", "%#!", ":dfs"};
        stringBloomIndex.setExpectedNumOfEntries(testValues.length);
        stringBloomIndex.addValues(testValues);

        assertTrue(stringBloomIndex.mightContain("a"));
        assertTrue(stringBloomIndex.mightContain("ab"));
        assertTrue(stringBloomIndex.mightContain("测试"));
        assertTrue(stringBloomIndex.mightContain("\n"));
        assertTrue(stringBloomIndex.mightContain("%#!"));
        assertTrue(stringBloomIndex.mightContain(":dfs"));
        assertFalse(stringBloomIndex.mightContain("random"));
        assertFalse(stringBloomIndex.mightContain("abc"));

        // Test with the generic type to be Object
        BloomIndex<Object> objectBloomIndex = new BloomIndex<>();
        testValues = new String[]{"a", "ab", "测试", "\n", "%#!", ":dfs"};
        objectBloomIndex.addValues(testValues);

        assertTrue(objectBloomIndex.mightContain("a"));
        assertTrue(objectBloomIndex.mightContain("ab"));
        assertTrue(objectBloomIndex.mightContain("测试"));
        assertTrue(objectBloomIndex.mightContain("\n"));
        assertTrue(objectBloomIndex.mightContain("%#!"));
        assertTrue(objectBloomIndex.mightContain(":dfs"));
        assertFalse(objectBloomIndex.mightContain("random"));
        assertFalse(objectBloomIndex.mightContain("abc"));

        // Test single insertion
        BloomIndex<Object> simpleBloomIndex = new BloomIndex<>();
        simpleBloomIndex.addValues(new String[]{"a"});
        simpleBloomIndex.addValues(new String[]{"ab"});
        simpleBloomIndex.addValues(new String[]{"测试"});
        simpleBloomIndex.addValues(new String[]{"\n"});
        simpleBloomIndex.addValues(new String[]{"%#!"});
        simpleBloomIndex.addValues(new String[]{":dfs"});

        assertTrue(simpleBloomIndex.mightContain("a"));
        assertTrue(simpleBloomIndex.mightContain("ab"));
        assertTrue(simpleBloomIndex.mightContain("测试"));
        assertTrue(simpleBloomIndex.mightContain("\n"));
        assertTrue(simpleBloomIndex.mightContain("%#!"));
        assertTrue(simpleBloomIndex.mightContain(":dfs"));
        assertFalse(simpleBloomIndex.mightContain("random"));
        assertFalse(simpleBloomIndex.mightContain("abc"));
    }

    @Test
    public void testPersist() throws IOException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File testFile = folder.newFile();

            BloomIndex<Object> objectBloomIndex = new BloomIndex<>();
            String[] testValues = new String[]{"%#!", ":dfs", "测试", "\n", "ab", "a"};
            objectBloomIndex.addValues(testValues);

            try (FileOutputStream fo = new FileOutputStream(testFile)) {
                objectBloomIndex.persist(fo);
            }
            try (FileInputStream fi = new FileInputStream(testFile)) {
                assertTrue(fi.available() != 0, "Persisted bloom index file is empty");
            }
        }
    }

    @Test
    public void testLoadEmpty() throws IOException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File testFile = folder.newFile();

            BloomIndex<Object> objectBloomIndex = new BloomIndex<>();
            try (InputStream is = new FileInputStream(testFile)) {
                assertThrows(IOException.class, () -> objectBloomIndex.load(is));
            }
        }
    }

    @Test
    public void testLoad() throws IOException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File testFile = folder.newFile();

            // Persist it using one object
            BloomIndex<Object> objectBloomIndex = new BloomIndex<>();
            String[] testValues = new String[]{"a", "ab", "测试", "\n", "%#!", ":dfs"};
            objectBloomIndex.addValues(testValues);
            try (FileOutputStream fo = new FileOutputStream(testFile)) {
                objectBloomIndex.persist(fo);
            }

            // Load it using another object
            BloomIndex<String> readBloomIndex = new BloomIndex<>();
            try (FileInputStream fi = new FileInputStream(testFile)) {
                readBloomIndex.load(fi);
            }
            // Check the result validity
            assertTrue(readBloomIndex.mightContain("a"));
            assertTrue(readBloomIndex.mightContain("ab"));
            assertTrue(readBloomIndex.mightContain("测试"));
            assertTrue(readBloomIndex.mightContain("\n"));
            assertTrue(readBloomIndex.mightContain("%#!"));
            assertTrue(readBloomIndex.mightContain(":dfs"));
            assertFalse(readBloomIndex.mightContain("random"));
            assertFalse(readBloomIndex.mightContain("abc"));

            // Load it using a weired object
            BloomIndex<Integer> intBloomIndex = new BloomIndex<>();
            try (FileInputStream fi = new FileInputStream(testFile)) {
                intBloomIndex.load(fi);
            }
            assertFalse(intBloomIndex.mightContain(1));
            assertFalse(intBloomIndex.mightContain(0));
            assertFalse(intBloomIndex.mightContain(1000));
            assertFalse(intBloomIndex.mightContain("a".hashCode()));
        }
    }

    @Test
    public void testGetProperties()
    {
        BloomIndex<Object> testBloomIndex = new BloomIndex<>();
        assertNull(testBloomIndex.getProperties());
    }

    @Test
    public void testSetProperties()
    {
        BloomIndex<Object> testBloomIndex = new BloomIndex<>();
        Properties properties = new Properties();
        properties.setProperty("abc", "test");
        testBloomIndex.setProperties(properties);

        assertNotNull(testBloomIndex.getProperties());
        assertEquals(testBloomIndex.getProperties(), properties);
    }

    @Test
    public void testSupports()
    {
        BloomIndex index = new BloomIndex();
        assertTrue(index.supports(Operator.EQUAL));
        assertFalse(index.supports(Operator.LESS_THAN));
        assertFalse(index.supports(Operator.LESS_THAN_OR_EQUAL));
        assertFalse(index.supports(Operator.GREATER_THAN));
        assertFalse(index.supports(Operator.GREATER_THAN_OR_EQUAL));
        assertFalse(index.supports(Operator.NOT_EQUAL));
    }

    @Test
    public void testUnsupportedOperator()
    {
        BloomIndex index = new BloomIndex();
        Integer[] testValues = new Integer[]{1, 2, 3};
        index.addValues(testValues);
        assertThrows(IllegalArgumentException.class, () -> index.matches(1, Operator.NOT_EQUAL));
    }

    @Test
    public void testSize()
    {
        // adding 3 values to default size should pass
        BloomIndex defaultSizedIndex = new BloomIndex();
        assertEquals(defaultSizedIndex.getExpectedNumOfEntries(), BloomIndex.DEFAULT_EXPECTED_NUM_OF_SIZE);
        defaultSizedIndex.addValues(new Float[]{1f, 2f, 3f});

        // adding 2 values to an index of size 2 should pass
        BloomIndex equalSizedIndex = new BloomIndex();
        equalSizedIndex.setExpectedNumOfEntries(2);
        assertEquals(equalSizedIndex.getExpectedNumOfEntries(), 2);
        equalSizedIndex.addValues(new Float[]{1f, 2f});

        // adding 3 values to an index of size 2 should pass (bloom doesn't have strict size limit)
        BloomIndex smallSizedIndex = new BloomIndex();
        smallSizedIndex.setExpectedNumOfEntries(2);
        assertEquals(smallSizedIndex.getExpectedNumOfEntries(), 2);
        smallSizedIndex.addValues(new Float[]{1f, 2f, 3f});
    }
}
