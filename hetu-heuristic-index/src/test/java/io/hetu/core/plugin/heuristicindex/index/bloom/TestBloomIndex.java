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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.hetu.core.common.filesystem.TempFolder;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Expression;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
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
        assertEquals("BLOOM", new BloomIndex().getId());
    }

    @Test
    public void testMatches()
    {
        BloomIndex bloomIndex = new BloomIndex();
        List<Object> bloomValues = ImmutableList.of("a", "b", "c", "d");
        bloomIndex.setExpectedNumOfEntries(bloomValues.size());
        bloomIndex.addValues(ImmutableMap.of("testColumn", bloomValues));

        Expression expression1 = new SqlParser().createExpression("(testColumn = 'a')", new ParsingOptions());
        Expression expression2 = new SqlParser().createExpression("(testColumn = 'e')", new ParsingOptions());

        assertTrue(bloomIndex.matches(expression1));
        assertFalse(bloomIndex.matches(expression2));
    }

    @Test
    public void testDomainMatching()
    {
        BloomIndex stringBloomIndex = new BloomIndex();
        List<Object> testValues = ImmutableList.of("a", "ab", "测试", "\n", "%#!", ":dfs");
        stringBloomIndex.setExpectedNumOfEntries(testValues.size());
        stringBloomIndex.addValues(ImmutableMap.of("testColumn", testValues));

        ValueSet valueSet = mock(ValueSet.class);
        Type mockType = mock(Type.class);
        when(valueSet.isSingleValue()).thenReturn(true);
        when(valueSet.getType()).thenReturn(mockType);

        when(valueSet.getSingleValue()).thenReturn("a");
        assertTrue(stringBloomIndex.matches(Domain.create(valueSet, false)));

        when(valueSet.getSingleValue()).thenReturn("%#!");
        assertTrue(stringBloomIndex.matches(Domain.create(valueSet, false)));

        when(valueSet.getSingleValue()).thenReturn("bb");
        assertFalse(stringBloomIndex.matches(Domain.create(valueSet, false)));
    }

    @Test
    public void testMatching()
    {
        // Test String bloom indexer
        BloomIndex stringBloomIndex = new BloomIndex();
        List<Object> testValues = ImmutableList.of("a", "ab", "测试", "\n", "%#!", ":dfs");
        stringBloomIndex.setExpectedNumOfEntries(testValues.size());
        stringBloomIndex.addValues(ImmutableMap.of("testColumn", testValues));

        assertTrue(mightContain(stringBloomIndex, "a"));
        assertTrue(mightContain(stringBloomIndex, "ab"));
        assertTrue(mightContain(stringBloomIndex, "测试"));
        assertTrue(mightContain(stringBloomIndex, "\n"));
        assertTrue(mightContain(stringBloomIndex, "%#!"));
        assertTrue(mightContain(stringBloomIndex, ":dfs"));
        assertFalse(mightContain(stringBloomIndex, "random"));
        assertFalse(mightContain(stringBloomIndex, "abc"));

        // Test with the generic type to be Object
        BloomIndex objectBloomIndex = new BloomIndex();
        testValues = ImmutableList.of("a", "ab", "测试", "\n", "%#!", ":dfs");
        objectBloomIndex.addValues(ImmutableMap.of("testColumn", testValues));

        assertTrue(mightContain(objectBloomIndex, "a"));
        assertTrue(mightContain(objectBloomIndex, "ab"));
        assertTrue(mightContain(objectBloomIndex, "测试"));
        assertTrue(mightContain(objectBloomIndex, "\n"));
        assertTrue(mightContain(objectBloomIndex, "%#!"));
        assertTrue(mightContain(objectBloomIndex, ":dfs"));
        assertFalse(mightContain(objectBloomIndex, "random"));
        assertFalse(mightContain(objectBloomIndex, "abc"));

        // Test single insertion
        BloomIndex simpleBloomIndex = new BloomIndex();
        simpleBloomIndex.addValues(ImmutableMap.of("testColumn", ImmutableList.of("a")));
        simpleBloomIndex.addValues(ImmutableMap.of("testColumn", ImmutableList.of("ab")));
        simpleBloomIndex.addValues(ImmutableMap.of("testColumn", ImmutableList.of("测试")));
        simpleBloomIndex.addValues(ImmutableMap.of("testColumn", ImmutableList.of("\n")));
        simpleBloomIndex.addValues(ImmutableMap.of("testColumn", ImmutableList.of("%#!")));
        simpleBloomIndex.addValues(ImmutableMap.of("testColumn", ImmutableList.of(":dfs")));

        assertTrue(mightContain(simpleBloomIndex, "a"));
        assertTrue(mightContain(simpleBloomIndex, "ab"));
        assertTrue(mightContain(simpleBloomIndex, "测试"));
        assertTrue(mightContain(simpleBloomIndex, "\n"));
        assertTrue(mightContain(simpleBloomIndex, "%#!"));
        assertTrue(mightContain(simpleBloomIndex, ":dfs"));
        assertFalse(mightContain(simpleBloomIndex, "random"));
        assertFalse(mightContain(simpleBloomIndex, "abc"));
    }

    @Test
    public void testPersist()
            throws IOException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File testFile = folder.newFile();

            BloomIndex objectBloomIndex = new BloomIndex();
            List<Object> testValues = ImmutableList.of("%#!", ":dfs", "测试", "\n", "ab", "a");
            objectBloomIndex.addValues(ImmutableMap.of("testColumn", testValues));

            try (FileOutputStream fo = new FileOutputStream(testFile)) {
                objectBloomIndex.serialize(fo);
            }
            try (FileInputStream fi = new FileInputStream(testFile)) {
                assertTrue(fi.available() != 0, "Persisted bloom index file is empty");
            }
        }
    }

    @Test
    public void testLoadEmpty()
            throws IOException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File testFile = folder.newFile();

            BloomIndex objectBloomIndex = new BloomIndex();
            try (InputStream is = new FileInputStream(testFile)) {
                assertThrows(IOException.class, () -> objectBloomIndex.deserialize(is));
            }
        }
    }

    @Test
    public void testLoad()
            throws IOException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File testFile = folder.newFile();

            // Persist it using one object
            BloomIndex objectBloomIndex = new BloomIndex();
            List<Object> testValues = ImmutableList.of("a", "ab", "测试", "\n", "%#!", ":dfs");
            objectBloomIndex.addValues(ImmutableMap.of("testColumn", testValues));
            try (FileOutputStream fo = new FileOutputStream(testFile)) {
                objectBloomIndex.serialize(fo);
            }

            // Load it using another object
            BloomIndex readBloomIndex = new BloomIndex();
            try (FileInputStream fi = new FileInputStream(testFile)) {
                readBloomIndex.deserialize(fi);
            }
            // Check the result validity
            assertTrue(mightContain(readBloomIndex, "a"));
            assertTrue(mightContain(readBloomIndex, "ab"));
            assertTrue(mightContain(readBloomIndex, "测试"));
            assertTrue(mightContain(readBloomIndex, "\n"));
            assertTrue(mightContain(readBloomIndex, "%#!"));
            assertTrue(mightContain(readBloomIndex, ":dfs"));
            assertFalse(mightContain(readBloomIndex, "random"));
            assertFalse(mightContain(readBloomIndex, "abc"));

            // Load it using a weired object
            BloomIndex intBloomIndex = new BloomIndex();
            try (FileInputStream fi = new FileInputStream(testFile)) {
                intBloomIndex.deserialize(fi);
            }
            assertFalse(mightContain(intBloomIndex, 1));
            assertFalse(mightContain(intBloomIndex, 0));
            assertFalse(mightContain(intBloomIndex, 1000));
            assertFalse(mightContain(intBloomIndex, "a".hashCode()));
        }
    }

    @Test
    public void testGetProperties()
    {
        BloomIndex testBloomIndex = new BloomIndex();
        assertNull(testBloomIndex.getProperties());
    }

    @Test
    public void testSetProperties()
    {
        BloomIndex testBloomIndex = new BloomIndex();
        Properties properties = new Properties();
        properties.setProperty("abc", "test");
        testBloomIndex.setProperties(properties);

        assertNotNull(testBloomIndex.getProperties());
        assertEquals(testBloomIndex.getProperties(), properties);
    }

    @Test
    public void testSize()
    {
        // adding 3 values to default size should pass
        BloomIndex defaultSizedIndex = new BloomIndex();
        assertEquals(defaultSizedIndex.getExpectedNumOfEntries(), BloomIndex.DEFAULT_EXPECTED_NUM_OF_SIZE);
        defaultSizedIndex.addValues(ImmutableMap.of("testColumn", ImmutableList.of(1f, 2f, 3f)));

        // adding 2 values to an index of size 2 should pass
        BloomIndex equalSizedIndex = new BloomIndex();
        equalSizedIndex.setExpectedNumOfEntries(2);
        assertEquals(equalSizedIndex.getExpectedNumOfEntries(), 2);
        equalSizedIndex.addValues(ImmutableMap.of("testColumn", ImmutableList.of(1f, 2f)));

        // adding 3 values to an index of size 2 should pass (bloom doesn't have strict size limit)
        BloomIndex smallSizedIndex = new BloomIndex();
        smallSizedIndex.setExpectedNumOfEntries(2);
        assertEquals(smallSizedIndex.getExpectedNumOfEntries(), 2);
        smallSizedIndex.addValues(ImmutableMap.of("testColumn", ImmutableList.of(1f, 2f, 3f)));
    }

    @Test
    public void testMemorySize()
    {
        BloomIndex index = new BloomIndex();
        index.setMemorySize(10);
        assertEquals(index.getMemorySize(), 10);
    }

    private boolean mightContain(BloomIndex index, Object value)
    {
        Expression expression = new SqlParser().createExpression(String.format("(testColumn = '%s')", value), new ParsingOptions());
        return index.matches(expression);
    }
}
