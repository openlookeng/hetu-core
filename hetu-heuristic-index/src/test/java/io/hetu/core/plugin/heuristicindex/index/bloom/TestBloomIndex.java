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
import io.hetu.core.common.filesystem.TempFolder;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static io.prestosql.spi.sql.RowExpressionUtils.simplePredicate;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
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
        bloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", bloomValues)));

        RowExpression expression1 = simplePredicate(OperatorType.EQUAL, "testColumn", VARCHAR, "a");
        RowExpression expression2 = simplePredicate(OperatorType.EQUAL, "testColumn", VARCHAR, "e");

        assertTrue(bloomIndex.matches(expression1));
        assertFalse(bloomIndex.matches(expression2));
    }

    @Test
    public void testDomainMatching()
    {
        BloomIndex stringBloomIndex = new BloomIndex();
        List<Object> testValues = ImmutableList.of("a", "ab", "测试", "\n", "%#!", ":dfs");
        stringBloomIndex.setExpectedNumOfEntries(testValues.size());
        stringBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", testValues)));

        ValueSet valueSet = mock(ValueSet.class);
        when(valueSet.isSingleValue()).thenReturn(true);
        when(valueSet.getType()).thenReturn(VARCHAR);

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
        stringBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", testValues)));

        assertTrue(mightContain(stringBloomIndex, VARCHAR, "a"));
        assertTrue(mightContain(stringBloomIndex, VARCHAR, "ab"));
        assertTrue(mightContain(stringBloomIndex, VARCHAR, "测试"));
        assertTrue(mightContain(stringBloomIndex, VARCHAR, "\n"));
        assertTrue(mightContain(stringBloomIndex, VARCHAR, "%#!"));
        assertTrue(mightContain(stringBloomIndex, VARCHAR, ":dfs"));
        assertFalse(mightContain(stringBloomIndex, VARCHAR, "random"));
        assertFalse(mightContain(stringBloomIndex, VARCHAR, "abc"));

        // Test with the generic type to be Object
        BloomIndex objectBloomIndex = new BloomIndex();
        testValues = ImmutableList.of("a", "ab", "测试", "\n", "%#!", ":dfs");
        objectBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", testValues)));

        assertTrue(mightContain(objectBloomIndex, VARCHAR, "a"));
        assertTrue(mightContain(objectBloomIndex, VARCHAR, "ab"));
        assertTrue(mightContain(objectBloomIndex, VARCHAR, "测试"));
        assertTrue(mightContain(objectBloomIndex, VARCHAR, "\n"));
        assertTrue(mightContain(objectBloomIndex, VARCHAR, "%#!"));
        assertTrue(mightContain(objectBloomIndex, VARCHAR, ":dfs"));
        assertFalse(mightContain(objectBloomIndex, VARCHAR, "random"));
        assertFalse(mightContain(objectBloomIndex, VARCHAR, "abc"));

        // Test single insertion
        BloomIndex simpleBloomIndex = new BloomIndex();
        simpleBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of("a"))));
        simpleBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of("ab"))));
        simpleBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of("测试"))));
        simpleBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of("\n"))));
        simpleBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of("%#!"))));
        simpleBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of(":dfs"))));

        assertTrue(mightContain(simpleBloomIndex, VARCHAR, "a"));
        assertTrue(mightContain(simpleBloomIndex, VARCHAR, "ab"));
        assertTrue(mightContain(simpleBloomIndex, VARCHAR, "测试"));
        assertTrue(mightContain(simpleBloomIndex, VARCHAR, "\n"));
        assertTrue(mightContain(simpleBloomIndex, VARCHAR, "%#!"));
        assertTrue(mightContain(simpleBloomIndex, VARCHAR, ":dfs"));
        assertFalse(mightContain(simpleBloomIndex, VARCHAR, "random"));
        assertFalse(mightContain(simpleBloomIndex, VARCHAR, "abc"));
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
            objectBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", testValues)));

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
            objectBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", testValues)));
            try (FileOutputStream fo = new FileOutputStream(testFile)) {
                objectBloomIndex.serialize(fo);
            }

            // Load it using another object
            BloomIndex readBloomIndex = new BloomIndex();
            try (FileInputStream fi = new FileInputStream(testFile)) {
                readBloomIndex.deserialize(fi);
            }
            // Check the result validity
            assertTrue(mightContain(readBloomIndex, VARCHAR, "a"));
            assertTrue(mightContain(readBloomIndex, VARCHAR, "ab"));
            assertTrue(mightContain(readBloomIndex, VARCHAR, "测试"));
            assertTrue(mightContain(readBloomIndex, VARCHAR, "\n"));
            assertTrue(mightContain(readBloomIndex, VARCHAR, "%#!"));
            assertTrue(mightContain(readBloomIndex, VARCHAR, ":dfs"));
            assertFalse(mightContain(readBloomIndex, VARCHAR, "random"));
            assertFalse(mightContain(readBloomIndex, VARCHAR, "abc"));

            // Load it using a weired object
            BloomIndex intBloomIndex = new BloomIndex();
            try (FileInputStream fi = new FileInputStream(testFile)) {
                intBloomIndex.deserialize(fi);
            }
            assertFalse(mightContain(intBloomIndex, BIGINT, 1));
            assertFalse(mightContain(intBloomIndex, BIGINT, 0));
            assertFalse(mightContain(intBloomIndex, BIGINT, 1000));
            assertFalse(mightContain(intBloomIndex, BIGINT, "a".hashCode()));
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
        defaultSizedIndex.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of(1f, 2f, 3f))));

        // adding 2 values to an index of size 2 should pass
        BloomIndex equalSizedIndex = new BloomIndex();
        equalSizedIndex.setExpectedNumOfEntries(2);
        equalSizedIndex.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of(1f, 2f))));

        // adding 3 values to an index of size 2 should pass (bloom doesn't have strict size limit)
        BloomIndex smallSizedIndex = new BloomIndex();
        smallSizedIndex.setExpectedNumOfEntries(2);
        smallSizedIndex.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of(1f, 2f, 3f))));
    }

    @Test
    public void testMemorySize()
    {
        BloomIndex index = new BloomIndex();
        index.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of(1f, 2f, 3f))));

        assertTrue(index.getMemoryUsage() > 0);
    }

    private boolean mightContain(BloomIndex index, Type type, Object value)
    {
        CallExpression expression = simplePredicate(OperatorType.EQUAL, "testColumn", type, value);
        return index.matches(expression);
    }
}
