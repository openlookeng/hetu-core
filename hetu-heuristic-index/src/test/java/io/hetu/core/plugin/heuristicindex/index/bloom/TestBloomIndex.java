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
import io.prestosql.spi.util.BloomFilter;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static io.hetu.core.HeuristicIndexTestUtils.simplePredicate;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
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
    public void testGetId() throws IOException
    {
        try (BloomIndex index = new BloomIndex()) {
            assertEquals("BLOOM", index.getId());
        }
    }

    @Test
    public void testMatches()
            throws IOException
    {
        try (TempFolder folder = new TempFolder(); BloomIndex bloomIndex = new BloomIndex()) {
            folder.create();
            File testFile = folder.newFile();

            List<Object> bloomValues = ImmutableList.of("a", "b", "c", "d");
            bloomIndex.setExpectedNumOfEntries(bloomValues.size());
            bloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", bloomValues)));

            try (FileOutputStream fo = new FileOutputStream(testFile)) {
                bloomIndex.serialize(fo);
            }

            try (FileInputStream fi = new FileInputStream(testFile)) {
                bloomIndex.deserialize(fi);
            }

            RowExpression expression1 = simplePredicate(OperatorType.EQUAL, "testColumn", VARCHAR, "a");
            RowExpression expression2 = simplePredicate(OperatorType.EQUAL, "testColumn", VARCHAR, "e");

            assertTrue(bloomIndex.matches(expression1));
            assertFalse(bloomIndex.matches(expression2));
        }
    }

    @Test
    public void testDomainMatching()
            throws IOException
    {
        try (TempFolder folder = new TempFolder(); BloomIndex stringBloomIndex = new BloomIndex()) {
            folder.create();
            File testFile = folder.newFile();

            List<Object> testValues = ImmutableList.of("a", "ab", "测试", "\n", "%#!", ":dfs");
            stringBloomIndex.setExpectedNumOfEntries(testValues.size());
            stringBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", testValues)));

            try (FileOutputStream fo = new FileOutputStream(testFile)) {
                stringBloomIndex.serialize(fo);
            }

            try (FileInputStream fi = new FileInputStream(testFile)) {
                stringBloomIndex.deserialize(fi);
            }

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
    }

    @Test
    public void testMatching()
            throws IOException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File testFile = folder.newFile();
            List<Object> testValues;

            // Test String bloom indexer
            try (BloomIndex stringBloomIndex = new BloomIndex()) {
                testValues = ImmutableList.of("a", "ab", "测试", "\n", "%#!", ":dfs");
                stringBloomIndex.setExpectedNumOfEntries(testValues.size());
                stringBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", testValues)));

                try (FileOutputStream fo = new FileOutputStream(testFile)) {
                    stringBloomIndex.serialize(fo);
                }

                try (FileInputStream fi = new FileInputStream(testFile)) {
                    stringBloomIndex.deserialize(fi);
                }

                assertTrue(mightContain(stringBloomIndex, VARCHAR, "a"));
                assertTrue(mightContain(stringBloomIndex, VARCHAR, "ab"));
                assertTrue(mightContain(stringBloomIndex, VARCHAR, "测试"));
                assertTrue(mightContain(stringBloomIndex, VARCHAR, "\n"));
                assertTrue(mightContain(stringBloomIndex, VARCHAR, "%#!"));
                assertTrue(mightContain(stringBloomIndex, VARCHAR, ":dfs"));
                assertFalse(mightContain(stringBloomIndex, VARCHAR, "random"));
                assertFalse(mightContain(stringBloomIndex, VARCHAR, "abc"));
            }

            // Test with the generic type to be Object
            try (BloomIndex objectBloomIndex = new BloomIndex()) {
                testValues = ImmutableList.of("a", "ab", "测试", "\n", "%#!", ":dfs");
                objectBloomIndex.setExpectedNumOfEntries(testValues.size());
                objectBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", testValues)));

                try (FileOutputStream fo = new FileOutputStream(testFile)) {
                    objectBloomIndex.serialize(fo);
                }

                try (FileInputStream fi = new FileInputStream(testFile)) {
                    objectBloomIndex.deserialize(fi);
                }

                assertTrue(mightContain(objectBloomIndex, VARCHAR, "a"));
                assertTrue(mightContain(objectBloomIndex, VARCHAR, "ab"));
                assertTrue(mightContain(objectBloomIndex, VARCHAR, "测试"));
                assertTrue(mightContain(objectBloomIndex, VARCHAR, "\n"));
                assertTrue(mightContain(objectBloomIndex, VARCHAR, "%#!"));
                assertTrue(mightContain(objectBloomIndex, VARCHAR, ":dfs"));
                assertFalse(mightContain(objectBloomIndex, VARCHAR, "random"));
                assertFalse(mightContain(objectBloomIndex, VARCHAR, "abc"));
            }

            // Test single insertion
            try (BloomIndex simpleBloomIndex = new BloomIndex()) {
                simpleBloomIndex.setExpectedNumOfEntries(6);
                simpleBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of("a"))));
                simpleBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of("ab"))));
                simpleBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of("测试"))));
                simpleBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of("\n"))));
                simpleBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of("%#!"))));
                simpleBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of(":dfs"))));

                try (FileOutputStream fo = new FileOutputStream(testFile)) {
                    simpleBloomIndex.serialize(fo);
                }

                try (FileInputStream fi = new FileInputStream(testFile)) {
                    simpleBloomIndex.deserialize(fi);
                }

                assertTrue(mightContain(simpleBloomIndex, VARCHAR, "a"));
                assertTrue(mightContain(simpleBloomIndex, VARCHAR, "ab"));
                assertTrue(mightContain(simpleBloomIndex, VARCHAR, "测试"));
                assertTrue(mightContain(simpleBloomIndex, VARCHAR, "\n"));
                assertTrue(mightContain(simpleBloomIndex, VARCHAR, "%#!"));
                assertTrue(mightContain(simpleBloomIndex, VARCHAR, ":dfs"));
                assertFalse(mightContain(simpleBloomIndex, VARCHAR, "random"));
                assertFalse(mightContain(simpleBloomIndex, VARCHAR, "abc"));
            }
        }
    }

    @Test
    public void testPersist()
            throws IOException
    {
        try (TempFolder folder = new TempFolder(); BloomIndex objectBloomIndex = new BloomIndex()) {
            folder.create();
            File testFile = folder.newFile();

            List<Object> testValues = ImmutableList.of("%#!", ":dfs", "测试", "\n", "ab", "a");
            objectBloomIndex.setExpectedNumOfEntries(testValues.size());
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
        try (TempFolder folder = new TempFolder(); BloomIndex objectBloomIndex = new BloomIndex()) {
            folder.create();
            File testFile = folder.newFile();

            try (InputStream is = new FileInputStream(testFile)) {
                assertThrows(IOException.class, () -> objectBloomIndex.deserialize(is));
            }
        }
    }

    @Test
    public void testLoad()
            throws IOException
    {
        try (TempFolder folder = new TempFolder();
                BloomIndex objectBloomIndex = new BloomIndex();
                BloomIndex readBloomIndex = new BloomIndex();
                BloomIndex intBloomIndex = new BloomIndex()) {
            folder.create();
            File testFile = folder.newFile();

            // Persist it using one object
            List<Object> testValues = ImmutableList.of("a", "ab", "测试", "\n", "%#!", ":dfs");
            objectBloomIndex.setExpectedNumOfEntries(testValues.size());
            objectBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", testValues)));
            try (FileOutputStream fo = new FileOutputStream(testFile)) {
                objectBloomIndex.serialize(fo);
            }

            // Load it using another object
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
    public void testGetProperties() throws IOException
    {
        try (BloomIndex testBloomIndex = new BloomIndex()) {
            assertNull(testBloomIndex.getProperties());
        }
    }

    @Test
    public void testSetProperties() throws IOException
    {
        try (BloomIndex testBloomIndex = new BloomIndex()) {
            Properties properties = new Properties();
            properties.setProperty("abc", "test");
            testBloomIndex.setProperties(properties);

            assertNotNull(testBloomIndex.getProperties());
            assertEquals(testBloomIndex.getProperties(), properties);
        }
    }

    @Test
    public void testSize() throws IOException
    {
        // adding 3 values to default size should pass
        try (BloomIndex defaultSizedIndex = new BloomIndex()) {
            defaultSizedIndex.setExpectedNumOfEntries(3);
            defaultSizedIndex.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of(1f, 2f, 3f))));
        }

        // adding 2 values to an index of size 2 should pass
        try (BloomIndex equalSizedIndex = new BloomIndex()) {
            equalSizedIndex.setExpectedNumOfEntries(2);
            equalSizedIndex.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of(1f, 2f))));
        }

        // adding 3 values to an index of size 2 should pass (bloom doesn't have strict size limit)
        try (BloomIndex smallSizedIndex = new BloomIndex()) {
            smallSizedIndex.setExpectedNumOfEntries(2);
            smallSizedIndex.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of(1f, 2f, 3f))));
        }
    }

    @Test
    public void testMemorySize() throws IOException
    {
        try (BloomIndex index = new BloomIndex()) {
            index.setExpectedNumOfEntries(3);
            index.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of(1f, 2f, 3f))));

            assertTrue(index.getMemoryUsage() > 0);
        }
    }

    private boolean mightContain(BloomIndex index, Type type, Object value)
    {
        CallExpression expression = simplePredicate(OperatorType.EQUAL, "testColumn", type, value);
        return index.matches(expression);
    }

    @Test
    public void testMmapUse()
            throws IOException
    {
        // experiment test to understand the performance of using mmap
        try (TempFolder folder = new TempFolder();
                BloomIndex objectBloomIndex = new BloomIndex();
                BloomIndex bloomIndexMemory = new BloomIndex();
                BloomIndex bloomIndexMmap = new BloomIndex();
                BloomIndex objectBloomIndexString = new BloomIndex();
                BloomIndex objectBloomIndexDouble = new BloomIndex();
                BloomIndex bloomIndexMemoryString = new BloomIndex();
                BloomIndex bloomIndexMemoryDouble = new BloomIndex();
                BloomIndex bloomIndexMmapDouble = new BloomIndex();
                BloomIndex bloomIndexMmapString = new BloomIndex()) {
            folder.create();
            int dataEntryNum = 2000000;
            int queryNum = 10000;
            long startTime;
            long stopTime;
            long elapsedTime;

            // compare the performance on int data with 2000000 values
            File testFile = folder.newFile("int");
            objectBloomIndex.setExpectedNumOfEntries(dataEntryNum);
            Random rd = new Random();
            List<Integer> arr = new ArrayList<>();
            for (int i = 0; i < dataEntryNum; i++) {
                arr.add(rd.nextInt());
            }
            objectBloomIndex.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of(arr))));
            try (FileOutputStream fo = new FileOutputStream(testFile)) {
                objectBloomIndex.serialize(fo);
            }

            bloomIndexMemory.setMmapEnabled(false);
            bloomIndexMemory.setExpectedNumOfEntries(dataEntryNum);
            try (FileInputStream fi = new FileInputStream(testFile)) {
                bloomIndexMemory.deserialize(fi);
            }

            bloomIndexMmap.setMmapEnabled(true);
            bloomIndexMmap.setExpectedNumOfEntries(dataEntryNum);
            try (FileInputStream fi = new FileInputStream(testFile)) {
                bloomIndexMmap.deserialize(fi);
            }

            System.out.println(testFile);
            Random rdTest = new Random();
            // get query time using memory
            startTime = System.currentTimeMillis();
            for (int i = 0; i < queryNum; i++) {
                int testNum = rdTest.nextInt();
                RowExpression expression = simplePredicate(OperatorType.EQUAL, "testColumn", INTEGER, testNum);
                bloomIndexMemory.matches(expression);
            }
            stopTime = System.currentTimeMillis();
            elapsedTime = stopTime - startTime;
            System.out.println(elapsedTime);

            // get query time using mmap
            startTime = System.currentTimeMillis();
            for (int i = 0; i < queryNum; i++) {
                int testNum = rdTest.nextInt();
                RowExpression expression = simplePredicate(OperatorType.EQUAL, "testColumn", INTEGER, testNum);
                bloomIndexMmap.matches(expression);
            }
            stopTime = System.currentTimeMillis();
            elapsedTime = stopTime - startTime;
            System.out.println(elapsedTime);

            BloomFilter memoryFilter = bloomIndexMemory.getFilter();
            BloomFilter mmapFilter = bloomIndexMmap.getFilter();
            assertEquals(mmapFilter, memoryFilter);

            long usage1 = bloomIndexMemory.getMemoryUsage();
            long usage2 = bloomIndexMmap.getMemoryUsage();
            assertTrue(usage1 > usage2, "mmap should use less memory.");

            long fileUsage1 = bloomIndexMemory.getDiskUsage();
            long fileUsage2 = bloomIndexMmap.getDiskUsage();
            assertTrue(fileUsage1 < fileUsage2, "mmap should use file space.");

            // compare the performance on double data with 2000000 entries
            File testFileDouble = folder.newFile("double");
            objectBloomIndexDouble.setExpectedNumOfEntries(dataEntryNum);
            Random rdDouble = new Random();
            List<Double> arrDouble = new ArrayList<>();
            for (int i = 0; i < dataEntryNum; i++) {
                arrDouble.add(rdDouble.nextDouble());
            }
            objectBloomIndexDouble.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of(arrDouble))));
            try (FileOutputStream fo = new FileOutputStream(testFileDouble)) {
                objectBloomIndexDouble.serialize(fo);
            }

            bloomIndexMemoryDouble.setMmapEnabled(false);
            bloomIndexMemoryDouble.setExpectedNumOfEntries(dataEntryNum);
            try (FileInputStream fi = new FileInputStream(testFileDouble)) {
                bloomIndexMemoryDouble.deserialize(fi);
            }

            bloomIndexMmapDouble.setMmapEnabled(true);
            bloomIndexMmapDouble.setExpectedNumOfEntries(dataEntryNum);
            try (FileInputStream fi = new FileInputStream(testFileDouble)) {
                bloomIndexMmapDouble.deserialize(fi);
            }

            System.out.println(testFileDouble);
            Random rdTestDouble = new Random();
            // get query time using memory
            startTime = System.currentTimeMillis();
            for (int i = 0; i < queryNum; i++) {
                double testDouble = rdTestDouble.nextDouble();
                RowExpression expression = simplePredicate(OperatorType.EQUAL, "testColumn", DOUBLE, testDouble);
                bloomIndexMemoryDouble.matches(expression);
            }
            stopTime = System.currentTimeMillis();
            elapsedTime = stopTime - startTime;
            System.out.println(elapsedTime);

            // get query time using mmap
            startTime = System.currentTimeMillis();
            for (int i = 0; i < queryNum; i++) {
                double testDouble = rdTestDouble.nextDouble();
                RowExpression expression = simplePredicate(OperatorType.EQUAL, "testColumn", DOUBLE, testDouble);
                bloomIndexMmapDouble.matches(expression);
            }

            stopTime = System.currentTimeMillis();
            elapsedTime = stopTime - startTime;
            System.out.println(elapsedTime);

            memoryFilter = bloomIndexMemoryDouble.getFilter();
            mmapFilter = bloomIndexMmapDouble.getFilter();
            assertEquals(mmapFilter, memoryFilter);

            usage1 = bloomIndexMemoryDouble.getMemoryUsage();
            usage2 = bloomIndexMmapDouble.getMemoryUsage();
            assertTrue(usage1 > usage2, "mmap should use less memory.");

            fileUsage1 = bloomIndexMemoryDouble.getDiskUsage();
            fileUsage2 = bloomIndexMmapDouble.getDiskUsage();
            assertTrue(fileUsage1 < fileUsage2, "mmap should use file space.");

            // compare the performance on UUID string with 2000000 entries

            File testFileString = folder.newFile("string");
            objectBloomIndexString.setExpectedNumOfEntries(dataEntryNum);
            List<String> arrString = new ArrayList<>();
            for (int i = 0; i < dataEntryNum; i++) {
                arrString.add(UUID.randomUUID().toString());
            }
            objectBloomIndexString.addValues(Collections.singletonList(new Pair<>("testColumn", ImmutableList.of(arrString))));
            try (FileOutputStream fo = new FileOutputStream(testFileString)) {
                objectBloomIndexString.serialize(fo);
            }

            bloomIndexMemoryString.setMmapEnabled(false);
            bloomIndexMemoryString.setExpectedNumOfEntries(dataEntryNum);
            try (FileInputStream fi = new FileInputStream(testFileString)) {
                bloomIndexMemoryString.deserialize(fi);
            }

            bloomIndexMmapString.setMmapEnabled(true);
            bloomIndexMmapString.setExpectedNumOfEntries(dataEntryNum);
            try (FileInputStream fi = new FileInputStream(testFileString)) {
                bloomIndexMmapString.deserialize(fi);
            }

            System.out.println(testFileString);
            // get query time using memory
            startTime = System.currentTimeMillis();
            for (int i = 0; i < queryNum; i++) {
                String testString = UUID.randomUUID().toString();
                RowExpression expression = simplePredicate(OperatorType.EQUAL, "testColumn", VARCHAR, testString);
                bloomIndexMemoryString.matches(expression);
            }
            stopTime = System.currentTimeMillis();
            elapsedTime = stopTime - startTime;
            System.out.println(elapsedTime);

            // get query time using mmap
            startTime = System.currentTimeMillis();
            for (int i = 0; i < queryNum; i++) {
                String testString = UUID.randomUUID().toString();
                RowExpression expression = simplePredicate(OperatorType.EQUAL, "testColumn", VARCHAR, testString);
                bloomIndexMmapString.matches(expression);
            }
            stopTime = System.currentTimeMillis();
            elapsedTime = stopTime - startTime;
            System.out.println(elapsedTime);

            memoryFilter = bloomIndexMemoryString.getFilter();
            mmapFilter = bloomIndexMmapString.getFilter();
            assertEquals(mmapFilter, memoryFilter);

            usage1 = bloomIndexMemoryString.getMemoryUsage();
            usage2 = bloomIndexMmapString.getMemoryUsage();
            assertTrue(usage1 > usage2, "mmap should use less memory.");

            fileUsage1 = bloomIndexMemoryString.getDiskUsage();
            fileUsage2 = bloomIndexMmapString.getDiskUsage();
            assertTrue(fileUsage1 < fileUsage2, "mmap should use file space.");
        }
    }
}
