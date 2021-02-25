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
package io.hetu.core.plugin.heuristicindex.index.btree;

import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static io.prestosql.spi.sql.RowExpressionUtils.simplePredicate;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestBTreeIndex
{
    @Test
    public void testStringKey()
            throws IOException
    {
        BTreeIndex index = new BTreeIndex();
        String value = "001:3,002:3,003:3,004:3,005:3,006:3,007:3,008:3,009:3,002:3,010:3,002:3,011:3,012:3,101:3,102:3,103:3,104:3,105:3,106:3,107:3,108:3,109:3,102:3,110:3,102:3,111:3,112:3";
        List<Pair> pairs = new ArrayList<>();
        pairs.add(new Pair("key1", value));
        Pair pair = new Pair("dummyCol", pairs);
        index.addKeyValues(Collections.singletonList(pair));
        File file = getFile();
        index.serialize(new FileOutputStream(file));
        BTreeIndex readIndex = new BTreeIndex();
        readIndex.deserialize(new FileInputStream(file));
        RowExpression comparisonExpression = simplePredicate(OperatorType.EQUAL, "dummyCol", VARCHAR, "key1");
        assertTrue(readIndex.matches(comparisonExpression), "Key should exists");
        index.close();
    }

    @Test
    public void testLongKey()
            throws IOException
    {
        BTreeIndex index = new BTreeIndex();
        String value = "001:3,002:3,003:3,004:3,005:3,006:3,007:3,008:3,009:3,002:3,010:3,002:3,011:3,012:3,101:3,102:3,103:3,104:3,105:3,106:3,107:3,108:3,109:3,102:3,110:3,102:3,111:3,112:3";
        List<Pair> pairs = new ArrayList<>();
        Long key = 1211231231L;
        pairs.add(new Pair(key, value));
        Pair pair = new Pair("dummyCol", pairs);
        index.addKeyValues(Collections.singletonList(pair));
        File file = getFile();
        index.serialize(new FileOutputStream(file));
        BTreeIndex readIndex = new BTreeIndex();
        readIndex.deserialize(new FileInputStream(file));
        RowExpression comparisonExpression = simplePredicate(OperatorType.EQUAL, "dummyCol", BIGINT, key);
        assertTrue(readIndex.matches(comparisonExpression), "Key should exists");
    }

    @Test
    public void testLookup()
            throws IOException
    {
        BTreeIndex index = new BTreeIndex();
        for (int i = 0; i < 100; i++) {
            List<Pair> pairs = new ArrayList<>();
            Long key = Long.valueOf(100 + i);
            String value = "value" + i;
            pairs.add(new Pair(key, value));
            Pair pair = new Pair("dummyCol", pairs);
            index.addKeyValues(Collections.singletonList(pair));
        }
        File file = getFile();
        index.serialize(new FileOutputStream(file));
        BTreeIndex readIndex = new BTreeIndex();
        readIndex.deserialize(new FileInputStream(file));
        RowExpression comparisonExpression = simplePredicate(OperatorType.EQUAL, "dummyCol", BIGINT, 101L);
        Iterator<String> result = readIndex.lookUp(comparisonExpression);
        assertNotNull(result, "Result shouldn't be null");
        assertTrue(result.hasNext());
        assertEquals("value1", result.next().toString());
        index.close();
    }

    @Test
    public void testBetween()
            throws IOException
    {
        BTreeIndex index = new BTreeIndex();
        for (int i = 0; i < 20; i++) {
            List<Pair> pairs = new ArrayList<>();
            Long key = Long.valueOf(100 + i);
            String value = "value" + i;
            pairs.add(new Pair(key, value));
            Pair pair = new Pair("dummyCol", pairs);
            index.addKeyValues(Collections.singletonList(pair));
        }
        File file = getFile();
        index.serialize(new FileOutputStream(file));
        BTreeIndex readIndex = new BTreeIndex();
        readIndex.deserialize(new FileInputStream(file));
        RowExpression betweenPredicate = new SpecialForm(SpecialForm.Form.BETWEEN, BOOLEAN,
                new VariableReferenceExpression("dummyCol", VARCHAR),
                new ConstantExpression(111L, BIGINT),
                new ConstantExpression(114L, BIGINT));
        Iterator<String> result = readIndex.lookUp(betweenPredicate);
        assertNotNull(result, "Result shouldn't be null");
        assertTrue(result.hasNext());
        for (int i = 11; i <= 14; i++) {
            assertEquals("value" + i, result.next());
        }
        assertFalse(result.hasNext());
        index.close();
    }

    @Test
    public void testIn()
            throws IOException
    {
        BTreeIndex index = new BTreeIndex();
        for (int i = 0; i < 20; i++) {
            List<Pair> pairs = new ArrayList<>();
            Long key = Long.valueOf(100 + i);
            String value = "value" + i;
            pairs.add(new Pair(key, value));
            Pair pair = new Pair("dummyCol", pairs);
            index.addKeyValues(Collections.singletonList(pair));
        }
        File file = getFile();
        index.serialize(new FileOutputStream(file));
        BTreeIndex readIndex = new BTreeIndex();
        readIndex.deserialize(new FileInputStream(file));
        RowExpression inPredicate = new SpecialForm(SpecialForm.Form.IN, BOOLEAN,
                new VariableReferenceExpression("dummyCol", VARCHAR),
                new ConstantExpression(111L, BIGINT),
                new ConstantExpression(115L, BIGINT),
                new ConstantExpression(118L, BIGINT),
                new ConstantExpression(150L, BIGINT));
        Iterator<String> result = readIndex.lookUp(inPredicate);
        assertNotNull(result, "Result shouldn't be null");
        assertTrue(result.hasNext());
        assertEquals("value11", result.next());
        assertEquals("value15", result.next());
        assertEquals("value18", result.next());
        assertFalse(result.hasNext());
        index.close();
    }

    @Test
    public void testGreaterThan()
            throws IOException
    {
        BTreeIndex index = new BTreeIndex();
        for (int i = 0; i < 25; i++) {
            List<Pair> pairs = new ArrayList<>();
            Long key = Long.valueOf(100 + i);
            String value = "value" + i;
            pairs.add(new Pair(key, value));
            Pair pair = new Pair("dummyCol", pairs);
            index.addKeyValues(Collections.singletonList(pair));
        }
        File file = getFile();
        index.serialize(new FileOutputStream(file));
        BTreeIndex readIndex = new BTreeIndex();
        readIndex.deserialize(new FileInputStream(file));
        RowExpression comparisonExpression = simplePredicate(OperatorType.GREATER_THAN, "dummyCol", BIGINT, 120L);
        Iterator<String> result = readIndex.lookUp(comparisonExpression);
        assertNotNull(result, "Result shouldn't be null");
        System.out.println(result.hasNext());
        for (int i = 21; i < 25; i++) {
            Object data = result.next();
            assertEquals("value" + i, data.toString());
        }
        assertFalse(result.hasNext());
        index.close();
    }

    @Test
    public void testGreaterThanEqualTo()
            throws IOException
    {
        BTreeIndex index = new BTreeIndex();
        for (int i = 0; i < 100; i++) {
            List<Pair> pairs = new ArrayList<>();
            Long key = Long.valueOf(100 + i);
            String value = "value" + i;
            pairs.add(new Pair(key, value));
            Pair pair = new Pair("dummyCol", pairs);
            index.addKeyValues(Collections.singletonList(pair));
        }
        File file = getFile();
        index.serialize(new FileOutputStream(file));
        BTreeIndex readIndex = new BTreeIndex();
        readIndex.deserialize(new FileInputStream(file));
        RowExpression comparisonExpression = simplePredicate(OperatorType.GREATER_THAN_OR_EQUAL, "dummyCol", BIGINT, 120L);
        Iterator<String> result = readIndex.lookUp(comparisonExpression);
        assertNotNull(result, "Result shouldn't be null");
        for (int i = 20; i < 100; i++) {
            Object data = result.next();
            assertEquals("value" + i, data.toString());
        }
        assertFalse(result.hasNext());
        index.close();
    }

    @Test
    public void testLessThan()
            throws IOException
    {
        BTreeIndex index = new BTreeIndex();
        for (int i = 0; i < 100; i++) {
            List<Pair> pairs = new ArrayList<>();
            Long key = Long.valueOf(100 + i);
            String value = "value" + i;
            pairs.add(new Pair(key, value));
            Pair pair = new Pair("dummyCol", pairs);
            index.addKeyValues(Collections.singletonList(pair));
        }
        File file = getFile();
        index.serialize(new FileOutputStream(file));
        BTreeIndex readIndex = new BTreeIndex();
        readIndex.deserialize(new FileInputStream(file));
        RowExpression comparisonExpression = simplePredicate(OperatorType.LESS_THAN, "dummyCol", BIGINT, 120L);
        Iterator<String> result = readIndex.lookUp(comparisonExpression);
        assertNotNull(result, "Result shouldn't be null");
        assertTrue(result.hasNext());
        Object[] arr = IntStream.iterate(0, n -> n + 1).limit(20).mapToObj(i -> "value" + i).toArray();
        Arrays.sort(arr);
        for (int i = 0; i < 20; i++) {
            assertEquals(arr[i], result.next());
        }
        assertFalse(result.hasNext());
        index.close();
    }

    @Test
    public void testLessThanEqualTo()
            throws IOException
    {
        BTreeIndex index = new BTreeIndex();
        for (int i = 0; i < 100; i++) {
            List<Pair> pairs = new ArrayList<>();
            Long key = Long.valueOf(100 + i);
            String value = "value" + i;
            pairs.add(new Pair(key, value));
            Pair pair = new Pair("dummyCol", pairs);
            index.addKeyValues(Collections.singletonList(pair));
        }
        File file = getFile();
        index.serialize(new FileOutputStream(file));
        BTreeIndex readIndex = new BTreeIndex();
        readIndex.deserialize(new FileInputStream(file));
        RowExpression comparisonExpression = simplePredicate(OperatorType.LESS_THAN_OR_EQUAL, "dummyCol", BIGINT, 120L);
        Iterator<String> result = readIndex.lookUp(comparisonExpression);
        assertNotNull(result, "Result shouldn't be null");
        assertTrue(result.hasNext());
        Object[] arr = IntStream.iterate(0, n -> n + 1).limit(21).mapToObj(i -> "value" + i).toArray();
        Arrays.sort(arr);
        for (int i = 0; i <= 20; i++) {
            assertEquals(arr[i], result.next());
        }
        assertFalse(result.hasNext());
        index.close();
    }

    @Test
    public void testSerialize()
            throws IOException
    {
        BTreeIndex index = new BTreeIndex();
        String value = "001:3,002:3,003:3,004:3,005:3,006:3,007:3,008:3,009:3,002:3,010:3,002:3,011:3,012:3,101:3,102:3,103:3,104:3,105:3,106:3,107:3,108:3,109:3,102:3,110:3,102:3,111:3,112:3";
        for (int i = 0; i < 1000; i++) {
            List<Pair> pairs = new ArrayList<>();
            Long key = Long.valueOf(100 + i);
            pairs.add(new Pair(key, value));
            Pair pair = new Pair("dummyCol", pairs);
            index.addKeyValues(Collections.singletonList(pair));
        }
        File file = File.createTempFile("test-serialize-", UUID.randomUUID().toString());
        index.serialize(new FileOutputStream(file));
        file.delete();
        index.close();
    }

    @Test
    public void testDeserialize()
            throws IOException
    {
        BTreeIndex index = new BTreeIndex();
        String value = "foo bar";
        for (int i = 0; i < 1000; i++) {
            List<Pair> pairs = new ArrayList<>();
            Long key = Long.valueOf(100 + i);
            pairs.add(new Pair(key, value));
            Pair pair = new Pair("dummyCol", pairs);
            index.addKeyValues(Collections.singletonList(pair));
        }
        File file = File.createTempFile("test-serialize-", UUID.randomUUID().toString());
        index.serialize(new FileOutputStream(file));

        Index readindex = new BTreeIndex();
        readindex.deserialize(new FileInputStream(file));
        RowExpression comparisonExpression = simplePredicate(OperatorType.EQUAL, "column", BIGINT, 101L);

        Iterator<String> result = readindex.lookUp(comparisonExpression);
        assertNotNull(result, "Result shouldn't be null");
        assertTrue(result.hasNext());
        assertEquals(value, result.next());
        index.close();
    }

    private File getFile()
            throws IOException
    {
        return File.createTempFile("Btreetest-", String.valueOf(System.nanoTime()));
    }
}
