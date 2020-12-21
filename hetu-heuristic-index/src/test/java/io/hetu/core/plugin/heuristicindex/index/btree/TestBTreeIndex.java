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

import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.KeyValue;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SymbolReference;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

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
        List<KeyValue> keyValues = new ArrayList<>();
        keyValues.add(new KeyValue("key1", value));
        Pair pair = new Pair("dummyCol", keyValues);
        index.addKeyValues(Collections.singletonList(pair));
        File file = getFile();
        index.serialize(new FileOutputStream(file));
        BTreeIndex readIndex = new BTreeIndex();
        readIndex.deserialize(new FileInputStream(file));
        ComparisonExpression comparisonExpression = new ComparisonExpression(ComparisonExpression.Operator.EQUAL,
                new StringLiteral("column"), new StringLiteral("key1"));
        assertTrue(readIndex.matches(comparisonExpression), "Key should exists");
        index.close();
    }

    @Test
    public void testLongKey()
            throws IOException
    {
        BTreeIndex index = new BTreeIndex();
        String value = "001:3,002:3,003:3,004:3,005:3,006:3,007:3,008:3,009:3,002:3,010:3,002:3,011:3,012:3,101:3,102:3,103:3,104:3,105:3,106:3,107:3,108:3,109:3,102:3,110:3,102:3,111:3,112:3";
        List<KeyValue> keyValues = new ArrayList<>();
        Long key = Long.valueOf(1211231231);
        keyValues.add(new KeyValue(key, value));
        Pair pair = new Pair("dummyCol", keyValues);
        index.addKeyValues(Collections.singletonList(pair));
        File file = getFile();
        index.serialize(new FileOutputStream(file));
        BTreeIndex readIndex = new BTreeIndex();
        readIndex.deserialize(new FileInputStream(file));
        ComparisonExpression comparisonExpression = new ComparisonExpression(ComparisonExpression.Operator.EQUAL,
                new StringLiteral("column"), new LongLiteral(key.toString()));
        assertTrue(readIndex.matches(comparisonExpression), "Key should exists");
    }

    @Test
    public void testLookup()
            throws IOException
    {
        BTreeIndex index = new BTreeIndex();
        for (int i = 0; i < 100; i++) {
            List<KeyValue> keyValues = new ArrayList<>();
            Long key = Long.valueOf(100 + i);
            String value = "value" + i;
            keyValues.add(new KeyValue(key, value));
            Pair pair = new Pair("dummyCol", keyValues);
            index.addKeyValues(Collections.singletonList(pair));
        }
        File file = getFile();
        index.serialize(new FileOutputStream(file));
        BTreeIndex readIndex = new BTreeIndex();
        readIndex.deserialize(new FileInputStream(file));
        ComparisonExpression comparisonExpression = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new StringLiteral("column"), new LongLiteral("101"));
        Iterator result = readIndex.lookUp(comparisonExpression);
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
            List<KeyValue> keyValues = new ArrayList<>();
            Long key = Long.valueOf(100 + i);
            String value = "value" + i;
            keyValues.add(new KeyValue(key, value));
            Pair pair = new Pair("dummyCol", keyValues);
            index.addKeyValues(Collections.singletonList(pair));
        }
        File file = getFile();
        index.serialize(new FileOutputStream(file));
        BTreeIndex readIndex = new BTreeIndex();
        readIndex.deserialize(new FileInputStream(file));
        BetweenPredicate betweenPredicate = new BetweenPredicate(new StringLiteral("column"), new LongLiteral("111"), new LongLiteral("114"));
        Iterator result = readIndex.lookUp(betweenPredicate);
        assertNotNull(result, "Result shouldn't be null");
        assertTrue(result.hasNext());
        for (int i = 11; i <= 14; i++) {
            assertEquals("value" + i, result.next().toString());
        }
        assertFalse(result.hasNext());
        index.close();
    }

    @Test
    public void testGreaterThan()
            throws IOException
    {
        BTreeIndex index = new BTreeIndex();
        for (int i = 0; i < 25; i++) {
            List<KeyValue> keyValues = new ArrayList<>();
            Long key = Long.valueOf(100 + i);
            String value = "value" + i;
            keyValues.add(new KeyValue(key, value));
            Pair pair = new Pair("dummyCol", keyValues);
            index.addKeyValues(Collections.singletonList(pair));
        }
        File file = getFile();
        index.serialize(new FileOutputStream(file));
        BTreeIndex readIndex = new BTreeIndex();
        readIndex.deserialize(new FileInputStream(file));
        ComparisonExpression comparisonExpression = new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN, new SymbolReference("dummyCol"), new LongLiteral("120"));
        Iterator result = readIndex.lookUp(comparisonExpression);
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
            List<KeyValue> keyValues = new ArrayList<>();
            Long key = Long.valueOf(100 + i);
            String value = "value" + i;
            keyValues.add(new KeyValue(key, value));
            Pair pair = new Pair("dummyCol", keyValues);
            index.addKeyValues(Collections.singletonList(pair));
        }
        File file = getFile();
        index.serialize(new FileOutputStream(file));
        BTreeIndex readIndex = new BTreeIndex();
        readIndex.deserialize(new FileInputStream(file));
        ComparisonExpression comparisonExpression = new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, new SymbolReference("dummyCol"), new LongLiteral("120"));
        Iterator result = readIndex.lookUp(comparisonExpression);
        assertNotNull(result, "Result shouldn't be null");
        System.out.println(result.hasNext());
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
            List<KeyValue> keyValues = new ArrayList<>();
            Long key = Long.valueOf(100 + i);
            String value = "value" + i;
            keyValues.add(new KeyValue(key, value));
            Pair pair = new Pair("dummyCol", keyValues);
            index.addKeyValues(Collections.singletonList(pair));
        }
        File file = getFile();
        index.serialize(new FileOutputStream(file));
        BTreeIndex readIndex = new BTreeIndex();
        readIndex.deserialize(new FileInputStream(file));
        ComparisonExpression comparisonExpression = new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN, new SymbolReference("dummyCol"), new LongLiteral("120"));
        Iterator result = readIndex.lookUp(comparisonExpression);
        assertNotNull(result, "Result shouldn't be null");
        assertTrue(result.hasNext());
        for (int i = 0; i < 20; i++) {
            assertEquals("value" + i, result.next().toString());
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
            List<KeyValue> keyValues = new ArrayList<>();
            Long key = Long.valueOf(100 + i);
            String value = "value" + i;
            keyValues.add(new KeyValue(key, value));
            Pair pair = new Pair("dummyCol", keyValues);
            index.addKeyValues(Collections.singletonList(pair));
        }
        File file = getFile();
        index.serialize(new FileOutputStream(file));
        BTreeIndex readIndex = new BTreeIndex();
        readIndex.deserialize(new FileInputStream(file));
        ComparisonExpression comparisonExpression = new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, new SymbolReference("dummyCol"), new LongLiteral("120"));
        Iterator result = readIndex.lookUp(comparisonExpression);
        assertNotNull(result, "Result shouldn't be null");
        assertTrue(result.hasNext());
        for (int i = 0; i <= 20; i++) {
            assertEquals("value" + i, result.next().toString());
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
            List<KeyValue> keyValues = new ArrayList<>();
            Long key = Long.valueOf(100 + i);
            keyValues.add(new KeyValue(key, value));
            Pair pair = new Pair("dummyCol", keyValues);
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
        String value = "001:3,002:3,003:3,004:3,005:3,006:3,007:3,008:3,009:3,002:3,010:3,002:3,011:3,012:3,101:3,102:3,103:3,104:3,105:3,106:3,107:3,108:3,109:3,102:3,110:3,102:3,111:3,112:3";
        for (int i = 0; i < 1000; i++) {
            List<KeyValue> keyValues = new ArrayList<>();
            Long key = Long.valueOf(100 + i);
            keyValues.add(new KeyValue(key, value));
            Pair pair = new Pair("dummyCol", keyValues);
            index.addKeyValues(Collections.singletonList(pair));
        }
        File file = File.createTempFile("test-serialize-", UUID.randomUUID().toString());
        index.serialize(new FileOutputStream(file));

        Index readindex = new BTreeIndex();
        readindex.deserialize(new FileInputStream(file));
        ComparisonExpression comparisonExpression = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new StringLiteral("column"), new LongLiteral("101"));

        Iterator result = readindex.lookUp(comparisonExpression);
        assertNotNull(result, "Result shouldn't be null");
        assertTrue(result.hasNext());
        assertEquals(value, result.next().toString());
        index.close();
    }

    private File getFile()
            throws IOException
    {
        return File.createTempFile("Btreetest-", String.valueOf(System.nanoTime()));
    }
}
