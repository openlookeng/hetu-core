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
package io.hetu.core.heuristicindex.bitmap;

import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.Type;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.predicate.Range.equal;
import static io.prestosql.spi.predicate.Range.greaterThan;
import static io.prestosql.spi.predicate.Range.greaterThanOrEqual;
import static io.prestosql.spi.predicate.Range.lessThan;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestBitMapIndex
{
    @Test
    public void testDefault()
            throws IOException
    {
        Map<String, Type> columnTypes = new HashMap<>();

        columnTypes.put("testCol", CharType.createCharType(10000));

        BitMapIndex bitMapIndex = new BitMapIndex();

        String[] values = {"a", "y", "z", "t", "e", "f", "g"};

        bitMapIndex.addValues(values);

        File tmp = new File("tmp.bitmap");

        try (OutputStream outputStream = new FileOutputStream(tmp)) {
            bitMapIndex.persist(outputStream);
        }

        bitMapIndex.load(new FileInputStream(tmp));

        Domain predicate = Domain.create(ValueSet.ofRanges(equal(createUnboundedVarcharType(), utf8Slice("y"))), false);

        Iterator<Integer> res = bitMapIndex.getMatches(predicate);

        Set<Integer> resultSet = new HashSet<>();
        while (res.hasNext()) {
            resultSet.add(res.next());
        }
        FileUtils.deleteQuietly(tmp);
        assertTrue(resultSet.contains(1));
        assertTrue(resultSet.size() == 1);
    }

    @Test
    public void testDefault2()
            throws IOException
    {
        Map<String, Type> columnTypes = new HashMap<>();

        columnTypes.put("testCol", CharType.createCharType(10000));

        BitMapIndex bitMapIndex = new BitMapIndex();

        String[] values = {"测试", "ab", "aer", "asdasd", "%#!", ":dfs", "测试"};

        bitMapIndex.addValues(values);

        File tmp = new File("tmp.bitmap");

        try (OutputStream outputStream = new FileOutputStream(tmp)) {
            bitMapIndex.persist(outputStream);
        }

        bitMapIndex.load(new FileInputStream(tmp));

        Domain predicate = Domain.create(ValueSet.ofRanges(equal(createUnboundedVarcharType(), utf8Slice(":dfs"))), false);

        Iterator<Integer> res = bitMapIndex.getMatches(predicate);

        Set<Integer> resultSet = new HashSet<>();
        while (res.hasNext()) {
            resultSet.add(res.next());
        }
        FileUtils.deleteQuietly(tmp);
        assertTrue(resultSet.contains(5));
        assertTrue(resultSet.size() == 1);
    }

    @Test
    public void testDefaultNumbers()
            throws IOException
    {
        Map<String, Type> columnTypes = new HashMap<>();

        columnTypes.put("testCol", IntegerType.INTEGER);

        BitMapIndex bitMapIndex = new BitMapIndex();

        Object[] values = {8, 4, 4, 8, 51, null, 8, 1, 8, 2, 4, 7, 6, 1};

        bitMapIndex.addValues(values);

        File tmp = new File("tmp.bitmap");

        try (OutputStream outputStream = new FileOutputStream(tmp)) {
            bitMapIndex.persist(outputStream);
        }

        bitMapIndex.load(new FileInputStream(tmp));

        Domain predicate = Domain.create(ValueSet.ofRanges(equal(IntegerType.INTEGER, 8L)), false);

        Iterator<Integer> res = bitMapIndex.getMatches(predicate);

        Set<Integer> resultSet = new HashSet<>();
        while (res.hasNext()) {
            resultSet.add(res.next());
        }
        FileUtils.deleteQuietly(tmp);
        assertTrue(resultSet.contains(0));
        assertTrue(resultSet.contains(6));
        assertTrue(resultSet.size() == 4);
    }

    @Test
    public void testLargeValues()
            throws IOException
    {
        BitMapIndex bitMapIndex = new BitMapIndex();

        Object[] values = new Object[1000000];
        Random r = new Random();
        values[0] = 8;
        for (int i = 1; i < 1000000; i++) {
            values[i] = r.nextInt(999999999);
        }
        bitMapIndex.addValues(values);

        File tmp = new File("tmp.bitmap");

        try (OutputStream outputStream = new FileOutputStream(tmp)) {
            bitMapIndex.persist(outputStream);
        }

        bitMapIndex.load(new FileInputStream(tmp));

        Domain predicate = Domain.create(ValueSet.ofRanges(equal(IntegerType.INTEGER, 8L)), false);
        Iterator<Integer> res = bitMapIndex.getMatches(predicate);
        Set<Integer> resultSet = new HashSet<>();
        while (res.hasNext()) {
            resultSet.add(res.next());
        }
        FileUtils.deleteQuietly(tmp);
        assertTrue(resultSet.contains(0));
    }

    @Test
    public void testRange()
            throws IOException
    {
        Map<String, Type> columnTypes = new HashMap<>();

        columnTypes.put("testCol", IntegerType.INTEGER);

        BitMapIndex bitMapIndex = new BitMapIndex();

        Object[] values = {"8", "4", "4", "8", "51", "52", "8", "1", "8", "2", "4", "7", "6", "1"};

        bitMapIndex.addValues(values);

        File tmp = new File("tmp.bitmap");

        try (OutputStream outputStream = new FileOutputStream(tmp)) {
            bitMapIndex.persist(outputStream);
        }

        bitMapIndex.load(new FileInputStream(tmp));

        Domain greaterThanOrEqual = Domain.create(ValueSet.ofRanges(greaterThanOrEqual(IntegerType.INTEGER, 8L)), false);
        Iterator<Integer> res = bitMapIndex.getMatches(greaterThanOrEqual);

        Set<Integer> resultSet = new HashSet<>();
        while (res.hasNext()) {
            resultSet.add(res.next());
        }

        assertTrue(resultSet.containsAll(Arrays.asList(0, 3, 4, 5, 6, 8)));

        Domain greaterThan = Domain.create(ValueSet.ofRanges(greaterThan(IntegerType.INTEGER, 8L)), false);
        res = bitMapIndex.getMatches(greaterThan);

        resultSet = new HashSet<>();
        while (res.hasNext()) {
            resultSet.add(res.next());
        }

        assertTrue(resultSet.size() == 2);
        assertTrue(resultSet.containsAll(Arrays.asList(4, 5)));

        Domain lessThan = Domain.create(ValueSet.ofRanges(lessThan(IntegerType.INTEGER, 8L)), false);
        res = bitMapIndex.getMatches(lessThan);

        resultSet = new HashSet<>();
        while (res.hasNext()) {
            resultSet.add(res.next());
        }
        FileUtils.deleteQuietly(tmp);
        assertTrue(resultSet.size() == 8);
        assertTrue(resultSet.containsAll(Arrays.asList(1, 2, 7, 9, 10, 11, 12, 13)));
    }

    @Test
    public void floatTest() throws IOException
    {
        BitMapIndex bitMapIndexWrite = new BitMapIndex();
        Float[] floatValues = new Float[] {8f, 4f, 4f, 8f, 51f, null, 8f, 1f, 8f, 2f, 4f, 7f, 6f, 1f};
        bitMapIndexWrite.addValues(Stream.of(floatValues).map(floatValue -> {
            if (floatValue == null) {
                return floatValue;
            }
            return Float.floatToRawIntBits(floatValue);
        }).toArray());

        File tmp = File.createTempFile(getClass().getSimpleName(), "floatTest");
        tmp.deleteOnExit();

        try (OutputStream outputStream = new FileOutputStream(tmp)) {
            bitMapIndexWrite.persist(outputStream);
        }

        BitMapIndex bitMapIndexRead = new BitMapIndex();
        bitMapIndexRead.load(new FileInputStream(tmp));

        Domain eqPredicate = Domain.create(ValueSet.ofRanges(equal(RealType.REAL, (long) Float.floatToRawIntBits(51f))), false);

        Iterator<Integer> eqResult = bitMapIndexRead.getMatches(eqPredicate);

        Set<Integer> resultSet = new HashSet<>();
        while (eqResult.hasNext()) {
            resultSet.add(eqResult.next());
        }

        assertEquals(resultSet.size(), 1);
        assertTrue(resultSet.contains(4));

        Domain grtOrEqPredicate = Domain.create(ValueSet.ofRanges(greaterThanOrEqual(RealType.REAL, (long) Float.floatToRawIntBits(7f))), false);
        Iterator<Integer> grtOrEqResult = bitMapIndexRead.getMatches(grtOrEqPredicate);

        Set<Integer> grtOrEqResultSet = new HashSet<>();
        while (grtOrEqResult.hasNext()) {
            grtOrEqResultSet.add(grtOrEqResult.next());
        }

        assertEquals(grtOrEqResultSet.size(), 6);
        assertTrue(grtOrEqResultSet.containsAll(Arrays.asList(0, 3, 4, 6, 8, 11)));
    }
}
