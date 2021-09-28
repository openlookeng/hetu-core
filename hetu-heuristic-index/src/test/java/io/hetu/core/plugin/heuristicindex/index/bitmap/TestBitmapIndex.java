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
package io.hetu.core.plugin.heuristicindex.index.bitmap;

import com.google.common.collect.ImmutableList;
import io.hetu.core.common.filesystem.TempFolder;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import org.testng.annotations.DataProvider;
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
import java.util.stream.IntStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.predicate.Range.equal;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static org.testng.Assert.assertEquals;

public class TestBitmapIndex
{
    @Test
    public void testString() throws IOException
    {
        try (TempFolder folder = new TempFolder();
                BitmapIndex bitmapIndexWrite = new BitmapIndex();
                BitmapIndex bitmapIndexRead = new BitmapIndex()) {
            folder.create();
            File file = folder.newFile();

            String columnName = "column";
            List<Object> columnValues = ImmutableList.of("foo", "bar", "baz", "foo", "foo", "baz");

            bitmapIndexWrite.setExpectedNumOfEntries(columnValues.size());
            bitmapIndexWrite.addValues(Collections.singletonList(new Pair<>(columnName, columnValues)));

            try (FileOutputStream os = new FileOutputStream(file); FileInputStream is = new FileInputStream(file)) {
                bitmapIndexWrite.serialize(os);
                bitmapIndexRead.deserialize(is);
            }

            assertEquals(iteratorToList(bitmapIndexRead.lookUp(
                    Domain.create(ValueSet.ofRanges(equal(createUnboundedVarcharType(), utf8Slice("foo"))), false))),
                    ImmutableList.of(0, 3, 4));
            assertEquals(iteratorToList(bitmapIndexRead.lookUp(
                    Domain.create(ValueSet.ofRanges(equal(createUnboundedVarcharType(), utf8Slice("bar"))), false))),
                    ImmutableList.of(1));
            assertEquals(iteratorToList(bitmapIndexRead.lookUp(
                    Domain.create(ValueSet.ofRanges(equal(createUnboundedVarcharType(), utf8Slice("notfound"))), false))),
                    ImmutableList.of());
        }
    }

    @Test
    public void testInteger() throws IOException
    {
        try (TempFolder folder = new TempFolder();
                BitmapIndex bitmapIndexWrite = new BitmapIndex();
                BitmapIndex bitmapIndexRead = new BitmapIndex()) {
            folder.create();
            File file = folder.newFile();

            String columnName = "column";
            List<Object> columnValues = ImmutableList.of(3, 1024, 12345, 3, 2048, 999);

            bitmapIndexWrite.setExpectedNumOfEntries(columnValues.size());
            bitmapIndexWrite.addValues(Collections.singletonList(new Pair<>(columnName, columnValues)));

            try (FileOutputStream os = new FileOutputStream(file); FileInputStream is = new FileInputStream(file)) {
                bitmapIndexWrite.serialize(os);
                bitmapIndexRead.deserialize(is);
            }

            assertEquals(iteratorToList(bitmapIndexRead.lookUp(
                    Domain.create(ValueSet.ofRanges(equal(IntegerType.INTEGER, 3L)), false))),
                    ImmutableList.of(0, 3));
            assertEquals(iteratorToList(bitmapIndexRead.lookUp(
                    Domain.create(ValueSet.ofRanges(equal(IntegerType.INTEGER, 3L)), false))),
                    ImmutableList.of(0, 3));
            assertEquals(iteratorToList(bitmapIndexRead.lookUp(
                    Domain.create(ValueSet.ofRanges(equal(IntegerType.INTEGER, 2048L)), false))),
                    ImmutableList.of(4));
            assertEquals(iteratorToList(bitmapIndexRead.lookUp(
                    Domain.create(ValueSet.ofRanges(equal(IntegerType.INTEGER, 0L)), false))),
                    ImmutableList.of());
        }
    }

    @Test
    public void testLargeEntries() throws IOException
    {
        try (TempFolder folder = new TempFolder();
                BitmapIndex bitmapIndexWrite = new BitmapIndex();
                BitmapIndex bitmapIndexRead = new BitmapIndex()) {
            folder.create();
            File file = folder.newFile();

            String columnName = "column";
            Long[] values = new Long[10000000];
            for (int i = 0; i < values.length; i++) {
                values[i] = Long.valueOf(i);
            }
            List<Object> columnValues = Arrays.asList(values);

            bitmapIndexWrite.setExpectedNumOfEntries(columnValues.size());
            bitmapIndexWrite.addValues(Collections.singletonList(new Pair<>(columnName, columnValues)));

            try (FileOutputStream os = new FileOutputStream(file); FileInputStream is = new FileInputStream(file)) {
                bitmapIndexWrite.serialize(os);
                bitmapIndexRead.deserialize(is);
            }

            assertEquals(iteratorToList(bitmapIndexRead.lookUp(
                    Domain.create(ValueSet.ofRanges(equal(IntegerType.INTEGER, 3L)), false))),
                    ImmutableList.of(3));
            assertEquals(iteratorToList(bitmapIndexRead.lookUp(
                    Domain.create(ValueSet.ofRanges(equal(IntegerType.INTEGER, 2048L)), false))),
                    ImmutableList.of(2048));
            assertEquals(iteratorToList(bitmapIndexRead.lookUp(
                    Domain.create(ValueSet.ofRanges(equal(IntegerType.INTEGER, Long.valueOf(values.length))), false))),
                    ImmutableList.of());
        }
    }

    @Test
    public void testMultiThread() throws IOException
    {
        try (TempFolder folder = new TempFolder();
                BitmapIndex bitmapIndexWrite = new BitmapIndex();
                BitmapIndex bitmapIndexRead = new BitmapIndex()) {
            folder.create();
            File file = folder.newFile();

            String columnName = "column";
            List<Object> columnValues = ImmutableList.of(3, 1024, 12345, 3, 2048, 999);

            bitmapIndexWrite.setExpectedNumOfEntries(columnValues.size());
            bitmapIndexWrite.addValues(Collections.singletonList(new Pair<>(columnName, columnValues)));

            try (FileOutputStream os = new FileOutputStream(file); FileInputStream is = new FileInputStream(file)) {
                bitmapIndexWrite.serialize(os);
                bitmapIndexRead.deserialize(is);
            }

            IntStream.range(1, 10).parallel().forEach(i -> {
                assertEquals(iteratorToList(bitmapIndexRead.lookUp(
                        Domain.create(ValueSet.ofRanges(equal(IntegerType.INTEGER, 3L)), false))),
                        ImmutableList.of(0, 3));
                assertEquals(iteratorToList(bitmapIndexRead.lookUp(
                        Domain.create(ValueSet.ofRanges(equal(IntegerType.INTEGER, 2048L)), false))),
                        ImmutableList.of(4));
                assertEquals(iteratorToList(bitmapIndexRead.lookUp(
                        Domain.create(ValueSet.ofRanges(equal(IntegerType.INTEGER, 0L)), false))),
                        ImmutableList.of());
            });
        }
    }

    @DataProvider(name = "bitmapBetweenForDataTypes")
    public Object[][] bitmapBetweenForDataTypes()
    {
        return new Object[][] {
                {ImmutableList.of("a", "b", "c", "a", "a", "b"), createUnboundedVarcharType(), utf8Slice("a"), utf8Slice("b"), ImmutableList.of(0, 1, 3, 4, 5)},
                {ImmutableList.of("a", "b", "c", "a", "a", "b"), createUnboundedVarcharType(), utf8Slice("d"), utf8Slice("d"), ImmutableList.of()},
                {ImmutableList.of("a", "b", "c", "a", "a", "b"), CharType.createCharType(1), utf8Slice("a"), utf8Slice("b"), ImmutableList.of(0, 1, 3, 4, 5)},
                {ImmutableList.of("a", "b", "c", "a", "a", "b"), CharType.createCharType(1), utf8Slice("d"), utf8Slice("d"), ImmutableList.of()},
                {ImmutableList.of(1, 2, 3, 4, 5, 5, 5, 8, 9), IntegerType.INTEGER, 1L, 5L, ImmutableList.of(0, 1, 2, 3, 4, 5, 6)},
                {ImmutableList.of(1, 2, 3, 4, 5, 5, 5, 8, 9), IntegerType.INTEGER, 10L, 15L, ImmutableList.of()},
                {ImmutableList.of(1, 2, 3, 4, 5, 5, 5, 8, 9), BigintType.BIGINT, 1L, 5L, ImmutableList.of(0, 1, 2, 3, 4, 5, 6)},
                {ImmutableList.of(1, 2, 3, 4, 5, 5, 5, 8, 9), BigintType.BIGINT, 10L, 15L, ImmutableList.of()},
                {ImmutableList.of(1, 2, 3, 4, 5, 5, 5, 8, 9), TinyintType.TINYINT, 1L, 5L, ImmutableList.of(0, 1, 2, 3, 4, 5, 6)},
                {ImmutableList.of(1, 2, 3, 4, 5, 5, 5, 8, 9), TinyintType.TINYINT, 10L, 15L, ImmutableList.of()},
                {ImmutableList.of(1, 2, 3, 4, 5, 5, 5, 8, 9), SmallintType.SMALLINT, 1L, 5L, ImmutableList.of(0, 1, 2, 3, 4, 5, 6)},
                {ImmutableList.of(1, 2, 3, 4, 5, 5, 5, 8, 9), SmallintType.SMALLINT, 10L, 15L, ImmutableList.of()},
                {ImmutableList.of(1.1d, 2.2d, 3.3d, 4.4d, 5.5d, 5.5d, 5.5d, 8.8d, 9.9d), DoubleType.DOUBLE, 1.1d, 5.5d, ImmutableList.of(0, 1, 2, 3, 4, 5, 6)},
                {ImmutableList.of(1.1d, 2.2d, 3.3d, 4.4d, 5.5d, 5.5d, 5.5d, 8.8d, 9.9d), DoubleType.DOUBLE, 1.2d, 1.3d, ImmutableList.of()},
                {ImmutableList.of(18611L, 18612L, 18613L, 18611L), DateType.DATE, 18611L, 18612L, ImmutableList.of(0, 1, 3)},
                {ImmutableList.of(18611L, 18612L, 18613L, 18611L), DateType.DATE, 18609L, 18610L, ImmutableList.of()}
        };
    }

    @Test(dataProvider = "bitmapBetweenForDataTypes")
    public void testBitmapBetweenForDataTypes(List<Object> columnValues, Type type, Object matchVal1, Object matchVal2, List<Object> matchedList)
            throws IOException
    {
        System.out.println("Testing for " + columnValues + " for type " + type.getDisplayName() + " from " + matchVal1.toString() + " to " + matchVal2.toString());
        try (TempFolder folder = new TempFolder();
                BitmapIndex bitmapIndexWrite = new BitmapIndex();
                BitmapIndex bitmapIndexRead = new BitmapIndex()) {
            folder.create();
            File file = folder.newFile();

            String columnName = "column";

            bitmapIndexWrite.setExpectedNumOfEntries(columnValues.size());
            bitmapIndexWrite.addValues(Collections.singletonList(new Pair<>(columnName, columnValues)));

            try (FileOutputStream os = new FileOutputStream(file); FileInputStream is = new FileInputStream(file)) {
                bitmapIndexWrite.serialize(os);
                bitmapIndexRead.deserialize(is);
            }

            assertEquals(iteratorToList(bitmapIndexRead.lookUp(Domain.create(ValueSet.ofRanges(Range.range(type, matchVal1, true, matchVal2, true)), false))), matchedList);
        }
    }

    private List<Integer> iteratorToList(Iterator<Integer> iterator)
    {
        List<Integer> list = new ArrayList<>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }
}
