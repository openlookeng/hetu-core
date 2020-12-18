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
package io.hetu.core.plugin.heuristicindex.index.bitmap;

import com.google.common.collect.ImmutableList;
import io.hetu.core.common.filesystem.TempFolder;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.IntegerType;
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

    private List<Integer> iteratorToList(Iterator<Integer> iterator)
    {
        List<Integer> list = new ArrayList<>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }
}
