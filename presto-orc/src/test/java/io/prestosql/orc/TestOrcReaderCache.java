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

/*
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
package io.prestosql.orc;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.orc.metadata.CompressionKind;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Writable;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.orc.OrcReader.MAX_BATCH_SIZE;
import static io.prestosql.orc.OrcTester.Format.ORC_12;
import static io.prestosql.orc.OrcTester.createCustomOrcRecordReader;
import static io.prestosql.orc.OrcTester.createOrcRecordWriter;
import static io.prestosql.orc.OrcTester.createSettableStructObjectInspector;
import static io.prestosql.orc.TestOrcReaderPositions.flushWriter;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestOrcReaderCache
{
    @DataProvider(name = "orcCacheProvider")
    public Object[][] orcCacheProvider()
    {
        return new Object[][]{
                {new OrcCacheProperties(false, false, false, false, false), newOrcCacheStore()},
                {new OrcCacheProperties(true, true, true, true, false), newOrcCacheStore()},
                {new OrcCacheProperties(true, false, false, false, false), newOrcCacheStore()},
                {new OrcCacheProperties(true, true, true, true, true), newOrcCacheStore()},
        };
    }

    private OrcCacheStore newOrcCacheStore()
    {
        return OrcCacheStore.builder().newCacheStore(100, Duration.ofMinutes(10),
                100, Duration.ofMinutes(10),
                100, Duration.ofMinutes(10),
                100, Duration.ofMinutes(10),
                new DataSize(100, MEGABYTE), Duration.ofMinutes(10),
                false);
    }

    @Test(dataProvider = "orcCacheProvider")
    public void testCacheStoreUtilised(OrcCacheProperties orcCacheProperties, OrcCacheStore orcCacheStore)
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            createMultiStripeFile(tempFile.getFile());

            //run 0 populates the cache and run 1 uses the cache
            for (int run = 0; run < 2; run++) {
                try (OrcRecordReader reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, ImmutableList.of("test"),
                        ImmutableList.of(BIGINT), MAX_BATCH_SIZE, orcCacheStore, orcCacheProperties)) {
                    assertEquals(reader.getReaderRowCount(), 40000);
                    assertEquals(reader.getReaderPosition(), 0);
                    assertEquals(reader.getFileRowCount(), reader.getReaderRowCount());
                    assertEquals(reader.getFilePosition(), reader.getReaderPosition());

                    if (run == 0) {
                        assertEquals(orcCacheStore.getStripeFooterCache().size(), 0);
                        assertEquals(orcCacheStore.getRowDataCache().size(), 0);
                        assertEquals(orcCacheStore.getRowIndexCache().asMap().values().stream().mapToInt(
                                Collection::size).sum(), 0);
                        assertEquals(orcCacheStore.getBloomFiltersCache().asMap().values().stream().mapToInt(
                                Collection::size).sum(), 0);
                    }
                    else {
                        assertEquals(orcCacheStore.getStripeFooterCache().size(),
                                (orcCacheProperties.isStripeFooterCacheEnabled() ? 2 : 0));
                        assertEquals(orcCacheStore.getRowDataCache().size(),
                                (orcCacheProperties.isRowDataCacheEnabled() ? 5 : 0));
                        assertEquals(orcCacheStore.getRowIndexCache().asMap().values().stream().mapToInt(
                                Collection::size).sum(), (orcCacheProperties.isRowIndexCacheEnabled() ? 5 : 0));
                        assertEquals(orcCacheStore.getBloomFiltersCache().asMap().values().stream().mapToInt(
                                Collection::size).sum(), (orcCacheProperties.isBloomFilterCacheEnabled() ? 5 : 0));
                    }

                    long position = 0;
                    while (true) {
                        Page page = reader.nextPage();
                        if (page == null) {
                            break;
                        }
                        page = page.getLoadedPage();

                        Block block = page.getBlock(0);
                        for (int i = 0; i < block.getPositionCount(); i++) {
                            assertEquals(BIGINT.getLong(block, i), position + i);
                        }

                        assertEquals(reader.getFilePosition(), position);
                        assertEquals(reader.getReaderPosition(), position);
                        position += page.getPositionCount();
                    }

                    assertNull(reader.nextPage());
                    assertEquals(reader.getReaderPosition(), 40000);
                    assertEquals(reader.getFilePosition(), reader.getReaderPosition());

                    assertEquals(orcCacheStore.getStripeFooterCache().size(),
                            (orcCacheProperties.isStripeFooterCacheEnabled() ? 2 : 0));
                    assertEquals(orcCacheStore.getRowDataCache().size(),
                            (orcCacheProperties.isRowDataCacheEnabled() ? 5 : 0));
                    assertEquals(
                            orcCacheStore.getRowIndexCache().asMap().values().stream().mapToInt(Collection::size).sum(),
                            (orcCacheProperties.isRowIndexCacheEnabled() ? 5 : 0));
                    assertEquals(orcCacheStore.getBloomFiltersCache().asMap().values().stream().mapToInt(
                            Collection::size).sum(), (orcCacheProperties.isBloomFilterCacheEnabled() ? 5 : 0));
                }
            }
        }
    }

    // write 2 stripes. 1st stripe with 3 row groups and 2nd stripe with 2 row groups: (((0,..9999),(10000,..,19999),(20000,..,24999)), ((25000,..,34999),(35000,39999))
    private static void createMultiStripeFile(File file)
            throws IOException, ReflectiveOperationException, SerDeException
    {
        RecordWriter writer = createOrcRecordWriter(file, ORC_12, CompressionKind.SNAPPY, BIGINT);

        Serializer serde = new OrcSerde();
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", BIGINT);
        Object row = objectInspector.create();
        StructField field = objectInspector.getAllStructFieldRefs().get(0);

        for (int i = 0; i < 40000; i += 1) {
            if (i == 25000) {
                flushWriter(writer);
            }
            objectInspector.setStructFieldData(row, field, (long) i);
            Writable record = serde.serialize(row, objectInspector);
            writer.write(record);
        }

        writer.close(false);
    }

    @Test
    public void testOrcCacheReadColumnOrderChanged()
            throws Exception
    {
        OrcCacheProperties orcCacheProperties = new OrcCacheProperties(true, true, true, true, true);
        OrcCacheStore orcCacheStore = newOrcCacheStore();

        assertEquals(orcCacheStore.getStripeFooterCache().size(), 0);
        assertEquals(orcCacheStore.getRowDataCache().size(), 0);
        assertEquals(orcCacheStore.getRowIndexCache().asMap().values().stream().mapToInt(
                Collection::size).sum(), 0);
        assertEquals(orcCacheStore.getBloomFiltersCache().asMap().values().stream().mapToInt(
                Collection::size).sum(), 0);

        try (TempFile tempFile = new TempFile()) {
            createFileWithMultipleColumns(tempFile.getFile());

            List<String> names;
            List<Type> types;

            //read only name column
            names = ImmutableList.of("name");
            types = ImmutableList.of(VARCHAR);
            assertRead(tempFile, names, types, orcCacheStore, orcCacheProperties);

            assertEquals(orcCacheStore.getStripeFooterCache().size(), 1);
            assertEquals(orcCacheStore.getRowDataCache().size(), 1);
            assertEquals(orcCacheStore.getRowIndexCache().asMap().values().stream().mapToInt(
                    Collection::size).sum(), 1);
            assertEquals(orcCacheStore.getBloomFiltersCache().asMap().values().stream().mapToInt(
                    Collection::size).sum(), 0);

            //read only id column
            names = ImmutableList.of("id");
            types = ImmutableList.of(BIGINT);
            assertRead(tempFile, names, types, orcCacheStore, orcCacheProperties);

            assertEquals(orcCacheStore.getStripeFooterCache().size(), 1);
            assertEquals(orcCacheStore.getRowDataCache().size(), 2);
            assertEquals(orcCacheStore.getRowIndexCache().asMap().values().stream().mapToInt(
                    Collection::size).sum(), 2);
            assertEquals(orcCacheStore.getBloomFiltersCache().asMap().values().stream().mapToInt(
                    Collection::size).sum(), 0);

            //read gain, name, id columns
            names = ImmutableList.of("gain", "name", "id");
            types = ImmutableList.of(INTEGER, VARCHAR, BIGINT);
            assertRead(tempFile, names, types, orcCacheStore, orcCacheProperties);

            assertEquals(orcCacheStore.getStripeFooterCache().size(), 1);
            assertEquals(orcCacheStore.getRowDataCache().size(), 3);
            assertEquals(orcCacheStore.getRowIndexCache().asMap().values().stream().mapToInt(
                    Collection::size).sum(), 3);
            assertEquals(orcCacheStore.getBloomFiltersCache().asMap().values().stream().mapToInt(
                    Collection::size).sum(), 0);

            //read id, name, gain, loss columns
            names = ImmutableList.of("loss", "id", "gain", "name");
            types = ImmutableList.of(INTEGER, BIGINT, INTEGER, VARCHAR);
            assertRead(tempFile, names, types, orcCacheStore, orcCacheProperties);

            assertEquals(orcCacheStore.getStripeFooterCache().size(), 1);
            assertEquals(orcCacheStore.getRowDataCache().size(), 4);
            assertEquals(orcCacheStore.getRowIndexCache().asMap().values().stream().mapToInt(
                    Collection::size).sum(), 4);
            assertEquals(orcCacheStore.getBloomFiltersCache().asMap().values().stream().mapToInt(
                    Collection::size).sum(), 0);
        }
    }

    private static void assertRead(TempFile tempFile, List<String> names, List<Type> types, OrcCacheStore orcCacheStore, OrcCacheProperties orcCacheProperties) throws IOException
    {
        try (OrcRecordReader reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, names, types, MAX_BATCH_SIZE, orcCacheStore, orcCacheProperties)) {
            long position = 0;
            while (true) {
                Page page = reader.nextPage();
                if (page == null) {
                    break;
                }
                page = page.getLoadedPage();

                for (int idx = 0; idx < names.size(); idx++) {
                    Block block = page.getBlock(idx);
                    switch (names.get(idx)) {
                        case "id" :
                            for (int i = 0; i < block.getPositionCount(); i++) {
                                assertEquals(BIGINT.getLong(block, i), position + i);
                            }
                            break;
                        case "gain" :
                            for (int i = 0; i < block.getPositionCount(); i++) {
                                assertEquals(INTEGER.getLong(block, i), (position + i + 10));
                            }
                            break;
                        case "loss" :
                            for (int i = 0; i < block.getPositionCount(); i++) {
                                assertEquals(INTEGER.getLong(block, i), -(position + i + 10));
                            }
                            break;
                        case "name" :
                            for (int i = 0; i < block.getPositionCount(); i++) {
                                assertEquals(VARCHAR.getSlice(block, i).toStringUtf8(), "val" + (position + i));
                            }
                            break;
                    }
                }
                position += page.getPositionCount();
            }
            assertEquals(position, 7000);
        }
    }

    private static void createFileWithMultipleColumns(File file)
            throws IOException, ReflectiveOperationException, SerDeException
    {
        List<String> names = ImmutableList.of("id", "name", "loss", "gain");
        List<Type> types = ImmutableList.of(BIGINT, VARCHAR, INTEGER, INTEGER);

        RecordWriter writer = createOrcRecordWriter(file, ORC_12, CompressionKind.SNAPPY, names, ImmutableList.of(), types);

        Serializer serde = new OrcSerde();
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector(names, types);
        Object row = objectInspector.create();

        StructField[] fields = objectInspector.getAllStructFieldRefs().toArray(new StructField[0]);
        for (int i = 0; i < 7000; i++) {
            objectInspector.setStructFieldData(row, fields[0], (long) i);
            objectInspector.setStructFieldData(row, fields[1], "val" + i);
            objectInspector.setStructFieldData(row, fields[2], -(i + 10));
            objectInspector.setStructFieldData(row, fields[3], (i + 10));
            Writable record = serde.serialize(row, objectInspector);
            writer.write(record);
        }
        flushWriter(writer);
        writer.close(false);
    }
}
