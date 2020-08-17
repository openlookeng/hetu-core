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

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.orc.OrcReader.MAX_BATCH_SIZE;
import static io.prestosql.orc.OrcTester.Format.ORC_12;
import static io.prestosql.orc.OrcTester.createCustomOrcRecordReader;
import static io.prestosql.orc.OrcTester.createOrcRecordWriter;
import static io.prestosql.orc.OrcTester.createSettableStructObjectInspector;
import static io.prestosql.orc.TestOrcReaderPositions.flushWriter;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestOrcCacheStatsLister
{
    @DataProvider(name = "orcCacheProvider")
    public Object[][] orcCacheProvider()
    {
        return new Object[][] {
                {new OrcCacheProperties(false, false, false, false, false), newOrcCacheStore()},
                {new OrcCacheProperties(true, true, true, true, false), newOrcCacheStore()},
                {new OrcCacheProperties(true, false, false, false, false), newOrcCacheStore()},
                {new OrcCacheProperties(true, true, true, true, true), newOrcCacheStore()},
        };
    }

    @DataProvider(name = "smallOrcCacheProvider")
    public Object[][] smallOrcCacheProvider()
    {
        return new Object[][] {
                {new OrcCacheProperties(false, false, false, false, false), newSmallOrcCacheStore()},
                {new OrcCacheProperties(true, true, true, true, false), newSmallOrcCacheStore()},
                {new OrcCacheProperties(true, false, false, false, false), newSmallOrcCacheStore()},
                {new OrcCacheProperties(true, true, true, true, true), newSmallOrcCacheStore()},
        };
    }

    private OrcCacheStore newOrcCacheStore()
    {
        return OrcCacheStore.builder().newCacheStore(100, Duration.ofMinutes(10),
                100, Duration.ofMinutes(10),
                100, Duration.ofMinutes(10),
                100, Duration.ofMinutes(10),
                new DataSize(100, MEGABYTE), Duration.ofMinutes(10),
                true);
    }

    private OrcCacheStore newSmallOrcCacheStore()
    {
        return OrcCacheStore.builder().newCacheStore(1, Duration.ofMinutes(10),
                1, Duration.ofMinutes(10),
                1, Duration.ofMinutes(10),
                1, Duration.ofMinutes(10),
                new DataSize(0, MEGABYTE), Duration.ofMinutes(10),
                true);
    }

    @Test(dataProvider = "orcCacheProvider")
    public void testHitCount(OrcCacheProperties orcCacheProperties, OrcCacheStore orcCacheStore)
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            createMultiStripeFile(tempFile.getFile());
            OrcCacheStatsListerWrapper orcCacheStatsListerWrapper = new OrcCacheStatsListerWrapper(orcCacheStore);
            for (int run = 0; run < 3; run++) {
                try (OrcRecordReader reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, ImmutableList.of("test"),
                        ImmutableList.of(BIGINT), MAX_BATCH_SIZE, orcCacheStore, orcCacheProperties)) {
                    while (true) {
                        Page page = reader.nextPage();
                        if (page == null) {
                            break;
                        }
                    }
                    if (run == 0) {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getHitCount(), 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getHitCount(), 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getHitCount(), 0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getHitCount(), 0);
                    }
                    else if (run == 1) {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getHitCount(),
                                orcCacheProperties.isBloomFilterCacheEnabled() ? 2 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getHitCount(),
                                orcCacheProperties.isRowDataCacheEnabled() ? 5 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getHitCount(),
                                orcCacheProperties.isRowIndexCacheEnabled() ? 2 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getHitCount(),
                                orcCacheProperties.isStripeFooterCacheEnabled() ? 2 : 0);
                    }
                    else {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getHitCount(),
                                orcCacheProperties.isBloomFilterCacheEnabled() ? 4 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getHitCount(),
                                orcCacheProperties.isRowDataCacheEnabled() ? 10 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getHitCount(),
                                orcCacheProperties.isRowIndexCacheEnabled() ? 4 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getHitCount(),
                                orcCacheProperties.isStripeFooterCacheEnabled() ? 4 : 0);
                    }
                }
            }
        }
    }

    @Test(dataProvider = "orcCacheProvider")
    public void testMissCount(OrcCacheProperties orcCacheProperties, OrcCacheStore orcCacheStore)
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            createMultiStripeFile(tempFile.getFile());
            OrcCacheStatsListerWrapper orcCacheStatsListerWrapper = new OrcCacheStatsListerWrapper(orcCacheStore);
            for (int run = 0; run < 3; run++) {
                try (OrcRecordReader reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, ImmutableList.of("test"),
                        ImmutableList.of(BIGINT), MAX_BATCH_SIZE, orcCacheStore, orcCacheProperties)) {
                    if (run == 0) {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getMissCount(), 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getMissCount(), 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getMissCount(), 0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getMissCount(), 0);
                    }
                    else {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getMissCount(),
                                orcCacheProperties.isBloomFilterCacheEnabled() ? 2 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getMissCount(),
                                orcCacheProperties.isRowDataCacheEnabled() ? 5 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getMissCount(),
                                orcCacheProperties.isRowIndexCacheEnabled() ? 2 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getMissCount(),
                                orcCacheProperties.isStripeFooterCacheEnabled() ? 2 : 0);
                    }
                    while (true) {
                        Page page = reader.nextPage();
                        if (page == null) {
                            break;
                        }
                    }
                }
            }
        }
    }

    @Test(dataProvider = "orcCacheProvider")
    public void testLoadSuccessCount(OrcCacheProperties orcCacheProperties, OrcCacheStore orcCacheStore)
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            createMultiStripeFile(tempFile.getFile());
            OrcCacheStatsListerWrapper orcCacheStatsListerWrapper = new OrcCacheStatsListerWrapper(orcCacheStore);
            for (int run = 0; run < 3; run++) {
                try (OrcRecordReader reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, ImmutableList.of("test"),
                        ImmutableList.of(BIGINT), MAX_BATCH_SIZE, orcCacheStore, orcCacheProperties)) {
                    if (run == 0) {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getLoadSuccessCount(), 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getLoadSuccessCount(), 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getLoadSuccessCount(), 0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getLoadSuccessCount(), 0);
                    }
                    else {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getLoadSuccessCount(),
                                orcCacheProperties.isBloomFilterCacheEnabled() ? 2 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getLoadSuccessCount(),
                                orcCacheProperties.isRowDataCacheEnabled() ? 5 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getLoadSuccessCount(),
                                orcCacheProperties.isRowIndexCacheEnabled() ? 2 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getLoadSuccessCount(),
                                orcCacheProperties.isStripeFooterCacheEnabled() ? 2 : 0);
                    }
                    while (true) {
                        Page page = reader.nextPage();
                        if (page == null) {
                            break;
                        }
                    }
                }
            }
        }
    }

    @Test(dataProvider = "orcCacheProvider")
    public void testLoadExceptionCount(OrcCacheProperties orcCacheProperties, OrcCacheStore orcCacheStore)
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            createMultiStripeFile(tempFile.getFile());
            OrcCacheStatsListerWrapper orcCacheStatsListerWrapper = new OrcCacheStatsListerWrapper(orcCacheStore);
            for (int run = 0; run < 3; run++) {
                try (OrcRecordReader reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, ImmutableList.of("test"),
                        ImmutableList.of(BIGINT), MAX_BATCH_SIZE, orcCacheStore, orcCacheProperties)) {
                    assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getLoadExceptionCount(), 0);
                    assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getLoadExceptionCount(), 0);
                    assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getLoadExceptionCount(), 0);
                    assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getLoadExceptionCount(), 0);
                    while (true) {
                        Page page = reader.nextPage();
                        if (page == null) {
                            break;
                        }
                    }
                }
            }
        }
    }

    @Test(dataProvider = "orcCacheProvider")
    public void testTotalTime(OrcCacheProperties orcCacheProperties, OrcCacheStore orcCacheStore)
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            createMultiStripeFile(tempFile.getFile());
            OrcCacheStatsListerWrapper orcCacheStatsListerWrapper = new OrcCacheStatsListerWrapper(orcCacheStore);

            long totalTimeForBloomFilterCacheOnRun1;
            long totalTimeForRowDataCacheOnRun1;
            long totalTimeForRowIndexCacheOnRun1;
            long totalTimeForStripeFooterOnRun1;

            long totalTimeForBloomFilterCacheOnRun2;
            long totalTimeForRowDataCacheOnRun2;
            long totalTimeForRowIndexCacheOnRun2;
            long totalTimeForStripeFooterOnRun2;

            try (OrcRecordReader reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, ImmutableList.of("test"),
                    ImmutableList.of(BIGINT), MAX_BATCH_SIZE, orcCacheStore, orcCacheProperties)) {
                assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getTotalLoadTime(), 0);
                assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getTotalLoadTime(), 0);
                assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getTotalLoadTime(), 0);
                assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getTotalLoadTime(), 0);

                reader.nextPage();

                totalTimeForBloomFilterCacheOnRun1 = orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getTotalLoadTime();
                if (orcCacheProperties.isBloomFilterCacheEnabled()) {
                    assertTrue(totalTimeForBloomFilterCacheOnRun1 > 0);
                }
                else {
                    assertEquals(totalTimeForBloomFilterCacheOnRun1, 0);
                }
                totalTimeForRowDataCacheOnRun1 = orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getTotalLoadTime();
                if (orcCacheProperties.isRowDataCacheEnabled()) {
                    assertTrue(totalTimeForRowDataCacheOnRun1 > 0);
                }
                else {
                    assertEquals(totalTimeForRowDataCacheOnRun1, 0);
                }
                totalTimeForRowIndexCacheOnRun1 = orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getTotalLoadTime();
                if (orcCacheProperties.isRowIndexCacheEnabled()) {
                    assertTrue(totalTimeForRowIndexCacheOnRun1 > 0);
                }
                else {
                    assertEquals(totalTimeForRowIndexCacheOnRun1, 0);
                }
                totalTimeForStripeFooterOnRun1 = orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getTotalLoadTime();
                if (orcCacheProperties.isStripeFooterCacheEnabled()) {
                    assertTrue(totalTimeForStripeFooterOnRun1 > 0);
                }
                else {
                    assertEquals(totalTimeForStripeFooterOnRun1, 0);
                }

                reader.nextPage();

                totalTimeForBloomFilterCacheOnRun2 = orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getTotalLoadTime();
                totalTimeForRowDataCacheOnRun2 = orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getTotalLoadTime();
                totalTimeForRowIndexCacheOnRun2 = orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getTotalLoadTime();
                totalTimeForStripeFooterOnRun2 = orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getTotalLoadTime();

                assertEquals(totalTimeForBloomFilterCacheOnRun1, totalTimeForBloomFilterCacheOnRun2);
                assertEquals(totalTimeForRowDataCacheOnRun1, totalTimeForRowDataCacheOnRun2);
                assertEquals(totalTimeForRowIndexCacheOnRun1, totalTimeForRowIndexCacheOnRun2);
                assertEquals(totalTimeForStripeFooterOnRun1, totalTimeForStripeFooterOnRun2);
            }
        }
    }

    @Test(dataProvider = "smallOrcCacheProvider")
    public void testEvictionCount(OrcCacheProperties orcCacheProperties, OrcCacheStore orcCacheStore)
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            createMultiStripeFile(tempFile.getFile());
            OrcCacheStatsListerWrapper orcCacheStatsListerWrapper = new OrcCacheStatsListerWrapper(orcCacheStore);
            for (int run = 0; run < 2; run++) {
                try (OrcRecordReader reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, ImmutableList.of("test"),
                        ImmutableList.of(BIGINT), MAX_BATCH_SIZE, orcCacheStore, orcCacheProperties)) {
                    if (run == 0) {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getEvictionCount(), 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getEvictionCount(), 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getEvictionCount(), 0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getEvictionCount(), 0);
                    }
                    else {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getLoadCount(),
                                orcCacheProperties.isBloomFilterCacheEnabled() ? 2 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getLoadCount(),
                                orcCacheProperties.isRowDataCacheEnabled() ? 5 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getLoadCount(),
                                orcCacheProperties.isRowIndexCacheEnabled() ? 2 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getLoadCount(),
                                orcCacheProperties.isStripeFooterCacheEnabled() ? 2 : 0);

                        assertEquals(orcCacheStore.getBloomFiltersCache().size(),
                                orcCacheProperties.isBloomFilterCacheEnabled() ? 1 : 0);
                        assertEquals(orcCacheStore.getRowDataCache().size(),
                                orcCacheProperties.isRowDataCacheEnabled() ? 0 : 0);
                        assertEquals(orcCacheStore.getRowIndexCache().size(),
                                orcCacheProperties.isRowIndexCacheEnabled() ? 1 : 0);
                        assertEquals(orcCacheStore.getStripeFooterCache().size(),
                                orcCacheProperties.isStripeFooterCacheEnabled() ? 1 : 0);

                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getEvictionCount(),
                                orcCacheProperties.isBloomFilterCacheEnabled() ? 1 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getEvictionCount(),
                                orcCacheProperties.isRowDataCacheEnabled() ? 5 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getEvictionCount(),
                                orcCacheProperties.isRowIndexCacheEnabled() ? 1 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getEvictionCount(),
                                orcCacheProperties.isStripeFooterCacheEnabled() ? 1 : 0);
                    }
                    while (true) {
                        Page page = reader.nextPage();
                        if (page == null) {
                            break;
                        }
                    }
                }
            }
        }
    }

    @Test(dataProvider = "orcCacheProvider")
    public void testRequestCount(OrcCacheProperties orcCacheProperties, OrcCacheStore orcCacheStore)
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            createMultiStripeFile(tempFile.getFile());
            OrcCacheStatsListerWrapper orcCacheStatsListerWrapper = new OrcCacheStatsListerWrapper(orcCacheStore);
            for (int run = 0; run < 3; run++) {
                try (OrcRecordReader reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, ImmutableList.of("test"),
                        ImmutableList.of(BIGINT), MAX_BATCH_SIZE, orcCacheStore, orcCacheProperties)) {
                    if (run == 0) {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getRequestCount(), 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getRequestCount(), 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getRequestCount(), 0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getRequestCount(), 0);
                    }
                    else if (run == 1) {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getRequestCount(),
                                orcCacheProperties.isBloomFilterCacheEnabled() ? 2 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getRequestCount(),
                                orcCacheProperties.isRowDataCacheEnabled() ? 5 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getRequestCount(),
                                orcCacheProperties.isRowIndexCacheEnabled() ? 2 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getRequestCount(),
                                orcCacheProperties.isStripeFooterCacheEnabled() ? 2 : 0);
                    }
                    else {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getRequestCount(),
                                orcCacheProperties.isBloomFilterCacheEnabled() ? 4 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getRequestCount(),
                                orcCacheProperties.isRowDataCacheEnabled() ? 10 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getRequestCount(),
                                orcCacheProperties.isRowIndexCacheEnabled() ? 4 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getRequestCount(),
                                orcCacheProperties.isStripeFooterCacheEnabled() ? 4 : 0);
                    }
                    while (true) {
                        Page page = reader.nextPage();
                        if (page == null) {
                            break;
                        }
                    }
                }
            }
        }
    }

    @Test(dataProvider = "orcCacheProvider")
    public void testLoadCount(OrcCacheProperties orcCacheProperties, OrcCacheStore orcCacheStore)
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            createMultiStripeFile(tempFile.getFile());
            OrcCacheStatsListerWrapper orcCacheStatsListerWrapper = new OrcCacheStatsListerWrapper(orcCacheStore);
            for (int run = 0; run < 3; run++) {
                try (OrcRecordReader reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, ImmutableList.of("test"),
                        ImmutableList.of(BIGINT), MAX_BATCH_SIZE, orcCacheStore, orcCacheProperties)) {
                    if (run == 0) {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getLoadCount(), 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getLoadCount(), 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getLoadCount(), 0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getLoadCount(), 0);
                    }
                    else {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getLoadCount(),
                                orcCacheProperties.isBloomFilterCacheEnabled() ? 2 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getLoadCount(),
                                orcCacheProperties.isRowDataCacheEnabled() ? 5 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getLoadCount(),
                                orcCacheProperties.isRowIndexCacheEnabled() ? 2 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getLoadCount(),
                                orcCacheProperties.isStripeFooterCacheEnabled() ? 2 : 0);
                    }
                    while (true) {
                        Page page = reader.nextPage();
                        if (page == null) {
                            break;
                        }
                    }
                }
            }
        }
    }

    @Test(dataProvider = "orcCacheProvider")
    public void testHitRate(OrcCacheProperties orcCacheProperties, OrcCacheStore orcCacheStore)
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            createMultiStripeFile(tempFile.getFile());
            OrcCacheStatsListerWrapper orcCacheStatsListerWrapper = new OrcCacheStatsListerWrapper(orcCacheStore);
            for (int run = 0; run < 4; run++) {
                try (OrcRecordReader reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, ImmutableList.of("test"),
                        ImmutableList.of(BIGINT), MAX_BATCH_SIZE, orcCacheStore, orcCacheProperties)) {
                    while (true) {
                        Page page = reader.nextPage();
                        if (page == null) {
                            break;
                        }
                    }
                    if (run == 0) {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getHitRate(),
                                orcCacheProperties.isBloomFilterCacheEnabled() ? 0.0 : 1.0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getHitRate(),
                                orcCacheProperties.isRowDataCacheEnabled() ? 0.0 : 1.0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getHitRate(),
                                orcCacheProperties.isRowIndexCacheEnabled() ? 0.0 : 1.0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getHitRate(),
                                orcCacheProperties.isStripeFooterCacheEnabled() ? 0.0 : 1.0);
                    }
                    else if (run == 1) {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getHitRate(),
                                orcCacheProperties.isBloomFilterCacheEnabled() ? 0.5 : 1.0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getHitRate(),
                                orcCacheProperties.isRowDataCacheEnabled() ? 0.5 : 1.0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getHitRate(),
                                orcCacheProperties.isRowIndexCacheEnabled() ? 0.5 : 1.0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getHitRate(),
                                orcCacheProperties.isStripeFooterCacheEnabled() ? 0.5 : 1.0);
                    }
                    else if (run == 2) {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getHitRate(),
                                orcCacheProperties.isBloomFilterCacheEnabled() ? 0.6666666666666666 : 1.0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getHitRate(),
                                orcCacheProperties.isRowDataCacheEnabled() ? 0.6666666666666666 : 1.0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getHitRate(),
                                orcCacheProperties.isRowIndexCacheEnabled() ? 0.6666666666666666 : 1.0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getHitRate(),
                                orcCacheProperties.isStripeFooterCacheEnabled() ? 0.6666666666666666 : 1.0);
                    }
                }
            }
        }
    }

    @Test(dataProvider = "orcCacheProvider")
    public void testMissRate(OrcCacheProperties orcCacheProperties, OrcCacheStore orcCacheStore)
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            createMultiStripeFile(tempFile.getFile());
            OrcCacheStatsListerWrapper orcCacheStatsListerWrapper = new OrcCacheStatsListerWrapper(orcCacheStore);
            for (int run = 0; run < 4; run++) {
                try (OrcRecordReader reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, ImmutableList.of("test"),
                        ImmutableList.of(BIGINT), MAX_BATCH_SIZE, orcCacheStore, orcCacheProperties)) {
                    while (true) {
                        Page page = reader.nextPage();
                        if (page == null) {
                            break;
                        }
                    }
                    if (run == 0) {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getMissRate(),
                                orcCacheProperties.isBloomFilterCacheEnabled() ? 1.0 : 0.0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getMissRate(),
                                orcCacheProperties.isRowDataCacheEnabled() ? 1.0 : 0.0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getMissRate(),
                                orcCacheProperties.isRowIndexCacheEnabled() ? 1.0 : 0.0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getMissRate(),
                                orcCacheProperties.isStripeFooterCacheEnabled() ? 1.0 : 0.0);
                    }
                    else if (run == 1) {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getMissRate(),
                                orcCacheProperties.isBloomFilterCacheEnabled() ? 0.5 : 0.0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getMissRate(),
                                orcCacheProperties.isRowDataCacheEnabled() ? 0.5 : 0.0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getMissRate(),
                                orcCacheProperties.isRowIndexCacheEnabled() ? 0.5 : 0.0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getMissRate(),
                                orcCacheProperties.isStripeFooterCacheEnabled() ? 0.5 : 0.0);
                    }
                    else if (run == 2) {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getMissRate(),
                                orcCacheProperties.isBloomFilterCacheEnabled() ? 0.3333333333333333 : 0.0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getMissRate(),
                                orcCacheProperties.isRowDataCacheEnabled() ? 0.3333333333333333 : 0.0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getMissRate(),
                                orcCacheProperties.isRowIndexCacheEnabled() ? 0.3333333333333333 : 0.0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getMissRate(),
                                orcCacheProperties.isStripeFooterCacheEnabled() ? 0.3333333333333333 : 0.0);
                    }
                }
            }
        }
    }

    @Test(dataProvider = "orcCacheProvider")
    public void testAverageLoadPenalty(OrcCacheProperties orcCacheProperties, OrcCacheStore orcCacheStore)
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            createMultiStripeFile(tempFile.getFile());
            OrcCacheStatsListerWrapper orcCacheStatsListerWrapper = new OrcCacheStatsListerWrapper(orcCacheStore);
            for (int run = 0; run < 3; run++) {
                try (OrcRecordReader reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, ImmutableList.of("test"),
                        ImmutableList.of(BIGINT), MAX_BATCH_SIZE, orcCacheStore, orcCacheProperties)) {
                    if (run == 0) {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getAverageLoadPenalty(), 0.0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getAverageLoadPenalty(), 0.0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getAverageLoadPenalty(), 0.0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getAverageLoadPenalty(), 0.0);
                    }
                    else {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getAverageLoadPenalty(),
                                orcCacheProperties.isBloomFilterCacheEnabled() ? ((double) orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getTotalLoadTime() / orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getLoadCount()) : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getAverageLoadPenalty(),
                                orcCacheProperties.isRowDataCacheEnabled() ? ((double) orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getTotalLoadTime() / orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getLoadCount()) : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getAverageLoadPenalty(),
                                orcCacheProperties.isRowIndexCacheEnabled() ? ((double) orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getTotalLoadTime() / orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getLoadCount()) : 0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getAverageLoadPenalty(),
                                orcCacheProperties.isStripeFooterCacheEnabled() ? ((double) orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getTotalLoadTime() / orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getLoadCount()) : 0);
                    }
                    while (true) {
                        Page page = reader.nextPage();
                        if (page == null) {
                            break;
                        }
                    }
                }
            }
        }
    }

    @Test(dataProvider = "orcCacheProvider")
    public void testLoadExceptionRate(OrcCacheProperties orcCacheProperties, OrcCacheStore orcCacheStore)
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            createMultiStripeFile(tempFile.getFile());
            OrcCacheStatsListerWrapper orcCacheStatsListerWrapper = new OrcCacheStatsListerWrapper(orcCacheStore);
            for (int run = 0; run < 3; run++) {
                try (OrcRecordReader reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, ImmutableList.of("test"),
                        ImmutableList.of(BIGINT), MAX_BATCH_SIZE, orcCacheStore, orcCacheProperties)) {
                    assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getLoadExceptionRate(), 0.0);
                    assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getLoadExceptionRate(), 0.0);
                    assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getLoadExceptionRate(), 0.0);
                    assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getLoadExceptionRate(), 0.0);
                    while (true) {
                        Page page = reader.nextPage();
                        if (page == null) {
                            break;
                        }
                    }
                }
            }
        }
    }

    @Test(dataProvider = "orcCacheProvider")
    public void testSize(OrcCacheProperties orcCacheProperties, OrcCacheStore orcCacheStore)
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            createMultiStripeFile(tempFile.getFile());
            OrcCacheStatsListerWrapper orcCacheStatsListerWrapper = new OrcCacheStatsListerWrapper(orcCacheStore);
            for (int run = 0; run < 3; run++) {
                try (OrcRecordReader reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, ImmutableList.of("test"),
                        ImmutableList.of(BIGINT), MAX_BATCH_SIZE, orcCacheStore, orcCacheProperties)) {
                    if (run == 0) {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getSize(), 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getSize(), 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getSize(), 0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getSize(), 0);
                    }
                    else {
                        assertEquals(orcCacheStatsListerWrapper.getBloomFilterCacheStatsLister().getSize(),
                                orcCacheProperties.isBloomFilterCacheEnabled() ? 2 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowDataCacheStatsLister().getSize(),
                                orcCacheProperties.isRowDataCacheEnabled() ? 5 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getRowIndexCacheStatsLister().getSize(),
                                orcCacheProperties.isRowIndexCacheEnabled() ? 2 : 0);
                        assertEquals(orcCacheStatsListerWrapper.getStripeFooterCacheStatsLister().getSize(),
                                orcCacheProperties.isStripeFooterCacheEnabled() ? 2 : 0);
                    }
                    while (true) {
                        Page page = reader.nextPage();
                        if (page == null) {
                            break;
                        }
                    }
                }
            }
        }
    }

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
}
