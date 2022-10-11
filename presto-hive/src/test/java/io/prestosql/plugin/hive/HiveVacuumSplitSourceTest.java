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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.RealType.REAL;
import static org.mockito.MockitoAnnotations.initMocks;

public class HiveVacuumSplitSourceTest
{
    @Mock
    private HiveSplitSource mockSplitSource;
    @Mock
    private HiveVacuumTableHandle mockVacuumTableHandle;
    @Mock
    private HdfsEnvironment mockHdfsEnvironment;
    @Mock
    private ConnectorSession mockSession;

    private HiveVacuumSplitSource hiveVacuumSplitSourceUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        ColumnMetadata ptdMetadata = new ColumnMetadata("pt_d", DATE);
        Set<TupleDomain<ColumnMetadata>> cachePredicates = ImmutableSet.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(REAL, (long) Float.floatToRawIntBits(1.0f)))),
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(REAL, (long) Float.floatToRawIntBits(1000.10f)))));
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                new VacuumCleanerTest.ConnectorSession(),
                "database",
                "table",
                10,
                10000,
                new DataSize(10, MEGABYTE),
                Integer.MAX_VALUE,
                null,
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                cachePredicates, null,
                new HiveConfig(),
                HiveStorageFormat.ORC);
        hiveVacuumSplitSourceUnderTest = new HiveVacuumSplitSource(
                hiveSplitSource,
                mockVacuumTableHandle,
                mockHdfsEnvironment,
                new HdfsEnvironment.HdfsContext(new VacuumCleanerTest.ConnectorSession(), "schemaName", "tableName"),
                mockSession);
    }

    @Test
    public void testGetNextBatch()
    {
        // Setup
        final ConnectorPartitionHandle partitionHandle = new HivePartitionHandle(10);

        // Run the test
        final CompletableFuture<ConnectorSplitSource.ConnectorSplitBatch> result = hiveVacuumSplitSourceUnderTest.getNextBatch(
                partitionHandle, 0);

        // Verify the results
    }

    @Test
    public void testGetNextBatch_HiveSplitSourceReturnsFailure()
    {
        // Setup
        final ConnectorPartitionHandle partitionHandle = null;

        // Run the test
        final CompletableFuture<ConnectorSplitSource.ConnectorSplitBatch> result = hiveVacuumSplitSourceUnderTest.getNextBatch(
                partitionHandle, 0);

        // Verify the results
    }

    @Test
    public void testGetNextBatch_HdfsEnvironmentGetConfigurationReturnsNoItems()
    {
        // Setup
        final ConnectorPartitionHandle partitionHandle = null;

        // Run the test
        final CompletableFuture<ConnectorSplitSource.ConnectorSplitBatch> result = hiveVacuumSplitSourceUnderTest.getNextBatch(
                partitionHandle, 0);

        // Verify the results
    }

    @Test
    public void testGetNextBatch_HdfsEnvironmentDoAsThrowsE()
    {
        // Setup
        final ConnectorPartitionHandle partitionHandle = null;

        // Run the test
        final CompletableFuture<ConnectorSplitSource.ConnectorSplitBatch> result = hiveVacuumSplitSourceUnderTest.getNextBatch(
                partitionHandle, 0);

        // Verify the results
    }

    @Test
    public void testGetNextBatch_HiveVacuumTableHandleGetSuitableRangeReturnsNoItems()
    {
        // Setup
        final ConnectorPartitionHandle partitionHandle = null;

        // Run the test
        final CompletableFuture<ConnectorSplitSource.ConnectorSplitBatch> result = hiveVacuumSplitSourceUnderTest.getNextBatch(
                partitionHandle, 0);

        // Verify the results
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        hiveVacuumSplitSourceUnderTest.close();
    }

    @Test
    public void testIsFinished()
    {
        final boolean result = hiveVacuumSplitSourceUnderTest.isFinished();
    }
}
