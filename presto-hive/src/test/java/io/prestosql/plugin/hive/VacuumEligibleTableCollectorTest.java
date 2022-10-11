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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.file.FileHiveMetastore;
import io.prestosql.spi.connector.ConnectorVacuumTableInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class VacuumEligibleTableCollectorTest
{
    private VacuumEligibleTableCollector vacuumEligibleTableCollectorUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        vacuumEligibleTableCollectorUnderTest = null /* TODO: construct the instance */;
    }

    @Test
    public void testCreateInstance() throws Exception
    {
        // Setup
        final HiveConfig hiveConfig = new HiveConfig();
        hiveConfig.setTargetMaxFileSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setMaxInitialSplits(0);
        hiveConfig.setMaxInitialSplitSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setSplitLoaderConcurrency(0);
        hiveConfig.setMaxSplitsPerSecond(0);
        hiveConfig.setDomainCompactionThreshold(0);
        hiveConfig.setWriterSortBufferSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setForceLocalScheduling(false);
        hiveConfig.setMaxConcurrentFileRenames(0);
        hiveConfig.setRecursiveDirWalkerEnabled(false);
        hiveConfig.setMaxSplitSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setMaxPartitionsPerScan(0);
        hiveConfig.setMaxOutstandingSplits(0);
        hiveConfig.setMaxOutstandingSplitsSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setMaxSplitIteratorThreads(0);
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveConfig), ImmutableSet.of());
        FileHiveMetastore name = FileHiveMetastore.createTestingFileHiveMetastore(new File("name"));
        final SemiTransactionalHiveMetastore metastore = new SemiTransactionalHiveMetastore(
                new HdfsEnvironment(hdfsConfiguration, hiveConfig, new NoHdfsAuthentication()),
                name,
                MoreExecutors.directExecutor(),
                io.prestosql.hadoop.$internal.io.netty.util.concurrent.GlobalEventExecutor.INSTANCE,
                new Duration(0.0, TimeUnit.MILLISECONDS),
                false,
                false,
                Optional.of(new Duration(0.0, TimeUnit.MILLISECONDS)),
                io.prestosql.hadoop.$internal.io.netty.util.concurrent.GlobalEventExecutor.INSTANCE,
                io.prestosql.hadoop.$internal.io.netty.util.concurrent.GlobalEventExecutor.INSTANCE,
                0);
        final HiveConfig hiveConfig1 = new HiveConfig();
        hiveConfig1.setTargetMaxFileSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig1.setMaxInitialSplits(0);
        hiveConfig1.setMaxInitialSplitSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig1.setSplitLoaderConcurrency(0);
        hiveConfig1.setMaxSplitsPerSecond(0);
        hiveConfig1.setDomainCompactionThreshold(0);
        hiveConfig1.setWriterSortBufferSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig1.setForceLocalScheduling(false);
        hiveConfig1.setMaxConcurrentFileRenames(0);
        hiveConfig1.setRecursiveDirWalkerEnabled(false);
        hiveConfig1.setMaxSplitSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig1.setMaxPartitionsPerScan(0);
        hiveConfig1.setMaxOutstandingSplits(0);
        hiveConfig1.setMaxOutstandingSplitsSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig1.setMaxSplitIteratorThreads(0);
        final HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hiveConfig1, new NoHdfsAuthentication());
        final ScheduledExecutorService executorService = io.prestosql.hadoop.$internal.io.netty.util.concurrent.GlobalEventExecutor.INSTANCE;

        // Run the test
        VacuumEligibleTableCollector.createInstance(metastore, hdfsEnvironment, 1, 1.0, executorService, 1L);

        VacuumEligibleTableCollector.finishVacuum("schemaTable");
        // Verify the results
    }

    @Test
    public void testGetVacuumTableList()
    {
        // Setup
        final HiveConfig hiveConfig = new HiveConfig();
        hiveConfig.setTargetMaxFileSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setMaxInitialSplits(0);
        hiveConfig.setMaxInitialSplitSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setSplitLoaderConcurrency(0);
        hiveConfig.setMaxSplitsPerSecond(0);
        hiveConfig.setDomainCompactionThreshold(0);
        hiveConfig.setWriterSortBufferSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setForceLocalScheduling(false);
        hiveConfig.setMaxConcurrentFileRenames(0);
        hiveConfig.setRecursiveDirWalkerEnabled(false);
        hiveConfig.setMaxSplitSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setMaxPartitionsPerScan(0);
        hiveConfig.setMaxOutstandingSplits(0);
        hiveConfig.setMaxOutstandingSplitsSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setMaxSplitIteratorThreads(0);
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveConfig), ImmutableSet.of());
        FileHiveMetastore name = FileHiveMetastore.createTestingFileHiveMetastore(new File("name"));
        final SemiTransactionalHiveMetastore metastore = new SemiTransactionalHiveMetastore(
                new HdfsEnvironment(hdfsConfiguration, hiveConfig, new NoHdfsAuthentication()), name, MoreExecutors.directExecutor(), io.prestosql.hadoop.$internal.io.netty.util.concurrent.GlobalEventExecutor.INSTANCE,
                new Duration(0.0, TimeUnit.MILLISECONDS), false, false,
                Optional.of(new Duration(0.0, TimeUnit.MILLISECONDS)), io.prestosql.hadoop.$internal.io.netty.util.concurrent.GlobalEventExecutor.INSTANCE, io.prestosql.hadoop.$internal.io.netty.util.concurrent.GlobalEventExecutor.INSTANCE, 0);
        final HiveConfig hiveConfig1 = new HiveConfig();
        hiveConfig1.setTargetMaxFileSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig1.setMaxInitialSplits(0);
        hiveConfig1.setMaxInitialSplitSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig1.setSplitLoaderConcurrency(0);
        hiveConfig1.setMaxSplitsPerSecond(0);
        hiveConfig1.setDomainCompactionThreshold(0);
        hiveConfig1.setWriterSortBufferSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig1.setForceLocalScheduling(false);
        hiveConfig1.setMaxConcurrentFileRenames(0);
        hiveConfig1.setRecursiveDirWalkerEnabled(false);
        hiveConfig1.setMaxSplitSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig1.setMaxPartitionsPerScan(0);
        hiveConfig1.setMaxOutstandingSplits(0);
        hiveConfig1.setMaxOutstandingSplitsSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig1.setMaxSplitIteratorThreads(0);
        final HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hiveConfig, new NoHdfsAuthentication());
        final ScheduledExecutorService executorService = io.prestosql.hadoop.$internal.io.netty.util.concurrent.GlobalEventExecutor.INSTANCE;

        // Run the test
        final List<ConnectorVacuumTableInfo> result = VacuumEligibleTableCollector.getVacuumTableList(metastore,
                hdfsEnvironment, 1, 1.0, executorService, 1L);

        // Verify the results
    }
}
