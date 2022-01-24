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
package io.prestosql.plugin.hive.metastore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveBasicStatistics;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.plugin.hive.OrcFileWriterConfig;
import io.prestosql.plugin.hive.ParquetFileWriterConfig;
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.thrift.MetastoreLocator;
import io.prestosql.plugin.hive.metastore.thrift.MockThriftMetastoreClient;
import io.prestosql.plugin.hive.metastore.thrift.ThriftHiveMetastore;
import io.prestosql.plugin.hive.metastore.thrift.ThriftHiveMetastoreConfig;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreClient;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreStats;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.testing.TestingConnectorSession;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.Function;

import static io.prestosql.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_DATABASE;
import static io.prestosql.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_PARTITION_UP1;
import static io.prestosql.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_PARTITION_UP2;
import static io.prestosql.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_TABLE_UP;
import static io.prestosql.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_TABLE_UP_NAME;
import static io.prestosql.plugin.hive.util.Statistics.merge;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestSemiTransactionalHiveMetastore
{
    private static final HiveIdentity IDENTITY = new HiveIdentity(SESSION);
    private MockThriftMetastoreClient mockClient;
    private ThriftHiveMetastore thriftHiveMetastore;
    private ThriftMetastoreStats stats;

    protected String database;
    protected HdfsEnvironment hdfsEnvironment;

    protected static final PartitionStatistics BASIC_STATISTICS_1 = new PartitionStatistics(new HiveBasicStatistics(OptionalLong.of(2), OptionalLong.of(10), OptionalLong.empty(), OptionalLong.empty()), ImmutableMap.of());

    private static final PartitionStatistics STATISTICS_1 =
            new PartitionStatistics(
                    BASIC_STATISTICS_1.getBasicStatistics(),
                    ImmutableMap.<String, HiveColumnStatistics>builder()
                            .put("t_bigint", HiveColumnStatistics.createIntegerColumnStatistics(OptionalLong.of(2), OptionalLong.of(5), OptionalLong.of(0), OptionalLong.of(4)))
                            .build());

    protected static final PartitionStatistics BASIC_STATISTICS_2 = new PartitionStatistics(new HiveBasicStatistics(OptionalLong.of(2), OptionalLong.of(10), OptionalLong.empty(), OptionalLong.empty()), ImmutableMap.of());

    private static final PartitionStatistics STATISTICS_2 =
            new PartitionStatistics(
                    BASIC_STATISTICS_2.getBasicStatistics(),
                    ImmutableMap.<String, HiveColumnStatistics>builder()
                            .put("t_bigint", HiveColumnStatistics.createIntegerColumnStatistics(OptionalLong.of(1), OptionalLong.of(4), OptionalLong.of(0), OptionalLong.of(4)))
                            .build());

    protected static final PartitionStatistics BASIC_STATISTICS_3 = new PartitionStatistics(new HiveBasicStatistics(OptionalLong.of(4), OptionalLong.of(20), OptionalLong.empty(), OptionalLong.empty()), ImmutableMap.of());
    private static final PartitionStatistics STATISTICS_3 =
            new PartitionStatistics(
                    BASIC_STATISTICS_3.getBasicStatistics(),
                    ImmutableMap.<String, HiveColumnStatistics>builder()
                            .put("t_bigint", HiveColumnStatistics.createIntegerColumnStatistics(OptionalLong.of(1), OptionalLong.of(5), OptionalLong.of(0), OptionalLong.of(4)))
                            .build());

    private static List<String> partitions = ImmutableList.of(MockThriftMetastoreClient.TEST_PARTITION_UP1, MockThriftMetastoreClient.TEST_PARTITION_UP2);

    private static final Map<String, PartitionStatistics> PARTITION_STATISTICS_MAP = new HashMap<String, PartitionStatistics>(){{
            put(TEST_PARTITION_UP1, STATISTICS_1);
            put(TEST_PARTITION_UP2, STATISTICS_1);
        }};

    @BeforeMethod
    public void setUp()
    {
        mockClient = new MockThriftMetastoreClient();
        MetastoreLocator metastoreLocator = new MockMetastoreLocator(mockClient);
        thriftHiveMetastore = new ThriftHiveMetastore(metastoreLocator, new ThriftHiveMetastoreConfig());
        stats = thriftHiveMetastore.getStats();
    }

    private void updatePartitionsStatistics()
    {
        Map<String, Function<PartitionStatistics, PartitionStatistics>> partNamesUpdateMap = new HashMap<>();
        List<PartitionStatistics> statistics = ImmutableList.of(STATISTICS_1, STATISTICS_1);
        for (int index = 0; index < partitions.size(); index++) {
            PartitionStatistics partitionStatistics = statistics.get(index);
            partNamesUpdateMap.put(partitions.get(index), actualStatistics -> partitionStatistics);
        }
        thriftHiveMetastore.updatePartitionsStatistics(IDENTITY, MockThriftMetastoreClient.TEST_DATABASE, MockThriftMetastoreClient.TEST_TABLE_UP_NAME, partNamesUpdateMap);
    }

    private PartitionStatistics skipStats(PartitionStatistics currentStatistics, PartitionStatistics updatedStatistics, boolean isCollectColumnStatisticsOnWrite)
    {
        if (isCollectColumnStatisticsOnWrite) {
            return merge(currentStatistics, updatedStatistics);
        }
        else {
            return updatedStatistics;
        }
    }

    protected ConnectorSession newSession(Map<String, Object> propertyValues)
    {
        HiveSessionProperties properties = new HiveSessionProperties(new HiveConfig(), new OrcFileWriterConfig(), new ParquetFileWriterConfig());
        return new TestingConnectorSession(properties.getSessionProperties(), propertyValues);
    }

    @Test
    public void testIsCollectColumnStatisticsOnWriteTrue()
    {
        ConnectorSession session = newSession(ImmutableMap.of("collect_column_statistics_on_write", true));
        assertEquals(STATISTICS_3, skipStats(STATISTICS_1, STATISTICS_2, HiveSessionProperties.isCollectColumnStatisticsOnWrite(session)));
    }

    @Test
    public void testIsCollectColumnStatisticsOnWriteFalse()
    {
        ConnectorSession session = newSession(ImmutableMap.of("collect_column_statistics_on_write", false));
        assertEquals(STATISTICS_2, skipStats(STATISTICS_1, STATISTICS_2, HiveSessionProperties.isCollectColumnStatisticsOnWrite(session)));
    }

    @Test
    public void testGetPartitionStatistics()
    {
        updatePartitionsStatistics();
        assertEquals(PARTITION_STATISTICS_MAP, thriftHiveMetastore.getPartitionStatistics(IDENTITY, TEST_TABLE_UP, thriftHiveMetastore.getPartitionsByNames(IDENTITY, TEST_DATABASE, TEST_TABLE_UP_NAME, partitions)));
    }

    @Test
    public void testUpdatePartitionsStatistics()
    {
        updatePartitionsStatistics();
        assertEquals(STATISTICS_1, thriftHiveMetastore.getPartitionStatistics(IDENTITY, TEST_TABLE_UP, thriftHiveMetastore.getPartitionsByNames(IDENTITY, TEST_DATABASE, TEST_TABLE_UP_NAME, partitions)).get(TEST_PARTITION_UP1));
        assertEquals(STATISTICS_1, thriftHiveMetastore.getPartitionStatistics(IDENTITY, TEST_TABLE_UP, thriftHiveMetastore.getPartitionsByNames(IDENTITY, TEST_DATABASE, TEST_TABLE_UP_NAME, partitions)).get(TEST_PARTITION_UP2));
    }

    @Test
    public void testAlterPartitions()
    {
        updatePartitionsStatistics();
        assertEquals(mockClient.getAlterPartitionCount(), 1);
    }

    private static class MockMetastoreLocator
            implements MetastoreLocator
    {
        private final ThriftMetastoreClient client;

        private MockMetastoreLocator(ThriftMetastoreClient client)
        {
            this.client = client;
        }

        @Override
        public ThriftMetastoreClient createMetastoreClient()
        {
            return client;
        }
    }
}
