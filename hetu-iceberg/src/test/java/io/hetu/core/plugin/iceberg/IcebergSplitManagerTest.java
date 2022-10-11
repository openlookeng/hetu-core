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
package io.hetu.core.plugin.iceberg;

import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.units.DataSize;
import io.hetu.core.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.hetu.core.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.hetu.core.plugin.iceberg.util.IcebergTestUtil;
import io.prestosql.plugin.hive.HdfsConfiguration;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveHdfsConfiguration;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.plugin.hive.OrcFileWriterConfig;
import io.prestosql.plugin.hive.ParquetFileWriterConfig;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.metastore.CachingHiveMetastore;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.RetryMode;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.testing.TestingTypeManager;
import io.prestosql.sql.planner.TestingConnectorTransactionHandle;
import io.prestosql.testing.TestingConnectorSession;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;

public class IcebergSplitManagerTest
{
    @Mock
    private IcebergTransactionManager mockTransactionManager;
    @Mock
    private TypeManager mockTypeManager;

    private IcebergSplitManager icebergSplitManagerUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        icebergSplitManagerUnderTest = new IcebergSplitManager(mockTransactionManager, mockTypeManager);
    }

    @Test
    public void testGetSplits() throws IOException
    {
        HiveConfig hdfsConfig = new HiveConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
        IcebergMetadata icebergMetadata = new IcebergMetadata(new TestingTypeManager(), new JsonCodecFactory(
                () -> new ObjectMapperProvider().get())
                .jsonCodec(CommitTaskData.class),
                new TrinoHiveCatalog(
                new CatalogName("test_catalog"),
                CachingHiveMetastore.memoizeMetastore(IcebergTestUtil.getHiveMetastore(), 1000),
                hdfsEnvironment,
                mockTypeManager,
                new FileMetastoreTableOperationsProvider(new HdfsFileIoProvider(hdfsEnvironment)),
                "test",
                false,
                false,
                false),
                hdfsEnvironment);
        ConnectorSession session = new TestingConnectorSession(
                new HiveSessionProperties(new HiveConfig().setOrcLazyReadSmallRanges(false), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
        IcebergTableHandle icebergTableHandle = new IcebergTableHandle("schemaName", "tableName", TableType.DATA,
                Optional.of(0L), "tableSchemaJson", "partitionSpecJson", 0,
                TupleDomain.withColumnDomains(new HashMap<>()),
                TupleDomain.withColumnDomains(new HashMap<>()), new HashSet<>(
                Arrays.asList(new IcebergColumnHandle(
                        new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), new IcebergTableHandleTest.Type(),
                        Arrays.asList(0), new IcebergTableHandleTest.Type(), Optional.of("value")))),
                Optional.of("value"), "tableLocation", new HashMap<>(), RetryMode.NO_RETRIES,
                Arrays.asList(new IcebergColumnHandle(
                        new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), new IcebergTableHandleTest.Type(),
                        Arrays.asList(0), new IcebergTableHandleTest.Type(), Optional.of("value"))), false,
                Optional.of(DataSize.ofBytes(0L)));
        ConnectorSplitSource result = icebergSplitManagerUnderTest.getSplits(TestingConnectorTransactionHandle.INSTANCE,
                session, icebergTableHandle,
                ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING, DynamicFilter.empty, new Constraint(TupleDomain.all()));

        // Verify the results
    }
}
