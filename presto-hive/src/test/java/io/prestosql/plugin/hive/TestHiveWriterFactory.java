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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.PagesIndexPageSorter;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.PagesIndex;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.metastore.CachingHiveMetastore;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.HivePageSinkMetadata;
import io.prestosql.plugin.hive.metastore.HivePageSinkMetadataProvider;
import io.prestosql.plugin.hive.metastore.Storage;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.prestosql.plugin.hive.metastore.thrift.MetastoreLocator;
import io.prestosql.plugin.hive.metastore.thrift.MockThriftMetastoreClient;
import io.prestosql.plugin.hive.metastore.thrift.ThriftHiveMetastore;
import io.prestosql.plugin.hive.metastore.thrift.ThriftHiveMetastoreConfig;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreClient;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.testing.TestingConnectorSession;
import io.prestosql.testing.TestingNodeManager;
import io.prestosql.type.InternalTypeManager;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.plugin.hive.HiveStorageFormat.ORC;
import static io.prestosql.plugin.hive.HiveTestUtils.getDefaultHiveFileWriterFactories;
import static io.prestosql.plugin.hive.HiveTestUtils.getDefaultOrcFileWriterFactory;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.apache.hadoop.hive.ql.exec.Utilities.getBucketIdFromFile;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

public class TestHiveWriterFactory
{
    private ThriftMetastoreClient mockClient;
    protected ExecutorService executor;
    protected ExecutorService executorRefresh;
    protected HiveMetastore metastore;

    private void setUp()
    {
        mockClient = new MockThriftMetastoreClient();
        executor = newCachedThreadPool(daemonThreadsNamed("hive-%s"));
        executorRefresh = newCachedThreadPool(daemonThreadsNamed("hive-refresh-%s"));
        MetastoreLocator metastoreLocator = new MockMetastoreLocator(mockClient);
        metastore = new CachingHiveMetastore(
                new BridgingHiveMetastore(new ThriftHiveMetastore(metastoreLocator, new ThriftHiveMetastoreConfig())),
                executor,
                executorRefresh, Duration.valueOf("1m"),
                Duration.valueOf("15s"),
                Duration.valueOf("1m"),
                Duration.valueOf("15s"),
                10000,
                false);
    }

    @Test
    public void testComputeBucketedFileName()
    {
        String name = HiveWriterFactory.computeBucketedFileName("20180102_030405_00641_x1y2z", 1234);
        assertEquals(name, "001234_0_20180102_030405_00641_x1y2z");
        assertEquals(getBucketIdFromFile(name), 1234);
    }

    @Test
    public void testSortingPath()
    {
        setUp();
        String targetPath = "/tmp";
        String writePath = "/tmp/table";
        Optional<WriteIdInfo> writeIdInfo = Optional.of(new WriteIdInfo(1, 1, 0));
        StorageFormat storageFormat = StorageFormat.fromHiveStorageFormat(ORC);
        Storage storage = new Storage(storageFormat, "", Optional.empty(), false, ImmutableMap.of());
        Table table = new Table("schema",
                "table",
                "user",
                "MANAGED_TABLE",
                storage,
                ImmutableList.of(new Column("col_1", HiveType.HIVE_INT, Optional.empty())),
                ImmutableList.of(),
                ImmutableMap.of("transactional", "true"),
                Optional.of("original"),
                Optional.of("expanded"));
        HiveConfig hiveConfig = getHiveConfig();
        HivePageSinkMetadata hivePageSinkMetadata = new HivePageSinkMetadata(new SchemaTableName("schema", "table"), Optional.of(table), ImmutableMap.of());
        PageSorter pageSorter = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));
        Metadata metadata = createTestMetadataManager();
        TypeManager typeManager = new InternalTypeManager(metadata.getFunctionAndTypeManager());
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hiveConfig, new NoHdfsAuthentication());
        LocationService locationService = new HiveLocationService(hdfsEnvironment);
        ConnectorSession session = newSession();
        HiveWriterFactory hiveWriterFactory = new HiveWriterFactory(
                getDefaultHiveFileWriterFactories(hiveConfig),
                "schema",
                "table",
                false,
                HiveACIDWriteType.DELETE,
                ImmutableList.of(new HiveColumnHandle("col_1", HiveType.HIVE_INT, new TypeSignature("integer", ImmutableList.of()), 0, HiveColumnHandle.ColumnType.REGULAR, Optional.empty())),
                ORC,
                ORC,
                ImmutableMap.of(),
                OptionalInt.empty(),
                ImmutableList.of(),
                new LocationHandle(targetPath, writePath, false, LocationHandle.WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY, writeIdInfo),
                locationService,
                session.getQueryId(),
                new HivePageSinkMetadataProvider(hivePageSinkMetadata, CachingHiveMetastore.memoizeMetastore(metastore, 1000), new HiveIdentity(session)),
                typeManager,
                hdfsEnvironment,
                pageSorter,
                hiveConfig.getWriterSortBufferSize(),
                hiveConfig.getMaxOpenSortFiles(),
                false,
                UTC,
                session,
                new TestingNodeManager("fake-environment"),
                new HiveEventClient(),
                new HiveSessionProperties(hiveConfig, new OrcFileWriterConfig(), new ParquetFileWriterConfig()),
                new HiveWriterStats(),
                getDefaultOrcFileWriterFactory(hiveConfig));
        HiveWriter hiveWriter = hiveWriterFactory.createWriter(ImmutableList.of(), OptionalInt.empty(), Optional.empty());
        assertEquals(((SortingFileWriter) hiveWriter.getFileWriter()).getTempFilePrefix().getName(), ".tmp-sort.bucket_00000");
    }

    protected HiveConfig getHiveConfig()
    {
        return new HiveConfig()
                .setMaxOpenSortFiles(10)
                .setWriterSortBufferSize(new DataSize(100, KILOBYTE));
    }

    protected ConnectorSession newSession()
    {
        return newSession(ImmutableMap.of());
    }

    protected ConnectorSession newSession(Map<String, Object> propertyValues)
    {
        HiveSessionProperties properties = new HiveSessionProperties(new HiveConfig(), new OrcFileWriterConfig(), new ParquetFileWriterConfig());
        return new TestingConnectorSession(properties.getSessionProperties(), propertyValues);
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
