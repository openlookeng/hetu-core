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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.hetu.core.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.hetu.core.plugin.iceberg.catalog.TrinoCatalog;
import io.hetu.core.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.hetu.core.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.hetu.core.plugin.iceberg.util.IcebergTestUtil;
import io.prestosql.Session;
import io.prestosql.icebergutil.TestSchemaMetadata;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.plugin.hive.HdfsConfiguration;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveHdfsConfiguration;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.metastore.CachingHiveMetastore;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ConnectorMaterializedViewDefinition;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.TypeId;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.testing.TestingTypeManager;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.AbstractTestQueryFramework;
import io.prestosql.tests.DistributedQueryRunner;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class PartitionFieldsTest
        extends AbstractTestQueryFramework
{
    private JsonCodec<CommitTaskData> mockCommitTaskCodec;
    private TrinoCatalog mockCatalog;
    private HdfsEnvironment mockHdfsEnvironment;
    private ConnectorSession connectorSession;
    private CachingHiveMetastore cachingHiveMetastore;
    private IcebergTableOperationsProvider mockTableOperationsProvider;
    private TrinoHiveCatalog trinoHiveCatalog;
    private TypeManager mockTypeManager;
    private TestSchemaMetadata metadata;
    private HiveMetastore hiveMetastore;
    private TrinoCatalog catalog;

    protected PartitionFieldsTest()
    {
        super(PartitionFieldsTest::createQueryRunner);
    }

    private static QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("test_catalog")
                .setSchema("test_schema")
                .build();
        return new DistributedQueryRunner(session, 1);
    }

    @BeforeMethod
    public void setUp() throws Exception
    {
        mockTypeManager = new TestingTypeManager();
        metadata = IcebergTestUtil.getMetadata();
        SessionPropertyManager sessionPropertyManager = metadata.metadata.getSessionPropertyManager();
        HashMap<CatalogName, Map<String, String>> catalogNameMapHashMap = new HashMap<>();
        CatalogName testCatalog = new CatalogName("test_catalog");
        catalogNameMapHashMap.put(testCatalog, ImmutableMap.of("projection_pushdown_enabled", "true", "statistics_enabled", "true"));
        List<PropertyMetadata<?>> property = IcebergTestUtil.getProperty();
        sessionPropertyManager.addConnectorSessionProperties(testCatalog, property);
        connectorSession = IcebergTestUtil.getConnectorSession3(getSession(), sessionPropertyManager, catalogNameMapHashMap, metadata);
        hiveMetastore = IcebergTestUtil.getHiveMetastore();
        hiveMetastore.createDatabase(
                new HiveIdentity(connectorSession),
                Database.builder()
                        .setDatabaseName("test_schema")
                        .setOwnerName("public")
                        .setOwnerType(PrincipalType.ROLE)
                        .build());
        HiveConfig hdfsConfig = new HiveConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        mockHdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
        IcebergTableOperationsProvider tableOperationsProvider = new FileMetastoreTableOperationsProvider(new HdfsFileIoProvider(mockHdfsEnvironment));
        cachingHiveMetastore = CachingHiveMetastore.memoizeMetastore(hiveMetastore, 1000);
        mockCatalog = new TrinoHiveCatalog(new CatalogName("test_catalog"), cachingHiveMetastore, mockHdfsEnvironment, mockTypeManager, tableOperationsProvider, "test", false, false, false);
        mockCommitTaskCodec = new JsonCodecFactory(() -> new ObjectMapperProvider().get()).jsonCodec(CommitTaskData.class);
        mockTableOperationsProvider = new FileMetastoreTableOperationsProvider(new HdfsFileIoProvider(mockHdfsEnvironment));
        trinoHiveCatalog = new TrinoHiveCatalog(new CatalogName("catalogName"), cachingHiveMetastore,
                mockHdfsEnvironment, mockTypeManager, mockTableOperationsProvider, "trinoVersion", false, false, false);
        IcebergTableOperationsProvider operationsProvider = new FileMetastoreTableOperationsProvider(new HdfsFileIoProvider(mockHdfsEnvironment));
        catalog = new TrinoHiveCatalog(
                new CatalogName("test_catalog"),
                cachingHiveMetastore,
                mockHdfsEnvironment,
                new TestingTypeManager(),
                operationsProvider,
                "test_version",
                false,
                false,
                false);
    }

    @Test
    public void testParsePartitionFields()
    {
        ImmutableMap<String, TypeId> map = ImmutableMap.of(
                "test", TypeId.of("date"),
                "year(test)", TypeId.of("date"));
        List<ConnectorMaterializedViewDefinition.Column> columns = new ArrayList<>();
        map.forEach((k, v) -> {
            try {
                columns.addAll(ImmutableList.of(new ConnectorMaterializedViewDefinition.Column(k, v)));
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
        ConnectorMaterializedViewDefinition definition = new ConnectorMaterializedViewDefinition("select 1",
                Optional.of(new CatalogSchemaTableName("test_catalog", "test_schema", "another_table")),
                Optional.of("value"),
                Optional.of("value"),
                columns,
                Optional.of("value"),
                Optional.of("value"),
                new HashMap<>());
        SchemaTableName schemaTableName = new SchemaTableName("test_schema", "another_table");
        trinoHiveCatalog.createMaterializedView(IcebergTestUtil.getConnectorSession(metadata), schemaTableName, definition, false, false);
        List<SchemaTableName> schemaTableNames = catalog.listTables(connectorSession, Optional.of("test_schema"));

        Table nationTable = catalog.loadTable(connectorSession, schemaTableNames.stream().filter(item -> item.getTableName().startsWith("st")).findFirst().get());
        List<String> strings = new ArrayList<>(map.keySet());
        PartitionSpec result = PartitionFields.parsePartitionFields(nationTable.schema(), strings);
        List<String> strings1 = PartitionFields.toPartitionFields(result);
    }

    @Test
    public void testParsePartitionField()
    {
        // Setup
        final PartitionSpec.Builder builder = null;

        // Run the test
        PartitionFields.parsePartitionField(builder, "field");

        // Verify the results
    }

    @Test
    public void testToPartitionFields()
    {
        // Setup
        final PartitionSpec spec = PartitionSpec.unpartitioned();

        // Run the test
        final List<String> result = PartitionFields.toPartitionFields(spec);

        // Verify the results
        assertEquals(Arrays.asList("value"), result);
    }
}
