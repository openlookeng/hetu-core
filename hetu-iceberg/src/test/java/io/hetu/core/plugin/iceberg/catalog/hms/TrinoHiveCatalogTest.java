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
package io.hetu.core.plugin.iceberg.catalog.hms;

import com.google.common.collect.ImmutableSet;
import io.hetu.core.plugin.iceberg.ColumnIdentity;
import io.hetu.core.plugin.iceberg.HdfsFileIoProvider;
import io.hetu.core.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.hetu.core.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.hetu.core.plugin.iceberg.util.IcebergTestUtil;
import io.prestosql.Session;
import io.prestosql.icebergutil.TestSchemaMetadata;
import io.prestosql.metadata.QualifiedTablePrefix;
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
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.TypeId;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.spi.type.testing.TestingTypeManager;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static java.nio.file.Files.createTempDirectory;
import static org.mockito.MockitoAnnotations.initMocks;

public class TrinoHiveCatalogTest
{
    private CachingHiveMetastore mockMetastore;
    private HdfsEnvironment mockHdfsEnvironment;
    private TypeManager mockTypeManager;
    private IcebergTableOperationsProvider mockTableOperationsProvider;
    private TrinoHiveCatalog trinoHiveCatalogUnderTest;
    private TestSchemaMetadata metadata;
    private ConnectorSession connectorSession;
    private Session session;
    private final String dataBaseName = "testDatabasename";
    private final String schemaName = "test_schema";
    private final String tableName = "test_table";

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);

        metadata = IcebergTestUtil.getMetadata();
        connectorSession = IcebergTestUtil.getConnectorSession(metadata);
        session = IcebergTestUtil.getSession(metadata);

        mockTypeManager = new TestingTypeManager();
        File hiveDir = new File(createTempDirectory("TestTpcdsCostBasedPlanPushdown").toFile(), "test_schema");
        HiveMetastore metastore = createTestingFileHiveMetastore(hiveDir);
        metastore.createDatabase(
                new HiveIdentity(SESSION),
                Database.builder()
                        .setDatabaseName("test_schema")
                        .setOwnerName("public")
                        .setOwnerType(PrincipalType.ROLE)
                        .build());

        mockMetastore = CachingHiveMetastore.memoizeMetastore(metastore, 1000);

        HiveConfig hiveConfig = new HiveConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveConfig), ImmutableSet.of());
        mockHdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hiveConfig, new NoHdfsAuthentication());
        mockTableOperationsProvider = new FileMetastoreTableOperationsProvider(new HdfsFileIoProvider(mockHdfsEnvironment));
        trinoHiveCatalogUnderTest = new TrinoHiveCatalog(new CatalogName("catalogName"), mockMetastore,
                mockHdfsEnvironment, mockTypeManager, mockTableOperationsProvider, "trinoVersion", false, false, false);
    }

    @Test
    public void testGetMetastore()
    {
        trinoHiveCatalogUnderTest.getMetastore();
    }

    @Test
    public void testListNamespaces1()
    {
        trinoHiveCatalogUnderTest.listNamespaces(connectorSession);
    }

    @Test
    public void testLoadNamespaceMetadata()
    {
        testCreateNamespace();
        trinoHiveCatalogUnderTest.loadNamespaceMetadata(connectorSession, dataBaseName);
    }

    @Test
    public void testGetNamespacePrincipal()
    {
        testCreateNamespace();
        trinoHiveCatalogUnderTest.getNamespacePrincipal(connectorSession, dataBaseName);
    }

    @Test
    public void testCreateNamespace()
    {
        Map<String, Object> properties = new HashMap<>();
        properties.put("aa", "int");
        properties.put("bb", "int");
        PrestoPrincipal owner = new PrestoPrincipal(PrincipalType.USER, "name");
        trinoHiveCatalogUnderTest.createNamespace(connectorSession, dataBaseName, properties, owner);
    }

    @Test
    public void testDropNamespace()
    {
        testCreateNamespace();
        trinoHiveCatalogUnderTest.dropNamespace(connectorSession, dataBaseName);
    }

    @Test
    public void testRenameNamespace()
    {
        testCreateNamespace();
        trinoHiveCatalogUnderTest.renameNamespace(connectorSession, dataBaseName, "new_" + dataBaseName);
    }

    @Test
    public void testSetNamespacePrincipal()
    {
        testCreateNamespace();
        PrestoPrincipal principal = new PrestoPrincipal(PrincipalType.USER, "name");
        trinoHiveCatalogUnderTest.setNamespacePrincipal(connectorSession, dataBaseName, principal);
    }

    @Test
    public void testNewCreateTableTransaction()
    {
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        Map<String, Type> map = new HashMap<>();
        map.put("a", Types.BooleanType.get());
        map.put("b", Types.IntegerType.get());
        Schema schema = IcebergTestUtil.createSchema2(map);
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        PartitionSpec partitionSpec = builder.build();
        Map<String, String> properties = new HashMap<>();
        properties.put("format", "ORC");
        trinoHiveCatalogUnderTest.newCreateTableTransaction(connectorSession, schemaTableName, schema, partitionSpec, "location", properties);
    }

    private List<SchemaTableName> testListTables()
    {
        return trinoHiveCatalogUnderTest.listTables(connectorSession, Optional.of(schemaName));
    }

    private void testRenameTable(SchemaTableName from)
    {
        try {
            SchemaTableName to = new SchemaTableName(schemaName, "newtestRenameTable");
            trinoHiveCatalogUnderTest.renameTable(connectorSession, from, to);
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void testLoadTable(SchemaTableName schemaTableName)
    {
        try {
            trinoHiveCatalogUnderTest.loadTable(connectorSession, schemaTableName);
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void testUpdateTableComment(SchemaTableName old)
    {
        try {
            trinoHiveCatalogUnderTest.updateTableComment(connectorSession, old, Optional.empty());
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void testUpdateColumnComment(SchemaTableName old)
    {
        try {
            ColumnIdentity columnIdentity = new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList());
            trinoHiveCatalogUnderTest.updateColumnComment(connectorSession, old, columnIdentity, Optional.empty());
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void testDefaultTableLocation()
    {
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, "testDefaultTableLocation");
        trinoHiveCatalogUnderTest.defaultTableLocation(connectorSession, schemaTableName);
    }

    @Test
    public void testCreateMaterializedView()
    {
        TestSchemaMetadata schemaMetadata = IcebergTestUtil.getMetadata();
        Session dialogue = IcebergTestUtil.getSession(schemaMetadata);
        List<QualifiedObjectName> iceberg = schemaMetadata.metadata.listTables(dialogue, new QualifiedTablePrefix("test_catalog"));
        SchemaTableName schemaTableName = new SchemaTableName("test_schema", "another_table");
        List<ConnectorMaterializedViewDefinition.Column> columns = Arrays.asList(new ConnectorMaterializedViewDefinition.Column("test", TypeId.of("integer")));
        ConnectorMaterializedViewDefinition definition = new ConnectorMaterializedViewDefinition(
                "select 1",
                Optional.of(new CatalogSchemaTableName("test_catalog", "test_schema", "another_table")),
                Optional.of("value"),
                Optional.of("value"),
                columns,
                Optional.of("value"),
                Optional.of("value"),
                new HashMap<>());
        // Run the test
        trinoHiveCatalogUnderTest.createMaterializedView(IcebergTestUtil.getConnectorSession(schemaMetadata), schemaTableName, definition, false, false);

        List<SchemaTableName> list = testListTables();

        testLoadTable(list.stream().findFirst().get());

        testGetMaterializedView(list.get(1));

        List<SchemaTableName> schemaTableNames = testListMaterializedViews();
        testUpdateTableComment(schemaTableNames.stream().findFirst().get());

        List<SchemaTableName> schemaTableNames0 = testListMaterializedViews();
        testUpdateColumnComment(schemaTableNames0.stream().findFirst().get());

        List<SchemaTableName> schemaTableNames1 = testListMaterializedViews();
        testRenameTable(schemaTableNames1.stream().findFirst().get());

        List<SchemaTableName> schemaTableNames2 = testListMaterializedViews();
        testRenameMaterializedView(schemaTableNames2.stream().findFirst().get());

        List<SchemaTableName> schemaTableNames3 = testListMaterializedViews();
        testSetTablePrincipal(schemaTableNames3.stream().findFirst().get());

        List<SchemaTableName> list1 = testListTables();
        testDropTable(list1.stream().findFirst().get());

        List<SchemaTableName> list2 = testListTables();
        testDropMaterializedView(list2.stream().findFirst().get());
    }

    private void testDropMaterializedView(SchemaTableName old)
    {
        try {
            trinoHiveCatalogUnderTest.dropMaterializedView(connectorSession, old);
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void testDropTable(SchemaTableName old)
    {
        try {
            trinoHiveCatalogUnderTest.dropTable(connectorSession, old);
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void testSetTablePrincipal(SchemaTableName old)
    {
        try {
            trinoHiveCatalogUnderTest.setTablePrincipal(connectorSession, old, new PrestoPrincipal(PrincipalType.USER, "name"));
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void testRenameMaterializedView(SchemaTableName old)
    {
        try {
            SchemaTableName newschemaTableName = new SchemaTableName(schemaName, "newschemaName");
            trinoHiveCatalogUnderTest.renameMaterializedView(connectorSession, old, newschemaTableName);
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void testGetMaterializedView(SchemaTableName schemaTableName)
    {
        trinoHiveCatalogUnderTest.getMaterializedView(connectorSession, schemaTableName);
    }

    private List<SchemaTableName> testListMaterializedViews()
    {
        List<SchemaTableName> schemaTableNames = new ArrayList<>();
        try {
            schemaTableNames = trinoHiveCatalogUnderTest.listMaterializedViews(connectorSession, Optional.of(schemaName));
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return schemaTableNames;
    }

    @Test
    public void testCreateView()
    {
        SchemaTableName schemaViewName = new SchemaTableName(schemaName, "testCreateView");
        ConnectorViewDefinition definition = new ConnectorViewDefinition(
                "select 1",
                Optional.of("catalogName"),
                Optional.of(schemaName),
                Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))),
                Optional.of("owner"),
                false);
        trinoHiveCatalogUnderTest.createView(connectorSession, schemaViewName, definition, false);

        List<SchemaTableName> schemaTableNames = testListViews();

        testGetView(schemaTableNames.stream().findFirst().get());

        testRenameView(schemaTableNames.stream().findFirst().get());

        List<SchemaTableName> schemaTableNames1 = testListViews();
        testSetViewPrincipal(schemaTableNames1.stream().findFirst().get());

        List<SchemaTableName> schemaTableNames2 = testListViews();
        testDropView(schemaTableNames2.stream().findFirst().get());
    }

    private void testGetView(SchemaTableName old)
    {
        trinoHiveCatalogUnderTest.getView(connectorSession, old);
    }

    private List<SchemaTableName> testListViews()
    {
        return trinoHiveCatalogUnderTest.listViews(connectorSession, Optional.of(schemaName));
    }

    private void testRenameView(SchemaTableName old)
    {
        try {
            SchemaTableName newschemaViewName = new SchemaTableName(schemaName, "newtestCreateView");
            trinoHiveCatalogUnderTest.renameView(connectorSession, old, newschemaViewName);
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void testSetViewPrincipal(SchemaTableName old)
    {
        try {
            trinoHiveCatalogUnderTest.setViewPrincipal(connectorSession, old, new PrestoPrincipal(PrincipalType.ROLE, "name"));
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void testDropView(SchemaTableName old)
    {
        try {
            trinoHiveCatalogUnderTest.dropView(connectorSession, old);
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void testRedirectTable()
    {
        ConnectorSession dialogue = new ConnectorSession()
        {
            @Override
            public String getQueryId()
            {
                return "queryId";
            }

            @Override
            public Optional<String> getSource()
            {
                return Optional.empty();
            }

            @Override
            public ConnectorIdentity getIdentity()
            {
                return new ConnectorIdentity("value", Optional.empty(), Optional.empty());
            }

            @Override
            public TimeZoneKey getTimeZoneKey()
            {
                return null;
            }

            @Override
            public Locale getLocale()
            {
                return null;
            }

            @Override
            public Optional<String> getTraceToken()
            {
                return Optional.empty();
            }

            @Override
            public long getStartTime()
            {
                return 0;
            }

            @Override
            public <T> T getProperty(String name, Class<T> type)
            {
                return (T) "hive_catalog_name";
            }
        };
        SchemaTableName schemaTableName = new SchemaTableName("namespace", "tableName");
        Optional<CatalogSchemaTableName> expectedResult = Optional.of(
                new CatalogSchemaTableName("catalogName", "schemaName", "tableName"));

        trinoHiveCatalogUnderTest.redirectTable(dialogue, schemaTableName);
    }
}
