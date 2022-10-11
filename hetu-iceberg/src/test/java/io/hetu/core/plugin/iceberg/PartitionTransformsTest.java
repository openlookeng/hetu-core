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
import io.prestosql.queryeditorui.output.persistors.FlatFilePersistor;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ConnectorMaterializedViewDefinition;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeId;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.testing.TestingTypeManager;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.AbstractTestQueryFramework;
import io.prestosql.tests.DistributedQueryRunner;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TimeType.TIME_MICROS;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.type.UuidType.UUID;
import static org.testng.Assert.assertEquals;

public class PartitionTransformsTest
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

    private static final Logger LOG = LoggerFactory.getLogger(FlatFilePersistor.class);

    protected PartitionTransformsTest()
    {
        super(PartitionTransformsTest::createQueryRunner);
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
    public void testGetColumnTransform()
    {
        Map<String, TypeId> map = new HashMap<>();
        map.put("a", TypeId.of("boolean"));
        map.put("testyear", TypeId.of("date"));
        map.put("year(testyear)", TypeId.of("date"));
        map.put("testmonth", TypeId.of("date"));
        map.put("month(testmonth)", TypeId.of("date"));
        map.put("testday", TypeId.of("date"));
        map.put("day(testday)", TypeId.of("date"));
        map.put("testvoid", TypeId.of("integer"));
        map.put("void(testvoid)", TypeId.of("integer"));
        List<ConnectorMaterializedViewDefinition.Column> columns = new ArrayList<>();
        map.forEach((k, v) -> {
            try {
                columns.addAll(ImmutableList.of(new ConnectorMaterializedViewDefinition.Column(k, v)));
            }
            catch (Exception e) {
                LOG.error(e.getMessage());
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
        result.fields().forEach(item -> {
            switch (item.transform().toString()) {
                case "identity":
                    test(item, BOOLEAN);
                    break;
                case "void":
                    test(item, BigintType.BIGINT);
                    break;
                case "hour":
                    test(item, TIMESTAMP_MICROS);
                    test(item, TIMESTAMP_TZ_MICROS);
                    break;
                default:
                    test(item, DATE);
                    test(item, TIMESTAMP_MICROS);
                    test(item, TIMESTAMP_TZ_MICROS);
                    break;
            }
        });
    }

    @Test
    public void testBucket_INTEGER()
    {
        PartitionTransforms.bucket(INTEGER, 1);
    }

    @Test
    public void testBucket_BIGINT()
    {
        PartitionTransforms.bucket(BIGINT, 1);
    }

    @Test
    public void testBucket_DATE()
    {
        PartitionTransforms.bucket(DATE, 1);
    }

    @Test
    public void testBucket_TIME_MICROS()
    {
        PartitionTransforms.bucket(TIME_MICROS, 1);
    }

    @Test
    public void testBucket_TIMESTAMP_MICROS()
    {
        PartitionTransforms.bucket(TIMESTAMP_MICROS, 1);
    }

    @Test
    public void testBucket_TIMESTAMP_TZ_MICROS()
    {
        PartitionTransforms.bucket(TIMESTAMP_TZ_MICROS, 1);
    }

    @Test
    public void testBucket_VarcharType()
    {
        PartitionTransforms.bucket(VARCHAR, 1);
    }

    @Test
    public void testBucket_VARBINARY()
    {
        PartitionTransforms.bucket(VARBINARY, 1);
    }

    @Test
    public void testBucket_UUID()
    {
        PartitionTransforms.bucket(UUID, 1);
    }

    private void test(PartitionField field, Type type)
    {
        try {
            PartitionTransforms.getColumnTransform(field, type);
        }
        catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    @Test
    public void testEpochYear()
    {
        assertEquals(0L, PartitionTransforms.epochYear(0L));
    }

    @Test
    public void testEpochMonth()
    {
        assertEquals(0L, PartitionTransforms.epochMonth(0L));
    }

    @Test
    public void testEpochDay()
    {
        assertEquals(0L, PartitionTransforms.epochDay(0L));
    }

    @Test
    public void testEpochHour()
    {
        assertEquals(0L, PartitionTransforms.epochHour(0L));
    }
}
