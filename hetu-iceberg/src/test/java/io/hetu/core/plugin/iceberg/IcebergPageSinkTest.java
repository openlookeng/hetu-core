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
import io.airlift.slice.Slice;
import io.airlift.testing.TempFile;
import io.hetu.core.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.hetu.core.plugin.iceberg.catalog.TrinoCatalog;
import io.hetu.core.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.hetu.core.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.hetu.core.plugin.iceberg.util.IcebergTestUtil;
import io.prestosql.GroupByHashPageIndexerFactory;
import io.prestosql.Session;
import io.prestosql.icebergutil.TestSchemaMetadata;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsConfiguration;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveHdfsConfiguration;
import io.prestosql.plugin.hive.NodeVersion;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.metastore.CachingHiveMetastore;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.orc.OrcWriterConfig;
import io.prestosql.queryeditorui.output.persistors.FlatFilePersistor;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.SplitWeight;
import io.prestosql.spi.block.ArrayBlockBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ConnectorMaterializedViewDefinition;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeId;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.testing.TestingTypeManager;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.AbstractTestQueryFramework;
import io.prestosql.tests.DistributedQueryRunner;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.LocationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.hetu.core.plugin.iceberg.IcebergPageSourceProviderTest.writeOrcContent;
import static io.prestosql.spi.connector.RetryMode.NO_RETRIES;
import static io.prestosql.spi.security.PrincipalType.USER;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class IcebergPageSinkTest
        extends AbstractTestQueryFramework
{
    private Schema schema;
    private PartitionSpec partitionSpec;
    private LocationProvider mockLocationProvider;
    private IcebergFileWriterFactory icebergFileWriterFactory;
    private PageIndexerFactory mockPageIndexerFactory;
    private JsonCodec<CommitTaskData> jsonCodec;
    private TypeManager typeManager;
    private TestSchemaMetadata metadata;
    private ConnectorSession connectorSession;
    private HdfsEnvironment hdfsEnvironment;
    private TrinoCatalog catalog;
    private Session session;
    private CachingHiveMetastore cachingHiveMetastore;
    private IcebergSplitSource icebergSplitSource;
    private TrinoHiveCatalog trinoHiveCatalog;
    private HiveMetastore hiveMetastore;

    private IcebergPageSink icebergPageSinkUnderTest;

    private static final Logger LOG = LoggerFactory.getLogger(FlatFilePersistor.class);

    protected IcebergPageSinkTest()
    {
        super(IcebergPageSinkTest::createQueryRunner);
    }

    private static QueryRunner createQueryRunner()
            throws Exception
    {
        Session dialogue = testSessionBuilder()
                .setCatalog("test_catalog")
                .setSchema("test_schema")
                .build();
        return new DistributedQueryRunner(dialogue, 1);
    }

    @BeforeMethod
    public void setUp() throws Exception
    {
        typeManager = new TestingTypeManager();
        metadata = IcebergTestUtil.getMetadata();
        session = IcebergTestUtil.getSession(metadata);
        SessionPropertyManager sessionPropertyManager = metadata.metadata.getSessionPropertyManager();
        HashMap<CatalogName, Map<String, String>> catalogNameMapHashMap = new HashMap<>();
        CatalogName testCatalog = new CatalogName("test_catalog");
        catalogNameMapHashMap.put(testCatalog, ImmutableMap.of("projection_pushdown_enabled", "true", "statistics_enabled", "true"));
        List<PropertyMetadata<?>> property = IcebergTestUtil.getProperty();
        sessionPropertyManager.addConnectorSessionProperties(testCatalog, property);
        connectorSession = IcebergTestUtil.getConnectorSession3(getSession(), sessionPropertyManager, catalogNameMapHashMap, metadata);
        FileFormatDataSourceStats fileFormatDataSourceStats = new FileFormatDataSourceStats();
        HiveConfig hdfsConfig = new HiveConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
        icebergFileWriterFactory = new IcebergFileWriterFactory(
                hdfsEnvironment,
                typeManager,
                new NodeVersion("trino-version"),
                fileFormatDataSourceStats,
                new OrcWriterConfig());
        IcebergColumnHandle icebergColumnHandle = new IcebergColumnHandle(
                new ColumnIdentity(1, "test", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                INTEGER,
                ImmutableList.of(),
                INTEGER,
                Optional.empty());
        LocationProvider location = LocationProviders.locationsFor("/tmp", new HashMap<>());
        IcebergTableOperationsProvider operationsProvider = new FileMetastoreTableOperationsProvider(new HdfsFileIoProvider(hdfsEnvironment));
        hiveMetastore = IcebergTestUtil.getHiveMetastore();
        cachingHiveMetastore = CachingHiveMetastore.memoizeMetastore(hiveMetastore, 1000);
        trinoHiveCatalog = new TrinoHiveCatalog(new CatalogName("catalogName"), cachingHiveMetastore,
                hdfsEnvironment, typeManager, operationsProvider, "trinoVersion", false, false, false);
        JoinCompiler joinCompiler = new JoinCompiler(metadata.metadata);
        GroupByHashPageIndexerFactory groupByHashPageIndexerFactory = new GroupByHashPageIndexerFactory(joinCompiler);
        catalog = new TrinoHiveCatalog(
                new CatalogName("test_catalog"),
                cachingHiveMetastore,
                hdfsEnvironment,
                new TestingTypeManager(),
                operationsProvider,
                "test_version",
                false,
                false,
                false);
        testCreatePageSource(IcebergFileFormat.ORC);
        List<SchemaTableName> schemaTableNames = catalog.listTables(connectorSession, Optional.of("test_schema"));
        SchemaTableName schemaTableName = new SchemaTableName("test_schema", schemaTableNames.stream().filter(item -> item.getTableName().startsWith("st")).findFirst().get().getTableName());
        Table nationTable = catalog.loadTable(connectorSession, schemaTableName);
        schema = nationTable.schema();
        partitionSpec = testParsePartitionFields();
        jsonCodec = new JsonCodecFactory(() -> new ObjectMapperProvider().get()).jsonCodec(CommitTaskData.class);
        icebergPageSinkUnderTest = new IcebergPageSink(
                schema,
                partitionSpec,
                location,
                icebergFileWriterFactory,
                groupByHashPageIndexerFactory,
                hdfsEnvironment,
                new HdfsEnvironment.HdfsContext(connectorSession, "test_schema", "another_table"),
                Arrays.asList(icebergColumnHandle),
                jsonCodec,
                connectorSession,
                IcebergFileFormat.ORC,
                new HashMap<>(),
                10);
    }

    private PartitionSpec testParsePartitionFields()
    {
        ImmutableMap<String, TypeId> map = ImmutableMap.of(
                "test", TypeId.of("integer"));
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
        return PartitionFields.parsePartitionFields(nationTable.schema(), strings);
    }

    private IcebergTableHandle testCreatePageSource(IcebergFileFormat type) throws Exception
    {
        TempFile tempFile = new TempFile();
        IcebergTableHandle tableHandle;
        try {
            writeOrcContent(tempFile.file());
            IcebergSplit icebergSplit = new IcebergSplit(
                    "file:///" + tempFile.file().getAbsolutePath(),
                    0,
                    tempFile.file().length(),
                    tempFile.file().length(),
                    0, // This is incorrect, but the value is only used for delete operations
                    type,
                    ImmutableList.of(),
                    PartitionSpecParser.toJson(PartitionSpec.unpartitioned()),
                    PartitionData.toJson(new PartitionData(new Object[] {})),
                    ImmutableList.of(),
                    SplitWeight.standard());

            SchemaTableName schemaTableName = new SchemaTableName("test_schema", "test_table");
            catalog.createNamespace(connectorSession, "test_schema", Collections.emptyMap(), new PrestoPrincipal(USER, "grantee"));
            List<ConnectorMaterializedViewDefinition.Column> columns = Arrays.asList(new ConnectorMaterializedViewDefinition.Column("test", TypeId.of("integer")));
            ConnectorMaterializedViewDefinition definition = new ConnectorMaterializedViewDefinition(
                    "select 1",
                    Optional.of(new CatalogSchemaTableName("test_catalog", "test_schema", "test_table")),
                    Optional.of("value"),
                    Optional.of("value"),
                    columns,
                    Optional.of("value"),
                    Optional.of("value"),
                    new HashMap<>());
            catalog.createMaterializedView(connectorSession, schemaTableName, definition, false, false);
            List<SchemaTableName> schemaTableNames = catalog.listTables(connectorSession, Optional.of("test_schema"));
            Table nationTable = catalog.loadTable(connectorSession, schemaTableNames.stream().filter(item -> item.getTableName().startsWith("st")).findFirst().get());
            tableHandle = new IcebergTableHandle(
                    schemaTableName.getSchemaName(),
                    schemaTableName.getTableName(),
                    TableType.DATA,
                    Optional.empty(),
                    SchemaParser.toJson(nationTable.schema()),
                    PartitionSpecParser.toJson(nationTable.spec()),
                    1,
                    TupleDomain.all(),
                    TupleDomain.all(),
                    ImmutableSet.of(),
                    Optional.empty(),
                    nationTable.location(),
                    nationTable.properties(),
                    NO_RETRIES,
                    ImmutableList.of());
            return tableHandle;
        }
        catch (Exception e) {
            LOG.error(e.getMessage());
        }
        finally {
            tempFile.close();
        }
        return null;
    }

    @Test
    public void testGetCompletedBytes()
    {
        icebergPageSinkUnderTest.getSystemMemoryUsage();
        icebergPageSinkUnderTest.getValidationCpuNanos();
        icebergPageSinkUnderTest.getValidationCpuNanos();
        assertEquals(0L, icebergPageSinkUnderTest.getCompletedBytes());
    }

    @Test
    public void testAppendPage()
    {
        Page page = new Page(getArrayBlock(INTEGER));
        CompletableFuture<?> result = icebergPageSinkUnderTest.appendPage(page);
    }

    @Test
    public void testFinish()
    {
        CompletableFuture<Collection<Slice>> result = icebergPageSinkUnderTest.finish();
    }

    @Test
    public void testFinish_JsonCodecThrowsIllegalArgumentException()
    {
        CompletableFuture<Collection<Slice>> result = icebergPageSinkUnderTest.finish();
    }

    @Test
    public void testAbort()
    {
        icebergPageSinkUnderTest.abort();
    }

    @Test
    public void testGetIcebergValue1()
    {
        Object result = IcebergPageSink.getIcebergValue(getArrayBlock(BIGINT), 0, BIGINT);
    }

    @Test
    public void testGetIcebergValue2()
    {
        Object result = IcebergPageSink.getIcebergValue(getArrayBlock(INTEGER), 0, INTEGER);
    }

    @Test
    public void testGetIcebergValue3()
    {
        Object result = IcebergPageSink.getIcebergValue(getArrayBlock2(BOOLEAN), 0, BOOLEAN);
    }

    private Block getArrayBlock2(Type elementType)
    {
        BlockBuilder blockBuilder = new ArrayBlockBuilder(elementType, null, 100, 100);
        BlockBuilder elementBlockBuilder = BOOLEAN.createBlockBuilder(null, 0);
        BOOLEAN.writeBoolean(blockBuilder, false);
        blockBuilder.appendStructure(elementBlockBuilder);
        return blockBuilder;
    }

    private Block getArrayBlock(Type elementType)
    {
        int[] arraySize = new int[] {16, 0, 13, 1, 2, 11, 4, 7};
        long[][] expectedValues = new long[arraySize.length][];
        SecureRandom secureRandom = new SecureRandom();
        for (int i = 0; i < arraySize.length; i++) {
            expectedValues[i] = secureRandom.longs(arraySize[i]).toArray();
        }
        BlockBuilder blockBuilder = new ArrayBlockBuilder(elementType, null, 100, 100);
        for (long[] expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                BlockBuilder elementBlockBuilder = BIGINT.createBlockBuilder(null, expectedValue.length);
                for (long v : expectedValue) {
                    BIGINT.writeLong(elementBlockBuilder, v);
                }
                blockBuilder.appendStructure(elementBlockBuilder);
            }
        }
        return blockBuilder;
    }
}
