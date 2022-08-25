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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.hetu.core.common.util.DataSizeOfUtil;
import io.hetu.core.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.hetu.core.plugin.iceberg.catalog.TrinoCatalog;
import io.hetu.core.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.hetu.core.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.hetu.core.plugin.iceberg.delete.DummyFileScanTask;
import io.hetu.core.plugin.iceberg.procedure.OptimizeTableProcedure;
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
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.metastore.CachingHiveMetastore;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.orc.OrcReaderConfig;
import io.prestosql.plugin.hive.orc.OrcWriterConfig;
import io.prestosql.plugin.hive.parquet.ParquetReaderConfig;
import io.prestosql.spi.SplitWeight;
import io.prestosql.spi.block.IntArrayBlock;
import io.prestosql.spi.connector.BeginTableExecuteResult;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMaterializedViewDefinition;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableExecuteHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.MaterializedViewFreshness;
import io.prestosql.spi.connector.ProjectionApplicationResult;
import io.prestosql.spi.connector.RetryMode;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.connector.TableColumnsMetadata;
import io.prestosql.spi.connector.TableProcedureMetadata;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.expression.FieldDereference;
import io.prestosql.spi.expression.Variable;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.statistics.ColumnStatisticMetadata;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.statistics.TableStatisticType;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.TypeId;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.spi.type.testing.TestingTypeManager;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.planner.TestingConnectorTransactionHandle;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.AbstractTestQueryFramework;
import io.prestosql.tests.DistributedQueryRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Table;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.hetu.core.plugin.iceberg.IcebergFileFormat.ORC;
import static io.hetu.core.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.hetu.core.plugin.iceberg.util.IcebergTestUtil.validatePositive;
import static io.prestosql.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.prestosql.spi.session.PropertyMetadata.booleanProperty;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class IcebergMetadataTest
        extends AbstractTestQueryFramework
{
    private static final ObjectMapper SORTED_MAPPER = new ObjectMapperProvider().get().configure(ORDER_MAP_ENTRIES_BY_KEYS, true);
    private JsonCodec<CommitTaskData> mockCommitTaskCodec;
    private TrinoCatalog mockCatalog;
    private IcebergMetadata icebergMetadata;
    private HdfsEnvironment mockHdfsEnvironment;
    private ConnectorSession connectorSession;
    private static final CatalogSchemaName catalogSchemaName = new CatalogSchemaName("iceberg", "test_schema");
    private CachingHiveMetastore cachingHiveMetastore;
    private IcebergTableOperationsProvider mockTableOperationsProvider;
    private TrinoHiveCatalog trinoHiveCatalog;
    private TypeManager mockTypeManager;
    private TestSchemaMetadata metadata;
    private IcebergPageSourceProvider icebergPageSourceProvider;
    private HiveMetastore hiveMetastore;

    protected IcebergMetadataTest()
    {
        super(IcebergMetadataTest::createQueryRunner);
    }

    private static QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("test")
                .setSchema("default")
                .build();
        return new DistributedQueryRunner(session, 1);
    }

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        mockTypeManager = new TestingTypeManager();
        metadata = IcebergTestUtil.getMetadata();

        SessionPropertyManager sessionPropertyManager = metadata.metadata.getSessionPropertyManager();
        HashMap<CatalogName, Map<String, String>> catalogNameMapHashMap = new HashMap<>();
        CatalogName testCatalog = new CatalogName("test_catalog");
        catalogNameMapHashMap.put(testCatalog, ImmutableMap.of("projection_pushdown_enabled", "true", "statistics_enabled", "true"));

        List<PropertyMetadata<?>> sessionProperties = ImmutableList.of(
                dataSizeProperty(
                        "parquet_max_read_block_size",
                        "Parquet: Maximum size of a block to read",
                        new DataSize(8, MEGABYTE),
                        false),
                booleanProperty(
                        "orc_bloom_filters_enabled",
                        "ORC: Enable bloom filters for predicate pushdown",
                        true,
                        false),
                booleanProperty(
                        "orc_nested_lazy_enabled",
                        "Experimental: ORC: Lazily read nested data",
                        true,
                        false),
                booleanProperty(
                        "orc_lazy_read_small_ranges",
                        "Experimental: ORC: Read small file segments lazily",
                        true,
                        false),
                PropertyMetadata.dataSizeProperty(
                        "orc_max_read_block_size",
                        "ORC: Soft max size of Presto blocks produced by ORC reader",
                        new DataSize(8, MEGABYTE),
                        false),
                dataSizeProperty(
                        "orc_tiny_stripe_threshold",
                        "ORC: Threshold below which an ORC stripe or file will read in its entirety",
                        new DataSize(8, MEGABYTE),
                        false),
                PropertyMetadata.dataSizeProperty(
                        "orc_stream_buffer_size",
                        "ORC: Size of buffer for streaming reads",
                        new DataSize(8, MEGABYTE),
                        false),
                PropertyMetadata.dataSizeProperty(
                        "orc_max_buffer_size",
                        "ORC: Maximum size of a single read",
                        new DataSize(8, MEGABYTE),
                        false),
                dataSizeProperty(
                        "orc_max_merge_distance",
                        "ORC: Maximum size of gap between two reads to merge into a single read",
                        new DataSize(100, MEGABYTE),
                        false),
                booleanProperty(
                        "statistics_enabled",
                        "test property",
                        true,
                        false),
                booleanProperty(
                        "projection_pushdown_enabled",
                        "test property",
                        true,
                        false),
                new PropertyMetadata<>(
                        "positive_property",
                        "property that should be positive",
                        INTEGER,
                        Integer.class,
                        null,
                        false,
                        value -> validatePositive(value),
                        value -> value));
        sessionPropertyManager.addConnectorSessionProperties(testCatalog, sessionProperties);
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
        mockCatalog = new TrinoHiveCatalog(
                new CatalogName("test_catalog"),
                cachingHiveMetastore,
                mockHdfsEnvironment,
                mockTypeManager,
                tableOperationsProvider,
                "test",
                false,
                false,
                false);
        mockCommitTaskCodec = new JsonCodecFactory(
                () -> new ObjectMapperProvider().get())
                .jsonCodec(CommitTaskData.class);
        icebergMetadata = new IcebergMetadata(mockTypeManager, mockCommitTaskCodec, mockCatalog, mockHdfsEnvironment);

        mockTableOperationsProvider = new FileMetastoreTableOperationsProvider(new HdfsFileIoProvider(mockHdfsEnvironment));
        trinoHiveCatalog = new TrinoHiveCatalog(new CatalogName("catalogName"), cachingHiveMetastore,
                mockHdfsEnvironment, mockTypeManager, mockTableOperationsProvider, "trinoVersion", false, false, false);

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
        trinoHiveCatalog.createMaterializedView(IcebergTestUtil.getConnectorSession(metadata), schemaTableName, definition, false, false);

        OptimizeTableProcedure optimizeTableProcedure = new OptimizeTableProcedure();
        TableProcedureMetadata tableProcedureMetadata = optimizeTableProcedure.get();
        FileFormatDataSourceStats fileFormatDataSourceStats = new FileFormatDataSourceStats();
        OrcReaderConfig orcReaderConfig = new OrcReaderConfig();
        ParquetReaderConfig parquetReaderConfig = new ParquetReaderConfig();
        HdfsFileIoProvider hdfsFileIoProvider = new HdfsFileIoProvider(mockHdfsEnvironment);
        IcebergFileWriterFactory icebergFileWriterFactory = new IcebergFileWriterFactory(
                mockHdfsEnvironment,
                mockTypeManager,
                new NodeVersion("trino-version"),
                fileFormatDataSourceStats,
                new OrcWriterConfig());
        JoinCompiler joinCompiler = new JoinCompiler(metadata.metadata);
        GroupByHashPageIndexerFactory groupByHashPageIndexerFactory = new GroupByHashPageIndexerFactory(joinCompiler);
        IcebergConfig icebergConfig = new IcebergConfig();
        icebergPageSourceProvider = new IcebergPageSourceProvider(
                mockHdfsEnvironment,
                fileFormatDataSourceStats,
                orcReaderConfig,
                parquetReaderConfig,
                mockTypeManager,
                hdfsFileIoProvider,
                mockCommitTaskCodec,
                icebergFileWriterFactory,
                groupByHashPageIndexerFactory,
                icebergConfig);
    }

    @Test
    public void testSetTableProperties()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        icebergMetadata.setTableProperties(connectorSession, tableHandles.stream().findFirst().get(), ImmutableMap.of("format_version", Optional.of(2)));
    }

    @Test
    public void testSetTableProperties_NOT_SUPPORTED()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        icebergMetadata.setTableProperties(connectorSession, tableHandles.stream().findFirst().get(), ImmutableMap.of("test", Optional.of(2)));
    }

    @Test
    public void testSetTableProperties_FILE_FORMAT_PROPERTY()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        icebergMetadata.setTableProperties(connectorSession, tableHandles.stream().findFirst().get(), ImmutableMap.of("format", Optional.of(ORC)));
    }

    @Test
    public void testSetTableProperties_partitioning()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        icebergMetadata.setTableProperties(connectorSession, tableHandles.stream().findFirst().get(), ImmutableMap.of("partitioning", Optional.of(Arrays.asList("test"))));
    }

    @Test
    public void testListSchemaNames()
    {
        icebergMetadata.listSchemaNames(connectorSession);
    }

    @Test
    public void testGetSchemaProperties()
    {
        icebergMetadata.getSchemaProperties(connectorSession, catalogSchemaName);
    }

    @Test
    public void testGetSchemaOwner()
    {
        icebergMetadata.getSchemaOwner(connectorSession, catalogSchemaName);
    }

    @Test
    public void testGetTableHandle()
    {
        getTableHandles();
    }

    @Test
    public void testGetSystemTable()
    {
        List<TableType> collect = Stream.of(TableType.DATA,
                TableType.HISTORY,
                TableType.SNAPSHOTS,
                TableType.PARTITIONS,
                TableType.MANIFESTS,
                TableType.FILES,
                TableType.PROPERTIES).collect(Collectors.toList());
        List<String> strings = new ArrayList<>();
        collect.forEach(item -> {
            try {
                String tableName = "test" + item.name();
                SchemaTableName schemaTableName = new SchemaTableName("test_schema", tableName);
                strings.add(tableName + "$" + item.name());
                Map<String, Object> properties = new HashMap<>();
                properties.put("format", IcebergFileFormat.PARQUET);
                ConnectorTableMetadata connectorTableMetadata = new ConnectorTableMetadata(schemaTableName, Arrays.asList(new ColumnMetadata("test", VARCHAR)), properties);
                icebergMetadata.createTable(connectorSession, connectorTableMetadata, false);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
        strings.forEach(item -> {
            SchemaTableName schemaTableName = new SchemaTableName("test_schema", item);
            try {
                Optional<SystemTable> systemTable = icebergMetadata.getSystemTable(connectorSession, schemaTableName);
                if (item.contains("FILES") && systemTable.isPresent()) {
                    SystemTable systemTable1 = systemTable.get();
                    ConnectorTableMetadata tableMetadata = systemTable1.getTableMetadata();
                    SystemTable.Distribution distribution = systemTable1.getDistribution();
                    TestingConnectorTransactionHandle instance = TestingConnectorTransactionHandle.INSTANCE;
                    Map<Integer, Domain> domains = ImmutableMap.<Integer, Domain>builder()
                            .put(1, Domain.singleValue(BIGINT, 1L))
                            .put(2, Domain.singleValue(BIGINT, 2L))
                            .put(3, Domain.singleValue(BIGINT, 3L))
                            .build();
                    TupleDomain<Integer> domain = TupleDomain.withColumnDomains(domains);
                    ConnectorPageSource connectorPageSource = systemTable1.pageSource(instance, connectorSession, domain);
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void testGetSystemTable1()
    {
        List<String> list = Stream.of(TableType.FILES.name(),
                TableType.HISTORY.name(),
                TableType.SNAPSHOTS.name(),
                TableType.PARTITIONS.name(),
                TableType.MANIFESTS.name(),
                TableType.FILES.name(),
                TableType.PROPERTIES.name()).collect(Collectors.toList());
        list.forEach(item -> {
            SchemaTableName schemaTableName = new SchemaTableName("test_schema", Thread.currentThread().getName() + "$" + item);
            Map<String, Object> properties = new HashMap<>();
            properties.put("format", IcebergFileFormat.PARQUET);
            ConnectorTableMetadata connectorTableMetadata = new ConnectorTableMetadata(schemaTableName, Arrays.asList(new ColumnMetadata("test", VARCHAR)), properties);
            try {
                icebergMetadata.createTable(connectorSession, connectorTableMetadata, false);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
        List<SchemaTableName> schemaTableNames = icebergMetadata.listTables(connectorSession, Optional.of("test_schema"));
        schemaTableNames.forEach(item -> {
            try {
                icebergMetadata.getSystemTable(connectorSession, item);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private List<IcebergTableHandle> getTableHandles()
    {
        List<SchemaTableName> list = icebergMetadata.listTables(connectorSession, Optional.of("test_schema"));
        List<IcebergTableHandle> handles = new ArrayList<>();
        list.forEach(item -> {
            IcebergTableHandle tableHandle = icebergMetadata.getTableHandle(connectorSession, item);
            handles.add(tableHandle);
        });
        handles.removeIf(Objects::isNull);
        return handles;
    }

    @Test
    public void testCreatePageSource()
    {
        IcebergColumnHandle icebergColumnHandle = new IcebergColumnHandle(
                new ColumnIdentity(1, "test", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                BIGINT,
                ImmutableList.of(),
                BIGINT,
                Optional.empty());
        List<IcebergTableHandle> tableHandles = getTableHandles();
        tableHandles.forEach(item -> {
            try {
                String a = "/tmp/TestTpcdsCostBase6532466639682776674/test_schema/test_schema/st_aa0d0cb7785b49a58cfd1ce2217731c1/metadata/snap-4159755072727657893-1-2a2acaf9-68c2-4f76-812c-487208747cee.avro";
                String tableLocation = item.getTableLocation();
                IcebergSplit split = new IcebergSplit(
                        a,
                        0,
                        a.length(),
                        a.length(),
                        0, // This is incorrect, but the value is only used for delete operations
                        PARQUET,
                        ImmutableList.of(),
                        PartitionSpecParser.toJson(PartitionSpec.unpartitioned()),
                        PartitionData.toJson(new PartitionData(new Object[] {})),
                        ImmutableList.of(),
                        SplitWeight.standard());
                ConnectorPageSource result = icebergPageSourceProvider.createPageSource(
                        TestingConnectorTransactionHandle.INSTANCE,
                        connectorSession,
                        split,
                        item,
                        Arrays.asList(icebergColumnHandle));
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void testCreatePageSourceOrc()
    {
        IcebergColumnHandle icebergColumnHandle = new IcebergColumnHandle(
                new ColumnIdentity(1, "test", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                BIGINT,
                ImmutableList.of(),
                BIGINT,
                Optional.empty());
        List<IcebergTableHandle> tableHandles = getTableHandles();
        tableHandles.forEach(item -> {
            try {
                String a = "/tmp/TestTpcdsCostBase6532466639682776674/test_schema/test_schema/st_aa0d0cb7785b49a58cfd1ce2217731c1/metadata/snap-4159755072727657893-1-2a2acaf9-68c2-4f76-812c-487208747cee.avro";
                String tableLocation = item.getTableLocation();
                IcebergSplit split = new IcebergSplit(
                        a,
                        0,
                        a.length(),
                        a.length(),
                        0, // This is incorrect, but the value is only used for delete operations
                        ORC,
                        ImmutableList.of(),
                        PartitionSpecParser.toJson(PartitionSpec.unpartitioned()),
                        PartitionData.toJson(new PartitionData(new Object[] {})),
                        ImmutableList.of(),
                        SplitWeight.standard());
                ConnectorPageSource result = icebergPageSourceProvider.createPageSource(
                        TestingConnectorTransactionHandle.INSTANCE,
                        connectorSession,
                        split,
                        item,
                        Arrays.asList(icebergColumnHandle));
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void testGetTableProperties()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        tableHandles.forEach(item -> {
            try {
                ConnectorTableProperties result = icebergMetadata.getTableProperties(connectorSession, item);
            }
            catch (Exception e) {
            }
        });
    }

    @Test
    public void testGetTableMetadata1()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        tableHandles.forEach(item -> {
            try {
                ConnectorTableMetadata result = icebergMetadata.getTableMetadata(connectorSession, item);
            }
            catch (Exception e) {
            }
        });
    }

    @Test
    public void testListTables()
    {
        List<SchemaTableName> result = icebergMetadata.listTables(connectorSession, Optional.of("test_schema"));
    }

    @Test
    public void testGetColumnHandles()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        tableHandles.forEach(item -> {
            try {
                Map<String, ColumnHandle> result = icebergMetadata.getColumnHandles(connectorSession, item);
            }
            catch (Exception e) {
            }
        });
    }

    @Test
    public void testGetColumnMetadata()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        tableHandles.forEach(item -> {
            try {
                Map<String, ColumnHandle> columnHandles = icebergMetadata.getColumnHandles(connectorSession, item);
                ColumnMetadata result = icebergMetadata.getColumnMetadata(connectorSession, item, columnHandles.values().stream().findFirst().get());
            }
            catch (Exception e) {
            }
        });
    }

    @Test
    public void testListTableColumns()
    {
        SchemaTablePrefix schemaTablePrefix = new SchemaTablePrefix("test_schema", "another_table");
        Map<SchemaTableName, List<ColumnMetadata>> result = icebergMetadata.listTableColumns(connectorSession, schemaTablePrefix);
    }

    @Test
    public void testStreamTableColumns()
    {
        SchemaTablePrefix schemaTablePrefix = new SchemaTablePrefix("test_schema", "another_table");
        Stream<TableColumnsMetadata> result = icebergMetadata.streamTableColumns(connectorSession, schemaTablePrefix);
    }

    private void testCreateSchema()
    {
        Map<String, Object> properties = new HashMap<>();
        properties.put("format", IcebergFileFormat.PARQUET);
        PrestoPrincipal owner = new PrestoPrincipal(PrincipalType.USER, "test");
        icebergMetadata.createSchema(connectorSession, "schemaName", properties, owner);
    }

    @Test
    public void testDropSchema()
    {
        testCreateSchema();
        icebergMetadata.dropSchema(connectorSession, "schemaName");
        icebergMetadata.listSchemaNames(connectorSession);
    }

    @Test
    public void testRenameSchema()
    {
        testCreateSchema();
        icebergMetadata.renameSchema(connectorSession, "schemaName", "newschemaName");
    }

    @Test
    public void testSetSchemaAuthorization()
    {
        testCreateSchema();
        PrestoPrincipal principal = new PrestoPrincipal(PrincipalType.USER, "newname");
        icebergMetadata.setSchemaAuthorization(connectorSession, "schemaName", principal);
    }

    @Test
    public void testCreateTable()
    {
        SchemaTableName schemaTableName = new SchemaTableName("test_schema", "test_table");
        Map<String, Object> properties = new HashMap<>();
        properties.put("format", IcebergFileFormat.PARQUET);
        ConnectorTableMetadata connectorTableMetadata = new ConnectorTableMetadata(schemaTableName, Arrays.asList(new ColumnMetadata("test", VARCHAR)), properties);
        icebergMetadata.createTable(connectorSession, connectorTableMetadata, false);
    }

    @Test
    public void testSetTableComment()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        tableHandles.forEach(item -> {
            try {
                icebergMetadata.setTableComment(connectorSession, item, Optional.of("testComment"));
            }
            catch (Exception e) {
            }
        });
    }

    @Test
    public void testGetNewTableLayout()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        tableHandles.forEach(item -> {
            try {
                ConnectorTableMetadata connectorTableMetadata = icebergMetadata.getTableMetadata(connectorSession, item);
                Optional<ConnectorNewTableLayout> result = icebergMetadata.getNewTableLayout(connectorSession, connectorTableMetadata);
            }
            catch (Exception e) {
            }
        });
    }

    @Test
    public void testBeginCreateTable()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        tableHandles.forEach(item -> {
            try {
                ConnectorTableMetadata connectorTableMetadata = icebergMetadata.getTableMetadata(connectorSession, item);
                IcebergColumnHandle icebergColumnHandle = new IcebergColumnHandle(
                        new ColumnIdentity(1, "test", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                        BIGINT,
                        ImmutableList.of(),
                        BIGINT,
                        Optional.empty());
                IcebergPartitioningHandle icebergPartitioningHandle = new IcebergPartitioningHandle(Arrays.asList("1"), Arrays.asList(icebergColumnHandle));
                ConnectorNewTableLayout connectorNewTableLayout = new ConnectorNewTableLayout(icebergPartitioningHandle, Arrays.asList("1"));
                icebergMetadata.beginCreateTable(connectorSession, connectorTableMetadata, Optional.of(connectorNewTableLayout), RetryMode.NO_RETRIES);
            }
            catch (Exception e) {
            }
        });
    }

    private Collection<Slice> getFragments(FileContent fileContent, long deletedRowCount)
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        Collection<Slice> fragments = new ArrayList<>();
        tableHandles.forEach(item -> {
            try {
                ConnectorTableMetadata connectorTableMetadata = icebergMetadata.getTableMetadata(connectorSession, item);
                IcebergColumnHandle icebergColumnHandle = new IcebergColumnHandle(
                        new ColumnIdentity(1, "test", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                        BIGINT,
                        ImmutableList.of(),
                        BIGINT,
                        Optional.empty());
                IcebergPartitioningHandle icebergPartitioningHandle = new IcebergPartitioningHandle(Arrays.asList("1"), Arrays.asList(icebergColumnHandle));
                ConnectorNewTableLayout connectorNewTableLayout = new ConnectorNewTableLayout(icebergPartitioningHandle, Arrays.asList("1"));
                ConnectorOutputTableHandle connectorOutputTableHandle = icebergMetadata.beginCreateTable(connectorSession, connectorTableMetadata, Optional.of(connectorNewTableLayout), RetryMode.NO_RETRIES);
                Long recordCount = 123L;
                Map<Integer, Long> columnSizes = ImmutableMap.of(3, 321L, 5, 543L);
                Map<Integer, Long> valueCounts = ImmutableMap.of(7, 765L, 9, 987L);
                Map<Integer, Long> nullValueCounts = ImmutableMap.of(2, 234L, 4, 456L);
                Map<Integer, Long> nanValueCounts = ImmutableMap.of(1, 2L, 3, 4L);
                Map<Integer, ByteBuffer> lowerBounds = ImmutableMap.of(13, ByteBuffer.wrap(new byte[] {0, 8, 9}));
                Map<Integer, ByteBuffer> upperBounds = ImmutableMap.of(17, ByteBuffer.wrap(new byte[] {5, 4, 0}));
                Metrics metrics = new Metrics(recordCount, columnSizes, valueCounts, nullValueCounts, nanValueCounts, lowerBounds, upperBounds);
                CommitTaskData task = new CommitTaskData(
                        "/outputPath",
                        IcebergFileFormat.PARQUET,
                        10L,
                        new MetricsWrapper(metrics),
                        PartitionSpecParser.toJson(PartitionSpec.unpartitioned()),
                        Optional.of(item.getPartitionSpecJson()),
                        fileContent,
                        Optional.of(item.getTableLocation()),
                        Optional.of(10L),
                        Optional.of(deletedRowCount));
                Slice slice = Slices.wrappedBuffer(SORTED_MAPPER.writeValueAsBytes(task));
                fragments.add(slice);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
        return fragments;
    }

    private Collection<Slice> getFragments2(FileContent fileContent, long deletedRowCount)
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        Collection<Slice> fragments = new ArrayList<>();
        tableHandles.forEach(item -> {
            try {
                Long recordCount = 123L;
                Map<Integer, Long> columnSizes = ImmutableMap.of(3, 321L, 5, 543L);
                Map<Integer, Long> valueCounts = ImmutableMap.of(7, 765L, 9, 987L);
                Map<Integer, Long> nullValueCounts = ImmutableMap.of(2, 234L, 4, 456L);
                Map<Integer, Long> nanValueCounts = ImmutableMap.of(1, 2L, 3, 4L);
                Map<Integer, ByteBuffer> lowerBounds = ImmutableMap.of(13, ByteBuffer.wrap(new byte[] {0, 8, 9}));
                Map<Integer, ByteBuffer> upperBounds = ImmutableMap.of(17, ByteBuffer.wrap(new byte[] {5, 4, 0}));
                Metrics metrics = new Metrics(recordCount, columnSizes, valueCounts, nullValueCounts, nanValueCounts, lowerBounds, upperBounds);
                CommitTaskData task = new CommitTaskData(
                        "/outputPath",
                        IcebergFileFormat.PARQUET,
                        10L,
                        new MetricsWrapper(metrics),
                        PartitionSpecParser.toJson(PartitionSpec.unpartitioned()),
                        Optional.of(item.getPartitionSpecJson()),
                        fileContent,
                        Optional.of(item.getTableLocation()),
                        Optional.of(10L),
                        Optional.of(deletedRowCount));
                Slice slice = Slices.wrappedBuffer(SORTED_MAPPER.writeValueAsBytes(task));
                fragments.add(slice);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
        return fragments;
    }

    @Test
    public void testFinishCreateTable()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        tableHandles.forEach(item -> {
            try {
                ConnectorTableMetadata connectorTableMetadata = icebergMetadata.getTableMetadata(connectorSession, item);
                IcebergColumnHandle icebergColumnHandle = new IcebergColumnHandle(
                        new ColumnIdentity(1, "test", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                        BIGINT,
                        ImmutableList.of(),
                        BIGINT,
                        Optional.empty());
                IcebergPartitioningHandle icebergPartitioningHandle = new IcebergPartitioningHandle(Arrays.asList("1"), Arrays.asList(icebergColumnHandle));
                ConnectorNewTableLayout connectorNewTableLayout = new ConnectorNewTableLayout(icebergPartitioningHandle, Arrays.asList("1"));
                ConnectorOutputTableHandle connectorOutputTableHandle = icebergMetadata.beginCreateTable(connectorSession, connectorTableMetadata, Optional.of(connectorNewTableLayout), RetryMode.NO_RETRIES);
                Long recordCount = 123L;
                Map<Integer, Long> columnSizes = ImmutableMap.of(3, 321L, 5, 543L);
                Map<Integer, Long> valueCounts = ImmutableMap.of(7, 765L, 9, 987L);
                Map<Integer, Long> nullValueCounts = ImmutableMap.of(2, 234L, 4, 456L);
                Map<Integer, Long> nanValueCounts = ImmutableMap.of(1, 2L, 3, 4L);
                Map<Integer, ByteBuffer> lowerBounds = ImmutableMap.of(13, ByteBuffer.wrap(new byte[] {0, 8, 9}));
                Map<Integer, ByteBuffer> upperBounds = ImmutableMap.of(17, ByteBuffer.wrap(new byte[] {5, 4, 0}));
                Metrics metrics = new Metrics(recordCount, columnSizes, valueCounts, nullValueCounts, nanValueCounts, lowerBounds, upperBounds);
                CommitTaskData task = new CommitTaskData(
                        "/outputPath",
                        IcebergFileFormat.PARQUET,
                        10L,
                        new MetricsWrapper(metrics),
                        PartitionSpecParser.toJson(PartitionSpec.unpartitioned()),
                        Optional.of(item.getPartitionSpecJson()),
                        FileContent.POSITION_DELETES,
                        Optional.empty(),
                        Optional.of(10L),
                        Optional.of(20L));

                Collection<Slice> fragments = Arrays.asList(Slices.utf8Slice(task.toString()));
                IntArrayBlock intArrayBlock = new IntArrayBlock(1, Optional.empty(), getValues(1024));
                ComputedStatistics.Builder test = ComputedStatistics.builder(Arrays.asList("test"), Arrays.asList(intArrayBlock));
                test.addTableStatistic(TableStatisticType.ROW_COUNT, intArrayBlock);
                ColumnStatisticMetadata columnStatisticMetadata = new ColumnStatisticMetadata("test", ColumnStatisticType.MIN_VALUE);
                test.addColumnStatistic(columnStatisticMetadata, intArrayBlock);
                List<ComputedStatistics> computedStatistics = Arrays.asList(test.build());
                Optional<ConnectorOutputMetadata> result = icebergMetadata.finishCreateTable(connectorSession, connectorOutputTableHandle, fragments, computedStatistics);
            }
            catch (Exception e) {
            }
        });
    }

    private int[] getValues(int count)
    {
        int[] values = new int[count];
        for (int i = 0; i < values.length; i++) {
            values[i] = i;
        }
        return values;
    }

    @Test
    public void testFinishCreateTable_JsonCodecThrowsIllegalArgumentException() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorOutputTableHandle tableHandle = null;
        final Collection<Slice> fragments = Arrays.asList();
        final Collection<ComputedStatistics> computedStatistics = Arrays.asList(
                ComputedStatistics.restoreComputedStatistics("state", null));
        when(mockCommitTaskCodec.fromJson(any(byte[].class))).thenThrow(IllegalArgumentException.class);

        // Configure HdfsEnvironment.getFileSystem(...).
        final FileSystem spyFileSystem = spy(FileSystem.get(new Configuration(false)));
        when(mockHdfsEnvironment.getFileSystem(any(HdfsEnvironment.HdfsContext.class),
                eq(new Path("scheme", "authority", "path")))).thenReturn(spyFileSystem);

        // Run the test
        final Optional<ConnectorOutputMetadata> result = icebergMetadata.finishCreateTable(session,
                tableHandle, fragments, computedStatistics);

        // Verify the results
        verify(spyFileSystem).close();
    }

    @Test
    public void testFinishCreateTable_HdfsEnvironmentThrowsIOException() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorOutputTableHandle tableHandle = null;
        final Collection<Slice> fragments = Arrays.asList();
        final Collection<ComputedStatistics> computedStatistics = Arrays.asList(
                ComputedStatistics.restoreComputedStatistics("state", null));

        // Configure JsonCodec.fromJson(...).
//        final CommitTaskData commitTaskData = new CommitTaskData("path", 0L,
//                new MetricsWrapper(0L, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(),
//                        new HashMap<>(), new HashMap<>()), Optional.of("value"));
        final CommitTaskData commitTaskData = new CommitTaskData(
                "path",
                ORC,
                1,
                new MetricsWrapper(0L, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>()),
                "partitionSpecJson",
                Optional.of("value"),
                FileContent.DATA,
                Optional.of("value"),
                Optional.of(Long.getLong("1")),
                Optional.of(Long.getLong("1")));
        when(mockCommitTaskCodec.fromJson(any(byte[].class))).thenReturn(commitTaskData);

        when(mockHdfsEnvironment.getFileSystem(any(HdfsEnvironment.HdfsContext.class),
                eq(new Path("scheme", "authority", "path"))))
                .thenThrow(IOException.class);

        // Run the test
        final Optional<ConnectorOutputMetadata> result = icebergMetadata.finishCreateTable(session,
                tableHandle, fragments, computedStatistics);

        // Verify the results
    }

    @Test
    public void testGetInsertLayout()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        tableHandles.forEach(item -> {
            try {
                Optional<ConnectorNewTableLayout> result = icebergMetadata.getInsertLayout(connectorSession, item);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private ConnectorInsertTableHandle testBeginInsert(RetryMode...retryModes)
    {
        if (retryModes.length == 0) {
            retryModes = new RetryMode[]{RetryMode.NO_RETRIES};
        }
        List<IcebergTableHandle> tableHandles = getTableHandles();
        IcebergColumnHandle icebergColumnHandle = new IcebergColumnHandle(
                new ColumnIdentity(1, "test", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                BIGINT,
                ImmutableList.of(),
                BIGINT,
                Optional.empty());
        return icebergMetadata.beginInsert(connectorSession, tableHandles.stream().findFirst().get(), Arrays.asList(icebergColumnHandle), retryModes[0]);
    }

    @Test
    public void testBeginInsert1()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        icebergMetadata.beginInsert(connectorSession, tableHandles.stream().findFirst().get());
    }

    @Test
    public void testFinishInsert()
    {
        ConnectorInsertTableHandle connectorInsertTableHandle = testBeginInsert();
        IntArrayBlock intArrayBlock = new IntArrayBlock(1, Optional.empty(), getValues(1024));
        ComputedStatistics.Builder test = ComputedStatistics.builder(Arrays.asList("test"), Arrays.asList(intArrayBlock));
        test.addTableStatistic(TableStatisticType.ROW_COUNT, intArrayBlock);
        ColumnStatisticMetadata columnStatisticMetadata = new ColumnStatisticMetadata("test", ColumnStatisticType.MIN_VALUE);
        test.addColumnStatistic(columnStatisticMetadata, intArrayBlock);
        List<ComputedStatistics> computedStatistics = Arrays.asList(test.build());
        Optional<ConnectorOutputMetadata> result = icebergMetadata.finishInsert(connectorSession, connectorInsertTableHandle, getFragments(FileContent.POSITION_DELETES, 10L), computedStatistics);
    }

    @Test
    public void testFinishInsert1()
    {
        testCreateTable();
        Collection<Slice> fragments = getFragments2(FileContent.POSITION_DELETES, 10L);
        ConnectorInsertTableHandle connectorInsertTableHandle = testBeginInsert(RetryMode.RETRIES_ENABLED);
        IntArrayBlock intArrayBlock = new IntArrayBlock(1, Optional.empty(), getValues(1024));
        ComputedStatistics.Builder test = ComputedStatistics.builder(Arrays.asList("test"), Arrays.asList(intArrayBlock));
        test.addTableStatistic(TableStatisticType.ROW_COUNT, intArrayBlock);
        ColumnStatisticMetadata columnStatisticMetadata = new ColumnStatisticMetadata("test", ColumnStatisticType.MIN_VALUE);
        test.addColumnStatistic(columnStatisticMetadata, intArrayBlock);
        List<ComputedStatistics> computedStatistics = Arrays.asList(test.build());
        Optional<ConnectorOutputMetadata> result = icebergMetadata.finishInsert(connectorSession, connectorInsertTableHandle, fragments, computedStatistics);
    }

    @Test
    public void testFinishInsert_JsonCodecThrowsIllegalArgumentException() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorInsertTableHandle insertHandle = null;
        final Collection<Slice> fragments = Arrays.asList();
        final Collection<ComputedStatistics> computedStatistics = Arrays.asList(
                ComputedStatistics.restoreComputedStatistics("state", null));
        when(mockCommitTaskCodec.fromJson(any(byte[].class))).thenThrow(IllegalArgumentException.class);

        // Configure HdfsEnvironment.getFileSystem(...).
        final FileSystem spyFileSystem = spy(FileSystem.get(new Configuration(false)));
        when(mockHdfsEnvironment.getFileSystem(any(HdfsEnvironment.HdfsContext.class),
                eq(new Path("scheme", "authority", "path")))).thenReturn(spyFileSystem);

        // Run the test
        final Optional<ConnectorOutputMetadata> result = icebergMetadata.finishInsert(session, insertHandle,
                fragments, computedStatistics);

        // Verify the results
        verify(spyFileSystem).close();
    }

    @Test
    public void testFinishInsert_HdfsEnvironmentThrowsIOException() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorInsertTableHandle insertHandle = null;
        final Collection<Slice> fragments = Arrays.asList();
        final Collection<ComputedStatistics> computedStatistics = Arrays.asList(
                ComputedStatistics.restoreComputedStatistics("state", null));

        // Configure JsonCodec.fromJson(...).
//        final CommitTaskData commitTaskData = new CommitTaskData("path", 0L,
//                new MetricsWrapper(0L, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(),
//                        new HashMap<>(), new HashMap<>()), Optional.of("value"));
        final CommitTaskData commitTaskData = new CommitTaskData(
                "path",
                ORC,
                1,
                new MetricsWrapper(0L, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>()),
                "partitionSpecJson",
                Optional.of("value"),
                FileContent.DATA,
                Optional.of("value"),
                Optional.of(Long.getLong("1")),
                Optional.of(Long.getLong("1")));
        when(mockCommitTaskCodec.fromJson(any(byte[].class))).thenReturn(commitTaskData);

        when(mockHdfsEnvironment.getFileSystem(any(HdfsEnvironment.HdfsContext.class),
                eq(new Path("scheme", "authority", "path"))))
                .thenThrow(IOException.class);

        // Run the test
        final Optional<ConnectorOutputMetadata> result = icebergMetadata.finishInsert(session, insertHandle,
                fragments, computedStatistics);

        // Verify the results
    }

    @Test
    public void testGetDeleteRowIdColumnHandle()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        tableHandles.forEach(item -> {
            try {
                ColumnHandle result = icebergMetadata.getDeleteRowIdColumnHandle(connectorSession, item);
            }
            catch (Exception e) {
            }
        });
    }

    private Optional<ConnectorTableExecuteHandle> testGetTableHandleForExecute()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        Map<String, Object> map = new HashMap<>();
        DataSize of = DataSizeOfUtil.of(10485760, DataSizeOfUtil.Unit.BYTE);
        map.put("file_size_threshold", of);
        Optional<ConnectorTableExecuteHandle> result = icebergMetadata.getTableHandleForExecute(
                connectorSession, tableHandles.stream().findFirst().get(), "OPTIMIZE", map, RetryMode.NO_RETRIES);
        return result;
    }

    @Test
    public void testGetLayoutForTableExecute()
    {
        Optional<ConnectorTableExecuteHandle> connectorTableExecuteHandle = testGetTableHandleForExecute();
        Optional<ConnectorNewTableLayout> result = icebergMetadata.getLayoutForTableExecute(connectorSession, connectorTableExecuteHandle.get());
    }

    @Test
    public BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> testBeginTableExecute()
    {
        Optional<ConnectorTableExecuteHandle> connectorTableExecuteHandle = testGetTableHandleForExecute();
        IcebergTableHandle updatedSourceTableHandle = getTableHandles().stream().findFirst().get();
        BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> result = icebergMetadata.beginTableExecute(
                connectorSession, connectorTableExecuteHandle.get(), updatedSourceTableHandle);
        return result;
    }

    @Test
    public void testFinishTableExecute()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        IcebergTableHandle icebergTableHandle = tableHandles.stream().findFirst().get();
        String tableLocation = icebergTableHandle.getTableLocation();
        DummyFileScanTask dummyFileScanTask = new DummyFileScanTask(tableLocation, Collections.emptyList());
        Collection<Slice> fragments = getFragments2(FileContent.POSITION_DELETES, 10L);
        testBeginTableExecute();
        icebergMetadata.finishTableExecute(connectorSession, testGetTableHandleForExecute().get(), fragments, Arrays.asList(dummyFileScanTask.file()));
    }

    @Test
    public void testFinishTableExecute_JsonCodecThrowsIllegalArgumentException() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableExecuteHandle tableExecuteHandle = null;
        final Collection<Slice> fragments = Arrays.asList();
        when(mockCommitTaskCodec.fromJson(any(byte[].class))).thenThrow(IllegalArgumentException.class);

        // Configure HdfsEnvironment.getFileSystem(...).
        final FileSystem spyFileSystem = spy(FileSystem.get(new Configuration(false)));
        when(mockHdfsEnvironment.getFileSystem(any(HdfsEnvironment.HdfsContext.class),
                eq(new Path("scheme", "authority", "path")))).thenReturn(spyFileSystem);

        // Run the test
        icebergMetadata.finishTableExecute(session, tableExecuteHandle, fragments, Arrays.asList("value"));

        // Verify the results
        verify(spyFileSystem).close();
    }

    @Test
    public void testFinishTableExecute_HdfsEnvironmentThrowsIOException() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableExecuteHandle tableExecuteHandle = null;
        final Collection<Slice> fragments = Arrays.asList();

        // Configure JsonCodec.fromJson(...).
//        final CommitTaskData commitTaskData = new CommitTaskData("path", 0L,
//                new MetricsWrapper(0L, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(),
//                        new HashMap<>(), new HashMap<>()), Optional.of("value"));
        final CommitTaskData commitTaskData = new CommitTaskData(
                "path",
                ORC,
                1,
                new MetricsWrapper(0L, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>()),
                "partitionSpecJson",
                Optional.of("value"),
                FileContent.DATA,
                Optional.of("value"),
                Optional.of(Long.getLong("1")),
                Optional.of(Long.getLong("1")));
        when(mockCommitTaskCodec.fromJson(any(byte[].class))).thenReturn(commitTaskData);

        when(mockHdfsEnvironment.getFileSystem(any(HdfsEnvironment.HdfsContext.class),
                eq(new Path("scheme", "authority", "path"))))
                .thenThrow(IOException.class);

        // Run the test
        icebergMetadata.finishTableExecute(session, tableExecuteHandle, fragments, Arrays.asList("value"));

        // Verify the results
    }

    @Test
    public void testGetInfo()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        tableHandles.forEach(item -> {
            try {
                Optional<Object> result = icebergMetadata.getInfo(item);
            }
            catch (Exception e) {
            }
        });
    }

    @Test
    public void testDropTable()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        tableHandles.forEach(item -> {
            try {
                icebergMetadata.dropTable(connectorSession, item);
            }
            catch (Exception e) {
            }
        });
    }

    @Test
    public void testRenameTable()
    {
        SchemaTableName newTable = new SchemaTableName("test_schema", "new");
        // Run the test
        List<IcebergTableHandle> tableHandles = getTableHandles();
        tableHandles.forEach(item -> {
            try {
                icebergMetadata.renameTable(connectorSession, item, newTable);
            }
            catch (Exception e) {
            }
        });
    }

    @Test
    public void testAddColumn()
    {
        ColumnMetadata columnMetadata = new ColumnMetadata("test", VARCHAR);
        getTableHandles().forEach(item -> {
            try {
                icebergMetadata.addColumn(connectorSession, item, columnMetadata);
            }
            catch (Exception e) {
            }
        });
    }

    @Test
    public void testAddColumn_TrinoCatalogThrowsUnknownTableTypeException()
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final ColumnMetadata column = new ColumnMetadata("test", null, false, "comment", "extraInfo", false,
                new HashMap<>(), false);
        when(mockCatalog.loadTable(any(ConnectorSession.class),
                eq(new SchemaTableName("schemaName", "tableName")))).thenThrow(UnknownTableTypeException.class);

        // Run the test
        icebergMetadata.addColumn(session, tableHandle, column);

        // Verify the results
    }

    @Test
    public void testDropColumn()
    {
        getTableHandles().forEach(item -> {
            try {
                Map<String, ColumnHandle> columnHandles = icebergMetadata.getColumnHandles(connectorSession, item);
                columnHandles.values().forEach(ite -> {
                    icebergMetadata.dropColumn(connectorSession, item, ite);
                });
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void testRenameColumn()
    {
        getTableHandles().forEach(item -> {
            try {
                Map<String, ColumnHandle> columnHandles = icebergMetadata.getColumnHandles(connectorSession, item);
                columnHandles.values().forEach(it -> {
                    icebergMetadata.renameColumn(connectorSession, item, it, "new" + System.currentTimeMillis());
                });
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void testApplyDelete()
    {
        getTableHandles().forEach(item -> {
            try {
                Optional<ConnectorTableHandle> result = icebergMetadata.applyDelete(connectorSession, item);
            }
            catch (Exception e) {
            }
        });
    }

    @Test
    public void testBeginDelete()
    {
        getTableHandles().forEach(item -> {
            try {
                ConnectorTableHandle result = icebergMetadata.beginDelete(connectorSession, item);
            }
            catch (Exception e) {
            }
        });
    }

    @Test
    public void testCreateView()
    {
        SchemaTableName viewName = new SchemaTableName("test_schema", "new_another_table");
        ConnectorViewDefinition definition = new ConnectorViewDefinition(
                "select 1",
                Optional.of("test_catalog"),
                Optional.of("test_schema"),
                Arrays.asList(new ConnectorViewDefinition.ViewColumn("test",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))),
                Optional.of("owner"),
                false);
        icebergMetadata.createView(connectorSession, viewName, definition, false);
    }

    @Test
    public void testRenameView()
    {
        SchemaTableName source = new SchemaTableName("test_schema", "another_table");
        SchemaTableName target = new SchemaTableName("test_schema", "new_tableName");
        icebergMetadata.renameView(connectorSession, source, target);
    }

    @Test
    public void testSetViewAuthorization()
    {
        SchemaTableName viewName = new SchemaTableName("test_schema", "anoter_table");
        PrestoPrincipal principal = new PrestoPrincipal(PrincipalType.USER, "test");
        icebergMetadata.setViewAuthorization(connectorSession, viewName, principal);
    }

    @Test
    public void testDropView()
    {
        testListViews().forEach(item -> {
            icebergMetadata.dropView(connectorSession, item);
        });
    }

    @Test
    public List<SchemaTableName> testListViews()
    {
        testCreateView();
        List<SchemaTableName> schemaTableNames = icebergMetadata.listViews(connectorSession, Optional.of("test_schema"));
        return schemaTableNames;
    }

    @Test
    public void testGetViews()
    {
        testCreateView();
        Map<SchemaTableName, ConnectorViewDefinition> result = icebergMetadata.getViews(connectorSession, Optional.of("test_schema"));
    }

    @Test
    public void testExecuteDelete()
    {
        ConnectorTableHandle sourceHandle = testBeginTableExecute().getSourceHandle();
        OptionalLong result = icebergMetadata.executeDelete(connectorSession, sourceHandle);
    }

    @Test
    public void testRollback()
    {
        // Setup
        // Run the test
        icebergMetadata.rollback();

        // Verify the results
    }

    @Test
    public void testApplyFilter()
    {
        ImmutableMap.Builder<ColumnHandle, Domain> domains = new ImmutableMap.Builder<>();

        IcebergColumnHandle icebergColumnHandle = new IcebergColumnHandle(
                new ColumnIdentity(1, "test", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                VARCHAR,
                ImmutableList.of(),
                VARCHAR,
                Optional.empty());
        domains.put(icebergColumnHandle, Domain.singleValue(VARCHAR, Slices.utf8Slice("test_schema")));
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(domains.build()));

        ConnectorTableHandle sourceHandle = testBeginTableExecute().getSourceHandle();
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = icebergMetadata.applyFilter(
                connectorSession, sourceHandle, constraint);
    }

    @Test
    public void testApplyProjection()
    {
        ConnectorExpression doubleProjection = new Variable("double_projection", DoubleType.DOUBLE);
        ConnectorExpression connectorExpression = new FieldDereference(BooleanType.BOOLEAN, doubleProjection, 0);
        List<ConnectorExpression> projections = Stream.of(connectorExpression).collect(Collectors.toList());
        Map<String, ColumnHandle> assignments = new HashMap<>();
        ColumnIdentity child = new ColumnIdentity(2, "test2", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of());
        ColumnIdentity test = new ColumnIdentity(1, "test", ColumnIdentity.TypeCategory.ARRAY, Arrays.asList(child));
        IcebergColumnHandle icebergColumnHandle = new IcebergColumnHandle(
                test,
                VARCHAR,
                ImmutableList.of(),
                VARCHAR,
                Optional.empty());
        assignments.put("double_projection", icebergColumnHandle);
        ConnectorTableHandle sourceHandle = testBeginTableExecute().getSourceHandle();
        Optional<ProjectionApplicationResult<ConnectorTableHandle>> result = icebergMetadata.applyProjection(
                connectorSession, sourceHandle, projections, assignments);
    }

    @Test
    public void testApplyProjection2()
    {
        ConnectorExpression connectorExpression = new Variable("double_projection", DoubleType.DOUBLE);
        List<ConnectorExpression> projections = Stream.of(connectorExpression).collect(Collectors.toList());
        Map<String, ColumnHandle> assignments = new HashMap<>();
        IcebergColumnHandle icebergColumnHandle = new IcebergColumnHandle(
                new ColumnIdentity(1, "test", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                VARCHAR,
                ImmutableList.of(),
                VARCHAR,
                Optional.empty());
        assignments.put("double_projection", icebergColumnHandle);
        ConnectorTableHandle sourceHandle = testBeginTableExecute().getSourceHandle();
        Optional<ProjectionApplicationResult<ConnectorTableHandle>> result = icebergMetadata.applyProjection(
                connectorSession, sourceHandle, projections, assignments);
    }

    @Test
    public void testGetTableStatistics()
    {
        ImmutableMap.Builder<ColumnHandle, Domain> domains = new ImmutableMap.Builder<>();
        IcebergColumnHandle icebergColumnHandle = new IcebergColumnHandle(
                new ColumnIdentity(1, "test", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                VARCHAR,
                ImmutableList.of(),
                VARCHAR,
                Optional.empty());
        domains.put(icebergColumnHandle, Domain.singleValue(VARCHAR, Slices.utf8Slice("test_schema")));
        ConnectorTableHandle sourceHandle = testBeginTableExecute().getSourceHandle();
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(domains.build()));
        TableStatistics result = icebergMetadata.getTableStatistics(connectorSession, sourceHandle, constraint);
    }

    @Test
    public void testSetTableAuthorization()
    {
        PrestoPrincipal principal = new PrestoPrincipal(PrincipalType.USER, "test");
        testListViews().forEach(item -> {
            try {
                icebergMetadata.setTableAuthorization(connectorSession, item, principal);
            }
            catch (Exception e) {
            }
        });
    }

    @Test
    public void testGetIcebergTable()
    {
        testCreateTable();
        SchemaTableName schemaTableName = new SchemaTableName("test_schema", "test_table");
        Table result = icebergMetadata.getIcebergTable(connectorSession, schemaTableName);
    }

    @Test
    public void testCreateMaterializedView()
    {
        SchemaTableName schemaTableName = new SchemaTableName("test_schema", "test_table");
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
        icebergMetadata.createMaterializedView(connectorSession, schemaTableName, definition, false, false);
    }

    @Test
    public void testDropMaterializedView()
    {
        List<SchemaTableName> schemaTableNames = icebergMetadata.listMaterializedViews(connectorSession, Optional.of("test_schema"));
        icebergMetadata.dropMaterializedView(connectorSession, schemaTableNames.stream().findFirst().get());
    }

    @Test
    public void testDelegateMaterializedViewRefreshToConnector()
    {
        icebergMetadata.delegateMaterializedViewRefreshToConnector(connectorSession, new SchemaTableName("test_schema", "another_table"));
    }

    @Test
    public void testBeginRefreshMaterializedView()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        ConnectorTableHandle sourceHandle = testBeginTableExecute().getSourceHandle();
        ConnectorInsertTableHandle result = icebergMetadata.beginRefreshMaterializedView(connectorSession, tableHandles.stream().findFirst().get(), Arrays.asList(sourceHandle), RetryMode.NO_RETRIES);
    }

    @Test
    public void testFinishRefreshMaterializedView()
    {
        Collection<Slice> fragments = getFragments2(FileContent.POSITION_DELETES, 10L);
        IntArrayBlock intArrayBlock = new IntArrayBlock(1, Optional.empty(), getValues(1024));
        ComputedStatistics.Builder test = ComputedStatistics.builder(Arrays.asList("test"), Arrays.asList(intArrayBlock));
        test.addTableStatistic(TableStatisticType.ROW_COUNT, intArrayBlock);
        ColumnStatisticMetadata columnStatisticMetadata = new ColumnStatisticMetadata("test", ColumnStatisticType.MIN_VALUE);
        test.addColumnStatistic(columnStatisticMetadata, intArrayBlock);
        List<ComputedStatistics> computedStatistics = Arrays.asList(test.build());
        List<IcebergTableHandle> tableHandles = getTableHandles();
        ConnectorInsertTableHandle connectorInsertTableHandle = icebergMetadata.beginRefreshMaterializedView(connectorSession, tableHandles.stream().findFirst().get(), Arrays.asList(tableHandles.get(0)), RetryMode.NO_RETRIES);
        icebergMetadata.finishRefreshMaterializedView(connectorSession,
                tableHandles.get(0), connectorInsertTableHandle, fragments, computedStatistics, Arrays.asList(tableHandles.get(0)));
    }

    @Test
    public void testListMaterializedViews()
    {
        List<SchemaTableName> result = icebergMetadata.listMaterializedViews(connectorSession, Optional.of("test_schema"));
    }

    @Test
    public void testGetMaterializedViews()
    {
        Map<SchemaTableName, ConnectorMaterializedViewDefinition> result = icebergMetadata.getMaterializedViews(connectorSession, Optional.of("test_schema"));
    }

    @Test
    public void testRenameMaterializedView()
    {
        SchemaTableName target = new SchemaTableName("test_schema", "newtableName");
        List<SchemaTableName> result = icebergMetadata.listMaterializedViews(connectorSession, Optional.of("test_schema"));
        icebergMetadata.renameMaterializedView(connectorSession, result.stream().findFirst().get(), target);
    }

    @Test
    public void testGetMaterializedViewFreshness()
    {
        testCreateView();
        testGetMaterializedViews();
        SchemaTableName materializedViewName = new SchemaTableName("test_schema", "another_table");
        MaterializedViewFreshness result = icebergMetadata.getMaterializedViewFreshness(connectorSession, materializedViewName);
    }

    private IcebergColumnHandle getIcebergColumnHandle()
    {
        return new IcebergColumnHandle(
                new ColumnIdentity(1, "test", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                BIGINT,
                ImmutableList.of(),
                BIGINT,
                Optional.empty());
    }

    @Test
    public void testSetColumnComment()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        tableHandles.forEach(item -> {
            try {
                icebergMetadata.setColumnComment(connectorSession, item, getIcebergColumnHandle(), Optional.of("test_schema"));
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void testRedirectTable()
    {
        SchemaTableName tableName = new SchemaTableName("test_schema", "another_table");
        Optional<CatalogSchemaTableName> result = icebergMetadata.redirectTable(connectorSession, tableName);
    }

    @Test
    public void testGetUpdateRowIdColumnHandle()
    {
        IcebergColumnHandle icebergColumnHandle = getIcebergColumnHandle();
        ConnectorTableHandle sourceHandle = testBeginTableExecute().getSourceHandle();
        icebergMetadata.getUpdateRowIdColumnHandle(connectorSession, sourceHandle, Arrays.asList(icebergColumnHandle));
    }

    @Test
    public void testBeginUpdate()
    {
        List<IcebergTableHandle> tableHandles = getTableHandles();
        tableHandles.forEach(item -> {
            try {
                icebergMetadata.beginUpdate(connectorSession, item, Collections.emptyList());
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void testFinishUpdate()
    {
        testCreateTable();
        List<IcebergTableHandle> tableHandles = getTableHandles();
        Collection<Slice> fragments = getFragments(FileContent.POSITION_DELETES, 5L);
        tableHandles.forEach(item -> {
            try {
                icebergMetadata.finishUpdate(connectorSession, item, fragments);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void testFinishUpdate1()
    {
        testCreateTable();
        List<IcebergTableHandle> tableHandles = getTableHandles();
        Collection<Slice> fragments = getFragments(FileContent.POSITION_DELETES, 10L);
        tableHandles.forEach(item -> {
            try {
                icebergMetadata.finishUpdate(connectorSession, item, fragments);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void testFinishUpdate2()
    {
        testCreateTable();
        List<IcebergTableHandle> tableHandles = getTableHandles();
        Collection<Slice> fragments = getFragments(FileContent.DATA, 10L);
        tableHandles.forEach(item -> {
            try {
                icebergMetadata.finishUpdate(connectorSession, item, fragments);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
