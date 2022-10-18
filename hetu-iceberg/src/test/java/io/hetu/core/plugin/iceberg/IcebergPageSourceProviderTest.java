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
import io.prestosql.orc.OrcWriteValidation;
import io.prestosql.orc.OrcWriter;
import io.prestosql.orc.OrcWriterOptions;
import io.prestosql.orc.OrcWriterStats;
import io.prestosql.orc.OutputStreamOrcDataSink;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsConfiguration;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveHdfsConfiguration;
import io.prestosql.plugin.hive.NodeVersion;
import io.prestosql.plugin.hive.ReaderColumns;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.metastore.CachingHiveMetastore;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.orc.OrcReaderConfig;
import io.prestosql.plugin.hive.orc.OrcWriterConfig;
import io.prestosql.plugin.hive.parquet.ParquetReaderConfig;
import io.prestosql.queryeditorui.output.persistors.FlatFilePersistor;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.SplitWeight;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.IntArrayBlock;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorMaterializedViewDefinition;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.metrics.Metrics;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.AbstractType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeId;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.testing.TestingTypeManager;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.planner.TestingConnectorTransactionHandle;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.hetu.core.plugin.iceberg.ColumnIdentity.TypeCategory.ARRAY;
import static io.hetu.core.plugin.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static io.hetu.core.plugin.iceberg.IcebergFileFormat.ORC;
import static io.hetu.core.plugin.iceberg.util.IcebergTestUtil.getHiveConfig;
import static io.hetu.core.plugin.iceberg.util.IcebergTestUtil.getSession;
import static io.prestosql.orc.metadata.CompressionKind.NONE;
import static io.prestosql.plugin.hive.HiveType.HIVE_INT;
import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static io.prestosql.plugin.hive.metastore.CachingHiveMetastore.memoizeMetastore;
import static io.prestosql.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.prestosql.spi.connector.RetryMode.NO_RETRIES;
import static io.prestosql.spi.security.PrincipalType.USER;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.apache.iceberg.types.Types.NestedField.optional;

public class IcebergPageSourceProviderTest
{
    private static final Column KEY_COLUMN = new Column("a_integer", HIVE_INT, Optional.empty());
    private static final Column DATA_COLUMN = new Column("a_varchar", HIVE_STRING, Optional.empty());

    private static final ColumnIdentity child1 = new ColumnIdentity(1, "child1", PRIMITIVE, ImmutableList.of());
    private static final ColumnIdentity child2 = new ColumnIdentity(2, "child2", PRIMITIVE, ImmutableList.of());

    private static final ColumnIdentity KEY_COLUMN_IDENTITY = new ColumnIdentity(1, KEY_COLUMN.getName(), ARRAY, ImmutableList.of(child1));
    private static final ColumnIdentity DATA_COLUMN_IDENTITY = new ColumnIdentity(2, DATA_COLUMN.getName(), ARRAY, ImmutableList.of(child2));
    private static final int KEY_COLUMN_VALUE = 42;
    private static final String DATA_COLUMN_VALUE = "hello world";
    private static final Logger LOG = LoggerFactory.getLogger(FlatFilePersistor.class);
    private static final Schema TABLE_SCHEMA = new Schema(
            optional(KEY_COLUMN_IDENTITY.getId(), KEY_COLUMN.getName(), Types.IntegerType.get()),
            optional(DATA_COLUMN_IDENTITY.getId(), DATA_COLUMN.getName(), Types.StringType.get()));
    private IcebergPageSourceProvider icebergPageSourceProvider;
    private TypeManager typeManager;
    private TestSchemaMetadata metadata;
    private ConnectorSession connectorSession;
    private HdfsEnvironment hdfsEnvironment;
    private FileFormatDataSourceStats fileFormatDataSourceStats;
    private OrcReaderConfig orcReaderConfig;
    private ParquetReaderConfig parquetReaderConfig;
    private FileIoProvider hdfsFileIoProvider;
    private JsonCodec<CommitTaskData> partitionUpdateCodec;
    private IcebergFileWriterFactory icebergFileWriterFactory;
    private PageIndexerFactory pageIndexerFactory;
    private IcebergConfig icebergConfig;
    private TrinoCatalog catalog;
    private Session session;
    private CachingHiveMetastore cachingHiveMetastore;
    private String parentPath;
    private IcebergFileWriter icebergFileWriter;

    @BeforeMethod
    public void setUp() throws Exception
    {
        typeManager = new TestingTypeManager();
        metadata = IcebergTestUtil.getMetadata();
        session = getSession(metadata);

        SessionPropertyManager sessionPropertyManager = metadata.metadata.getSessionPropertyManager();
        HashMap<CatalogName, Map<String, String>> catalogNameMapHashMap = new HashMap<>();
        CatalogName testCatalog = new CatalogName("test_catalog");
        catalogNameMapHashMap.put(testCatalog, ImmutableMap.of("projection_pushdown_enabled", "true", "statistics_enabled", "true"));
        List<PropertyMetadata<?>> property = IcebergTestUtil.getProperty();
        sessionPropertyManager.addConnectorSessionProperties(testCatalog, property);
        connectorSession = IcebergTestUtil.getConnectorSession3(getSession(), sessionPropertyManager, catalogNameMapHashMap, metadata);
        String timeZone = "Asia/Kathmandu";
        HiveConfig hiveConfig = getHiveConfig()
                .setParquetTimeZone(timeZone)
                .setRcfileTimeZone(timeZone);
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveConfig), ImmutableSet.of());
        hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hiveConfig, new NoHdfsAuthentication());
        fileFormatDataSourceStats = new FileFormatDataSourceStats();
        orcReaderConfig = new OrcReaderConfig();
        parquetReaderConfig = new ParquetReaderConfig();
        hdfsFileIoProvider = new HdfsFileIoProvider(hdfsEnvironment);
        partitionUpdateCodec = JsonCodec.jsonCodec(CommitTaskData.class);
        OrcWriterConfig orcWriterConfig = new OrcWriterConfig();
        icebergFileWriterFactory = new IcebergFileWriterFactory(hdfsEnvironment, typeManager, new NodeVersion("test_version"), fileFormatDataSourceStats, orcWriterConfig);
        icebergConfig = new IcebergConfig();
        JoinCompiler joinCompiler = new JoinCompiler(metadata.metadata);
        pageIndexerFactory = new GroupByHashPageIndexerFactory(joinCompiler);
        icebergPageSourceProvider = new IcebergPageSourceProvider(
                hdfsEnvironment,
                fileFormatDataSourceStats,
                orcReaderConfig,
                parquetReaderConfig,
                typeManager,
                hdfsFileIoProvider,
                partitionUpdateCodec,
                icebergFileWriterFactory,
                pageIndexerFactory,
                icebergConfig);

        File tempDir = Files.createTempDirectory("test_iceberg_split_source").toFile();
        File metastoreDir = new File(tempDir, "iceberg_data");
        HiveMetastore metastore = createTestingFileHiveMetastore(metastoreDir);
        IcebergTableOperationsProvider operationsProvider = new FileMetastoreTableOperationsProvider(new HdfsFileIoProvider(hdfsEnvironment));
        cachingHiveMetastore = memoizeMetastore(metastore, 1000);
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
    }

    @Test
    public void testIcebergOrcFileWriter_GetMetrics() throws Exception
    {
        testCreatePageSource(ORC);
        Path path = new Path(parentPath, "/home/child");
        Schema schema = new Schema(0, Arrays.asList(Types.NestedField.required(0, "name", new Types.IntegerType())), new HashMap<>(), new HashSet<>(Arrays.asList(0)));
        ImmutableMap<String, String> of = ImmutableMap.of(
                "orc.bloom.filter.columns", "1",
                "orc.bloom.filter.fpp", "2");
        icebergFileWriter = icebergFileWriterFactory.createDataFileWriter(
                path,
                schema,
                new JobConf(IcebergFileWriterFactory.class),
                connectorSession,
                new HdfsEnvironment.HdfsContext(new HdfsFileIoProviderTest.ConnectorSession(), "test_schema", "test_table"),
                IcebergFileFormat.ORC,
                MetricsConfig.getDefault(),
                of);
        icebergFileWriter.commit();
        org.apache.iceberg.Metrics metrics = this.icebergFileWriter.getMetrics();
        System.out.println();
    }

    @Test
    public void testUpdateRowsrc() throws Exception
    {
        IcebergPageSource icebergPageSource = (IcebergPageSource) testCreatePageSource(ORC);
        Page page = icebergPageSource.getNextPage();
        icebergPageSource.updateRows(page, Arrays.asList(1), Arrays.asList("name"));
    }

    @Test
    public void testCreatePageSourceOrc() throws Exception
    {
        testCreatePageSource(ORC);
    }

    @Test
    public void testClose() throws Exception
    {
        IcebergPageSource icebergPageSource = (IcebergPageSource) testCreatePageSource(ORC);
        icebergPageSource.close();
    }

    @Test
    public void testAbort() throws Exception
    {
        IcebergPageSource icebergPageSource = (IcebergPageSource) testCreatePageSource(ORC);
        icebergPageSource.abort();
    }

    @Test
    public void testFinish() throws Exception
    {
        IcebergPageSource icebergPageSource = (IcebergPageSource) testCreatePageSource(ORC);
        icebergPageSource.finish();
    }

    @Test
    public void testDeleteRows() throws Exception
    {
        IcebergPageSource icebergPageSource = (IcebergPageSource) testCreatePageSource(ORC);
        boolean[] booleans = {false};
        Optional<boolean[]> t = Optional.of(booleans);
        IntArrayBlock intArrayBlock = new IntArrayBlock(0, t, new int[1]);
        icebergPageSource.deleteRows(intArrayBlock);
    }

    @Test
    public void testIsFinished() throws Exception
    {
        IcebergPageSource icebergPageSource = (IcebergPageSource) testCreatePageSource(ORC);
        boolean finished = icebergPageSource.isFinished();
    }

    @Test
    public void testGetReadTimeNanos() throws Exception
    {
        IcebergPageSource icebergPageSource = (IcebergPageSource) testCreatePageSource(ORC);
        long readTimeNanos = icebergPageSource.getReadTimeNanos();
    }

    @Test
    public void testGetMemoryUsage() throws Exception
    {
        IcebergPageSource icebergPageSource = (IcebergPageSource) testCreatePageSource(ORC);
        long memoryUsage = icebergPageSource.getMemoryUsage();
    }

    @Test
    public void testGetSystemMemoryUsage() throws Exception
    {
        IcebergPageSource icebergPageSource = (IcebergPageSource) testCreatePageSource(ORC);
        long systemMemoryUsage = icebergPageSource.getSystemMemoryUsage();
    }

    @Test
    public void testGetMetrics() throws Exception
    {
        IcebergPageSource icebergPageSource = (IcebergPageSource) testCreatePageSource(ORC);
        Metrics metrics = icebergPageSource.getMetrics();
    }

    @Test
    public void testGetCompletedBytes() throws Exception
    {
        IcebergPageSource icebergPageSource = (IcebergPageSource) testCreatePageSource(ORC);
        long completedBytes = icebergPageSource.getCompletedBytes();
    }

    @Test
    public void testGetCompletedPositions() throws Exception
    {
        IcebergPageSource icebergPageSource = (IcebergPageSource) testCreatePageSource(ORC);
        OptionalLong completedPositions = icebergPageSource.getCompletedPositions();
    }

    @Test
    public void testGetNextPage() throws Exception
    {
        IcebergPageSource icebergPageSource = (IcebergPageSource) testCreatePageSource(ORC);
        Page nextPage = icebergPageSource.getNextPage();
    }

    @Test
    public void testCreatePageSourceParquet() throws Exception
    {
        testCreatePageSource(IcebergFileFormat.PARQUET);
    }

    private ConnectorPageSource testCreatePageSource(IcebergFileFormat type) throws Exception
    {
        TestingConnectorTransactionHandle instance = TestingConnectorTransactionHandle.INSTANCE;
        TempFile tempFile = new TempFile();
        ConnectorPageSource pageSource;
        try {
            writeOrcContent(tempFile.file());
            parentPath = "file:///" + tempFile.file().getAbsolutePath();
            IcebergSplit icebergSplit = new IcebergSplit(
                    parentPath,
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
            IcebergTableHandle tableHandle = new IcebergTableHandle(
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
            List<IcebergColumnHandle> columnHandle = getColumnHandle();
            List<ColumnHandle> handles = new ArrayList<>();
            columnHandle.forEach(item -> {
                ColumnHandle item1 = item;
                handles.add(item1);
            });
            pageSource = icebergPageSourceProvider.createPageSource(
                    instance,
                    connectorSession,
                    icebergSplit,
                    tableHandle,
                    handles);
            return pageSource;
        }
        catch (Exception e) {
            LOG.error(e.getMessage());
        }
        finally {
            tempFile.close();
        }
        return null;
    }

    public static void writeOrcContent(File file)
            throws IOException
    {
        List<String> columnNames = ImmutableList.of(KEY_COLUMN.getName(), DATA_COLUMN.getName());
        List<Type> types = ImmutableList.of(INTEGER, VARCHAR);
        try (OutputStream out = new FileOutputStream(file); OrcWriter writer = new OrcWriter(new OutputStreamOrcDataSink(out), columnNames, types, TypeConverter.toOrcType(TABLE_SCHEMA), NONE, new OrcWriterOptions(), ImmutableMap.of(), true, OrcWriteValidation.OrcWriteValidationMode.BOTH, new OrcWriterStats())) {
            BlockBuilder keyBuilder = INTEGER.createBlockBuilder(null, 1);
            INTEGER.writeLong(keyBuilder, KEY_COLUMN_VALUE);
            BlockBuilder dataBuilder = VARCHAR.createBlockBuilder(null, 1);
            VARCHAR.writeString(dataBuilder, DATA_COLUMN_VALUE);
            writer.write(new Page(keyBuilder.build(), dataBuilder.build()));
        }
    }

    public static List<IcebergColumnHandle> getColumnHandle()
    {
        Map<AbstractType, Object> map = new HashMap<>();
        map.put(INTEGER, 1);
        map.put(VARCHAR, 2);
        List<IcebergColumnHandle> handles = new ArrayList<>();
        map.forEach((k, v) -> {
            IcebergColumnHandle bigintColumn = new IcebergColumnHandle(
                    new ColumnIdentity(Integer.valueOf(v.toString()), "name", ColumnIdentity.TypeCategory.ARRAY,
                            ImmutableList.of(new ColumnIdentity(Integer.valueOf(v.toString()), "childName", PRIMITIVE,
                                    ImmutableList.of()))),
                    k,
                    ImmutableList.of(Integer.valueOf(v.toString())),
                    k,
                    Optional.empty());
            handles.add(bigintColumn);
        });
        return handles;
    }

    @Test
    public void testProjectColumns()
    {
        List<IcebergColumnHandle> columnHandle = getColumnHandle();
        Optional<ReaderColumns> result = IcebergPageSourceProvider.projectColumns(columnHandle);
    }
}
