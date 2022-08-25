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
import io.airlift.testing.TempFile;
import io.airlift.units.Duration;
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
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.metastore.CachingHiveMetastore;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.spi.SplitWeight;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorMaterializedViewDefinition;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.TypeId;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.testing.TestingTypeManager;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.AbstractTestQueryFramework;
import io.prestosql.tests.DistributedQueryRunner;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.hetu.core.plugin.iceberg.IcebergPageSourceProviderTest.writeOrcContent;
import static io.hetu.core.plugin.iceberg.util.IcebergTestUtil.getHiveConfig;
import static io.prestosql.plugin.hive.metastore.CachingHiveMetastore.memoizeMetastore;
import static io.prestosql.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.prestosql.spi.connector.Constraint.alwaysTrue;
import static io.prestosql.spi.connector.RetryMode.NO_RETRIES;
import static io.prestosql.spi.security.PrincipalType.USER;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertTrue;

public class IcebergSplitSourceTest
        extends AbstractTestQueryFramework
{
    private TypeManager typeManager;
    private TestSchemaMetadata metadata;
    private ConnectorSession connectorSession;
    private HdfsEnvironment hdfsEnvironment;
    private TrinoCatalog catalog;
    private Session session;
    private CachingHiveMetastore cachingHiveMetastore;
    private IcebergSplitSource icebergSplitSource;

    protected IcebergSplitSourceTest()
    {
        super(IcebergSplitSourceTest::createQueryRunner);
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
        String timeZone = "Asia/Kathmandu";
        HiveConfig hiveConfig = getHiveConfig()
                .setParquetTimeZone(timeZone)
                .setRcfileTimeZone(timeZone);
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveConfig), ImmutableSet.of());
        hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hiveConfig, new NoHdfsAuthentication());

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
        testCreatePageSource(IcebergFileFormat.ORC);
        List<SchemaTableName> schemaTableNames = catalog.listTables(connectorSession, Optional.of("test_schema"));
        SchemaTableName schemaTableName = new SchemaTableName("test_schema", schemaTableNames.stream().filter(item -> item.getTableName().startsWith("st")).findFirst().get().getTableName());
        Table nationTable = catalog.loadTable(connectorSession, schemaTableName);
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
        icebergSplitSource = new IcebergSplitSource(
                tableHandle,
                nationTable.newScan(),
                Optional.empty(),
                new DynamicFilter()
                {
                    @Override
                    public CompletableFuture<?> isBlocked()
                    {
                        return CompletableFuture.runAsync(() -> {
                            try {
                                TimeUnit.HOURS.sleep(1);
                            }
                            catch (InterruptedException e) {
                                throw new IllegalStateException(e);
                            }
                        });
                    }

                    @Override
                    public boolean contains(Object value)
                    {
                        return false;
                    }

                    @Override
                    public long getSize()
                    {
                        return 0;
                    }

                    @Override
                    public DynamicFilter clone()
                    {
                        return null;
                    }

                    @Override
                    public boolean isEmpty()
                    {
                        return false;
                    }

                    @Override
                    public boolean isAwaitable()
                    {
                        return false;
                    }

                    @Override
                    public TupleDomain<ColumnHandle> getCurrentPredicate()
                    {
                        return TupleDomain.all();
                    }
                },
                new Duration(2, SECONDS),
                alwaysTrue(),
                new TestingTypeManager(),
                false,
                new IcebergConfig().getMinimumAssignedSplitWeight());
    }

    @Test
    public void testGetNextBatch() throws Exception
    {
        ImmutableList.Builder<IcebergSplit> splits = ImmutableList.builder();
        while (!icebergSplitSource.isFinished()) {
            icebergSplitSource.getNextBatch(null, 100).get()
                    .getSplits()
                    .stream()
                    .map(IcebergSplit.class::cast)
                    .forEach(splits::add);
        }
    }

    @Test
    public void testGetTableExecuteSplitsInfo()
    {
        icebergSplitSource.getTableExecuteSplitsInfo();
    }

    @Test
    public void testClose()
    {
        icebergSplitSource.close();
    }

    @Test
    public void testIsFinished()
    {
        icebergSplitSource.isFinished();
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
            e.printStackTrace();
        }
        finally {
            tempFile.close();
        }
        return null;
    }

//    @Test
//    public void testGetNextBatch()
//    {
//        // Setup
//        when(mockDynamicFilter.isAwaitable()).thenReturn(false);
//
//        // Configure DynamicFilter.getCurrentPredicate(...).
//        final TupleDomain<ColumnHandle> columnHandleTupleDomain = TupleDomain.withColumnDomains(new HashMap<>());
//        when(mockDynamicFilter.getCurrentPredicate()).thenReturn(columnHandleTupleDomain);
//
//        // Configure IcebergTableHandle.getUnenforcedPredicate(...).
//        final TupleDomain<IcebergColumnHandle> icebergColumnHandleTupleDomain = TupleDomain.withColumnDomains(
//                new HashMap<>());
//        when(mockTableHandle.getUnenforcedPredicate()).thenReturn(icebergColumnHandleTupleDomain);
//
//        // Configure IcebergTableHandle.getEnforcedPredicate(...).
//        final TupleDomain<IcebergColumnHandle> icebergColumnHandleTupleDomain1 = TupleDomain.withColumnDomains(
//                new HashMap<>());
//        when(mockTableHandle.getEnforcedPredicate()).thenReturn(icebergColumnHandleTupleDomain1);
//
//        when(mockTableScan.filter(any(Expression.class))).thenReturn(null);
//        when(mockTableScan.targetSplitSize()).thenReturn(0L);
//
//        // Run the test
//        final CompletableFuture<ConnectorSplitSource.ConnectorSplitBatch> result = icebergSplitSourceUnderTest.getNextBatch(
//                null, 0);
//
//        // Verify the results
//    }
//
//    @Test
//    public void testGetNextBatch_DynamicFilterGetCurrentPredicateReturnsNoItem()
//    {
//        // Setup
//        when(mockDynamicFilter.isAwaitable()).thenReturn(false);
//        when(mockDynamicFilter.getCurrentPredicate()).thenReturn(TupleDomain.none());
//
//        // Configure IcebergTableHandle.getUnenforcedPredicate(...).
//        final TupleDomain<IcebergColumnHandle> icebergColumnHandleTupleDomain = TupleDomain.withColumnDomains(
//                new HashMap<>());
//        when(mockTableHandle.getUnenforcedPredicate()).thenReturn(icebergColumnHandleTupleDomain);
//
//        // Configure IcebergTableHandle.getEnforcedPredicate(...).
//        final TupleDomain<IcebergColumnHandle> icebergColumnHandleTupleDomain1 = TupleDomain.withColumnDomains(
//                new HashMap<>());
//        when(mockTableHandle.getEnforcedPredicate()).thenReturn(icebergColumnHandleTupleDomain1);
//
//        when(mockTableScan.filter(any(Expression.class))).thenReturn(null);
//        when(mockTableScan.targetSplitSize()).thenReturn(0L);
//
//        // Run the test
//        final CompletableFuture<ConnectorSplitSource.ConnectorSplitBatch> result = icebergSplitSourceUnderTest.getNextBatch(
//                null, 0);
//
//        // Verify the results
//    }
//
//    @Test
//    public void testGetNextBatch_IcebergTableHandleGetUnenforcedPredicateReturnsNoItem()
//    {
//        // Setup
//        when(mockDynamicFilter.isAwaitable()).thenReturn(false);
//
//        // Configure DynamicFilter.getCurrentPredicate(...).
//        final TupleDomain<ColumnHandle> columnHandleTupleDomain = TupleDomain.withColumnDomains(new HashMap<>());
//        when(mockDynamicFilter.getCurrentPredicate()).thenReturn(columnHandleTupleDomain);
//
//        when(mockTableHandle.getUnenforcedPredicate()).thenReturn(TupleDomain.none());
//
//        // Configure IcebergTableHandle.getEnforcedPredicate(...).
//        final TupleDomain<IcebergColumnHandle> icebergColumnHandleTupleDomain = TupleDomain.withColumnDomains(
//                new HashMap<>());
//        when(mockTableHandle.getEnforcedPredicate()).thenReturn(icebergColumnHandleTupleDomain);
//
//        when(mockTableScan.filter(any(Expression.class))).thenReturn(null);
//        when(mockTableScan.targetSplitSize()).thenReturn(0L);
//
//        // Run the test
//        final CompletableFuture<ConnectorSplitSource.ConnectorSplitBatch> result = icebergSplitSourceUnderTest.getNextBatch(
//                null, 0);
//
//        // Verify the results
//    }
//
//    @Test
//    public void testGetNextBatch_IcebergTableHandleGetEnforcedPredicateReturnsNoItem()
//    {
//        // Setup
//        when(mockDynamicFilter.isAwaitable()).thenReturn(false);
//
//        // Configure DynamicFilter.getCurrentPredicate(...).
//        final TupleDomain<ColumnHandle> columnHandleTupleDomain = TupleDomain.withColumnDomains(new HashMap<>());
//        when(mockDynamicFilter.getCurrentPredicate()).thenReturn(columnHandleTupleDomain);
//
//        // Configure IcebergTableHandle.getUnenforcedPredicate(...).
//        final TupleDomain<IcebergColumnHandle> icebergColumnHandleTupleDomain = TupleDomain.withColumnDomains(
//                new HashMap<>());
//        when(mockTableHandle.getUnenforcedPredicate()).thenReturn(icebergColumnHandleTupleDomain);
//
//        when(mockTableHandle.getEnforcedPredicate()).thenReturn(TupleDomain.none());
//        when(mockTableScan.filter(any(Expression.class))).thenReturn(null);
//        when(mockTableScan.targetSplitSize()).thenReturn(0L);
//
//        // Run the test
//        final CompletableFuture<ConnectorSplitSource.ConnectorSplitBatch> result = icebergSplitSourceUnderTest.getNextBatch(
//                null, 0);
//
//        // Verify the results
//    }
//
//    @Test
//    public void testIsFinished()
//    {
//        // Setup
//        // Run the test
//        final boolean result = icebergSplitSourceUnderTest.isFinished();
//
//        // Verify the results
//        assertTrue(result);
//    }

//    @Test
//    public void testGetTableExecuteSplitsInfo()
//    {
//        // Setup
//        // Run the test
//        final Optional<List<Object>> result = icebergSplitSourceUnderTest.getTableExecuteSplitsInfo();
//
//        // Verify the results
//    }
//
//    @Test
//    public void testClose()
//    {
//        // Setup
//        // Run the test
//        icebergSplitSourceUnderTest.close();
//
//        // Verify the results
//    }

    @Test
    public void testFileMatchesPredicate()
    {
        // Setup
        final Map<Integer, Type.PrimitiveType> primitiveTypeForFieldId = new HashMap<>();
        final TupleDomain<IcebergColumnHandle> dynamicFilterPredicate = TupleDomain.withColumnDomains(new HashMap<>());
        final Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
        final Map<Integer, ByteBuffer> upperBounds = new HashMap<>();
        final Map<Integer, Long> nullValueCounts = new HashMap<>();

        // Run the test
        final boolean result = IcebergSplitSource.fileMatchesPredicate(primitiveTypeForFieldId, dynamicFilterPredicate,
                lowerBounds, upperBounds, nullValueCounts);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testPartitionMatchesConstraint()
    {
        // Setup
        final Set<IcebergColumnHandle> identityPartitionColumns = new HashSet<>(
                Arrays.asList(new IcebergColumnHandle(
                        new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), null,
                        Arrays.asList(0), null, Optional.of("value"))));
        final Supplier<Map<ColumnHandle, NullableValue>> partitionValues = () -> new HashMap<>();
        final Constraint constraint = new Constraint(TupleDomain.withColumnDomains(new HashMap<>()), val -> {
            return false;
        });

        // Run the test
        final boolean result = IcebergSplitSource.partitionMatchesConstraint(identityPartitionColumns, partitionValues,
                constraint);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testPartitionMatchesPredicate()
    {
        // Setup
        final Set<IcebergColumnHandle> identityPartitionColumns = new HashSet<>(
                Arrays.asList(new IcebergColumnHandle(
                        new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), null,
                        Arrays.asList(0), null, Optional.of("value"))));
        final Supplier<Map<ColumnHandle, NullableValue>> partitionValues = () -> new HashMap<>();
        final TupleDomain<IcebergColumnHandle> dynamicFilterPredicate = TupleDomain.withColumnDomains(new HashMap<>());

        // Run the test
        final boolean result = IcebergSplitSource.partitionMatchesPredicate(identityPartitionColumns, partitionValues,
                dynamicFilterPredicate);

        // Verify the results
        assertTrue(result);
    }
}
