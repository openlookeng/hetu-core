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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.prestosql.Session;
import io.prestosql.client.NodeVersion;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.dynamicfilter.DynamicFilterService;
import io.prestosql.execution.Lifespan;
import io.prestosql.execution.MockRemoteTaskFactory;
import io.prestosql.execution.NodeTaskMap;
import io.prestosql.execution.RemoteTask;
import io.prestosql.execution.SplitKey;
import io.prestosql.execution.SqlStageExecution;
import io.prestosql.execution.StageId;
import io.prestosql.execution.TableInfo;
import io.prestosql.execution.TestSqlTaskManager;
import io.prestosql.execution.scheduler.LegacyNetworkTopology;
import io.prestosql.execution.scheduler.NodeScheduler;
import io.prestosql.execution.scheduler.NodeSchedulerConfig;
import io.prestosql.execution.scheduler.NodeSelector;
import io.prestosql.execution.scheduler.SplitSchedulerStats;
import io.prestosql.failuredetector.NoOpFailureDetector;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.metadata.InsertTableHandle;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Split;
import io.prestosql.metadata.TableMetadata;
import io.prestosql.seedstore.SeedStoreManager;
import io.prestosql.snapshot.QuerySnapshotManager;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.SelectedRole;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.split.ConnectorAwareSplitSource;
import io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType;
import io.prestosql.sql.analyzer.FeaturesConfig.JoinReorderingStrategy;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.StageExecutionPlan;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.planprinter.IoPlanPrinter.ColumnConstraint;
import io.prestosql.sql.planner.planprinter.IoPlanPrinter.FormattedDomain;
import io.prestosql.sql.planner.planprinter.IoPlanPrinter.FormattedMarker;
import io.prestosql.sql.planner.planprinter.IoPlanPrinter.FormattedRange;
import io.prestosql.sql.planner.planprinter.IoPlanPrinter.IoPlan;
import io.prestosql.sql.planner.planprinter.IoPlanPrinter.IoPlan.TableColumnInfo;
import io.prestosql.statestore.LocalStateStoreProvider;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.testing.TestingSplit;
import io.prestosql.tests.AbstractTestIntegrationSmokeTest;
import io.prestosql.tests.DistributedQueryRunner;
import io.prestosql.util.FinalizerService;
import org.apache.hadoop.fs.Path;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.math.BigDecimal;
import java.net.URI;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.Files.asCharSink;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.ORDERS;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.SessionTestUtils.TEST_SESSION_REUSE;
import static io.prestosql.SystemSessionProperties.COLOCATED_JOIN;
import static io.prestosql.SystemSessionProperties.CONCURRENT_LIFESPANS_PER_NODE;
import static io.prestosql.SystemSessionProperties.DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION;
import static io.prestosql.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.prestosql.SystemSessionProperties.GROUPED_EXECUTION;
import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.prestosql.execution.SqlStageExecution.createSqlStageExecution;
import static io.prestosql.execution.scheduler.TestPhasedExecutionSchedule.createTableScanPlanFragment;
import static io.prestosql.execution.scheduler.TestSourcePartitionedScheduler.createFixedSplitSource;
import static io.prestosql.plugin.hive.HiveColumnHandle.BUCKET_COLUMN_NAME;
import static io.prestosql.plugin.hive.HiveColumnHandle.PATH_COLUMN_NAME;
import static io.prestosql.plugin.hive.HiveCompressionCodec.NONE;
import static io.prestosql.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static io.prestosql.plugin.hive.HiveTableProperties.BUCKETED_BY_PROPERTY;
import static io.prestosql.plugin.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static io.prestosql.plugin.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static io.prestosql.plugin.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static io.prestosql.plugin.hive.HiveTableProperties.TRANSACTIONAL;
import static io.prestosql.plugin.hive.HiveTestUtils.TYPE_MANAGER;
import static io.prestosql.plugin.hive.HiveUtil.columnExtraInfo;
import static io.prestosql.spi.predicate.Marker.Bound.EXACTLY;
import static io.prestosql.spi.security.SelectedRole.Type.ROLE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static io.prestosql.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.prestosql.sql.planner.planprinter.PlanPrinter.textLogicalPlan;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.prestosql.testing.TestingAccessControlManager.privilege;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.testing.TestingSnapshotUtils.NOOP_SNAPSHOT_UTILS;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static io.prestosql.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static io.prestosql.transaction.TransactionBuilder.transaction;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.FileAssert.assertFile;

public class TestHiveIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private static final CatalogName CONNECTOR_ID = new CatalogName("connector_id");
    private static final String TEST_CATALOG = "test_catalog";
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";

    private final String catalog;
    private final Session bucketedSession;
    private final Session autoVacuumSession;
    private final TypeTranslator typeTranslator;

    private Session testSessionSort;
    private Session testSessionSortPrcntDrv50;
    private Session testSessionSortPrcntDrv25;
    private Session testSessionSortPrcntDrv40;
    private FinalizerService finalizerService;
    private NodeTaskMap nodeTaskMap;
    private InMemoryNodeManager nodeManager;
    private NodeSelector nodeSelector;
    private Map<InternalNode, RemoteTask> taskMap;
    private ExecutorService remoteTaskExecutor;
    private ScheduledExecutorService remoteTaskScheduledExecutor;

    @SuppressWarnings("unused")
    public TestHiveIntegrationSmokeTest()
    {
        this(() -> HiveQueryRunner.createQueryRunner(ORDERS, CUSTOMER, LINE_ITEM),
                HiveQueryRunner.createBucketedSession(Optional.of(new SelectedRole(ROLE, Optional.of("admin")))),
                HiveQueryRunner.createAutoVacuumSession(Optional.of(new SelectedRole(SelectedRole.Type.ALL, Optional.empty()))),
                HiveQueryRunner.HIVE_CATALOG,
                new HiveTypeTranslator());
    }

    protected TestHiveIntegrationSmokeTest(QueryRunnerSupplier queryRunnerSupplier, Session bucketedSession, Session autoVacuumSession, String catalog, TypeTranslator typeTranslator)
    {
        super(queryRunnerSupplier);
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.bucketedSession = requireNonNull(bucketedSession, "bucketSession is null");
        this.autoVacuumSession = requireNonNull(autoVacuumSession, "autoVacuumSession is null");
        this.typeTranslator = requireNonNull(typeTranslator, "typeTranslator is null");

        this.remoteTaskExecutor = newCachedThreadPool(daemonThreadsNamed("remoteTaskExecutor-%s"));
        this.remoteTaskScheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("remoteTaskScheduledExecutor-%s"));
        PropertyService.setProperty(HetuConstant.SPLIT_CACHE_MAP_ENABLED, false);

        finalizerService = new FinalizerService();
        nodeTaskMap = new NodeTaskMap(finalizerService);
        nodeManager = new InMemoryNodeManager();

        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setMaxPendingSplitsPerTask(10);

        NodeScheduler nodeScheduler = new NodeScheduler(new LegacyNetworkTopology(), nodeManager, nodeSchedulerConfig, nodeTaskMap);
        // contents of taskMap indicate the node-task map for the current stage
        taskMap = new HashMap<>();
        nodeSelector = nodeScheduler.createNodeSelector(CONNECTOR_ID, false, null);
    }

    @Test
    public void testSchemaOperations()
    {
        Session admin = Session.builder(getQueryRunner().getDefaultSession())
                .setIdentity(new Identity("hive", Optional.empty(), ImmutableMap.of("hive", new SelectedRole(SelectedRole.Type.ROLE, Optional.of("admin")))))
                .build();

        assertUpdate(admin, "CREATE SCHEMA new_schema");

        assertUpdate(admin, "CREATE TABLE new_schema.test (x bigint)");

        assertQueryFails(admin, "DROP SCHEMA new_schema", "Schema not empty: new_schema");

        assertUpdate(admin, "DROP TABLE new_schema.test");

        assertUpdate(admin, "DROP SCHEMA new_schema");
    }

    @Test
    public void testIOExplain()
    {
        // Test IO explain with small number of discrete components.
        computeActual("CREATE TABLE test_orders WITH (partitioned_by = ARRAY['orderkey', 'processing']) AS select custkey, orderkey, orderstatus = 'P' processing FROM orders where orderkey < 3");

        MaterializedResult result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) INSERT INTO test_orders SELECT custkey, orderkey, processing FROM test_orders where custkey <= 10");
        assertEquals(
                jsonCodec(IoPlan.class).fromJson((String) getOnlyElement(result.getOnlyColumnAsSet())),
                new IoPlan(
                        ImmutableSet.of(new TableColumnInfo(
                                new CatalogSchemaTableName(catalog, "tpch", "test_orders"),
                                ImmutableSet.of(
                                        new ColumnConstraint(
                                                "orderkey",
                                                BIGINT.getTypeSignature(),
                                                new FormattedDomain(
                                                        false,
                                                        ImmutableSet.of(
                                                                new FormattedRange(
                                                                        new FormattedMarker(Optional.of("1"), EXACTLY),
                                                                        new FormattedMarker(Optional.of("1"), EXACTLY)),
                                                                new FormattedRange(
                                                                        new FormattedMarker(Optional.of("2"), EXACTLY),
                                                                        new FormattedMarker(Optional.of("2"), EXACTLY))))),
                                        new ColumnConstraint(
                                                "processing",
                                                BOOLEAN.getTypeSignature(),
                                                new FormattedDomain(
                                                        false,
                                                        ImmutableSet.of(
                                                                new FormattedRange(
                                                                        new FormattedMarker(Optional.of("false"), EXACTLY),
                                                                        new FormattedMarker(Optional.of("false"), EXACTLY)))))))),
                        Optional.of(new CatalogSchemaTableName(catalog, "tpch", "test_orders"))));

        assertUpdate("DROP TABLE test_orders");

        // Test IO explain with large number of discrete components where Domain::simpify comes into play.
        computeActual("CREATE TABLE test_orders WITH (partitioned_by = ARRAY['orderkey']) AS select custkey, orderkey FROM orders where orderkey < 200");

        result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) INSERT INTO test_orders SELECT custkey, orderkey + 10 FROM test_orders where custkey <= 10");
        assertEquals(
                jsonCodec(IoPlan.class).fromJson((String) getOnlyElement(result.getOnlyColumnAsSet())),
                new IoPlan(
                        ImmutableSet.of(new TableColumnInfo(
                                new CatalogSchemaTableName(catalog, "tpch", "test_orders"),
                                ImmutableSet.of(
                                        new ColumnConstraint(
                                                "orderkey",
                                                BIGINT.getTypeSignature(),
                                                new FormattedDomain(
                                                        false,
                                                        ImmutableSet.of(
                                                                new FormattedRange(
                                                                        new FormattedMarker(Optional.of("1"), EXACTLY),
                                                                        new FormattedMarker(Optional.of("199"), EXACTLY)))))))),
                        Optional.of(new CatalogSchemaTableName(catalog, "tpch", "test_orders"))));

        assertUpdate("DROP TABLE test_orders");
    }

    @Test
    public void testIoExplainWithPrimitiveTypes()
    {
        Map<Object, Type> data = new HashMap<>();
        data.put("foo", VarcharType.createUnboundedVarcharType());
        data.put(Byte.toString((byte) (Byte.MAX_VALUE / 2)), TinyintType.TINYINT);
        data.put(Short.toString((short) (Short.MAX_VALUE / 2)), SmallintType.SMALLINT);
        data.put(Integer.toString(Integer.MAX_VALUE / 2), IntegerType.INTEGER);
        data.put(Long.toString(Long.MAX_VALUE / 2), BigintType.BIGINT);
        data.put(Boolean.TRUE.toString(), BooleanType.BOOLEAN);
        data.put("bar", CharType.createCharType(3));
        data.put("1.2345678901234578E14", DoubleType.DOUBLE);
        data.put("123456789012345678901234.567", DecimalType.createDecimalType(30, 3));
        data.put("2019-01-01", DateType.DATE);
        data.put("2019-01-01 23:22:21.123", TimestampType.TIMESTAMP);
        for (Map.Entry<Object, Type> entry : data.entrySet()) {
            @Language("SQL") String query = format(
                    "CREATE TABLE test_types_table  WITH (partitioned_by = ARRAY['my_col']) AS " +
                            "SELECT 'foo' my_non_partition_col, CAST('%s' AS %s) my_col",
                    entry.getKey(),
                    entry.getValue().getDisplayName());

            assertUpdate(query, 1);
            MaterializedResult result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) SELECT * FROM test_types_table");
            assertEquals(
                    jsonCodec(IoPlan.class).fromJson((String) getOnlyElement(result.getOnlyColumnAsSet())),
                    new IoPlan(
                            ImmutableSet.of(new TableColumnInfo(
                                    new CatalogSchemaTableName(catalog, "tpch", "test_types_table"),
                                    ImmutableSet.of(
                                            new ColumnConstraint(
                                                    "my_col",
                                                    entry.getValue().getTypeSignature(),
                                                    new FormattedDomain(
                                                            false,
                                                            ImmutableSet.of(
                                                                    new FormattedRange(
                                                                            new FormattedMarker(Optional.of(entry.getKey().toString()), EXACTLY),
                                                                            new FormattedMarker(Optional.of(entry.getKey().toString()), EXACTLY)))))))),
                            Optional.empty()));

            assertUpdate("DROP TABLE test_types_table");
        }
    }

    @Test
    public void testReadNoColumns()
    {
        testWithAllStorageFormats(this::testReadNoColumns);
    }

    private void testReadNoColumns(Session session, HiveStorageFormat storageFormat)
    {
        assertUpdate(session, format("CREATE TABLE test_read_no_columns WITH (format = '%s') AS SELECT 0 x", storageFormat), 1);
        assertQuery(session, "SELECT count(*) FROM test_read_no_columns", "SELECT 1");
        assertUpdate(session, "DROP TABLE test_read_no_columns");
    }

    @Test
    public void createTableWithEveryType()
    {
        @Language("SQL") String query = "" +
                "CREATE TABLE test_types_table AS " +
                "SELECT" +
                " 'foo' _varchar" +
                ", cast('bar' as varbinary) _varbinary" +
                ", cast(1 as bigint) _bigint" +
                ", 2 _integer" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", DATE '1980-05-07' _date" +
                ", TIMESTAMP '1980-05-07 11:22:33.456' _timestamp" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long" +
                ", CAST('bar' AS CHAR(10)) _char";

        assertUpdate(query, 1);

        MaterializedResult results = getQueryRunner().execute(getSession(), "SELECT * FROM test_types_table").toTestTypes();
        assertEquals(results.getRowCount(), 1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertEquals(row.getField(0), "foo");
        assertEquals(row.getField(1), "bar".getBytes(UTF_8));
        assertEquals(row.getField(2), 1L);
        assertEquals(row.getField(3), 2);
        assertEquals(row.getField(4), 3.14);
        assertEquals(row.getField(5), true);
        assertEquals(row.getField(6), LocalDate.of(1980, 5, 7));
        assertEquals(row.getField(7), LocalDateTime.of(1980, 5, 7, 11, 22, 33, 456_000_000));
        assertEquals(row.getField(8), new BigDecimal("3.14"));
        assertEquals(row.getField(9), new BigDecimal("12345678901234567890.0123456789"));
        assertEquals(row.getField(10), "bar       ");
        assertUpdate("DROP TABLE test_types_table");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_types_table"));
    }

    @Test
    public void testCreatePartitionedTable()
    {
        testWithAllStorageFormats(this::testCreatePartitionedTable);
    }

    private void testCreatePartitionedTable(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitioned_table (" +
                "  _string VARCHAR" +
                ",  _varchar VARCHAR(65535)" +
                ", _char CHAR(10)" +
                ", _bigint BIGINT" +
                ", _integer INTEGER" +
                ", _smallint SMALLINT" +
                ", _tinyint TINYINT" +
                ", _real REAL" +
                ", _double DOUBLE" +
                ", _boolean BOOLEAN" +
                ", _decimal_short DECIMAL(3,2)" +
                ", _decimal_long DECIMAL(30,10)" +
                ", _partition_string VARCHAR" +
                ", _partition_varchar VARCHAR(65535)" +
                ", _partition_char CHAR(10)" +
                ", _partition_tinyint TINYINT" +
                ", _partition_smallint SMALLINT" +
                ", _partition_integer INTEGER" +
                ", _partition_bigint BIGINT" +
                ", _partition_boolean BOOLEAN" +
                ", _partition_decimal_short DECIMAL(3,2)" +
                ", _partition_decimal_long DECIMAL(30,10)" +
                ", _partition_date DATE" +
                ", _partition_timestamp TIMESTAMP" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ '_partition_string', '_partition_varchar', '_partition_char', '_partition_tinyint', '_partition_smallint', '_partition_integer', '_partition_bigint', '_partition_boolean', '_partition_decimal_short', '_partition_decimal_long', '_partition_date', '_partition_timestamp']" +
                ") ";

        if (storageFormat == HiveStorageFormat.AVRO) {
            createTable = createTable.replace(" _smallint SMALLINT,", " _smallint INTEGER,");
            createTable = createTable.replace(" _tinyint TINYINT,", " _tinyint INTEGER,");
        }

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        List<String> partitionedBy = ImmutableList.of(
                "_partition_string",
                "_partition_varchar",
                "_partition_char",
                "_partition_tinyint",
                "_partition_smallint",
                "_partition_integer",
                "_partition_bigint",
                "_partition_boolean",
                "_partition_decimal_short",
                "_partition_decimal_long",
                "_partition_date",
                "_partition_timestamp");
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), partitionedBy);
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            boolean partitionKey = partitionedBy.contains(columnMetadata.getName());
            assertEquals(columnMetadata.getExtraInfo(), columnExtraInfo(partitionKey));
        }

        assertColumnType(tableMetadata, "_string", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "_varchar", createVarcharType(65535));
        assertColumnType(tableMetadata, "_char", createCharType(10));
        assertColumnType(tableMetadata, "_partition_string", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "_partition_varchar", createVarcharType(65535));

        MaterializedResult result = computeActual("SELECT * from test_partitioned_table");
        assertEquals(result.getRowCount(), 0);

        @Language("SQL") String select = "" +
                "SELECT" +
                " 'foo' _string" +
                ", 'bar' _varchar" +
                ", CAST('boo' AS CHAR(10)) _char" +
                ", CAST(1 AS BIGINT) _bigint" +
                ", 2 _integer" +
                ", CAST (3 AS SMALLINT) _smallint" +
                ", CAST (4 AS TINYINT) _tinyint" +
                ", CAST('123.45' AS REAL) _real" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long" +
                ", 'foo' _partition_string" +
                ", 'bar' _partition_varchar" +
                ", CAST('boo' AS CHAR(10)) _partition_char" +
                ", CAST(1 AS TINYINT) _partition_tinyint" +
                ", CAST(1 AS SMALLINT) _partition_smallint" +
                ", 1 _partition_integer" +
                ", CAST (1 AS BIGINT) _partition_bigint" +
                ", true _partition_boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _partition_decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _partition_decimal_long" +
                ", CAST('2017-05-01' AS DATE) _partition_date" +
                ", CAST('2017-05-01 10:12:34' AS TIMESTAMP) _partition_timestamp";

        if (storageFormat == HiveStorageFormat.AVRO) {
            select = select.replace(" CAST (3 AS SMALLINT) _smallint,", " 3 _smallint,");
            select = select.replace(" CAST (4 AS TINYINT) _tinyint,", " 4 _tinyint,");
        }

        assertUpdate(session, "INSERT INTO test_partitioned_table " + select, 1);
        assertQuery(session, "SELECT * from test_partitioned_table", select);
        assertQuery(session,
                "SELECT * from test_partitioned_table WHERE" +
                        " 'foo' = _partition_string" +
                        " AND 'bar' = _partition_varchar" +
                        " AND CAST('boo' AS CHAR(10)) = _partition_char" +
                        " AND CAST(1 AS TINYINT) = _partition_tinyint" +
                        " AND CAST(1 AS SMALLINT) = _partition_smallint" +
                        " AND 1 = _partition_integer" +
                        " AND CAST(1 AS BIGINT) = _partition_bigint" +
                        " AND true = _partition_boolean" +
                        " AND CAST('3.14' AS DECIMAL(3,2)) = _partition_decimal_short" +
                        " AND CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) = _partition_decimal_long" +
                        " AND CAST('2017-05-01' AS DATE) = _partition_date" +
                        " AND CAST('2017-05-01 10:12:34' AS TIMESTAMP) = _partition_timestamp",
                select);

        assertUpdate(session, "DROP TABLE test_partitioned_table");

        assertFalse(getQueryRunner().tableExists(session, "test_partitioned_table"));
    }

    @Test
    public void createTableLike()
    {
        createTableLike("", false);
        createTableLike("EXCLUDING PROPERTIES", false);
        createTableLike("INCLUDING PROPERTIES", true);
    }

    private void createTableLike(String likeSuffix, boolean hasPartition)
    {
        // Create a non-partitioned table
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_table_original (" +
                "  tinyint_col tinyint " +
                ", smallint_col smallint" +
                ")";
        assertUpdate(createTable);

        // Verify the table is correctly created
        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_table_original");
        assertColumnType(tableMetadata, "tinyint_col", TINYINT);
        assertColumnType(tableMetadata, "smallint_col", SMALLINT);

        // Create a partitioned table
        @Language("SQL") String createPartitionedTable = "" +
                "CREATE TABLE test_partitioned_table_original (" +
                "  string_col VARCHAR" +
                ", decimal_long_col DECIMAL(30,10)" +
                ", partition_bigint BIGINT" +
                ", partition_decimal_long DECIMAL(30,10)" +
                ") " +
                "WITH (" +
                "partitioned_by = ARRAY['partition_bigint', 'partition_decimal_long']" +
                ")";
        assertUpdate(createPartitionedTable);

        // Verify the table is correctly created
        tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table_original");

        // Verify the partition keys are correctly created
        List<String> partitionedBy = ImmutableList.of("partition_bigint", "partition_decimal_long");
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), partitionedBy);

        // Verify the column types
        assertColumnType(tableMetadata, "string_col", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "partition_bigint", BIGINT);
        assertColumnType(tableMetadata, "partition_decimal_long", createDecimalType(30, 10));

        // Create a table using only one LIKE
        @Language("SQL") String createTableSingleLike = "" +
                "CREATE TABLE test_partitioned_table_single_like (" +
                "LIKE test_partitioned_table_original " + likeSuffix +
                ")";
        assertUpdate(createTableSingleLike);

        tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table_single_like");

        // Verify the partitioned keys are correctly created if copying partition columns
        verifyPartition(hasPartition, tableMetadata, partitionedBy);

        // Verify the column types
        assertColumnType(tableMetadata, "string_col", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "partition_bigint", BIGINT);
        assertColumnType(tableMetadata, "partition_decimal_long", createDecimalType(30, 10));

        @Language("SQL") String createTableLikeExtra = "" +
                "CREATE TABLE test_partitioned_table_like_extra (" +
                "  bigint_col BIGINT" +
                ", double_col DOUBLE" +
                ", LIKE test_partitioned_table_single_like " + likeSuffix +
                ")";
        assertUpdate(createTableLikeExtra);

        tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table_like_extra");

        // Verify the partitioned keys are correctly created if copying partition columns
        verifyPartition(hasPartition, tableMetadata, partitionedBy);

        // Verify the column types
        assertColumnType(tableMetadata, "bigint_col", BIGINT);
        assertColumnType(tableMetadata, "double_col", DOUBLE);
        assertColumnType(tableMetadata, "string_col", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "partition_bigint", BIGINT);
        assertColumnType(tableMetadata, "partition_decimal_long", createDecimalType(30, 10));

        @Language("SQL") String createTableDoubleLike = "" +
                "CREATE TABLE test_partitioned_table_double_like (" +
                "  LIKE test_table_original " +
                ", LIKE test_partitioned_table_like_extra " + likeSuffix +
                ")";
        assertUpdate(createTableDoubleLike);

        tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table_double_like");

        // Verify the partitioned keys are correctly created if copying partition columns
        verifyPartition(hasPartition, tableMetadata, partitionedBy);

        // Verify the column types
        assertColumnType(tableMetadata, "tinyint_col", TINYINT);
        assertColumnType(tableMetadata, "smallint_col", SMALLINT);
        assertColumnType(tableMetadata, "string_col", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "partition_bigint", BIGINT);
        assertColumnType(tableMetadata, "partition_decimal_long", createDecimalType(30, 10));

        assertUpdate("DROP TABLE test_table_original");
        assertUpdate("DROP TABLE test_partitioned_table_original");
        assertUpdate("DROP TABLE test_partitioned_table_single_like");
        assertUpdate("DROP TABLE test_partitioned_table_like_extra");
        assertUpdate("DROP TABLE test_partitioned_table_double_like");
    }

    @Test
    public void testCreateOrcTransactionalTable()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_orc_transactional_table " +
                "(" +
                "  a BIGINT," +
                "  b BIGINT" +
                ") " +
                "WITH (" +
                STORAGE_FORMAT_PROPERTY + " = 'ORC', " +
                TRANSACTIONAL + " = true" +
                ") ";
        assertUpdate(createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_orc_transactional_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), HiveStorageFormat.ORC);
        assertEquals(tableMetadata.getMetadata().getProperties().get(TRANSACTIONAL), true);

        assertColumnType(tableMetadata, "a", BIGINT);
        assertColumnType(tableMetadata, "b", BIGINT);

        assertUpdate(getSession(), "DROP TABLE test_orc_transactional_table");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_orc_transactional_table"));
    }

    @Test
    public void testVacuum()
    {
        String table = "tab1";
        String schema = "default";
        assertUpdate(String.format("CREATE SCHEMA IF NOT EXISTS %s", schema));

        assertUpdate(String.format("CREATE TABLE %s.%s (a int, b int) with (transactional=true, format='orc')",
                schema, table));

        assertUpdate(String.format("INSERT INTO %s.%s VALUES (1, 2)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (3, 4)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (5, 6)", schema, table), 1);

        assertUpdate(String.format("VACUUM TABLE %s.%s AND WAIT", schema, table), 3);

        TableMetadata tableMetadata = getTableMetadata("hive", schema, table);
        String tablePath = ((String) tableMetadata.getMetadata().getProperties().get("location"));

        assertFilesAfterCleanup(tablePath, 1);
        assertUpdate(String.format("DROP TABLE %s.%s", schema, table));
    }

    @Test
    public void testFullVacuum1()
    {
        String table = "tab2";
        String schema = "default";
        assertUpdate(String.format("CREATE SCHEMA IF NOT EXISTS %s", schema));

        assertUpdate(String.format("CREATE TABLE %s.%s (a int, b int) with (transactional=true, format='orc')",
                schema, table));

        assertUpdate(String.format("INSERT INTO %s.%s VALUES (1, 2)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (3, 4)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (5, 6)", schema, table), 1);

        assertUpdate(String.format("VACUUM TABLE %s.%s FULL AND WAIT", schema, table), 3);

        TableMetadata tableMetadata = getTableMetadata("hive", schema, table);
        String tablePath = ((String) tableMetadata.getMetadata().getProperties().get("location"));

        assertFilesAfterCleanup(tablePath, 1);
        assertUpdate(String.format("DROP TABLE %s.%s", schema, table));
    }

    @Test
    public void testFullUnifyVacuum1()
    {
        String table = "tab_fm_vacuum";
        String schema = "default";
        assertUpdate(String.format("CREATE SCHEMA IF NOT EXISTS %s", schema));

        assertUpdate(String.format("CREATE TABLE %s.%s (a int, b int) with (transactional=true, format='orc')",
                schema, table));

        assertUpdate(String.format("INSERT INTO %s.%s VALUES (1, 2)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (3, 4)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (5, 6)", schema, table), 1);

        assertUpdate(String.format("VACUUM TABLE %s.%s FULL UNIFY AND WAIT", schema, table), 3);

        TableMetadata tableMetadata = getTableMetadata("hive", schema, table);
        String tablePath = ((String) tableMetadata.getMetadata().getProperties().get("location"));

        assertFilesAfterCleanup(tablePath, 1);
        assertUpdate(String.format("DROP TABLE %s.%s", schema, table));
    }

    @Test
    public void testFullUnifyVacuum2()
    {
        String table = "tab_fm_vacuum_2";
        String schema = "default";
        assertUpdate(String.format("CREATE SCHEMA IF NOT EXISTS %s", schema));

        assertUpdate(String.format("CREATE TABLE %s.%s (a int, b int) with (transactional=true, format='orc')",
                schema, table));

        assertUpdate(String.format("INSERT INTO %s.%s VALUES (1, 2)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (3, 4)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (5, 6)", schema, table), 1);

        assertUpdate(String.format("VACUUM TABLE %s.%s AND WAIT", schema, table), 3);
        assertUpdate(String.format("VACUUM TABLE %s.%s FULL UNIFY AND WAIT", schema, table), 3);

        TableMetadata tableMetadata = getTableMetadata("hive", schema, table);
        String tablePath = ((String) tableMetadata.getMetadata().getProperties().get("location"));

        assertFilesAfterCleanup(tablePath, 1);
        assertUpdate(String.format("DROP TABLE %s.%s", schema, table));
    }

    @Test
    public void testFullVacuum2()
    {
        String table = "tab3";
        String schema = "default";
        assertUpdate(String.format("CREATE SCHEMA IF NOT EXISTS %s", schema));

        assertUpdate(String.format("CREATE TABLE %s.%s (a int, b int) with (transactional=true, format='orc')",
                schema, table));

        assertUpdate(String.format("INSERT INTO %s.%s VALUES (1, 2)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (3, 4)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (5, 6)", schema, table), 1);

        assertUpdate(String.format("VACUUM TABLE %s.%s AND WAIT", schema, table), 3);
        assertUpdate(String.format("VACUUM TABLE %s.%s FULL AND WAIT", schema, table), 3);

        TableMetadata tableMetadata = getTableMetadata("hive", schema, table);
        String tablePath = ((String) tableMetadata.getMetadata().getProperties().get("location"));

        assertFilesAfterCleanup(tablePath, 1);
        assertUpdate(String.format("DROP TABLE %s.%s", schema, table));
    }

    @Test
    public void testVacuumOnDeleteDelta()
    {
        String table = "tab4";
        String schema = "default";
        assertUpdate(String.format("CREATE SCHEMA IF NOT EXISTS %s", schema));

        assertUpdate(String.format("CREATE TABLE %s.%s (a int, b int) with (transactional=true, format='orc')",
                schema, table));

        assertUpdate(String.format("INSERT INTO %s.%s VALUES (1, 2)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (3, 4)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (5, 6)", schema, table), 1);
        assertUpdate(String.format("UPDATE %s.%s SET b = -1 WHERE a > 2", schema, table), 2);

        assertUpdate(String.format("VACUUM TABLE %s.%s AND WAIT", schema, table), 7);

        TableMetadata tableMetadata = getTableMetadata("hive", schema, table);
        String tablePath = ((String) tableMetadata.getMetadata().getProperties().get("location"));
        assertFilesAfterCleanup(tablePath, 2);

        assertUpdate(String.format("VACUUM TABLE %s.%s FULL AND WAIT", schema, table), 3);

        assertFilesAfterCleanup(tablePath, 1);
        assertUpdate(String.format("DROP TABLE %s.%s", schema, table));
    }

    @Test
    public void testVacuumOnPartitionedTable1()
    {
        String table = "tab5";
        String schema = "default";
        assertUpdate(String.format("CREATE SCHEMA IF NOT EXISTS %s", schema));

        String partitionedColumn = "b";
        assertUpdate(String.format("CREATE TABLE %s.%s (a int, b int) with (transactional=true, format='orc', partitioned_by=Array['%s'])",
                schema, table, partitionedColumn));

        assertUpdate(String.format("INSERT INTO %s.%s VALUES (1, 1)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (1, 2)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (2, 1)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (2, 2)", schema, table), 1);

        assertUpdate(String.format("VACUUM TABLE %s.%s AND WAIT", schema, table), 4);

        TableMetadata tableMetadata = getTableMetadata("hive", schema, table);
        String tablePath = (String) tableMetadata.getMetadata().getProperties().get("location");
        assertFilesAfterCleanupOnPartitionTable(tablePath, partitionedColumn, ImmutableList.of("1", "2"), 1);

        assertUpdate(String.format("UPDATE %s.%s SET a = -1 WHERE a = 2", schema, table), 2);

        assertUpdate(String.format("VACUUM TABLE %s.%s AND WAIT", schema, table), 8);
        assertFilesAfterCleanupOnPartitionTable(tablePath, partitionedColumn, ImmutableList.of("1", "2"), 2);

        assertUpdate(String.format("VACUUM TABLE %s.%s FULL AND WAIT", schema, table), 4);
        assertFilesAfterCleanupOnPartitionTable(tablePath, partitionedColumn, ImmutableList.of("1", "2"), 1);

        assertUpdate(String.format("DROP TABLE %s.%s", schema, table));
    }

    @Test
    public void testVacuumOnPartitionedTable2()
    {
        String table = "tab6";
        String schema = "default";
        assertUpdate(String.format("CREATE SCHEMA IF NOT EXISTS %s", schema));

        String partitionedColumn = "b";
        assertUpdate(String.format("CREATE TABLE %s.%s (a int, b int) with (transactional=true, format='orc', partitioned_by=Array['%s'])",
                schema, table, partitionedColumn));

        assertUpdate(String.format("INSERT INTO %s.%s VALUES (1, 1)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (1, 2)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (2, 1)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (2, 2)", schema, table), 1);

        assertUpdate(String.format("VACUUM TABLE %s.%s PARTITION '%s=1' AND WAIT", schema, table, partitionedColumn), 2);

        TableMetadata tableMetadata = getTableMetadata("hive", schema, table);
        String tablePath = (String) tableMetadata.getMetadata().getProperties().get("location");
        assertFilesAfterCleanupOnPartitionTable(tablePath, partitionedColumn, ImmutableList.of("1"), 1);
        assertFilesAfterCleanupOnPartitionTable(tablePath, partitionedColumn, ImmutableList.of("2"), 2);
    }

    @Test
    public void testVacuumOnTableWithZeroRows()
    {
        String table = "tab7";
        String schema = "default";
        assertUpdate(String.format("CREATE SCHEMA IF NOT EXISTS %s", schema));

        assertUpdate(String.format("CREATE TABLE %s.%s (a int, b int) with (transactional=true, format='orc')",
                schema, table));

        assertUpdate(String.format("INSERT INTO %s.%s VALUES (1, 2)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (3, 4)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (5, 6)", schema, table), 1);
        assertUpdate(String.format("DELETE FROM %s.%s", schema, table), 3);

        assertUpdate(String.format("VACUUM TABLE %s.%s FULL AND WAIT", schema, table), 0);

        TableMetadata tableMetadata = getTableMetadata("hive", schema, table);
        String tablePath = ((String) tableMetadata.getMetadata().getProperties().get("location"));
        assertFilesAfterCleanup(tablePath, 1);

        assertUpdate(String.format("DROP TABLE %s.%s", schema, table));
    }

    @Test
    public void testVacuumOnTableWithZeroRowsOnPartitionTable()
    {
        String table = "tab8";
        String schema = "default";
        assertUpdate(String.format("CREATE SCHEMA IF NOT EXISTS %s", schema));

        String partitionedColumn = "b";
        assertUpdate(String.format("CREATE TABLE %s.%s (a int, b int) with (transactional=true, format='orc', partitioned_by=Array['%s'])",
                schema, table, partitionedColumn));

        assertUpdate(String.format("INSERT INTO %s.%s VALUES (1, 1)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (1, 2)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (2, 1)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (2, 2)", schema, table), 1);
        assertUpdate(String.format("DELETE FROM %s.%s WHERE %s=2", schema, table, partitionedColumn), 2);

        assertUpdate(String.format("VACUUM TABLE %s.%s AND WAIT", schema, table), 6);

        TableMetadata tableMetadata = getTableMetadata("hive", schema, table);
        String tablePath = ((String) tableMetadata.getMetadata().getProperties().get("location"));

        assertFilesAfterCleanupOnPartitionTable(tablePath, partitionedColumn, ImmutableList.of("1"), 1);
        assertFilesAfterCleanupOnPartitionTable(tablePath, partitionedColumn, ImmutableList.of("2"), 2);
        assertUpdate(String.format("DROP TABLE %s.%s", schema, table));
    }

    @Test
    public void testVacuumOnPartitionedTable()
    {
        String table = "tab7_partitioned";
        String schema = "default";
        assertUpdate(String.format("CREATE SCHEMA IF NOT EXISTS %s", schema));

        String partitionedColumn = "b";
        assertUpdate(String.format("CREATE TABLE %s.%s (a int, b int) with (transactional=true, format='orc', partitioned_by=Array['%s'])",
                schema, table, partitionedColumn));
        TableMetadata tableMetadata = getTableMetadata("hive", schema, table);
        String tablePath = (String) tableMetadata.getMetadata().getProperties().get("location");

        assertUpdate(String.format("INSERT INTO %s.%s VALUES (1,1),(1,2)", schema, table), 2);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (2,1),(2,2)", schema, table), 2);
        assertUpdate(String.format("VACUUM TABLE %s.%s FULL AND WAIT", schema, table), 4);
        assertFilesAfterCleanupOnPartitionTable(tablePath, partitionedColumn, ImmutableList.of("2"), 1);
        //INSERT ONLY to partition b=1 and CALL VACUUM FULL, should compact only partition b=1 with 4 rows.
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (3, 1)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (4, 1)", schema, table), 1);
        String[] part2Dirs = listPartition(tablePath, "b=2");
        assertUpdate(String.format("VACUUM TABLE %s.%s FULL AND WAIT", schema, table), 4);
        verifyPartitionDirs(tablePath, "b=2", part2Dirs.length, part2Dirs);
        assertFilesAfterCleanupOnPartitionTable(tablePath, partitionedColumn, ImmutableList.of("1"), 1);
        //INSERT ONLY to partition b=3 and CALL VACUUM FULL, should compact only partition b=3 with 2 rows.
        String[] part1Dirs = listPartition(tablePath, "b=1");
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (3, 3)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (4, 3)", schema, table), 1);
        assertUpdate(String.format("VACUUM TABLE %s.%s FULL AND WAIT", schema, table), 2);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (5, 3)", schema, table), 1);
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (6, 3)", schema, table), 1);
        //partition 3 should now have baseDir along with 2 delta dirs.
        String[] part3Dirs = listPartition(tablePath, "b=3");
        long minId = Long.MAX_VALUE;
        long maxId = Long.MIN_VALUE;
        for (String delta : part3Dirs) {
            Matcher matcher = DELTA_PATTERN.matcher(delta);
            if (matcher.matches()) {
                minId = Math.min(Long.parseLong(matcher.group(2)), minId);
                maxId = Math.max(Long.parseLong(matcher.group(3)), maxId);
            }
        }
        assertUpdate(String.format("VACUUM TABLE %s.%s AND WAIT", schema, table), 2);
        verifyPartitionDirs(tablePath, "b=2", part2Dirs.length, part2Dirs);
        verifyPartitionDirs(tablePath, "b=1", part1Dirs.length, part1Dirs);
        assertFilesAfterCleanupOnPartitionTable(tablePath, partitionedColumn, ImmutableList.of("3"), 2);
        verifyPartitionDirs(tablePath, "b=3", 2, part3Dirs[0], String.format("delta_%07d_%07d", minId, maxId));
    }

    private static final Pattern DELTA_PATTERN = Pattern.compile("(delete_)?delta_(\\d+)_(\\d+)(_\\d+)?");
    private static final Pattern BASE_PATTERN = Pattern.compile("base_(\\d+)");

    private String[] listPartition(String tablePath, String partition)
    {
        if (tablePath.startsWith("file:")) {
            tablePath = tablePath.replace("file:", "");
        }
        String[] partitionDirs = new File(tablePath + "/" + partition).list((f, s) -> !s.startsWith("."));
        Arrays.sort(partitionDirs);
        return partitionDirs;
    }

    private void verifyPartitionDirs(String tablePath, String partition, int expectedDirs, String... expectedBaseFile)
    {
        String[] partitionDirs = listPartition(tablePath, partition);
        System.out.println(Arrays.toString(partitionDirs));
        assertEquals(partitionDirs.length, expectedDirs);
        for (int i = 0; i < expectedDirs; i++) {
            assertEquals(partitionDirs[i], expectedBaseFile[i]);
        }
    }

    private void assertFilesAfterCleanupOnPartitionTable(String tablePath, String partitionedColumn, ImmutableList<String> partitionValue, int expectedNumberOfDirectories)
    {
        partitionValue.forEach(value -> {
            String partitionPath = tablePath + "/" + partitionedColumn + "=" + value;
            assertFilesAfterCleanup(partitionPath, expectedNumberOfDirectories);
        });
    }

    private void assertFilesAfterCleanup(String tablePath, int expectedNumberOfDirectories)
    {
        int loopNumber = 50;
        if (tablePath.startsWith("file:")) {
            tablePath = tablePath.replace("file:", "");
        }
        String[] otherDirectories;
        do {
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException e) {
                // Ignore
            }
            otherDirectories = new File(tablePath).list(new FilenameFilter()
            {
                @Override
                public boolean accept(File file, String s)
                {
                    // Ignore hidden directories
                    return !s.startsWith(".");
                }
            });
            try {
                assertEquals(otherDirectories.length, expectedNumberOfDirectories);
                break;
            }
            catch (AssertionError e) {
                // Ignore
            }
        }
        while (loopNumber-- > 0);

        if (loopNumber < 1) {
            assertEquals(otherDirectories.length, expectedNumberOfDirectories,
                    String.format("Unexpected directories on path %s", tablePath));
        }
    }

    @Test
    public void testCreateTableAs()
    {
        testWithAllStorageFormats(this::testCreateTableAs);
    }

    private void testCreateTableAs(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String select = "SELECT" +
                " 'foo' _varchar" +
                ", CAST('bar' AS CHAR(10)) _char" +
                ", CAST (1 AS BIGINT) _bigint" +
                ", 2 _integer" +
                ", CAST (3 AS SMALLINT) _smallint" +
                ", CAST (4 AS TINYINT) _tinyint" +
                ", CAST ('123.45' as REAL) _real" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long";

        if (storageFormat == HiveStorageFormat.AVRO) {
            select = select.replace(" CAST (3 AS SMALLINT) _smallint,", " 3 _smallint,");
            select = select.replace(" CAST (4 AS TINYINT) _tinyint,", " 4 _tinyint,");
        }

        String createTableAs = format("CREATE TABLE test_format_table WITH (format = '%s') AS %s", storageFormat, select);

        assertUpdate(session, createTableAs, 1);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_format_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertColumnType(tableMetadata, "_varchar", createVarcharType(3));
        assertColumnType(tableMetadata, "_char", createCharType(10));

        // assure reader supports basic column reordering and pruning
        assertQuery(session, "SELECT _integer, _varchar, _integer from test_format_table", "SELECT 2, 'foo', 2");

        assertQuery(session, "SELECT * from test_format_table", select);

        assertUpdate(session, "DROP TABLE test_format_table");

        assertFalse(getQueryRunner().tableExists(session, "test_format_table"));
    }

    @Test
    public void testCreatePartitionedTableAs()
    {
        testWithAllStorageFormats(this::testCreatePartitionedTableAs);
        testWithAllStorageFormats(this::testCreatePartitionedTableAsWithPartitionedRedistribute);
    }

    private void testCreatePartitionedTableAs(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_create_partitioned_table_as " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'SHIP_PRIORITY', 'ORDER_STATUS' ]" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        assertUpdate(session, createTable, "SELECT count(*) from orders");

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_create_partitioned_table_as");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("ship_priority", "order_status"));

        List<?> partitions = getPartitions("test_create_partitioned_table_as");
        assertEquals(partitions.size(), 3);

        assertQuery(session, "SELECT * from test_create_partitioned_table_as", "SELECT orderkey, shippriority, orderstatus FROM orders");

        assertUpdate(session, "DROP TABLE test_create_partitioned_table_as");

        assertFalse(getQueryRunner().tableExists(session, "test_create_partitioned_table_as"));
    }

    // Presto: test case for partitioned redistribute writes type
    private void testCreatePartitionedTableAsWithPartitionedRedistribute(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_create_partitioned_table_as " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'SHIP_PRIORITY', 'ORDER_STATUS' ]" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        Long count = (Long) computeActual("SELECT count(*) from orders").getOnlyValue();
        assertUpdate(Session.builder(getSession())
                        .setSystemProperty("redistribute_writes_type", "PARTITIONED")
                        .build(),
                createTable, count, assertRemotePartitionedExchange("orderstatus"));

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_create_partitioned_table_as");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("ship_priority", "order_status"));

        List<?> partitions = getPartitions("test_create_partitioned_table_as");
        assertEquals(partitions.size(), 3);

        assertQuery(session, "SELECT * from test_create_partitioned_table_as", "SELECT orderkey, shippriority, orderstatus FROM orders");

        assertUpdate(session, "DROP TABLE test_create_partitioned_table_as");

        assertFalse(getQueryRunner().tableExists(session, "test_create_partitioned_table_as"));
    }

    @Test
    public void testPropertiesTable()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_show_properties" +
                " WITH (" +
                "format = 'orc', " +
                "partitioned_by = ARRAY['ship_priority', 'order_status']," +
                "orc_bloom_filter_columns = ARRAY['ship_priority', 'order_status']," +
                "orc_bloom_filter_fpp = 0.5" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        assertUpdate(createTable, "SELECT count(*) FROM orders");
        String queryId = (String) computeScalar("SELECT query_id FROM system.runtime.queries WHERE query LIKE 'CREATE TABLE test_show_properties%'");
        String nodeVersion = (String) computeScalar("SELECT node_version FROM system.runtime.nodes WHERE coordinator");
        assertQuery("SELECT * FROM \"test_show_properties$properties\"",
                "SELECT '" + "ship_priority,order_status" + "','" + "0.5" + "','" + queryId + "','" + nodeVersion + "'");
        assertUpdate("DROP TABLE test_show_properties");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Partition keys must be the last columns in the table and in the same order as the table properties.*")
    public void testCreatePartitionedTableInvalidColumnOrdering()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_invalid_column_ordering\n" +
                "(grape bigint, apple varchar, orange bigint, pear varchar)\n" +
                "WITH (partitioned_by = ARRAY['apple'])");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Partition keys must be the last columns in the table and in the same order as the table properties.*")
    public void testCreatePartitionedTableAsInvalidColumnOrdering()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_as_invalid_column_ordering " +
                "WITH (partitioned_by = ARRAY['SHIP_PRIORITY', 'ORDER_STATUS']) " +
                "AS " +
                "SELECT shippriority AS ship_priority, orderkey AS order_key, orderstatus AS order_status " +
                "FROM tpch.tiny.orders");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Table contains only partition columns")
    public void testCreateTableOnlyPartitionColumns()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_only_partition_columns\n" +
                "(grape bigint, apple varchar, orange bigint, pear varchar)\n" +
                "WITH (partitioned_by = ARRAY['grape', 'apple', 'orange', 'pear'])");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Partition columns .* not present in schema")
    public void testCreateTableNonExistentPartitionColumns()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_nonexistent_partition_columns\n" +
                "(grape bigint, apple varchar, orange bigint, pear varchar)\n" +
                "WITH (partitioned_by = ARRAY['dragonfruit'])");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Unsupported type .* for partition: .*")
    public void testCreateTableUnsupportedPartitionType()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_unsupported_partition_type " +
                "(foo bigint, bar ARRAY(varchar)) " +
                "WITH (partitioned_by = ARRAY['bar'])");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Unsupported type .* for partition: a")
    public void testCreateTableUnsupportedPartitionTypeAs()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_unsupported_partition_type_as " +
                "WITH (partitioned_by = ARRAY['a']) " +
                "AS " +
                "SELECT 123 x, ARRAY ['foo'] a");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Unsupported Hive type: varchar\\(65536\\)\\. Supported VARCHAR types: VARCHAR\\(<=65535\\), VARCHAR\\.")
    public void testCreateTableNonSupportedVarcharColumn()
    {
        assertUpdate("CREATE TABLE test_create_table_non_supported_varchar_column (apple varchar(65536))");
    }

    @Test
    public void testCreatePartitionedBucketedTableAsFewRows()
    {
        // go through all storage formats to make sure the empty buckets are correctly created
        testWithAllStorageFormats(this::testCreatePartitionedBucketedTableAsFewRows);
    }

    private void testCreatePartitionedBucketedTableAsFewRows(Session session, HiveStorageFormat storageFormat)
    {
        testCreatePartitionedBucketedTableAsFewRows(session, storageFormat, true);
        testCreatePartitionedBucketedTableAsFewRows(session, storageFormat, false);
    }

    private void testCreatePartitionedBucketedTableAsFewRows(Session session, HiveStorageFormat storageFormat, boolean createEmpty)
    {
        String tableName = "test_create_partitioned_bucketed_table_as_few_rows";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'partition_key' ], " +
                "bucketed_by = ARRAY[ 'bucket_key' ], " +
                "bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT * " +
                "FROM (" +
                "VALUES " +
                "  (VARCHAR 'a', VARCHAR 'b', VARCHAR 'c'), " +
                "  ('aa', 'bb', 'cc'), " +
                "  ('aaa', 'bbb', 'ccc')" +
                ") t(bucket_key, col, partition_key)";

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                Session.builder(getParallelWriteSession())
                        .setCatalogSessionProperty(catalog, "create_empty_bucket_files", String.valueOf(createEmpty))
                        .build(),
                createTable,
                3);

        verifyPartitionedBucketedTableAsFewRows(storageFormat, tableName);

        assertUpdate(session, "DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testCreatePartitionedBucketedTableAs()
    {
        testCreatePartitionedBucketedTableAs(HiveStorageFormat.RCBINARY);
    }

    private void testCreatePartitionedBucketedTableAs(HiveStorageFormat storageFormat)
    {
        String tableName = "test_create_partitioned_bucketed_table_as";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                "FROM tpch.tiny.orders";

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                getParallelWriteSession(),
                createTable,
                "SELECT count(*) from orders");

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testCreatePartitionedBucketedTableAsWithUnionAll()
    {
        testCreatePartitionedBucketedTableAsWithUnionAll(HiveStorageFormat.RCBINARY);
    }

    private void testCreatePartitionedBucketedTableAsWithUnionAll(HiveStorageFormat storageFormat)
    {
        String tableName = "test_create_partitioned_bucketed_table_as_with_union_all";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                "FROM tpch.tiny.orders " +
                "WHERE length(comment) % 2 = 0 " +
                "UNION ALL " +
                "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                "FROM tpch.tiny.orders " +
                "WHERE length(comment) % 2 = 1";

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                getParallelWriteSession(),
                createTable,
                "SELECT count(*) from orders");

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    private void verifyPartitionedBucketedTable(HiveStorageFormat storageFormat, String tableName)
    {
        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("orderstatus"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("custkey", "custkey2"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);

        List<?> partitions = getPartitions(tableName);
        assertEquals(partitions.size(), 3);

        assertQuery("SELECT * from " + tableName, "SELECT custkey, custkey, comment, orderstatus FROM orders");

        for (int i = 1; i <= 30; i++) {
            assertQuery(
                    format("SELECT * from " + tableName + " where custkey = %d and custkey2 = %d", i, i),
                    format("SELECT custkey, custkey, comment, orderstatus FROM orders where custkey = %d", i));
        }
    }

    @Test
    public void testCreateInvalidBucketedTable()
    {
        testCreateInvalidBucketedTable(HiveStorageFormat.RCBINARY);
    }

    private void testCreateInvalidBucketedTable(HiveStorageFormat storageFormat)
    {
        String tableName = "test_create_invalid_bucketed_table";

        try {
            computeActual("" +
                    "CREATE TABLE " + tableName + " (" +
                    "  a BIGINT," +
                    "  b DOUBLE," +
                    "  p VARCHAR" +
                    ") WITH (" +
                    "format = '" + storageFormat + "', " +
                    "partitioned_by = ARRAY[ 'p' ], " +
                    "bucketed_by = ARRAY[ 'a', 'c' ], " +
                    "bucket_count = 11 " +
                    ")");
            fail();
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "Bucketing columns [c] not present in schema");
        }

        try {
            computeActual("" +
                    "CREATE TABLE " + tableName + " " +
                    "WITH (" +
                    "format = '" + storageFormat + "', " +
                    "partitioned_by = ARRAY[ 'orderstatus' ], " +
                    "bucketed_by = ARRAY[ 'custkey', 'custkey3' ], " +
                    "bucket_count = 11 " +
                    ") " +
                    "AS " +
                    "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                    "FROM tpch.tiny.orders");
            fail();
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "Bucketing columns [custkey3] not present in schema");
        }

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testCreatePartitionedUnionAll()
    {
        assertUpdate("CREATE TABLE test_create_partitioned_union_all (a varchar, ds varchar) WITH (partitioned_by = ARRAY['ds'])");
        assertUpdate("INSERT INTO test_create_partitioned_union_all SELECT 'a', '2013-05-17' UNION ALL SELECT 'b', '2013-05-17'", 2);
        assertUpdate("DROP TABLE test_create_partitioned_union_all");
    }

    @Test
    public void testInsertPartitionedBucketedTableFewRows()
    {
        // go through all storage formats to make sure the empty buckets are correctly created
        testWithAllStorageFormats(this::testInsertPartitionedBucketedTableFewRows);
    }

    private void testInsertPartitionedBucketedTableFewRows(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_partitioned_bucketed_table_few_rows";

        assertUpdate(session, "" +
                "CREATE TABLE " + tableName + " (" +
                "  bucket_key varchar," +
                "  col varchar," +
                "  partition_key varchar)" +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'partition_key' ], " +
                "bucketed_by = ARRAY[ 'bucket_key' ], " +
                "bucket_count = 11)");

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                getParallelWriteSession(),
                "INSERT INTO " + tableName + " " +
                        "VALUES " +
                        "  (VARCHAR 'a', VARCHAR 'b', VARCHAR 'c'), " +
                        "  ('aa', 'bb', 'cc'), " +
                        "  ('aaa', 'bbb', 'ccc')",
                3);

        verifyPartitionedBucketedTableAsFewRows(storageFormat, tableName);

        assertUpdate(session, "DROP TABLE test_insert_partitioned_bucketed_table_few_rows");
        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    private void verifyPartitionedBucketedTableAsFewRows(HiveStorageFormat storageFormat, String tableName)
    {
        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("partition_key"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("bucket_key"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);

        List<?> partitions = getPartitions(tableName);
        assertEquals(partitions.size(), 3);

        MaterializedResult actual = computeActual("SELECT * from " + tableName);
        MaterializedResult expected = resultBuilder(getSession(), canonicalizeType(createUnboundedVarcharType()), canonicalizeType(createUnboundedVarcharType()), canonicalizeType(createUnboundedVarcharType()))
                .row("a", "b", "c")
                .row("aa", "bb", "cc")
                .row("aaa", "bbb", "ccc")
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testCastNullToColumnTypes()
    {
        String tableName = "test_cast_null_to_column_types";

        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  col1 bigint," +
                "  col2 map(bigint, bigint)," +
                "  partition_key varchar)" +
                "WITH (" +
                "  format = 'ORC', " +
                "  partitioned_by = ARRAY[ 'partition_key' ] " +
                ")");

        assertUpdate(format("INSERT INTO %s (col1) VALUES (1), (2), (3)", tableName), 3);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateEmptyNonBucketedPartition()
    {
        String tableName = "test_insert_empty_partitioned_unbucketed_table";
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  dummy_col bigint," +
                "  part varchar)" +
                "WITH (" +
                "  format = 'ORC', " +
                "  partitioned_by = ARRAY[ 'part' ] " +
                ")");
        assertQuery(format("SELECT count(*) FROM \"%s$partitions\"", tableName), "SELECT 0");

        // create an empty partition
        assertUpdate(String.format("CALL system.create_empty_partition('%s', '%s', ARRAY['part'], ARRAY['%s'])", TPCH_SCHEMA, tableName, "empty"));
        assertQuery(format("SELECT count(*) FROM \"%s$partitions\"", tableName), "SELECT 1");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateEmptyBucketedPartition()
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            testCreateEmptyBucketedPartition(storageFormat.getFormat());
        }
    }

    private void testCreateEmptyBucketedPartition(HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_empty_partitioned_bucketed_table";
        createPartitionedBucketedTable(tableName, storageFormat);

        List<String> orderStatusList = ImmutableList.of("F", "O", "P");
        for (int i = 0; i < orderStatusList.size(); i++) {
            String sql = String.format("CALL system.create_empty_partition('%s', '%s', ARRAY['orderstatus'], ARRAY['%s'])", TPCH_SCHEMA, tableName, orderStatusList.get(i));
            assertUpdate(sql);
            assertQuery(
                    format("SELECT count(*) FROM \"%s$partitions\"", tableName),
                    "SELECT " + (i + 1));

            assertQueryFails(sql, "Partition already exists.*");
        }

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testInsertPartitionedBucketedTable()
    {
        testInsertPartitionedBucketedTable(HiveStorageFormat.RCBINARY);
    }

    private void testInsertPartitionedBucketedTable(HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_partitioned_bucketed_table";
        createPartitionedBucketedTable(tableName, storageFormat);

        List<String> orderStatusList = ImmutableList.of("F", "O", "P");
        for (int i = 0; i < orderStatusList.size(); i++) {
            String orderStatus = orderStatusList.get(i);
            assertUpdate(
                    // make sure that we will get one file per bucket regardless of writer count configured
                    getParallelWriteSession(),
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderstatus = '%s'",
                            orderStatus),
                    format("SELECT count(*) from orders where orderstatus = '%s'", orderStatus));
        }

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    private void createPartitionedBucketedTable(String tableName, HiveStorageFormat storageFormat)
    {
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  custkey bigint," +
                "  custkey2 bigint," +
                "  comment varchar," +
                "  orderstatus varchar)" +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11)");
    }

    @Test
    public void testInsertPartitionedBucketedTableWithUnionAll()
    {
        testInsertPartitionedBucketedTableWithUnionAll(HiveStorageFormat.RCBINARY);
    }

    private void testInsertPartitionedBucketedTableWithUnionAll(HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_partitioned_bucketed_table_with_union_all";

        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  custkey bigint," +
                "  custkey2 bigint," +
                "  comment varchar," +
                "  orderstatus varchar)" +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11)");

        List<String> orderStatusList = ImmutableList.of("F", "O", "P");
        for (int i = 0; i < orderStatusList.size(); i++) {
            String orderStatus = orderStatusList.get(i);
            assertUpdate(
                    // make sure that we will get one file per bucket regardless of writer count configured
                    getParallelWriteSession(),
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderstatus = '%s' and length(comment) %% 2 = 0 " +
                                    "UNION ALL " +
                                    "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderstatus = '%s' and length(comment) %% 2 = 1",
                            orderStatus, orderStatus),
                    format("SELECT count(*) from orders where orderstatus = '%s'", orderStatus));
        }

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testInsert()
    {
        testWithAllStorageFormats(this::testInsert);
    }

    private void testInsert(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_insert_format_table " +
                "(" +
                "  _string VARCHAR," +
                "  _varchar VARCHAR(65535)," +
                "  _char CHAR(10)," +
                "  _bigint BIGINT," +
                "  _integer INTEGER," +
                "  _smallint SMALLINT," +
                "  _tinyint TINYINT," +
                "  _real REAL," +
                "  _double DOUBLE," +
                "  _boolean BOOLEAN," +
                "  _decimal_short DECIMAL(3,2)," +
                "  _decimal_long DECIMAL(30,10)" +
                ") " +
                "WITH (format = '" + storageFormat + "') ";

        if (storageFormat == HiveStorageFormat.AVRO) {
            createTable = createTable.replace(" _smallint SMALLINT,", " _smallint INTEGER,");
            createTable = createTable.replace(" _tinyint TINYINT,", " _tinyint INTEGER,");
        }

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_insert_format_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertColumnType(tableMetadata, "_string", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "_varchar", createVarcharType(65535));
        assertColumnType(tableMetadata, "_char", createCharType(10));

        @Language("SQL") String select = "SELECT" +
                " 'foo' _string" +
                ", 'bar' _varchar" +
                ", CAST('boo' AS CHAR(10)) _char" +
                ", 1 _bigint" +
                ", CAST(42 AS INTEGER) _integer" +
                ", CAST(43 AS SMALLINT) _smallint" +
                ", CAST(44 AS TINYINT) _tinyint" +
                ", CAST('123.45' AS REAL) _real" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long";

        if (storageFormat == HiveStorageFormat.AVRO) {
            select = select.replace(" CAST (43 AS SMALLINT) _smallint,", " 3 _smallint,");
            select = select.replace(" CAST (44 AS TINYINT) _tinyint,", " 4 _tinyint,");
        }

        assertUpdate(session, "INSERT INTO test_insert_format_table " + select, 1);

        assertQuery(session, "SELECT * from test_insert_format_table", select);

        assertUpdate(session, "INSERT INTO test_insert_format_table (_tinyint, _smallint, _integer, _bigint, _real, _double) SELECT CAST(1 AS TINYINT), CAST(2 AS SMALLINT), 3, 4, cast(14.3E0 as REAL), 14.3E0", 1);

        assertQuery(session, "SELECT * from test_insert_format_table where _bigint = 4", "SELECT null, null, null, 4, 3, 2, 1, 14.3, 14.3, null, null, null");

        assertQuery(session, "SELECT * from test_insert_format_table where _real = CAST(14.3 as REAL)", "SELECT null, null, null, 4, 3, 2, 1, 14.3, 14.3, null, null, null");

        assertUpdate(session, "INSERT INTO test_insert_format_table (_double, _bigint) SELECT 2.72E0, 3", 1);

        assertQuery(session, "SELECT * from test_insert_format_table where _bigint = 3", "SELECT null, null, null, 3, null, null, null, null, 2.72, null, null, null");

        assertUpdate(session, "INSERT INTO test_insert_format_table (_decimal_short, _decimal_long) SELECT DECIMAL '2.72', DECIMAL '98765432101234567890.0123456789'", 1);

        assertQuery(session, "SELECT * from test_insert_format_table where _decimal_long = DECIMAL '98765432101234567890.0123456789'", "SELECT null, null, null, null, null, null, null, null, null, null, 2.72, 98765432101234567890.0123456789");

        assertUpdate(session, "DROP TABLE test_insert_format_table");

        assertFalse(getQueryRunner().tableExists(session, "test_insert_format_table"));
    }

    @Test
    public void testInsertPartitionedTable()
    {
        testWithAllStorageFormats(this::testInsertPartitionedTable);
        testWithAllStorageFormats(this::testInsertPartitionedTableWithPartitionedRedistribute);
    }

    private void testInsertPartitionedTable(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_insert_partitioned_table " +
                "(" +
                "  ORDER_KEY BIGINT," +
                "  SHIP_PRIORITY INTEGER," +
                "  ORDER_STATUS VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'SHIP_PRIORITY', 'ORDER_STATUS' ]" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_insert_partitioned_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("ship_priority", "order_status"));

        String partitionsTable = "\"test_insert_partitioned_table$partitions\"";

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable,
                "SELECT shippriority, orderstatus FROM orders LIMIT 0");

        // Hive will reorder the partition keys, so we must insert into the table assuming the partition keys have been moved to the end
        Long count = (Long) computeActual("SELECT count(*) from orders").getOnlyValue();
        assertUpdate(
                session,
                "" +
                        "INSERT INTO test_insert_partitioned_table " +
                        "SELECT orderkey, shippriority, orderstatus " +
                        "FROM tpch.tiny.orders",
                "SELECT count(*) from orders");

        // verify the partitions
        List<?> partitions = getPartitions("test_insert_partitioned_table");
        assertEquals(partitions.size(), 3);

        assertQuery(session, "SELECT * from test_insert_partitioned_table", "SELECT orderkey, shippriority, orderstatus FROM orders");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable,
                "SELECT DISTINCT shippriority, orderstatus FROM orders");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable + " ORDER BY order_status LIMIT 2",
                "SELECT DISTINCT shippriority, orderstatus FROM orders ORDER BY orderstatus LIMIT 2");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable + " WHERE order_status = 'O'",
                "SELECT DISTINCT shippriority, orderstatus FROM orders WHERE orderstatus = 'O'");

        assertQueryFails(session, "SELECT * FROM " + partitionsTable + " WHERE no_such_column = 1", "line \\S*: Column 'no_such_column' cannot be resolved");
        assertQueryFails(session, "SELECT * FROM " + partitionsTable + " WHERE orderkey = 1", "line \\S*: Column 'orderkey' cannot be resolved");

        assertUpdate(session, "DROP TABLE test_insert_partitioned_table");

        assertFalse(getQueryRunner().tableExists(session, "test_insert_partitioned_table"));
    }

    // Presto: test case for partitioned redistribute writes type
    private void testInsertPartitionedTableWithPartitionedRedistribute(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_insert_partitioned_table " +
                "(" +
                "  ORDER_KEY BIGINT," +
                "  SHIP_PRIORITY INTEGER," +
                "  ORDER_STATUS VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'SHIP_PRIORITY', 'ORDER_STATUS' ]" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_insert_partitioned_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("ship_priority", "order_status"));

        String partitionsTable = "\"test_insert_partitioned_table$partitions\"";

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable,
                "SELECT shippriority, orderstatus FROM orders LIMIT 0");

        // Hive will reorder the partition keys, so we must insert into the table assuming the partition keys have been moved to the end
        Long count = (Long) computeActual("SELECT count(*) from orders").getOnlyValue();
        assertUpdate(
                Session.builder(getSession())
                        .setSystemProperty("redistribute_writes_type", "PARTITIONED")
                        .build(),
                "" +
                        "INSERT INTO test_insert_partitioned_table " +
                        "SELECT orderkey, shippriority, orderstatus " +
                        "FROM tpch.tiny.orders",
                count, assertRemotePartitionedExchange("orderstatus"));

        // verify the partitions
        List<?> partitions = getPartitions("test_insert_partitioned_table");
        assertEquals(partitions.size(), 3);

        assertQuery(session, "SELECT * from test_insert_partitioned_table", "SELECT orderkey, shippriority, orderstatus FROM orders");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable,
                "SELECT DISTINCT shippriority, orderstatus FROM orders");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable + " ORDER BY order_status LIMIT 2",
                "SELECT DISTINCT shippriority, orderstatus FROM orders ORDER BY orderstatus LIMIT 2");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable + " WHERE order_status = 'O'",
                "SELECT DISTINCT shippriority, orderstatus FROM orders WHERE orderstatus = 'O'");

        assertUpdate(session, "DROP TABLE test_insert_partitioned_table");

        assertFalse(getQueryRunner().tableExists(session, "test_insert_partitioned_table"));
    }

    @Test
    public void testInsertPartitionedTableExistingPartition()
    {
        testWithAllStorageFormats(this::testInsertPartitionedTableExistingPartition);
    }

    private void testInsertPartitionedTableExistingPartition(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_partitioned_table_existing_partition";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  order_key BIGINT," +
                "  comment VARCHAR," +
                "  order_status VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'order_status' ]" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("order_status"));

        for (int i = 0; i < 3; i++) {
            assertUpdate(
                    session,
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT orderkey, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderkey %% 3 = %d",
                            i),
                    format("SELECT count(*) from orders where orderkey %% 3 = %d", i));
        }

        // verify the partitions
        List<?> partitions = getPartitions(tableName);
        assertEquals(partitions.size(), 3);

        assertQuery(
                session,
                "SELECT * from " + tableName,
                "SELECT orderkey, comment, orderstatus FROM orders");

        assertUpdate(session, "DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testInsertPartitionedTableOverwriteExistingPartition()
    {
        testInsertPartitionedTableOverwriteExistingPartition(
                Session.builder(getSession())
                        .setCatalogSessionProperty(catalog, "insert_existing_partitions_behavior", "OVERWRITE")
                        .build(),
                HiveStorageFormat.ORC, false);
    }

    @Test
    public void testInsertPartitionedTxnTableOverwriteExistingPartition()
    {
        testInsertPartitionedTableOverwriteExistingPartition(
                Session.builder(getSession())
                        .setCatalogSessionProperty(catalog, "insert_existing_partitions_behavior", "OVERWRITE")
                        .build(),
                HiveStorageFormat.ORC, true);
    }

    private void testInsertPartitionedTableOverwriteExistingPartition(Session session, HiveStorageFormat storageFormat, boolean transactional)
    {
        String tableName = "test_insert_partitioned_table_overwrite_existing_partition";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  order_key BIGINT," +
                "  comment VARCHAR," +
                "  order_status VARCHAR" +
                ") " +
                "WITH (" +
                (transactional ? "transactional=true, " : "") +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'order_status' ]" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("order_status"));

        for (int i = 0; i < 3; i++) {
            assertUpdate(
                    session,
                    format(
                            "INSERT OVERWRITE " + tableName + " " +
                                    "SELECT orderkey, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderkey %% 3 = %d",
                            i),
                    format("SELECT count(*) from orders where orderkey %% 3 = %d", i));

            // verify the partitions
            List<?> partitions = getPartitions(tableName);
            assertEquals(partitions.size(), 3);

            assertQuery(
                    session,
                    "SELECT * from " + tableName,
                    format("SELECT orderkey, comment, orderstatus FROM orders where orderkey %% 3 = %d", i));
            if (transactional) {
                TableMetadata metadata = getTableMetadata("hive", session.getSchema().get(), tableName);
                String tablePath = (String) tableMetadata.getMetadata().getProperties().get("location");
                File file = new File(tablePath.replace("file:", ""));
                File[] partitionsLocations = file.listFiles((a) -> a.isDirectory() && !a.getName().startsWith("."));
                int expectedBaseCount = i + 1;
                Arrays.stream(partitionsLocations).forEach((partition) -> {
                    File[] baseDirectories = partition.listFiles((f) -> f.isDirectory() && f.getName().startsWith("base_"));
                    //In case of transactional insert_overwrite base directory is written directly instead of delta.
                    assertEquals(expectedBaseCount, baseDirectories.length);
                });
            }
        }
        assertUpdate(session, "DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testNullPartitionValues()
    {
        assertUpdate("" +
                "CREATE TABLE test_null_partition (test VARCHAR, part VARCHAR)\n" +
                "WITH (partitioned_by = ARRAY['part'])");

        assertUpdate("INSERT INTO test_null_partition VALUES ('hello', 'test'), ('world', null)", 2);

        assertQuery(
                "SELECT * FROM test_null_partition",
                "VALUES ('hello', 'test'), ('world', null)");

        assertQuery(
                "SELECT * FROM \"test_null_partition$partitions\"",
                "VALUES 'test', null");

        assertUpdate("DROP TABLE test_null_partition");
    }

    @Test
    public void testPartitionPerScanLimit()
    {
        TestingHiveStorageFormat storageFormat = new TestingHiveStorageFormat(getSession(), HiveStorageFormat.ORC);
        testWithStorageFormat(storageFormat, this::testPartitionPerScanLimit);
    }

    private void testPartitionPerScanLimit(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_partition_per_scan_limit";
        String partitionsTable = "\"" + tableName + "$partitions\"";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  foo VARCHAR," +
                "  part BIGINT" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'part' ]" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("part"));

        // insert 1200 partitions
        for (int i = 0; i < 12; i++) {
            int partStart = i * 100;
            int partEnd = (i + 1) * 100 - 1;

            @Language("SQL") String insertPartitions = "" +
                    "INSERT INTO " + tableName + " " +
                    "SELECT 'bar' foo, part " +
                    "FROM UNNEST(SEQUENCE(" + partStart + ", " + partEnd + ")) AS TMP(part)";

            assertUpdate(session, insertPartitions, 100);
        }

        // we are not constrained by hive.max-partitions-per-scan when listing partitions
        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable + " WHERE part > 490 and part <= 500",
                "VALUES 491, 492, 493, 494, 495, 496, 497, 498, 499, 500");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable + " WHERE part < 0",
                "SELECT null WHERE false");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable,
                "VALUES " + LongStream.range(0, 1200)
                        .mapToObj(String::valueOf)
                        .collect(joining(",")));

        // verify can query 1000 partitions
        assertQuery(
                session,
                "SELECT count(foo) FROM " + tableName + " WHERE part < 1000",
                "SELECT 1000");

        // verify the rest 200 partitions are successfully inserted
        assertQuery(
                session,
                "SELECT count(foo) FROM " + tableName + " WHERE part >= 1000 AND part < 1200",
                "SELECT 200");

        // verify cannot query more than 1000 partitions
        assertQueryFails(
                session,
                "SELECT * from " + tableName + " WHERE part < 1001",
                format("Query over table 'tpch.%s' can potentially read more than 1000 partitions", tableName));

        // verify cannot query all partitions
        assertQueryFails(
                session,
                "SELECT * from " + tableName,
                format("Query over table 'tpch.%s' can potentially read more than 1000 partitions", tableName));

        assertUpdate(session, "DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testShowColumnsFromPartitions()
    {
        String tableName = "test_show_columns_from_partitions";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  foo VARCHAR," +
                "  part1 BIGINT," +
                "  part2 VARCHAR" +
                ") " +
                "WITH (" +
                "partitioned_by = ARRAY[ 'part1', 'part2' ]" +
                ") ";

        assertUpdate(getSession(), createTable);

        assertQuery(
                getSession(),
                "SHOW COLUMNS FROM \"" + tableName + "$partitions\"",
                "VALUES ('part1', 'bigint', '', ''), ('part2', 'varchar', '', '')");

        assertQueryFails(
                getSession(),
                "SHOW COLUMNS FROM \"$partitions\"",
                ".*Table '.*\\.tpch\\.\\$partitions' does not exist");

        assertQueryFails(
                getSession(),
                "SHOW COLUMNS FROM \"orders$partitions\"",
                ".*Table '.*\\.tpch\\.orders\\$partitions' does not exist");

        assertQueryFails(
                getSession(),
                "SHOW COLUMNS FROM \"blah$partitions\"",
                ".*Table '.*\\.tpch\\.blah\\$partitions' does not exist");
    }

    @Test
    public void testPartitionsTableInvalidAccess()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitions_invalid " +
                "(" +
                "  foo VARCHAR," +
                "  part1 BIGINT," +
                "  part2 VARCHAR" +
                ") " +
                "WITH (" +
                "partitioned_by = ARRAY[ 'part1', 'part2' ]" +
                ") ";

        assertUpdate(getSession(), createTable);

        assertQueryFails(
                getSession(),
                "SELECT * FROM \"test_partitions_invalid$partitions$partitions\"",
                ".*Table .*\\.tpch\\.test_partitions_invalid\\$partitions\\$partitions does not exist");

        assertQueryFails(
                getSession(),
                "SELECT * FROM \"non_existent$partitions\"",
                ".*Table .*\\.tpch\\.non_existent\\$partitions does not exist");
    }

    @Test
    public void testInsertUnpartitionedTable()
    {
        testWithAllStorageFormats(this::testInsertUnpartitionedTable);
    }

    private void testInsertUnpartitionedTable(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_unpartitioned_table";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  order_key BIGINT," +
                "  comment VARCHAR," +
                "  order_status VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "'" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        for (int i = 0; i < 3; i++) {
            assertUpdate(
                    session,
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT orderkey, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderkey %% 3 = %d",
                            i),
                    format("SELECT count(*) from orders where orderkey %% 3 = %d", i));
        }

        assertQuery(
                session,
                "SELECT * from " + tableName,
                "SELECT orderkey, comment, orderstatus FROM orders");

        assertUpdate(session, "DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testDeleteFromUnpartitionedTable()
    {
        assertUpdate("CREATE TABLE test_delete_unpartitioned AS SELECT orderstatus FROM tpch.tiny.orders", "SELECT count(*) from orders");

        assertUpdate("DELETE FROM test_delete_unpartitioned");

        MaterializedResult result = computeActual("SELECT * from test_delete_unpartitioned");
        assertEquals(result.getRowCount(), 0);

        assertUpdate("DROP TABLE test_delete_unpartitioned");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_delete_unpartitioned"));
    }

    @Test
    public void testMetadataDelete()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_metadata_delete " +
                "(" +
                "  ORDER_KEY BIGINT," +
                "  LINE_NUMBER INTEGER," +
                "  LINE_STATUS VARCHAR" +
                ") " +
                "WITH (" +
                PARTITIONED_BY_PROPERTY + " = ARRAY[ 'LINE_NUMBER', 'LINE_STATUS' ]" +
                ") ";

        assertUpdate(createTable);

        assertUpdate("" +
                        "INSERT INTO test_metadata_delete " +
                        "SELECT orderkey, linenumber, linestatus " +
                        "FROM tpch.tiny.lineitem",
                "SELECT count(*) from lineitem");

        // Delete returns number of rows deleted, or null if obtaining the number is hard or impossible.
        // Currently, Hive implementation always returns null.
        assertUpdate("DELETE FROM test_metadata_delete WHERE LINE_STATUS='F' and LINE_NUMBER=CAST(3 AS INTEGER)");

        assertQuery("SELECT * from test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus<>'F' or linenumber<>3");

        assertUpdate("DELETE FROM test_metadata_delete WHERE LINE_STATUS='O'");

        assertQuery("SELECT * from test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus<>'O' and linenumber<>3");

        try {
            getQueryRunner().execute("DELETE FROM test_metadata_delete WHERE ORDER_KEY=1");
            fail("expected exception");
        }
        catch (RuntimeException e) {
            assertEquals(e.getMessage(), "This connector only supports delete where one or more partitions are deleted entirely for Non-Transactional tables");
        }

        assertQuery("SELECT * from test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus<>'O' and linenumber<>3");

        Session session1 = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                .build();
        assertQuery(session1, "SELECT * from test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus<>'O' and linenumber<>3");

        @Language("SQL") String multiPartTable = "" +
                "CREATE TABLE test_multi_part " +
                "(" +
                "  ID1 INTEGER," +
                "  ID2 INTEGER," +
                "  ID3 INTEGER," +
                "  ID4 INTEGER," +
                "  ID5 INTEGER," +
                "  ID6 INTEGER," +
                "  ID7 INTEGER," +
                "  ID8 INTEGER," +
                "  ID9 INTEGER," +
                "  ID10 INTEGER," +
                "  ID11 INTEGER," +
                "  ID12 INTEGER," +
                "  ID13 INTEGER," +
                "  ID14 INTEGER " +
                ") " +
                "WITH (" +
                PARTITIONED_BY_PROPERTY + " = ARRAY[ 'ID2','ID3','ID4','ID5','ID6','ID7','ID8','ID9','ID10','ID11','ID12','ID13','ID14']" +
                ") ";

        assertUpdate(multiPartTable);
        assertUpdate("" +
                        "INSERT INTO test_multi_part values(1,2,3,4,5,6,7,8,9,10,11,12,13,14) ",
                "SELECT 1");
        assertEquals(computeActual("SELECT *, \"$path\" FROM test_multi_part").getRowCount(), 1L);
        assertUpdate("DROP TABLE test_multi_part");
        assertUpdate("DROP TABLE test_metadata_delete");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_metadata_delete"));
    }

    private TableMetadata getTableMetadata(String catalog, String schema, String tableName)
    {
        Session session = getSession();
        Metadata metadata = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getMetadata();

        return transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .readOnly()
                .execute(session, transactionSession -> {
                    Optional<TableHandle> tableHandle = metadata.getTableHandle(transactionSession, new QualifiedObjectName(catalog, schema, tableName));
                    assertTrue(tableHandle.isPresent());
                    return metadata.getTableMetadata(transactionSession, tableHandle.get());
                });
    }

    private Object getHiveTableProperty(String tableName, Function<HiveTableHandle, Object> propertyGetter)
    {
        Session session = getSession();
        Metadata metadata = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getMetadata();

        return transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .readOnly()
                .execute(session, transactionSession -> {
                    QualifiedObjectName name = new QualifiedObjectName(catalog, TPCH_SCHEMA, tableName);
                    TableHandle table = metadata.getTableHandle(transactionSession, name)
                            .orElseThrow(() -> new AssertionError("table not found: " + name));
                    table = metadata.applyFilter(transactionSession, table, Constraint.alwaysTrue())
                            .orElseThrow(() -> new AssertionError("applyFilter did not return a result"))
                            .getHandle();
                    return propertyGetter.apply((HiveTableHandle) table.getConnectorHandle());
                });
    }

    private List<?> getPartitions(String tableName)
    {
        return (List<?>) getHiveTableProperty(tableName, handle -> handle.getPartitions().get());
    }

    private int getBucketCount(String tableName)
    {
        return (int) getHiveTableProperty(tableName, table -> table.getBucketHandle().get().getTableBucketCount());
    }

    @Test
    public void testShowColumnsPartitionKey()
    {
        assertUpdate("" +
                "CREATE TABLE test_show_columns_partition_key\n" +
                "(grape bigint, orange bigint, pear varchar(65535), mango integer, lychee smallint, kiwi tinyint, apple varchar, pineapple varchar(65535))\n" +
                "WITH (partitioned_by = ARRAY['apple', 'pineapple'])");

        MaterializedResult actual = computeActual("SHOW COLUMNS FROM test_show_columns_partition_key");
        Type unboundedVarchar = canonicalizeType(VARCHAR);
        MaterializedResult expected = resultBuilder(getSession(), unboundedVarchar, unboundedVarchar, unboundedVarchar, unboundedVarchar)
                .row("grape", canonicalizeTypeName("bigint"), "", "")
                .row("orange", canonicalizeTypeName("bigint"), "", "")
                .row("pear", canonicalizeTypeName("varchar(65535)"), "", "")
                .row("mango", canonicalizeTypeName("integer"), "", "")
                .row("lychee", canonicalizeTypeName("smallint"), "", "")
                .row("kiwi", canonicalizeTypeName("tinyint"), "", "")
                .row("apple", canonicalizeTypeName("varchar"), "partition key", "")
                .row("pineapple", canonicalizeTypeName("varchar(65535)"), "partition key", "")
                .build();
        assertEquals(actual, expected);
    }

    // TODO: These should be moved to another class, when more connectors support arrays
    @Test
    public void testArrays()
    {
        assertUpdate("CREATE TABLE tmp_array1 AS SELECT ARRAY[1, 2, NULL] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array1", "SELECT 2");
        assertQuery("SELECT col[3] FROM tmp_array1", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array2 AS SELECT ARRAY[1.0E0, 2.5E0, 3.5E0] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array2", "SELECT 2.5");

        assertUpdate("CREATE TABLE tmp_array3 AS SELECT ARRAY['puppies', 'kittens', NULL] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array3", "SELECT 'kittens'");
        assertQuery("SELECT col[3] FROM tmp_array3", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array4 AS SELECT ARRAY[TRUE, NULL] AS col", 1);
        assertQuery("SELECT col[1] FROM tmp_array4", "SELECT TRUE");
        assertQuery("SELECT col[2] FROM tmp_array4", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array5 AS SELECT ARRAY[ARRAY[1, 2], NULL, ARRAY[3, 4]] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array5", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array6 AS SELECT ARRAY[ARRAY['\"hi\"'], NULL, ARRAY['puppies']] AS col", 1);
        assertQuery("SELECT col[1][1] FROM tmp_array6", "SELECT '\"hi\"'");
        assertQuery("SELECT col[3][1] FROM tmp_array6", "SELECT 'puppies'");

        assertUpdate("CREATE TABLE tmp_array7 AS SELECT ARRAY[ARRAY[INTEGER'1', INTEGER'2'], NULL, ARRAY[INTEGER'3', INTEGER'4']] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array7", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array8 AS SELECT ARRAY[ARRAY[SMALLINT'1', SMALLINT'2'], NULL, ARRAY[SMALLINT'3', SMALLINT'4']] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array8", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array9 AS SELECT ARRAY[ARRAY[TINYINT'1', TINYINT'2'], NULL, ARRAY[TINYINT'3', TINYINT'4']] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array9", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array10 AS SELECT ARRAY[ARRAY[DECIMAL '3.14']] AS col1, ARRAY[ARRAY[DECIMAL '12345678901234567890.0123456789']] AS col2", 1);
        assertQuery("SELECT col1[1][1] FROM tmp_array10", "SELECT 3.14");
        assertQuery("SELECT col2[1][1] FROM tmp_array10", "SELECT 12345678901234567890.0123456789");

        assertUpdate("CREATE TABLE tmp_array13 AS SELECT ARRAY[ARRAY[REAL'1.234', REAL'2.345'], NULL, ARRAY[REAL'3.456', REAL'4.567']] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array13", "SELECT 2.345");
    }

    @Test
    public void testTemporalArrays()
    {
        assertUpdate("CREATE TABLE tmp_array11 AS SELECT ARRAY[DATE '2014-09-30'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM tmp_array11");
        assertUpdate("CREATE TABLE tmp_array12 AS SELECT ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM tmp_array12");
    }

    @Test
    public void testMaps()
    {
        assertUpdate("CREATE TABLE tmp_map1 AS SELECT MAP(ARRAY[0,1], ARRAY[2,NULL]) AS col", 1);
        assertQuery("SELECT col[0] FROM tmp_map1", "SELECT 2");
        assertQuery("SELECT col[1] FROM tmp_map1", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_map2 AS SELECT MAP(ARRAY[INTEGER'1'], ARRAY[INTEGER'2']) AS col", 1);
        assertQuery("SELECT col[INTEGER'1'] FROM tmp_map2", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_map3 AS SELECT MAP(ARRAY[SMALLINT'1'], ARRAY[SMALLINT'2']) AS col", 1);
        assertQuery("SELECT col[SMALLINT'1'] FROM tmp_map3", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_map4 AS SELECT MAP(ARRAY[TINYINT'1'], ARRAY[TINYINT'2']) AS col", 1);
        assertQuery("SELECT col[TINYINT'1'] FROM tmp_map4", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_map5 AS SELECT MAP(ARRAY[1.0], ARRAY[2.5]) AS col", 1);
        assertQuery("SELECT col[1.0] FROM tmp_map5", "SELECT 2.5");

        assertUpdate("CREATE TABLE tmp_map6 AS SELECT MAP(ARRAY['puppies'], ARRAY['kittens']) AS col", 1);
        assertQuery("SELECT col['puppies'] FROM tmp_map6", "SELECT 'kittens'");

        assertUpdate("CREATE TABLE tmp_map7 AS SELECT MAP(ARRAY[TRUE], ARRAY[FALSE]) AS col", 1);
        assertQuery("SELECT col[TRUE] FROM tmp_map7", "SELECT FALSE");

        assertUpdate("CREATE TABLE tmp_map8 AS SELECT MAP(ARRAY[DATE '2014-09-30'], ARRAY[DATE '2014-09-29']) AS col", 1);
        assertOneNotNullResult("SELECT col[DATE '2014-09-30'] FROM tmp_map8");
        assertUpdate("CREATE TABLE tmp_map9 AS SELECT MAP(ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'], ARRAY[TIMESTAMP '2001-08-22 03:04:05.321']) AS col", 1);
        assertOneNotNullResult("SELECT col[TIMESTAMP '2001-08-22 03:04:05.321'] FROM tmp_map9");

        assertUpdate("CREATE TABLE tmp_map10 AS SELECT MAP(ARRAY[DECIMAL '3.14', DECIMAL '12345678901234567890.0123456789'], " +
                "ARRAY[DECIMAL '12345678901234567890.0123456789', DECIMAL '3.0123456789']) AS col", 1);
        assertQuery("SELECT col[DECIMAL '3.14'], col[DECIMAL '12345678901234567890.0123456789'] FROM tmp_map10", "SELECT 12345678901234567890.0123456789, 3.0123456789");

        assertUpdate("CREATE TABLE tmp_map11 AS SELECT MAP(ARRAY[REAL'1.234'], ARRAY[REAL'2.345']) AS col", 1);
        assertQuery("SELECT col[REAL'1.234'] FROM tmp_map11", "SELECT 2.345");

        assertUpdate("CREATE TABLE tmp_map12 AS SELECT MAP(ARRAY[1.0E0], ARRAY[ARRAY[1, 2]]) AS col", 1);
        assertQuery("SELECT col[1.0][2] FROM tmp_map12", "SELECT 2");
    }

    @Test
    public void testRows()
    {
        assertUpdate("CREATE TABLE tmp_row1 AS SELECT cast(row(CAST(1 as BIGINT), CAST(NULL as BIGINT)) AS row(col0 bigint, col1 bigint)) AS a", 1);
        assertQuery(
                "SELECT a.col0, a.col1 FROM tmp_row1",
                "SELECT 1, cast(null as bigint)");
    }

    @Test
    public void testComplex()
    {
        assertUpdate("CREATE TABLE tmp_complex1 AS SELECT " +
                        "ARRAY [MAP(ARRAY['a', 'b'], ARRAY[2.0E0, 4.0E0]), MAP(ARRAY['c', 'd'], ARRAY[12.0E0, 14.0E0])] AS a",
                1);

        assertQuery(
                "SELECT a[1]['a'], a[2]['d'] FROM tmp_complex1",
                "SELECT 2.0, 14.0");
    }

    @Test
    public void testBucketedCatalog()
    {
        String bucketedCatalog = bucketedSession.getCatalog().get();
        String bucketedSchema = bucketedSession.getSchema().get();

        TableMetadata ordersTableMetadata = getTableMetadata(bucketedCatalog, bucketedSchema, "orders");
        assertEquals(ordersTableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("custkey"));
        assertEquals(ordersTableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);

        TableMetadata customerTableMetadata = getTableMetadata(bucketedCatalog, bucketedSchema, "customer");
        assertEquals(customerTableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("custkey"));
        assertEquals(customerTableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);
    }

    @Test
    public void testBucketedExecution()
    {
        assertQuery(bucketedSession, "select count(*) a from orders t1 join orders t2 on t1.custkey=t2.custkey");
        assertQuery(bucketedSession, "select count(*) a from orders t1 join customer t2 on t1.custkey=t2.custkey", "SELECT count(*) from orders");
        assertQuery(bucketedSession, "select count(distinct custkey) from orders");

        assertQuery(
                Session.builder(bucketedSession).setSystemProperty("task_writer_count", "1").build(),
                "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
        assertQuery(
                Session.builder(bucketedSession).setSystemProperty("task_writer_count", "4").build(),
                "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
    }

    @Test
    public void testScaleWriters()
    {
        try {
            // small table that will only have one writer
            assertUpdate(
                    Session.builder(getSession())
                            .setSystemProperty("scale_writers", "true")
                            .setSystemProperty("writer_min_size", "32MB")
                            .build(),
                    "CREATE TABLE scale_writers_small AS SELECT * FROM tpch.tiny.orders",
                    (long) computeActual("SELECT count(*) FROM tpch.tiny.orders").getOnlyValue());

            assertEquals(computeActual("SELECT count(DISTINCT \"$path\") FROM scale_writers_small").getOnlyValue(), 1L);

            // large table that will scale writers to multiple machines
            assertUpdate(
                    Session.builder(getSession())
                            .setSystemProperty("scale_writers", "true")
                            .setSystemProperty("writer_min_size", "1MB")
                            .build(),
                    "CREATE TABLE scale_writers_large WITH (format = 'RCBINARY') AS SELECT * FROM tpch.sf1.orders",
                    (long) computeActual("SELECT count(*) FROM tpch.sf1.orders").getOnlyValue());

            long files = (long) computeScalar("SELECT count(DISTINCT \"$path\") FROM scale_writers_large");
            long workers = (long) computeScalar("SELECT count(*) FROM system.runtime.nodes");
            assertThat(files).isBetween(2L, workers);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS scale_writers_large");
            assertUpdate("DROP TABLE IF EXISTS scale_writers_small");
        }
    }

    @Test
    public void testTableCommentsTable()
    {
        assertUpdate("CREATE TABLE test_comment (c1 bigint) COMMENT 'foo'");
        String selectTableComment = format("" +
                        "SELECT comment FROM system.metadata.table_comments " +
                        "WHERE catalog_name = '%s' AND schema_name = '%s' AND table_name = 'test_comment'",
                getSession().getCatalog().get(),
                getSession().getSchema().get());
        assertQuery(selectTableComment, "SELECT 'foo'");

        assertUpdate("DROP TABLE IF EXISTS test_comment");
    }

    @Test
    public void testShowCreateTable()
    {
        String createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   c1 bigint,\n" +
                        "   c2 double,\n" +
                        "   \"c 3\" varchar,\n" +
                        "   \"c'4\" array(bigint),\n" +
                        "   c5 map(bigint, varchar)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'RCBINARY'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_show_create_table");

        assertUpdate(createTableSql);
        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE test_show_create_table");
        assertShowCreateTableOutput(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

        createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   c1 bigint,\n" +
                        "   \"c 2\" varchar,\n" +
                        "   \"c'3\" array(bigint),\n" +
                        "   c4 map(bigint, varchar) COMMENT 'comment test4',\n" +
                        "   c5 double COMMENT 'comment test5'\n)\n" +
                        "COMMENT 'test'\n" +
                        "WITH (\n" +
                        "   bucket_count = 5,\n" +
                        "   bucketed_by = ARRAY['c1','c 2'],\n" +
                        "   bucketing_version = 1,\n" +
                        "   format = 'ORC',\n" +
                        "   orc_bloom_filter_columns = ARRAY['c1','c2'],\n" +
                        "   orc_bloom_filter_fpp = 7E-1,\n" +
                        "   partitioned_by = ARRAY['c5'],\n" +
                        "   sorted_by = ARRAY['c1','c 2 DESC']\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "\"test_show_create_table'2\"");
        assertUpdate(createTableSql);
        actualResult = computeActual("SHOW CREATE TABLE \"test_show_create_table'2\"");
        assertShowCreateTableOutput(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);
    }

    @Test
    public void testCreateExternalTable()
            throws Exception
    {
        File tempDir = createTempDirectory(getClass().getName()).toFile();
        File dataFile = new File(tempDir, "test.txt");
        asCharSink(dataFile, UTF_8).write("hello\nworld\n");

        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.%s.test_create_external (\n" +
                        "   name varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   location = '%s',\n" +
                        "   external = true,\n" +
                        "   format = 'TEXTFILE'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                new Path(tempDir.toURI().toASCIIString()).toString());

        assertUpdate(createTableSql);
        MaterializedResult actual = computeActual("SHOW CREATE TABLE test_create_external");
        assertShowCreateTableOutput(actual.getOnlyValue(), createTableSql);

        actual = computeActual("SELECT name FROM test_create_external");
        assertEquals(actual.getOnlyColumnAsSet(), ImmutableSet.of("hello", "world"));

        assertUpdate("DROP TABLE test_create_external");

        // file should still exist after drop
        assertFile(dataFile);

        deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testCreateTableWithSortedBy()
    {
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.%s.test_create_sorted (\n" +
                        "   viewTime int,\n" +
                        "   userID bigint\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   bucketed_by = ARRAY['userID'],\n" +
                        "   sorted_by = ARRAY['viewTime'],\n" +
                        "   bucket_count = 3,\n" +
                        "   format = 'TEXTFILE'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get());

        assertUpdate(createTableSql);

        String expectedSql = format("" +
                        "CREATE TABLE %s.%s.test_create_sorted (\n" +
                        "   viewtime int,\n" +
                        "   userid bigint\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   bucketed_by = ARRAY['userid'],\n" +
                        "   sorted_by = ARRAY['viewtime'],\n" +
                        "   bucket_count = 3,\n" +
                        "   format = 'TEXTFILE'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get());
        MaterializedResult actual = computeActual("SHOW CREATE TABLE test_create_sorted");
        assertShowCreateTableOutput(actual.getOnlyValue(), expectedSql);

        assertUpdate("DROP TABLE test_create_sorted");
    }

    @Test
    public void testCommentTable()
    {
        String createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   c1 bigint\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'RCBINARY'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_comment_table");

        assertUpdate(createTableSql);
        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE test_comment_table");
        assertShowCreateTableOutput(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

        assertUpdate("COMMENT ON TABLE test_comment_table IS 'new comment'");
        String commentedCreateTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   c1 bigint\n" +
                        ")\n" +
                        "COMMENT 'new comment'\n" +
                        "WITH (\n" +
                        "   format = 'RCBINARY'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_comment_table");
        actualResult = computeActual("SHOW CREATE TABLE test_comment_table");
        assertShowCreateTableOutput(getOnlyElement(actualResult.getOnlyColumnAsSet()), commentedCreateTableSql);

        assertUpdate("COMMENT ON TABLE test_comment_table IS 'updated comment'");
        commentedCreateTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   c1 bigint\n" +
                        ")\n" +
                        "COMMENT 'updated comment'\n" +
                        "WITH (\n" +
                        "   format = 'RCBINARY'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_comment_table");
        actualResult = computeActual("SHOW CREATE TABLE test_comment_table");
        assertShowCreateTableOutput(getOnlyElement(actualResult.getOnlyColumnAsSet()), commentedCreateTableSql);

        assertUpdate("COMMENT ON TABLE test_comment_table IS ''");
        commentedCreateTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   c1 bigint\n" +
                        ")\n" +
                        "COMMENT ''\n" +
                        "WITH (\n" +
                        "   format = 'RCBINARY'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_comment_table");
        actualResult = computeActual("SHOW CREATE TABLE test_comment_table");
        assertShowCreateTableOutput(getOnlyElement(actualResult.getOnlyColumnAsSet()), commentedCreateTableSql);

        assertUpdate("DROP TABLE test_comment_table");
    }

    @Test
    public void testCreateTableWithHeaderAndFooter()
    {
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.%s.test_table_skip_header (\n" +
                        "   name varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'TEXTFILE',\n" +
                        "   textfile_skip_header_line_count = 1\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get());

        assertUpdate(createTableSql);

        MaterializedResult actual = computeActual("SHOW CREATE TABLE test_table_skip_header");
        assertShowCreateTableOutput(actual.getOnlyValue(), createTableSql);
        assertUpdate("DROP TABLE test_table_skip_header");

        createTableSql = format("" +
                        "CREATE TABLE %s.%s.test_table_skip_footer (\n" +
                        "   name varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'TEXTFILE',\n" +
                        "   textfile_skip_footer_line_count = 1\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get());

        assertUpdate(createTableSql);

        actual = computeActual("SHOW CREATE TABLE test_table_skip_footer");
        assertShowCreateTableOutput(actual.getOnlyValue(), createTableSql);
        assertUpdate("DROP TABLE test_table_skip_footer");

        createTableSql = format("" +
                        "CREATE TABLE %s.%s.test_table_skip_header_footer (\n" +
                        "   name varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'TEXTFILE',\n" +
                        "   textfile_skip_footer_line_count = 1,\n" +
                        "   textfile_skip_header_line_count = 1\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get());

        assertUpdate(createTableSql);

        actual = computeActual("SHOW CREATE TABLE test_table_skip_header_footer");
        assertShowCreateTableOutput(actual.getOnlyValue(), createTableSql);
        assertUpdate("DROP TABLE test_table_skip_header_footer");
    }

    @Test
    public void testCreateTableWithInvalidProperties()
    {
        // ORC
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 bigint) WITH (format = 'TEXTFILE', orc_bloom_filter_columns = ARRAY['col1'])"))
                .hasMessageMatching("Cannot specify orc_bloom_filter_columns table property for storage format: TEXTFILE");

        // TEXTFILE
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE test_orc_skip_header (col1 bigint) WITH (format = 'ORC', textfile_skip_header_line_count = 1)"))
                .hasMessageMatching("Cannot specify textfile_skip_header_line_count table property for storage format: ORC");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE test_orc_skip_footer (col1 bigint) WITH (format = 'ORC', textfile_skip_footer_line_count = 1)"))
                .hasMessageMatching("Cannot specify textfile_skip_footer_line_count table property for storage format: ORC");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE test_invalid_skip_header (col1 bigint) WITH (format = 'TEXTFILE', textfile_skip_header_line_count = -1)"))
                .hasMessageMatching("Invalid value for textfile_skip_header_line_count property: -1");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE test_invalid_skip_footer (col1 bigint) WITH (format = 'TEXTFILE', textfile_skip_footer_line_count = -1)"))
                .hasMessageMatching("Invalid value for textfile_skip_footer_line_count property: -1");

        // CSV
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 bigint) WITH (format = 'ORC', csv_separator = 'S')"))
                .hasMessageMatching("Cannot specify csv_separator table property for storage format: ORC");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 varchar) WITH (format = 'CSV', csv_separator = 'SS')"))
                .hasMessageMatching("csv_separator must be a single character string, but was: 'SS'");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 bigint) WITH (format = 'ORC', csv_quote = 'Q')"))
                .hasMessageMatching("Cannot specify csv_quote table property for storage format: ORC");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 varchar) WITH (format = 'CSV', csv_quote = 'QQ')"))
                .hasMessageMatching("csv_quote must be a single character string, but was: 'QQ'");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 varchar) WITH (format = 'ORC', csv_escape = 'E')"))
                .hasMessageMatching("Cannot specify csv_escape table property for storage format: ORC");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 varchar) WITH (format = 'CSV', csv_escape = 'EE')"))
                .hasMessageMatching("csv_escape must be a single character string, but was: 'EE'");
    }

    @Test
    public void testPathHiddenColumn()
    {
        testWithAllStorageFormats(this::testPathHiddenColumn);
    }

    private void testPathHiddenColumn(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "CREATE TABLE test_path " +
                "WITH (" +
                "format = '" + storageFormat + "'," +
                "partitioned_by = ARRAY['col1']" +
                ") AS " +
                "SELECT * FROM (VALUES " +
                "(0, 0), (3, 0), (6, 0), " +
                "(1, 1), (4, 1), (7, 1), " +
                "(2, 2), (5, 2) " +
                " ) t(col0, col1) ";
        assertUpdate(session, createTable, 8);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_path"));

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_path");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        List<String> columnNames = ImmutableList.of("col0", "col1", PATH_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();
        assertEquals(columnMetadatas.size(), columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertEquals(columnMetadata.getName(), columnNames.get(i));
            if (columnMetadata.getName().equals(PATH_COLUMN_NAME)) {
                // $path should be hidden column
                assertTrue(columnMetadata.isHidden());
            }
        }
        assertEquals(getPartitions("test_path").size(), 3);

        MaterializedResult results = computeActual(session, format("SELECT *, \"%s\" FROM test_path", PATH_COLUMN_NAME));
        Map<Integer, String> partitionPathMap = new HashMap<>();
        for (int i = 0; i < results.getRowCount(); i++) {
            MaterializedRow row = results.getMaterializedRows().get(i);
            int col0 = (int) row.getField(0);
            int col1 = (int) row.getField(1);
            String pathName = (String) row.getField(2);
            String parentDirectory = new Path(pathName).getParent().toString();

            assertTrue(pathName.length() > 0);
            assertEquals(col0 % 3, col1);
            if (partitionPathMap.containsKey(col1)) {
                // the rows in the same partition should be in the same partition directory
                assertEquals(partitionPathMap.get(col1), parentDirectory);
            }
            else {
                partitionPathMap.put(col1, parentDirectory);
            }
        }
        assertEquals(partitionPathMap.size(), 3);

        assertUpdate(session, "DROP TABLE test_path");
        assertFalse(getQueryRunner().tableExists(session, "test_path"));
    }

    @Test
    public void testBucketHiddenColumn()
    {
        @Language("SQL") String createTable = "CREATE TABLE test_bucket_hidden_column " +
                "WITH (" +
                "bucketed_by = ARRAY['col0']," +
                "bucket_count = 2" +
                ") AS " +
                "SELECT * FROM (VALUES " +
                "(0, 11), (1, 12), (2, 13), " +
                "(3, 14), (4, 15), (5, 16), " +
                "(6, 17), (7, 18), (8, 19)" +
                " ) t (col0, col1) ";
        assertUpdate(createTable, 9);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_bucket_hidden_column"));

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_bucket_hidden_column");
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("col0"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 2);

        List<String> columnNames = ImmutableList.of("col0", "col1", PATH_COLUMN_NAME, BUCKET_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();
        assertEquals(columnMetadatas.size(), columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertEquals(columnMetadata.getName(), columnNames.get(i));
            if (columnMetadata.getName().equals(BUCKET_COLUMN_NAME)) {
                // $bucket_number should be hidden column
                assertTrue(columnMetadata.isHidden());
            }
        }
        assertEquals(getBucketCount("test_bucket_hidden_column"), 2);

        MaterializedResult results = computeActual(format("SELECT *, \"%1$s\" FROM test_bucket_hidden_column WHERE \"%1$s\" = 1",
                BUCKET_COLUMN_NAME));
        for (int i = 0; i < results.getRowCount(); i++) {
            MaterializedRow row = results.getMaterializedRows().get(i);
            int col0 = (int) row.getField(0);
            int col1 = (int) row.getField(1);
            int bucket = (int) row.getField(2);

            assertEquals(col1, col0 + 11);
            assertTrue(col1 % 2 == 0);

            // Because Hive's hash function for integer n is h(n) = n.
            assertEquals(bucket, col0 % 2);
        }
        assertEquals(results.getRowCount(), 4);

        assertUpdate("DROP TABLE test_bucket_hidden_column");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_bucket_hidden_column"));
    }

    @Test
    public void testDeleteAndInsert()
    {
        Session session = getSession();

        // Partition 1 is untouched
        // Partition 2 is altered (dropped and then added back)
        // Partition 3 is added
        // Partition 4 is dropped

        assertUpdate(
                session,
                "CREATE TABLE tmp_delete_insert WITH (partitioned_by=array ['z']) AS " +
                        "SELECT * from (VALUES (CAST (101 AS BIGINT), CAST (1 AS BIGINT)), (201, 2), (202, 2), (401, 4), (402, 4), (403, 4)) t(a, z)",
                6);

        List<MaterializedRow> expectedBefore = MaterializedResult.resultBuilder(session, BIGINT, BIGINT)
                .row(101L, 1L)
                .row(201L, 2L)
                .row(202L, 2L)
                .row(401L, 4L)
                .row(402L, 4L)
                .row(403L, 4L)
                .build()
                .getMaterializedRows();
        List<MaterializedRow> expectedAfter = MaterializedResult.resultBuilder(session, BIGINT, BIGINT)
                .row(101L, 1L)
                .row(203L, 2L)
                .row(204L, 2L)
                .row(205L, 2L)
                .row(301L, 2L)
                .row(302L, 3L)
                .build()
                .getMaterializedRows();

        try {
            transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                    .execute(session, transactionSession -> {
                        assertUpdate(transactionSession, "DELETE FROM tmp_delete_insert WHERE z >= 2");
                        assertUpdate(transactionSession, "INSERT INTO tmp_delete_insert VALUES (203, 2), (204, 2), (205, 2), (301, 2), (302, 3)", 5);
                        MaterializedResult actualFromAnotherTransaction = computeActual(session, "SELECT * FROM tmp_delete_insert");
                        assertEqualsIgnoreOrder(actualFromAnotherTransaction, expectedBefore);
                        MaterializedResult actualFromCurrentTransaction = computeActual(transactionSession, "SELECT * FROM tmp_delete_insert");
                        assertEqualsIgnoreOrder(actualFromCurrentTransaction, expectedAfter);
                        rollback();
                    });
        }
        catch (RollbackException e) {
            // ignore
        }

        MaterializedResult actualAfterRollback = computeActual(session, "SELECT * FROM tmp_delete_insert");
        assertEqualsIgnoreOrder(actualAfterRollback, expectedBefore);

        transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .execute(session, transactionSession -> {
                    assertUpdate(transactionSession, "DELETE FROM tmp_delete_insert WHERE z >= 2");
                    assertUpdate(transactionSession, "INSERT INTO tmp_delete_insert VALUES (203, 2), (204, 2), (205, 2), (301, 2), (302, 3)", 5);
                    MaterializedResult actualOutOfTransaction = computeActual(session, "SELECT * FROM tmp_delete_insert");
                    assertEqualsIgnoreOrder(actualOutOfTransaction, expectedBefore);
                    MaterializedResult actualInTransaction = computeActual(transactionSession, "SELECT * FROM tmp_delete_insert");
                    assertEqualsIgnoreOrder(actualInTransaction, expectedAfter);
                });

        MaterializedResult actualAfterTransaction = computeActual(session, "SELECT * FROM tmp_delete_insert");
        assertEqualsIgnoreOrder(actualAfterTransaction, expectedAfter);
    }

    @Test
    public void testCreateAndInsert()
    {
        Session session = getSession();

        List<MaterializedRow> expected = MaterializedResult.resultBuilder(session, BIGINT, BIGINT)
                .row(101L, 1L)
                .row(201L, 2L)
                .row(202L, 2L)
                .row(301L, 3L)
                .row(302L, 3L)
                .build()
                .getMaterializedRows();

        transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .execute(session, transactionSession -> {
                    assertUpdate(
                            transactionSession,
                            "CREATE TABLE tmp_create_insert WITH (partitioned_by=array ['z']) AS " +
                                    "SELECT * from (VALUES (CAST (101 AS BIGINT), CAST (1 AS BIGINT)), (201, 2), (202, 2)) t(a, z)",
                            3);
                    assertUpdate(transactionSession, "INSERT INTO tmp_create_insert VALUES (301, 3), (302, 3)", 2);
                    MaterializedResult actualFromCurrentTransaction = computeActual(transactionSession, "SELECT * FROM tmp_create_insert");
                    assertEqualsIgnoreOrder(actualFromCurrentTransaction, expected);
                });

        MaterializedResult actualAfterTransaction = computeActual(session, "SELECT * FROM tmp_create_insert");
        assertEqualsIgnoreOrder(actualAfterTransaction, expected);
    }

    @Test
    public void testAddColumn()
    {
        assertUpdate("CREATE TABLE test_add_column (a bigint COMMENT 'test comment AAA')");
        assertUpdate("ALTER TABLE test_add_column ADD COLUMN b bigint COMMENT 'test comment BBB'");
        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN a varchar", ".* Column 'a' already exists");
        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN c bad_type", ".* Unknown type 'bad_type' for column 'c'");
        assertQuery("SHOW COLUMNS FROM test_add_column", "VALUES ('a', 'bigint', '', 'test comment AAA'), ('b', 'bigint', '', 'test comment BBB')");
        assertUpdate("DROP TABLE test_add_column");
    }

    @Test
    public void testRenameColumn()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_rename_column\n" +
                "WITH (\n" +
                "  partitioned_by = ARRAY ['orderstatus']\n" +
                ")\n" +
                "AS\n" +
                "SELECT orderkey, orderstatus FROM orders";

        assertUpdate(createTable, "SELECT count(*) FROM orders");
        assertUpdate("ALTER TABLE test_rename_column RENAME COLUMN orderkey TO new_orderkey");
        assertQuery("SELECT new_orderkey, orderstatus FROM test_rename_column", "SELECT orderkey, orderstatus FROM orders where orderstatus != 'dfd'");
        assertQueryFails("ALTER TABLE test_rename_column RENAME COLUMN \"$path\" TO test", ".* Cannot rename hidden column");
        assertQueryFails("ALTER TABLE test_rename_column RENAME COLUMN orderstatus TO new_orderstatus", "Renaming partition columns is not supported");
        assertQuery("SELECT new_orderkey, orderstatus FROM test_rename_column", "SELECT orderkey, orderstatus FROM orders");
        assertUpdate("DROP TABLE test_rename_column");
    }

    @Test
    public void testDropColumn()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_drop_column\n" +
                "WITH (\n" +
                "  partitioned_by = ARRAY ['orderstatus']\n" +
                ")\n" +
                "AS\n" +
                "SELECT custkey, orderkey, orderstatus FROM orders";

        assertUpdate(createTable, "SELECT count(*) FROM orders");
        assertQuery("SELECT orderkey, orderstatus FROM test_drop_column", "SELECT orderkey, orderstatus FROM orders");

        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN \"$path\"", ".* Cannot drop hidden column");
        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN orderstatus", "Cannot drop partition columns");
        assertUpdate("ALTER TABLE test_drop_column DROP COLUMN orderkey");
        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN custkey", "Cannot drop the only non-partition column in a table");
        assertQuery("SELECT * FROM test_drop_column", "SELECT custkey, orderstatus FROM orders");

        assertUpdate("DROP TABLE test_drop_column");
    }

    @Test
    public void testDropBucketingColumn()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_drop_bucketing_column\n" +
                "WITH (\n" +
                "  bucket_count = 5, bucketed_by = ARRAY ['orderstatus']\n" +
                ")\n" +
                "AS\n" +
                "SELECT custkey, orderkey, orderstatus FROM orders";

        assertUpdate(createTable, "SELECT count(*) FROM orders");
        assertQuery("SELECT orderkey, orderstatus FROM test_drop_bucketing_column", "SELECT orderkey, orderstatus FROM orders");

        assertQueryFails("ALTER TABLE test_drop_bucketing_column DROP COLUMN orderstatus", "Cannot drop bucketing columns");
        assertQuery("SELECT * FROM test_drop_bucketing_column", "SELECT custkey, orderkey,  orderstatus FROM orders");

        assertUpdate("DROP TABLE test_drop_bucketing_column");
    }

    @Test
    private void testRenameBucketingColumn()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_rename_bucketing_column\n" +
                "WITH (\n" +
                "  bucket_count = 5, bucketed_by = ARRAY ['orderstatus']\n" +
                ")\n" +
                "AS\n" +
                "SELECT custkey, orderkey, orderstatus FROM orders";

        assertUpdate(createTable, "SELECT count(*) FROM orders");
        assertQuery("SELECT orderkey, orderstatus FROM test_rename_bucketing_column", "SELECT orderkey, orderstatus FROM orders");

        assertUpdate("ALTER TABLE test_rename_bucketing_column RENAME COLUMN orderstatus TO orderstatus1");
        assertQuery("SELECT orderkey, orderstatus1  FROM test_rename_bucketing_column", "SELECT orderkey, orderstatus  FROM orders");

        assertUpdate("DROP TABLE test_rename_bucketing_column");
    }

    @Test
    public void testAvroTypeValidation()
    {
        assertQueryFails("CREATE TABLE test_avro_types (x map(bigint, bigint)) WITH (format = 'AVRO')", "Column x has a non-varchar map key, which is not supported by Avro");
        assertQueryFails("CREATE TABLE test_avro_types (x tinyint) WITH (format = 'AVRO')", "Column x is tinyint, which is not supported by Avro. Use integer instead.");
        assertQueryFails("CREATE TABLE test_avro_types (x smallint) WITH (format = 'AVRO')", "Column x is smallint, which is not supported by Avro. Use integer instead.");

        assertQueryFails("CREATE TABLE test_avro_types WITH (format = 'AVRO') AS SELECT cast(42 AS smallint) z", "Column z is smallint, which is not supported by Avro. Use integer instead.");
    }

    @Test
    public void testOrderByChar()
    {
        assertUpdate("CREATE TABLE char_order_by (c_char char(2))");
        assertUpdate("INSERT INTO char_order_by (c_char) VALUES" +
                "(CAST('a' as CHAR(2)))," +
                "(CAST('a\0' as CHAR(2)))," +
                "(CAST('a  ' as CHAR(2)))", 3);

        MaterializedResult actual = computeActual(getSession(),
                "SELECT * FROM char_order_by ORDER BY c_char ASC");

        assertUpdate("DROP TABLE char_order_by");

        MaterializedResult expected = resultBuilder(getSession(), createCharType(2))
                .row("a\0")
                .row("a ")
                .row("a ")
                .build();

        assertEquals(actual, expected);
    }

    /**
     * Tests correctness of comparison of char(x) and varchar pushed down to a table scan as a TupleDomain
     */
    @Test
    public void testPredicatePushDownToTableScan()
    {
        // Test not specific to Hive, but needs a connector supporting table creation

        assertUpdate("CREATE TABLE test_table_with_char (a char(20))");
        try {
            assertUpdate("INSERT INTO test_table_with_char (a) VALUES" +
                    "(cast('aaa' as char(20)))," +
                    "(cast('bbb' as char(20)))," +
                    "(cast('bbc' as char(20)))," +
                    "(cast('bbd' as char(20)))", 4);

            assertQuery(
                    "SELECT a, a <= 'bbc' FROM test_table_with_char",
                    "VALUES (cast('aaa' as char(20)), true), " +
                            "(cast('bbb' as char(20)), true), " +
                            "(cast('bbc' as char(20)), true), " +
                            "(cast('bbd' as char(20)), false)");

            assertQuery(
                    "SELECT a FROM test_table_with_char WHERE a <= 'bbc'",
                    "VALUES cast('aaa' as char(20)), " +
                            "cast('bbb' as char(20)), " +
                            "cast('bbc' as char(20))");
        }
        finally {
            assertUpdate("DROP TABLE test_table_with_char");
        }
    }

    @Test
    public void testPartitionPruning()
    {
        assertUpdate("CREATE TABLE test_partition_pruning (v bigint, k varchar) WITH (partitioned_by = array['k'])");
        assertUpdate("INSERT INTO test_partition_pruning (v, k) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'e')", 4);

        try {
            String query = "SELECT * FROM test_partition_pruning WHERE k = 'a'";
            assertQuery(query, "VALUES (1, 'a')");
            assertConstraints(
                    query,
                    ImmutableSet.of(
                            new ColumnConstraint(
                                    "k",
                                    VARCHAR.getTypeSignature(),
                                    new FormattedDomain(
                                            false,
                                            ImmutableSet.of(
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("a"), EXACTLY),
                                                            new FormattedMarker(Optional.of("a"), EXACTLY)))))));

            query = "SELECT * FROM test_partition_pruning WHERE k IN ('a', 'b')";
            assertQuery(query, "VALUES (1, 'a'), (2, 'b')");
            assertConstraints(
                    query,
                    ImmutableSet.of(
                            new ColumnConstraint(
                                    "k",
                                    VARCHAR.getTypeSignature(),
                                    new FormattedDomain(
                                            false,
                                            ImmutableSet.of(
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("a"), EXACTLY),
                                                            new FormattedMarker(Optional.of("a"), EXACTLY)),
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("b"), EXACTLY),
                                                            new FormattedMarker(Optional.of("b"), EXACTLY)))))));

            query = "SELECT * FROM test_partition_pruning WHERE k >= 'b'";
            assertQuery(query, "VALUES (2, 'b'), (3, 'c'), (4, 'e')");
            assertConstraints(
                    query,
                    ImmutableSet.of(
                            new ColumnConstraint(
                                    "k",
                                    VARCHAR.getTypeSignature(),
                                    new FormattedDomain(
                                            false,
                                            ImmutableSet.of(
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("b"), EXACTLY),
                                                            new FormattedMarker(Optional.of("b"), EXACTLY)),
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("c"), EXACTLY),
                                                            new FormattedMarker(Optional.of("c"), EXACTLY)),
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("e"), EXACTLY),
                                                            new FormattedMarker(Optional.of("e"), EXACTLY)))))));

            query = "SELECT * FROM (" +
                    "    SELECT * " +
                    "    FROM test_partition_pruning " +
                    "    WHERE v IN (1, 2, 4) " +
                    ") t " +
                    "WHERE t.k >= 'b'";
            assertQuery(query, "VALUES (2, 'b'), (4, 'e')");
            assertConstraints(
                    query,
                    ImmutableSet.of(
                            new ColumnConstraint(
                                    "k",
                                    VARCHAR.getTypeSignature(),
                                    new FormattedDomain(
                                            false,
                                            ImmutableSet.of(
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("b"), EXACTLY),
                                                            new FormattedMarker(Optional.of("b"), EXACTLY)),
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("c"), EXACTLY),
                                                            new FormattedMarker(Optional.of("c"), EXACTLY)),
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("e"), EXACTLY),
                                                            new FormattedMarker(Optional.of("e"), EXACTLY)))))));
        }
        finally {
            assertUpdate("DROP TABLE test_partition_pruning");
        }
    }

    @Test
    public void testMismatchedBucketing()
    {
        try {
            assertUpdate(
                    "CREATE TABLE test_mismatch_bucketing16\n" +
                            "WITH (bucket_count = 16, bucketed_by = ARRAY['key16']) AS\n" +
                            "SELECT orderkey key16, comment value16 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_mismatch_bucketing32\n" +
                            "WITH (bucket_count = 32, bucketed_by = ARRAY['key32']) AS\n" +
                            "SELECT orderkey key32, comment value32 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_mismatch_bucketingN AS\n" +
                            "SELECT orderkey keyN, comment valueN FROM orders",
                    15000);

            Session withMismatchOptimization = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                    .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name())
                    .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                    .setCatalogSessionProperty(catalog, "optimize_mismatched_bucket_count", "true")
                    .build();
            Session withoutMismatchOptimization = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                    .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name())
                    .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                    .setCatalogSessionProperty(catalog, "optimize_mismatched_bucket_count", "false")
                    .build();

            @Language("SQL") String writeToTableWithMoreBuckets = "CREATE TABLE test_mismatch_bucketing_out32\n" +
                    "WITH (bucket_count = 32, bucketed_by = ARRAY['key16'])\n" +
                    "AS\n" +
                    "SELECT key16, value16, key32, value32, keyN, valueN\n" +
                    "FROM\n" +
                    "  test_mismatch_bucketing16\n" +
                    "JOIN\n" +
                    "  test_mismatch_bucketing32\n" +
                    "ON key16=key32\n" +
                    "JOIN\n" +
                    "  test_mismatch_bucketingN\n" +
                    "ON key16=keyN";
            @Language("SQL") String writeToTableWithFewerBuckets = "CREATE TABLE test_mismatch_bucketing_out8\n" +
                    "WITH (bucket_count = 8, bucketed_by = ARRAY['key16'])\n" +
                    "AS\n" +
                    "SELECT key16, value16, key32, value32, keyN, valueN\n" +
                    "FROM\n" +
                    "  test_mismatch_bucketing16\n" +
                    "JOIN\n" +
                    "  test_mismatch_bucketing32\n" +
                    "ON key16=key32\n" +
                    "JOIN\n" +
                    "  test_mismatch_bucketingN\n" +
                    "ON key16=keyN";

            assertUpdate(withoutMismatchOptimization, writeToTableWithMoreBuckets, 15000, assertRemoteExchangesCount(4));
            assertQuery("SELECT * FROM test_mismatch_bucketing_out32", "SELECT orderkey, comment, orderkey, comment, orderkey, comment from orders");
            assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketing_out32");

            assertUpdate(withMismatchOptimization, writeToTableWithMoreBuckets, 15000, assertRemoteExchangesCount(2));
            assertQuery("SELECT * FROM test_mismatch_bucketing_out32", "SELECT orderkey, comment, orderkey, comment, orderkey, comment from orders");

            assertUpdate(withMismatchOptimization, writeToTableWithFewerBuckets, 15000, assertRemoteExchangesCount(2));
            assertQuery("SELECT * FROM test_mismatch_bucketing_out8", "SELECT orderkey, comment, orderkey, comment, orderkey, comment from orders");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketing16");
            assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketing32");
            assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketingN");
            assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketing_out32");
            assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketing_out8");
        }
    }

    @Test
    public void testGroupedExecution()
    {
        try {
            assertUpdate(
                    "CREATE TABLE test_grouped_join1\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                            "SELECT orderkey key1, comment value1 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_grouped_join2\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key2']) AS\n" +
                            "SELECT orderkey key2, comment value2 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_grouped_join3\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key3']) AS\n" +
                            "SELECT orderkey key3, comment value3 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_grouped_join4\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key4_bucket']) AS\n" +
                            "SELECT orderkey key4_bucket, orderkey key4_non_bucket, comment value4 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_grouped_joinN AS\n" +
                            "SELECT orderkey keyN, comment valueN FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_grouped_joinDual\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['keyD']) AS\n" +
                            "SELECT orderkey keyD, comment valueD FROM orders CROSS JOIN UNNEST(repeat(NULL, 2))",
                    30000);
            assertUpdate(
                    "CREATE TABLE test_grouped_window\n" +
                            "WITH (bucket_count = 5, bucketed_by = ARRAY['key']) AS\n" +
                            "SELECT custkey key, orderkey value FROM orders WHERE custkey <= 5 ORDER BY orderkey LIMIT 10",
                    10);

            // NOT grouped execution; default
            Session notColocated = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "false")
                    .setSystemProperty(GROUPED_EXECUTION, "false")
                    .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                    .build();
            // Co-located JOIN with all groups at once, fixed schedule
            Session colocatedAllGroupsAtOnce = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "0")
                    .setSystemProperty(DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION, "false")
                    .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                    .build();
            // Co-located JOIN, 1 group per worker at a time, fixed schedule
            Session colocatedOneGroupAtATime = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                    .setSystemProperty(DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION, "false")
                    .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                    .build();
            // Co-located JOIN with all groups at once, dynamic schedule
            Session colocatedAllGroupsAtOnceDynamic = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "0")
                    .setSystemProperty(DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION, "true")
                    .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                    .build();
            // Co-located JOIN, 1 group per worker at a time, dynamic schedule
            Session colocatedOneGroupAtATimeDynamic = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                    .setSystemProperty(DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION, "true")
                    .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                    .build();
            // Broadcast JOIN, 1 group per worker at a time
            Session broadcastOneGroupAtATime = Session.builder(getSession())
                    .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                    .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                    .build();

            // Broadcast JOIN, 1 group per worker at a time, dynamic schedule
            Session broadcastOneGroupAtATimeDynamic = Session.builder(getSession())
                    .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                    .setSystemProperty(DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION, "true")
                    .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                    .build();

            //
            // HASH JOIN
            // =========

            @Language("SQL") String joinThreeBucketedTable =
                    "SELECT key1, value1, key2, value2, key3, value3\n" +
                            "FROM test_grouped_join1\n" +
                            "JOIN test_grouped_join2\n" +
                            "ON key1 = key2\n" +
                            "JOIN test_grouped_join3\n" +
                            "ON key2 = key3";
            @Language("SQL") String joinThreeMixedTable =
                    "SELECT key1, value1, key2, value2, keyN, valueN\n" +
                            "FROM test_grouped_join1\n" +
                            "JOIN test_grouped_join2\n" +
                            "ON key1 = key2\n" +
                            "JOIN test_grouped_joinN\n" +
                            "ON key2 = keyN";
            @Language("SQL") String expectedJoinQuery = "SELECT orderkey, comment, orderkey, comment, orderkey, comment from orders";
            @Language("SQL") String leftJoinBucketedTable =
                    "SELECT key1, value1, key2, value2\n" +
                            "FROM test_grouped_join1\n" +
                            "LEFT JOIN (SELECT * FROM test_grouped_join2 WHERE key2 % 2 = 0)\n" +
                            "ON key1 = key2";
            @Language("SQL") String rightJoinBucketedTable =
                    "SELECT key1, value1, key2, value2\n" +
                            "FROM (SELECT * FROM test_grouped_join2 WHERE key2 % 2 = 0)\n" +
                            "RIGHT JOIN test_grouped_join1\n" +
                            "ON key1 = key2";
            @Language("SQL") String expectedOuterJoinQuery = "SELECT orderkey, comment, CASE mod(orderkey, 2) WHEN 0 THEN orderkey END, CASE mod(orderkey, 2) WHEN 0 THEN comment END from orders";

            assertQuery(notColocated, joinThreeBucketedTable, expectedJoinQuery);
            assertQuery(notColocated, leftJoinBucketedTable, expectedOuterJoinQuery);
            assertQuery(notColocated, rightJoinBucketedTable, expectedOuterJoinQuery);

            assertQuery(colocatedAllGroupsAtOnce, joinThreeBucketedTable, expectedJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnce, joinThreeMixedTable, expectedJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, joinThreeBucketedTable, expectedJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, joinThreeMixedTable, expectedJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedAllGroupsAtOnceDynamic, joinThreeBucketedTable, expectedJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnceDynamic, joinThreeMixedTable, expectedJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATimeDynamic, joinThreeBucketedTable, expectedJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATimeDynamic, joinThreeMixedTable, expectedJoinQuery, assertRemoteExchangesCount(2));

            assertQuery(colocatedAllGroupsAtOnce, leftJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnce, rightJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, leftJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, rightJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnceDynamic, leftJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnceDynamic, rightJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATimeDynamic, leftJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATimeDynamic, rightJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));

            //
            // CROSS JOIN and HASH JOIN mixed
            // ==============================

            @Language("SQL") String crossJoin =
                    "SELECT key1, value1, key2, value2, key3, value3\n" +
                            "FROM test_grouped_join1\n" +
                            "JOIN test_grouped_join2\n" +
                            "ON key1 = key2\n" +
                            "CROSS JOIN (SELECT * FROM test_grouped_join3 WHERE key3 <= 3)";
            @Language("SQL") String expectedCrossJoinQuery =
                    "SELECT key1, value1, key1, value1, key3, value3\n" +
                            "FROM\n" +
                            "  (SELECT orderkey key1, comment value1 FROM orders)\n" +
                            "CROSS JOIN\n" +
                            "  (SELECT orderkey key3, comment value3 FROM orders where orderkey <= 3)";
            assertQuery(notColocated, crossJoin, expectedCrossJoinQuery);
            assertQuery(colocatedAllGroupsAtOnce, crossJoin, expectedCrossJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, crossJoin, expectedCrossJoinQuery, assertRemoteExchangesCount(2));

            //
            // Bucketed and unbucketed HASH JOIN mixed
            // =======================================
            @Language("SQL") String bucketedAndUnbucketedJoin =
                    "SELECT key1, value1, keyN, valueN, key2, value2, key3, value3\n" +
                            "FROM\n" +
                            "  test_grouped_join1\n" +
                            "JOIN (\n" +
                            "  SELECT *\n" +
                            "  FROM test_grouped_joinN\n" +
                            "  JOIN test_grouped_join2\n" +
                            "  ON keyN = key2\n" +
                            ")\n" +
                            "ON key1 = keyN\n" +
                            "JOIN test_grouped_join3\n" +
                            "ON key1 = key3";
            @Language("SQL") String expectedBucketedAndUnbucketedJoinQuery = "SELECT orderkey, comment, orderkey, comment, orderkey, comment, orderkey, comment from orders";
            assertQuery(notColocated, bucketedAndUnbucketedJoin, expectedBucketedAndUnbucketedJoinQuery);
            assertQuery(colocatedAllGroupsAtOnce, bucketedAndUnbucketedJoin, expectedBucketedAndUnbucketedJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, bucketedAndUnbucketedJoin, expectedBucketedAndUnbucketedJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATimeDynamic, bucketedAndUnbucketedJoin, expectedBucketedAndUnbucketedJoinQuery, assertRemoteExchangesCount(2));

            //
            // UNION ALL / GROUP BY
            // ====================

            @Language("SQL") String groupBySingleBucketed =
                    "SELECT\n" +
                            "  keyD,\n" +
                            "  count(valueD)\n" +
                            "FROM\n" +
                            "  test_grouped_joinDual\n" +
                            "GROUP BY keyD";
            @Language("SQL") String expectedSingleGroupByQuery = "SELECT orderkey, 2 from orders";
            @Language("SQL") String groupByOfUnionBucketed =
                    "SELECT\n" +
                            "  key\n" +
                            ", arbitrary(value1)\n" +
                            ", arbitrary(value2)\n" +
                            ", arbitrary(value3)\n" +
                            "FROM (\n" +
                            "  SELECT key1 key, value1, NULL value2, NULL value3\n" +
                            "  FROM test_grouped_join1\n" +
                            "UNION ALL\n" +
                            "  SELECT key2 key, NULL value1, value2, NULL value3\n" +
                            "  FROM test_grouped_join2\n" +
                            "  WHERE key2 % 2 = 0\n" +
                            "UNION ALL\n" +
                            "  SELECT key3 key, NULL value1, NULL value2, value3\n" +
                            "  FROM test_grouped_join3\n" +
                            "  WHERE key3 % 3 = 0\n" +
                            ")\n" +
                            "GROUP BY key";
            @Language("SQL") String groupByOfUnionMixed =
                    "SELECT\n" +
                            "  key\n" +
                            ", arbitrary(value1)\n" +
                            ", arbitrary(value2)\n" +
                            ", arbitrary(valueN)\n" +
                            "FROM (\n" +
                            "  SELECT key1 key, value1, NULL value2, NULL valueN\n" +
                            "  FROM test_grouped_join1\n" +
                            "UNION ALL\n" +
                            "  SELECT key2 key, NULL value1, value2, NULL valueN\n" +
                            "  FROM test_grouped_join2\n" +
                            "  WHERE key2 % 2 = 0\n" +
                            "UNION ALL\n" +
                            "  SELECT keyN key, NULL value1, NULL value2, valueN\n" +
                            "  FROM test_grouped_joinN\n" +
                            "  WHERE keyN % 3 = 0\n" +
                            ")\n" +
                            "GROUP BY key";
            @Language("SQL") String expectedGroupByOfUnion = "SELECT orderkey, comment, CASE mod(orderkey, 2) WHEN 0 THEN comment END, CASE mod(orderkey, 3) WHEN 0 THEN comment END from orders";
            // In this case:
            // * left side can take advantage of bucketed execution
            // * right side does not have the necessary organization to allow its parent to take advantage of bucketed execution
            // In this scenario, we give up bucketed execution altogether. This can potentially be improved.
            //
            //       AGG(key)
            //           |
            //       UNION ALL
            //      /         \
            //  AGG(key)  Scan (not bucketed)
            //     |
            // Scan (bucketed on key)
            @Language("SQL") String groupByOfUnionOfGroupByMixed =
                    "SELECT\n" +
                            "  key, sum(cnt) cnt\n" +
                            "FROM (\n" +
                            "  SELECT keyD key, count(valueD) cnt\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            "UNION ALL\n" +
                            "  SELECT keyN key, 1 cnt\n" +
                            "  FROM test_grouped_joinN\n" +
                            ")\n" +
                            "group by key";
            @Language("SQL") String expectedGroupByOfUnionOfGroupBy = "SELECT orderkey, 3 from orders";

            // Eligible GROUP BYs run in the same fragment regardless of colocated_join flag
            assertQuery(colocatedAllGroupsAtOnce, groupBySingleBucketed, expectedSingleGroupByQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, groupBySingleBucketed, expectedSingleGroupByQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATimeDynamic, groupBySingleBucketed, expectedSingleGroupByQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnce, groupByOfUnionBucketed, expectedGroupByOfUnion, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, groupByOfUnionBucketed, expectedGroupByOfUnion, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATimeDynamic, groupByOfUnionBucketed, expectedGroupByOfUnion, assertRemoteExchangesCount(1));

            // cannot be executed in a grouped manner but should still produce correct result
            assertQuery(colocatedOneGroupAtATime, groupByOfUnionMixed, expectedGroupByOfUnion, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, groupByOfUnionOfGroupByMixed, expectedGroupByOfUnionOfGroupBy, assertRemoteExchangesCount(2));

            //
            // GROUP BY and JOIN mixed
            // ========================
            @Language("SQL") String joinGroupedWithGrouped =
                    "SELECT key1, count1, count2\n" +
                            "FROM (\n" +
                            "  SELECT keyD key1, count(valueD) count1\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            ") JOIN (\n" +
                            "  SELECT keyD key2, count(valueD) count2\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            ")\n" +
                            "ON key1 = key2";
            @Language("SQL") String expectedJoinGroupedWithGrouped = "SELECT orderkey, 2, 2 from orders";
            @Language("SQL") String joinGroupedWithUngrouped =
                    "SELECT keyD, countD, valueN\n" +
                            "FROM (\n" +
                            "  SELECT keyD, count(valueD) countD\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            ") JOIN (\n" +
                            "  SELECT keyN, valueN\n" +
                            "  FROM test_grouped_joinN\n" +
                            ")\n" +
                            "ON keyD = keyN";
            @Language("SQL") String expectedJoinGroupedWithUngrouped = "SELECT orderkey, 2, comment from orders";
            @Language("SQL") String joinUngroupedWithGrouped =
                    "SELECT keyN, valueN, countD\n" +
                            "FROM (\n" +
                            "  SELECT keyN, valueN\n" +
                            "  FROM test_grouped_joinN\n" +
                            ") JOIN (\n" +
                            "  SELECT keyD, count(valueD) countD\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            ")\n" +
                            "ON keyN = keyD";
            @Language("SQL") String expectedJoinUngroupedWithGrouped = "SELECT orderkey, comment, 2 from orders";
            @Language("SQL") String groupOnJoinResult =
                    "SELECT keyD, count(valueD), count(valueN)\n" +
                            "FROM\n" +
                            "  test_grouped_joinDual\n" +
                            "JOIN\n" +
                            "  test_grouped_joinN\n" +
                            "ON keyD=keyN\n" +
                            "GROUP BY keyD";
            @Language("SQL") String expectedGroupOnJoinResult = "SELECT orderkey, 2, 2 from orders";

            @Language("SQL") String groupOnUngroupedJoinResult =
                    "SELECT key4_bucket, count(value4), count(valueN)\n" +
                            "FROM\n" +
                            "  test_grouped_join4\n" +
                            "JOIN\n" +
                            "  test_grouped_joinN\n" +
                            "ON key4_non_bucket=keyN\n" +
                            "GROUP BY key4_bucket";
            @Language("SQL") String expectedGroupOnUngroupedJoinResult = "SELECT orderkey, count(*), count(*) from orders group by orderkey";

            // Eligible GROUP BYs run in the same fragment regardless of colocated_join flag
            assertQuery(colocatedAllGroupsAtOnce, joinGroupedWithGrouped, expectedJoinGroupedWithGrouped, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, joinGroupedWithGrouped, expectedJoinGroupedWithGrouped, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATimeDynamic, joinGroupedWithGrouped, expectedJoinGroupedWithGrouped, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnce, joinGroupedWithUngrouped, expectedJoinGroupedWithUngrouped, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, joinGroupedWithUngrouped, expectedJoinGroupedWithUngrouped, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATimeDynamic, joinGroupedWithUngrouped, expectedJoinGroupedWithUngrouped, assertRemoteExchangesCount(2));
            assertQuery(colocatedAllGroupsAtOnce, groupOnJoinResult, expectedGroupOnJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, groupOnJoinResult, expectedGroupOnJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATimeDynamic, groupOnJoinResult, expectedGroupOnJoinResult, assertRemoteExchangesCount(2));

            assertQuery(broadcastOneGroupAtATime, groupOnJoinResult, expectedGroupOnJoinResult, assertRemoteExchangesCount(2));
            assertQuery(broadcastOneGroupAtATime, groupOnUngroupedJoinResult, expectedGroupOnUngroupedJoinResult, assertRemoteExchangesCount(2));
            assertQuery(broadcastOneGroupAtATimeDynamic, groupOnUngroupedJoinResult, expectedGroupOnUngroupedJoinResult, assertRemoteExchangesCount(2));

            // cannot be executed in a grouped manner but should still produce correct result
            assertQuery(colocatedOneGroupAtATime, joinUngroupedWithGrouped, expectedJoinUngroupedWithGrouped, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, groupOnUngroupedJoinResult, expectedGroupOnUngroupedJoinResult, assertRemoteExchangesCount(4));

            //
            // Outer JOIN (that involves LookupOuterOperator)
            // ==============================================

            // Chain on the probe side to test duplicating OperatorFactory
            @Language("SQL") String chainedOuterJoin =
                    "SELECT key1, value1, key2, value2, key3, value3\n" +
                            "FROM\n" +
                            "  (SELECT * FROM test_grouped_join1 where mod(key1, 2) = 0)\n" +
                            "RIGHT JOIN\n" +
                            "  (SELECT * FROM test_grouped_join2 where mod(key2, 3) = 0)\n" +
                            "ON key1 = key2\n" +
                            "FULL JOIN\n" +
                            "  (SELECT * FROM test_grouped_join3 where mod(key3, 5) = 0)\n" +
                            "ON key2 = key3";
            // Probe is grouped execution, but build is not
            @Language("SQL") String sharedBuildOuterJoin =
                    "SELECT key1, value1, keyN, valueN\n" +
                            "FROM\n" +
                            "  (SELECT key1, arbitrary(value1) value1 FROM test_grouped_join1 where mod(key1, 2) = 0 group by key1)\n" +
                            "RIGHT JOIN\n" +
                            "  (SELECT * FROM test_grouped_joinN where mod(keyN, 3) = 0)\n" +
                            "ON key1 = keyN";
            // The preceding test case, which then feeds into another join
            @Language("SQL") String chainedSharedBuildOuterJoin =
                    "SELECT key1, value1, keyN, valueN, key3, value3\n" +
                            "FROM\n" +
                            "  (SELECT key1, arbitrary(value1) value1 FROM test_grouped_join1 where mod(key1, 2) = 0 group by key1)\n" +
                            "RIGHT JOIN\n" +
                            "  (SELECT * FROM test_grouped_joinN where mod(keyN, 3) = 0)\n" +
                            "ON key1 = keyN\n" +
                            "FULL JOIN\n" +
                            "  (SELECT * FROM test_grouped_join3 where mod(key3, 5) = 0)\n" +
                            "ON keyN = key3";
            @Language("SQL") String expectedChainedOuterJoinResult = "SELECT\n" +
                    "  CASE WHEN mod(orderkey, 2 * 3) = 0 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 2 * 3) = 0 THEN comment END,\n" +
                    "  CASE WHEN mod(orderkey, 3) = 0 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 3) = 0 THEN comment END,\n" +
                    "  CASE WHEN mod(orderkey, 5) = 0 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 5) = 0 THEN comment END\n" +
                    "FROM ORDERS\n" +
                    "WHERE mod(orderkey, 3) = 0 OR mod(orderkey, 5) = 0";
            @Language("SQL") String expectedSharedBuildOuterJoinResult = "SELECT\n" +
                    "  CASE WHEN mod(orderkey, 2) = 0 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 2) = 0 THEN comment END,\n" +
                    "  orderkey,\n" +
                    "  comment\n" +
                    "FROM ORDERS\n" +
                    "WHERE mod(orderkey, 3) = 0";

            assertQuery(notColocated, chainedOuterJoin, expectedChainedOuterJoinResult);
            assertQuery(colocatedAllGroupsAtOnce, chainedOuterJoin, expectedChainedOuterJoinResult, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, chainedOuterJoin, expectedChainedOuterJoinResult, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATimeDynamic, chainedOuterJoin, expectedChainedOuterJoinResult, assertRemoteExchangesCount(1));
            assertQuery(notColocated, sharedBuildOuterJoin, expectedSharedBuildOuterJoinResult);
            assertQuery(colocatedAllGroupsAtOnce, sharedBuildOuterJoin, expectedSharedBuildOuterJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, sharedBuildOuterJoin, expectedSharedBuildOuterJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATimeDynamic, sharedBuildOuterJoin, expectedSharedBuildOuterJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, chainedSharedBuildOuterJoin, expectedChainedOuterJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATimeDynamic, chainedSharedBuildOuterJoin, expectedChainedOuterJoinResult, assertRemoteExchangesCount(2));

            //
            // Window function
            // ===============
            assertQuery(
                    colocatedOneGroupAtATime,
                    "SELECT key, count(*) OVER (PARTITION BY key ORDER BY value) FROM test_grouped_window",
                    "VALUES\n" +
                            "(1, 1),\n" +
                            "(2, 1),\n" +
                            "(2, 2),\n" +
                            "(4, 1),\n" +
                            "(4, 2),\n" +
                            "(4, 3),\n" +
                            "(4, 4),\n" +
                            "(4, 5),\n" +
                            "(5, 1),\n" +
                            "(5, 2)",
                    assertRemoteExchangesCount(1));

            assertQuery(
                    colocatedOneGroupAtATime,
                    "SELECT key, row_number() OVER (PARTITION BY key ORDER BY value) FROM test_grouped_window",
                    "VALUES\n" +
                            "(1, 1),\n" +
                            "(2, 1),\n" +
                            "(2, 2),\n" +
                            "(4, 1),\n" +
                            "(4, 2),\n" +
                            "(4, 3),\n" +
                            "(4, 4),\n" +
                            "(4, 5),\n" +
                            "(5, 1),\n" +
                            "(5, 2)",
                    assertRemoteExchangesCount(1));

            assertQuery(
                    colocatedOneGroupAtATime,
                    "SELECT key, n FROM (SELECT key, row_number() OVER (PARTITION BY key ORDER BY value) AS n FROM test_grouped_window) WHERE n <= 2",
                    "VALUES\n" +
                            "(1, 1),\n" +
                            "(2, 1),\n" +
                            "(2, 2),\n" +
                            "(4, 1),\n" +
                            "(4, 2),\n" +
                            "(5, 1),\n" +
                            "(5, 2)",
                    assertRemoteExchangesCount(1));

            //
            // Filter out all or majority of splits
            // ====================================
            @Language("SQL") String noSplits =
                    "SELECT key1, arbitrary(value1)\n" +
                            "FROM test_grouped_join1\n" +
                            "WHERE \"$bucket\" < 0\n" +
                            "GROUP BY key1";
            @Language("SQL") String joinMismatchedBuckets =
                    "SELECT key1, value1, key2, value2\n" +
                            "FROM (\n" +
                            "  SELECT *\n" +
                            "  FROM test_grouped_join1\n" +
                            "  WHERE \"$bucket\"=1\n" +
                            ")\n" +
                            "FULL OUTER JOIN (\n" +
                            "  SELECT *\n" +
                            "  FROM test_grouped_join2\n" +
                            "  WHERE \"$bucket\"=11\n" +
                            ")\n" +
                            "ON key1=key2";
            @Language("SQL") String expectedNoSplits = "SELECT 1, 'a' WHERE FALSE";
            @Language("SQL") String expectedJoinMismatchedBuckets = "SELECT\n" +
                    "  CASE WHEN mod(orderkey, 13) = 1 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 13) = 1 THEN comment END,\n" +
                    "  CASE WHEN mod(orderkey, 13) = 11 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 13) = 11 THEN comment END\n" +
                    "FROM ORDERS\n" +
                    "WHERE mod(orderkey, 13) IN (1, 11)";

            assertQuery(notColocated, noSplits, expectedNoSplits);
            assertQuery(colocatedAllGroupsAtOnce, noSplits, expectedNoSplits, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, noSplits, expectedNoSplits, assertRemoteExchangesCount(1));
            assertQuery(notColocated, joinMismatchedBuckets, expectedJoinMismatchedBuckets);
            assertQuery(colocatedAllGroupsAtOnce, joinMismatchedBuckets, expectedJoinMismatchedBuckets, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, joinMismatchedBuckets, expectedJoinMismatchedBuckets, assertRemoteExchangesCount(1));

            Session notColocated1 = Session.builder(notColocated)
                    .setCatalogSessionProperty(notColocated.getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                    .build();
            Session colocatedAllGroupsAtOnce1 = Session.builder(colocatedAllGroupsAtOnce)
                    .setCatalogSessionProperty(colocatedAllGroupsAtOnce.getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                    .build();
            Session colocatedOneGroupAtATime1 = Session.builder(colocatedOneGroupAtATime)
                    .setCatalogSessionProperty(colocatedOneGroupAtATime.getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                    .build();
            assertQuery(notColocated1, noSplits, expectedNoSplits);

            assertQuery(colocatedAllGroupsAtOnce1, noSplits, expectedNoSplits, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime1, noSplits, expectedNoSplits, assertRemoteExchangesCount(1));
            assertQuery(notColocated1, joinMismatchedBuckets, expectedJoinMismatchedBuckets);
            assertQuery(colocatedAllGroupsAtOnce1, joinMismatchedBuckets, expectedJoinMismatchedBuckets, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime1, joinMismatchedBuckets, expectedJoinMismatchedBuckets, assertRemoteExchangesCount(1));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_grouped_join1");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_join2");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_join3");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_join4");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_joinN");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_joinDual");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_window");
        }
    }

    private Consumer<Plan> assertRemoteExchangesCount(int expectedRemoteExchangesCount)
    {
        return plan ->
        {
            int actualRemoteExchangesCount = searchFrom(plan.getRoot())
                    .where(node -> node instanceof ExchangeNode && ((ExchangeNode) node).getScope() == ExchangeNode.Scope.REMOTE)
                    .findAll()
                    .size();
            if (actualRemoteExchangesCount != expectedRemoteExchangesCount) {
                Session session = getSession();
                Metadata metadata = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getMetadata();
                String formattedPlan = textLogicalPlan(plan.getRoot(), plan.getTypes(), metadata, StatsAndCosts.empty(), session, 0, false);
                throw new AssertionError(format(
                        "Expected [\n%s\n] remote exchanges but found [\n%s\n] remote exchanges. Actual plan is [\n\n%s\n]",
                        expectedRemoteExchangesCount,
                        actualRemoteExchangesCount,
                        formattedPlan));
            }
        };
    }

    // Presto: Check if there is a partitioned exchange node in logic plan
    private Consumer<Plan> assertRemotePartitionedExchange(String partitionColumn)
    {
        return plan ->
        {
            int partitionedExchangeCount = searchFrom(plan.getRoot())
                    .where(node -> node instanceof ExchangeNode && ((ExchangeNode) node).getScope() == ExchangeNode.Scope.REMOTE
                            && ((ExchangeNode) node).getPartitioningScheme().getPartitioning().getHandle().toString().equals("HASH")
                            && ((ExchangeNode) node).getPartitioningScheme().getPartitioning().getArguments().get(0).getColumn().getName().equals(partitionColumn))
                    .findAll()
                    .size();

            if (partitionedExchangeCount != 1) {
                throw new AssertionError(format(
                        "Found [\n%s\n] remote partitioned exchanges.",
                        partitionedExchangeCount));
            }
        };
    }

    @Test
    public void testRcTextCharDecoding()
    {
        assertUpdate("CREATE TABLE test_table_with_char_rc WITH (format = 'RCTEXT') AS SELECT CAST('khaki' AS CHAR(7)) char_column", 1);
        try {
            assertQuery(
                    "SELECT * FROM test_table_with_char_rc WHERE char_column = 'khaki  '",
                    "VALUES (CAST('khaki' AS CHAR(7)))");
        }
        finally {
            assertUpdate("DROP TABLE test_table_with_char_rc");
        }
    }

    @Test
    public void testInvalidPartitionValue()
    {
        assertUpdate("CREATE TABLE invalid_partition_value (a int, b varchar) WITH (partitioned_by = ARRAY['b'])");
        assertQueryFails(
                "INSERT INTO invalid_partition_value VALUES (4, 'test' || chr(13))",
                "\\QHive partition keys can only contain printable ASCII characters (0x20 - 0x7E). Invalid value: 74 65 73 74 0D\\E");
        assertUpdate("DROP TABLE invalid_partition_value");

        assertQueryFails(
                "CREATE TABLE invalid_partition_value (a, b) WITH (partitioned_by = ARRAY['b']) AS SELECT 4, chr(9731)",
                "\\QHive partition keys can only contain printable ASCII characters (0x20 - 0x7E). Invalid value: E2 98 83\\E");
    }

    @Test
    public void testShowColumnMetadata()
    {
        String tableName = "test_show_column_table";

        @Language("SQL") String createTable = "CREATE TABLE " + tableName + " (a bigint, b varchar, c double)";

        Session testSession = testSessionBuilder()
                .setIdentity(new Identity("test_access_owner", Optional.empty()))
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .build();

        assertUpdate(createTable);

        // verify showing columns over a table requires SELECT privileges for the table
        assertAccessAllowed("SHOW COLUMNS FROM " + tableName);
        assertAccessDenied(testSession,
                "SHOW COLUMNS FROM " + tableName,
                "Cannot show columns of table .*." + tableName + ".*",
                privilege(tableName, SELECT_COLUMN));

        @Language("SQL") String getColumnsSql = "" +
                "SELECT lower(column_name) " +
                "FROM information_schema.columns " +
                "WHERE table_name = '" + tableName + "'";
        assertEquals(computeActual(getColumnsSql).getOnlyColumnAsSet(), ImmutableSet.of("a", "b", "c"));

        // verify with no SELECT privileges on table, querying information_schema will return empty columns
        executeExclusively(() -> {
            try {
                getQueryRunner().getAccessControl().deny(privilege(tableName, SELECT_COLUMN));
                assertQueryReturnsEmptyResult(testSession, getColumnsSql);
            }
            finally {
                getQueryRunner().getAccessControl().reset();
            }
        });

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCurrentUserInView()
    {
        checkState(getSession().getCatalog().isPresent(), "catalog is not set");
        checkState(getSession().getSchema().isPresent(), "schema is not set");
        String testAccountsUnqualifiedName = "test_accounts";
        String testAccountsViewUnqualifiedName = "test_accounts_view";
        String testAccountsViewFullyQualifiedName = format("%s.%s.%s", getSession().getCatalog().get(), getSession().getSchema().get(), testAccountsViewUnqualifiedName);
        assertUpdate(format("CREATE TABLE %s AS SELECT user_name, account_name" +
                "  FROM (VALUES ('user1', 'account1'), ('user2', 'account2'))" +
                "  t (user_name, account_name)", testAccountsUnqualifiedName), 2);
        assertUpdate(format("CREATE VIEW %s AS SELECT account_name FROM test_accounts WHERE user_name = CURRENT_USER", testAccountsViewUnqualifiedName));
        assertUpdate(format("GRANT SELECT ON %s TO user1", testAccountsViewFullyQualifiedName));
        assertUpdate(format("GRANT SELECT ON %s TO user2", testAccountsViewFullyQualifiedName));

        Session user1 = testSessionBuilder()
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .setIdentity(new Identity("user1", getSession().getIdentity().getPrincipal()))
                .build();

        Session user2 = testSessionBuilder()
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .setIdentity(new Identity("user2", getSession().getIdentity().getPrincipal()))
                .build();

        assertQuery(user1, "SELECT account_name FROM test_accounts_view", "VALUES 'account1'");
        assertQuery(user2, "SELECT account_name FROM test_accounts_view", "VALUES 'account2'");
        assertUpdate("DROP VIEW test_accounts_view");
        assertUpdate("DROP TABLE test_accounts");
    }

    @Test
    public void testCollectColumnStatisticsOnCreateTable()
    {
        String tableName = "test_collect_column_statistics_on_create_table";
        assertUpdate(format("" +
                "CREATE TABLE %s " +
                "WITH ( " +
                "   partitioned_by = ARRAY['p_varchar'] " +
                ") " +
                "AS " +
                "SELECT c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar " +
                "FROM ( " +
                "  VALUES " +
                "    (null, null, null, null, null, null, 'p1'), " +
                "    (null, null, null, null, null, null, 'p1'), " +
                "    (true, BIGINT '1', DOUBLE '2.2', TIMESTAMP '2012-08-08 01:00', CAST('abc1' AS VARCHAR), CAST('bcd1' AS VARBINARY), 'p1')," +
                "    (false, BIGINT '0', DOUBLE '1.2', TIMESTAMP '2012-08-08 00:00', CAST('abc2' AS VARCHAR), CAST('bcd2' AS VARBINARY), 'p1')," +
                "    (null, null, null, null, null, null, 'p2'), " +
                "    (null, null, null, null, null, null, 'p2'), " +
                "    (true, BIGINT '2', DOUBLE '3.3', TIMESTAMP '2012-09-09 01:00', CAST('cba1' AS VARCHAR), CAST('dcb1' AS VARBINARY), 'p2'), " +
                "    (false, BIGINT '1', DOUBLE '2.3', TIMESTAMP '2012-09-09 00:00', CAST('cba2' AS VARCHAR), CAST('dcb2' AS VARBINARY), 'p2') " +
                ") AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar)", tableName), 8);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '0', '1'), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '1.2', '2.2'), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varchar', 8.0E0, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varbinary', 8.0E0, null, 0.5E0, null, null, null), " +
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '1', '2'), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '2.3', '3.3'), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varchar', 8.0E0, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varbinary', 8.0E0, null, 0.5E0, null, null, null), " +
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null)");

        // non existing partition
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 0E0, 0E0, null, null, null), " +
                        "('c_bigint', null, 0E0, 0E0, null, null, null), " +
                        "('c_double', null, 0E0, 0E0, null, null, null), " +
                        "('c_timestamp', null, 0E0, 0E0, null, null, null), " +
                        "('c_varchar', 0E0, 0E0, 0E0, null, null, null), " +
                        "('c_varbinary', null, 0E0, 0E0, null, null, null), " +
                        "('p_varchar', 0E0, 0E0, 0E0, null, null, null), " +
                        "(null, null, null, null, 0E0, null, null)");

        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testCollectColumnStatisticsOnInsert()
    {
        String tableName = "test_collect_column_statistics_on_insert";
        assertUpdate(format("" +
                "CREATE TABLE %s ( " +
                "   c_boolean BOOLEAN, " +
                "   c_bigint BIGINT, " +
                "   c_double DOUBLE, " +
                "   c_timestamp TIMESTAMP, " +
                "   c_varchar VARCHAR, " +
                "   c_varbinary VARBINARY, " +
                "   p_varchar VARCHAR " +
                ") " +
                "WITH ( " +
                "   partitioned_by = ARRAY['p_varchar'] " +
                ")", tableName));

        assertUpdate(format("" +
                "INSERT INTO %s " +
                "SELECT c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar " +
                "FROM ( " +
                "  VALUES " +
                "    (null, null, null, null, null, null, 'p1'), " +
                "    (null, null, null, null, null, null, 'p1'), " +
                "    (true, BIGINT '1', DOUBLE '2.2', TIMESTAMP '2012-08-08 01:00', CAST('abc1' AS VARCHAR), CAST('bcd1' AS VARBINARY), 'p1')," +
                "    (false, BIGINT '0', DOUBLE '1.2', TIMESTAMP '2012-08-08 00:00', CAST('abc2' AS VARCHAR), CAST('bcd2' AS VARBINARY), 'p1')," +
                "    (null, null, null, null, null, null, 'p2'), " +
                "    (null, null, null, null, null, null, 'p2'), " +
                "    (true, BIGINT '2', DOUBLE '3.3', TIMESTAMP '2012-09-09 01:00', CAST('cba1' AS VARCHAR), CAST('dcb1' AS VARBINARY), 'p2'), " +
                "    (false, BIGINT '1', DOUBLE '2.3', TIMESTAMP '2012-09-09 00:00', CAST('cba2' AS VARCHAR), CAST('dcb2' AS VARBINARY), 'p2') " +
                ") AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar)", tableName), 8);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '0', '1'), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '1.2', '2.2'), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varchar', 8.0E0, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varbinary', 8.0E0, null, 0.5E0, null, null, null), " +
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '1', '2'), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '2.3', '3.3'), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varchar', 8.0E0, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varbinary', 8.0E0, null, 0.5E0, null, null, null), " +
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null)");

        // non existing partition
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 0E0, 0E0, null, null, null), " +
                        "('c_bigint', null, 0E0, 0E0, null, null, null), " +
                        "('c_double', null, 0E0, 0E0, null, null, null), " +
                        "('c_timestamp', null, 0E0, 0E0, null, null, null), " +
                        "('c_varchar', 0E0, 0E0, 0E0, null, null, null), " +
                        "('c_varbinary', null, 0E0, 0E0, null, null, null), " +
                        "('p_varchar', 0E0, 0E0, 0E0, null, null, null), " +
                        "(null, null, null, null, 0E0, null, null)");

        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testAnalyzePropertiesSystemTable()
    {
        assertQuery(
                "SELECT * FROM system.metadata.analyze_properties WHERE catalog_name = 'hive'",
                "SELECT 'hive', 'partitions', '', 'array(array(varchar))', 'Partitions to be analyzed'");
    }

    @Test
    public void testAnalyzeEmptyTable()
    {
        String tableName = "test_analyze_empty_table";
        assertUpdate(format("CREATE TABLE %s (c_bigint BIGINT, c_varchar VARCHAR(2))", tableName));
        assertUpdate("ANALYZE " + tableName, 0);
    }

    @Test
    public void testInvalidAnalyzePartitionedTable()
    {
        String tableName = "test_invalid_analyze_partitioned_table";

        // Test table does not exist
        assertQueryFails("ANALYZE " + tableName, format(".*Table 'hive.tpch.%s' does not exist.*", tableName));

        createPartitionedTableForAnalyzeTest(tableName);

        // Test invalid property
        assertQueryFails(format("ANALYZE %s WITH (error = 1)", tableName), ".*'hive' does not support analyze property 'error'.*");
        assertQueryFails(format("ANALYZE %s WITH (partitions = 1)", tableName), ".*\\QCannot convert [1] to array(array(varchar))\\E.*");
        assertQueryFails(format("ANALYZE %s WITH (partitions = NULL)", tableName), ".*Invalid null value for analyze property.*");
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[NULL])", tableName), ".*Invalid null value in analyze partitions property.*");

        // Test non-existed partition
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p4', '10']])", tableName), ".*Partition no longer exists.*");

        // Test partition schema mismatch
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p4']])", tableName), "Partition value count does not match partition column count");
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p4', '10', 'error']])", tableName), "Partition value count does not match partition column count");

        // Drop the partitioned test table
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testInvalidAnalyzeUnpartitionedTable()
    {
        String tableName = "test_invalid_analyze_unpartitioned_table";

        // Test table does not exist
        assertQueryFails("ANALYZE " + tableName, ".*Table.*does not exist.*");

        createUnpartitionedTableForAnalyzeTest(tableName);

        // Test partition properties on unpartitioned table
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[])", tableName), "Partition list provided but table is not partitioned");
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p1']])", tableName), "Partition list provided but table is not partitioned");

        // Drop the partitioned test table
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testAnalyzePartitionedTable()
    {
        String tableName = "test_analyze_partitioned_table";
        createPartitionedTableForAnalyzeTest(tableName);

        // No column stats before running analyze
        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 24.0, 3.0, 0.25, null, null, null), " +
                        "('p_bigint', null, 2.0, 0.25, null, '7', '8'), " +
                        "(null, null, null, null, 16.0, null, null)");

        // No column stats after running an empty analyze
        assertUpdate(format("ANALYZE %s WITH (partitions = ARRAY[])", tableName), 0);
        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 24.0, 3.0, 0.25, null, null, null), " +
                        "('p_bigint', null, 2.0, 0.25, null, '7', '8'), " +
                        "(null, null, null, null, 16.0, null, null)");

        // Run analyze on 3 partitions including a null partition and a duplicate partition
        assertUpdate(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p1', '7'], ARRAY['p2', '7'], ARRAY['p2', '7'], ARRAY[NULL, NULL]])", tableName), 12);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '0', '1'), " +
                        "('c_double', null, 2.0, 0.5, null, '1.2', '2.2'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '1', '2'), " +
                        "('c_double', null, 2.0, 0.5, null, '2.3', '3.3'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 1.0, 0.0, null, null, null), " +
                        "('c_bigint', null, 4.0, 0.0, null, '4', '7'), " +
                        "('c_double', null, 4.0, 0.0, null, '4.7', '7.7'), " +
                        "('c_timestamp', null, 4.0, 0.0, null, null, null), " +
                        "('c_varchar', 16.0, 4.0, 0.0, null, null, null), " +
                        "('c_varbinary', 8.0, null, 0.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', null, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 4.0, null, null)");

        // Partition [p3, 8], [e1, 9], [e2, 9] have no column stats
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '8', '8'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 0.0, 0.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 0.0, 0.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");

        // Run analyze on the whole table
        assertUpdate("ANALYZE " + tableName, 16);

        // All partitions except empty partitions have column stats
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '0', '1'), " +
                        "('c_double', null, 2.0, 0.5, null, '1.2', '2.2'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '1', '2'), " +
                        "('c_double', null, 2.0, 0.5, null, '2.3', '3.3'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 1.0, 0.0, null, null, null), " +
                        "('c_bigint', null, 4.0, 0.0, null, '4', '7'), " +
                        "('c_double', null, 4.0, 0.0, null, '4.7', '7.7'), " +
                        "('c_timestamp', null, 4.0, 0.0, null, null, null), " +
                        "('c_varchar', 16.0, 4.0, 0.0, null, null, null), " +
                        "('c_varbinary', 8.0, null, 0.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', null, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '2', '3'), " +
                        "('c_double', null, 2.0, 0.5, null, '3.4', '4.4'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '8', '8'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 0.0, 0.0, null, null, null), " +
                        "('c_bigint', null, 0.0, 0.0, null, null, null), " +
                        "('c_double', null, 0.0, 0.0, null, null, null), " +
                        "('c_timestamp', null, 0.0, 0.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 0.0, null, null, null), " +
                        "('c_varbinary', 0.0, null, 0.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 0.0, 0.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 0.0, 0.0, null, null, null), " +
                        "('c_bigint', null, 0.0, 0.0, null, null, null), " +
                        "('c_double', null, 0.0, 0.0, null, null, null), " +
                        "('c_timestamp', null, 0.0, 0.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 0.0, null, null, null), " +
                        "('c_varbinary', 0.0, null, 0.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 0.0, 0.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");

        // Drop the partitioned test table
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testAnalyzeUnpartitionedTable()
    {
        String tableName = "test_analyze_unpartitioned_table";
        createUnpartitionedTableForAnalyzeTest(tableName);

        // No column stats before running analyze
        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, null, null, null, null, null), " +
                        "('p_bigint', null, null, null, null, null, null), " +
                        "(null, null, null, null, null, null, null)");

        // Run analyze on the whole table
        assertUpdate("ANALYZE " + tableName, 16);

        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.375, null, null, null), " +
                        "('c_bigint', null, 8.0, 0.375, null, '0', '7'), " +
                        "('c_double', null, 10.0, 0.375, null, '1.2', '7.7'), " +
                        "('c_timestamp', null, 10.0, 0.375, null, null, null), " +
                        "('c_varchar', 40.0, 10.0, 0.375, null, null, null), " +
                        "('c_varbinary', 20.0, null, 0.375, null, null, null), " +
                        "('p_varchar', 24.0, 3.0, 0.25, null, null, null), " +
                        "('p_bigint', null, 2.0, 0.25, null, '7', '8'), " +
                        "(null, null, null, null, 16.0, null, null)");

        // Drop the unpartitioned test table
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    protected void createPartitionedTableForAnalyzeTest(String tableName)
    {
        createTableForAnalyzeTest(tableName, true);
    }

    protected void createUnpartitionedTableForAnalyzeTest(String tableName)
    {
        createTableForAnalyzeTest(tableName, false);
    }

    private void createTableForAnalyzeTest(String tableName, boolean partitioned)
    {
        Session defaultSession = getSession();

        // Disable column statistics collection when creating the table
        Session disableColumnStatsSession = Session.builder(defaultSession)
                .setCatalogSessionProperty(defaultSession.getCatalog().get(), "collect_column_statistics_on_write", "false")
                .build();

        assertUpdate(
                disableColumnStatsSession,
                "" +
                        "CREATE TABLE " +
                        tableName +
                        (partitioned ? " WITH (partitioned_by = ARRAY['p_varchar', 'p_bigint'])\n" : " ") +
                        "AS " +
                        "SELECT c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar, p_bigint " +
                        "FROM ( " +
                        "  VALUES " +
                        // p_varchar = 'p1', p_bigint = BIGINT '7'
                        "    (null, null, null, null, null, null, 'p1', BIGINT '7'), " +
                        "    (null, null, null, null, null, null, 'p1', BIGINT '7'), " +
                        "    (true, BIGINT '1', DOUBLE '2.2', TIMESTAMP '2012-08-08 01:00', 'abc1', X'bcd1', 'p1', BIGINT '7'), " +
                        "    (false, BIGINT '0', DOUBLE '1.2', TIMESTAMP '2012-08-08 00:00', 'abc2', X'bcd2', 'p1', BIGINT '7'), " +
                        // p_varchar = 'p2', p_bigint = BIGINT '7'
                        "    (null, null, null, null, null, null, 'p2', BIGINT '7'), " +
                        "    (null, null, null, null, null, null, 'p2', BIGINT '7'), " +
                        "    (true, BIGINT '2', DOUBLE '3.3', TIMESTAMP '2012-09-09 01:00', 'cba1', X'dcb1', 'p2', BIGINT '7'), " +
                        "    (false, BIGINT '1', DOUBLE '2.3', TIMESTAMP '2012-09-09 00:00', 'cba2', X'dcb2', 'p2', BIGINT '7'), " +
                        // p_varchar = 'p3', p_bigint = BIGINT '8'
                        "    (null, null, null, null, null, null, 'p3', BIGINT '8'), " +
                        "    (null, null, null, null, null, null, 'p3', BIGINT '8'), " +
                        "    (true, BIGINT '3', DOUBLE '4.4', TIMESTAMP '2012-10-10 01:00', 'bca1', X'cdb1', 'p3', BIGINT '8'), " +
                        "    (false, BIGINT '2', DOUBLE '3.4', TIMESTAMP '2012-10-10 00:00', 'bca2', X'cdb2', 'p3', BIGINT '8'), " +
                        // p_varchar = NULL, p_bigint = NULL
                        "    (false, BIGINT '7', DOUBLE '7.7', TIMESTAMP '1977-07-07 07:07', 'efa1', X'efa1', NULL, NULL), " +
                        "    (false, BIGINT '6', DOUBLE '6.7', TIMESTAMP '1977-07-07 07:06', 'efa2', X'efa2', NULL, NULL), " +
                        "    (false, BIGINT '5', DOUBLE '5.7', TIMESTAMP '1977-07-07 07:05', 'efa3', X'efa3', NULL, NULL), " +
                        "    (false, BIGINT '4', DOUBLE '4.7', TIMESTAMP '1977-07-07 07:04', 'efa4', X'efa4', NULL, NULL) " +
                        ") AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar, p_bigint)", 16);

        if (partitioned) {
            // Create empty partitions
            assertUpdate(disableColumnStatsSession, String.format("CALL system.create_empty_partition('%s', '%s', ARRAY['p_varchar', 'p_bigint'], ARRAY['%s', '%s'])", TPCH_SCHEMA, tableName, "e1", "9"));
            assertUpdate(disableColumnStatsSession, String.format("CALL system.create_empty_partition('%s', '%s', ARRAY['p_varchar', 'p_bigint'], ARRAY['%s', '%s'])", TPCH_SCHEMA, tableName, "e2", "9"));
        }
    }

    @Test
    public void testInsertMultipleColumnsFromSameChannel()
    {
        String tableName = "test_insert_multiple_columns_same_channel";
        assertUpdate(format("" +
                "CREATE TABLE %s ( " +
                "   c_bigint_1 BIGINT, " +
                "   c_bigint_2 BIGINT, " +
                "   p_varchar_1 VARCHAR, " +
                "   p_varchar_2 VARCHAR " +
                ") " +
                "WITH ( " +
                "   partitioned_by = ARRAY['p_varchar_1', 'p_varchar_2'] " +
                ")", tableName));

        assertUpdate(format("" +
                "INSERT INTO %s " +
                "SELECT 1 c_bigint_1, 1 c_bigint_2, '2' p_varchar_1, '2' p_varchar_2 ", tableName), 1);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar_1 = '2' AND p_varchar_2 = '2')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_bigint_1', null, 1.0E0, 0.0E0, null, '1', '1'), " +
                        "('c_bigint_2', null, 1.0E0, 0.0E0, null, '1', '1'), " +
                        "('p_varchar_1', 1.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "('p_varchar_2', 1.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "(null, null, null, null, 1.0E0, null, null)");

        assertUpdate(format("" +
                "INSERT INTO %s (c_bigint_1, c_bigint_2, p_varchar_1, p_varchar_2) " +
                "SELECT orderkey, orderkey, orderstatus, orderstatus " +
                "FROM orders " +
                "WHERE orderstatus='O' AND orderkey = 15008", tableName), 1);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar_1 = 'O' AND p_varchar_2 = 'O')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_bigint_1', null, 1.0E0, 0.0E0, null, '15008', '15008'), " +
                        "('c_bigint_2', null, 1.0E0, 0.0E0, null, '15008', '15008'), " +
                        "('p_varchar_1', 1.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "('p_varchar_2', 1.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "(null, null, null, null, 1.0E0, null, null)");

        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testCreateAvroTableWithSchemaUrl()
            throws Exception
    {
        String tableName = "test_create_avro_table_with_schema_url";
        File schemaFile = createAvroSchemaFile();

        String createTableSql = getAvroCreateTableSql(tableName, schemaFile.getAbsolutePath());
        String expectedShowCreateTable = getAvroCreateTableSql(tableName, schemaFile.toURI().toString());

        assertUpdate(createTableSql);

        try {
            MaterializedResult actual = computeActual(format("SHOW CREATE TABLE %s", tableName));
            assertShowCreateTableOutput(actual.getOnlyValue(), expectedShowCreateTable);
        }
        finally {
            assertUpdate(format("DROP TABLE %s", tableName));
            verify(schemaFile.delete(), "cannot delete temporary file: %s", schemaFile);
        }
    }

    @Test
    public void testAlterAvroTableWithSchemaUrl()
            throws Exception
    {
        testAlterAvroTableWithSchemaUrl(true, true, true);
    }

    protected void testAlterAvroTableWithSchemaUrl(boolean renameColumn, boolean addColumn, boolean dropColumn)
            throws Exception
    {
        String tableName = "test_alter_avro_table_with_schema_url";
        File schemaFile = createAvroSchemaFile();

        assertUpdate(getAvroCreateTableSql(tableName, schemaFile.getAbsolutePath()));

        try {
            if (renameColumn) {
                assertQueryFails(format("ALTER TABLE %s RENAME COLUMN dummy_col TO new_dummy_col", tableName), "ALTER TABLE not supported when Avro schema url is set");
            }
            if (addColumn) {
                assertQueryFails(format("ALTER TABLE %s ADD COLUMN new_dummy_col VARCHAR", tableName), "ALTER TABLE not supported when Avro schema url is set");
            }
            if (dropColumn) {
                assertQueryFails(format("ALTER TABLE %s DROP COLUMN dummy_col", tableName), "ALTER TABLE not supported when Avro schema url is set");
            }
        }
        finally {
            assertUpdate(format("DROP TABLE %s", tableName));
            verify(schemaFile.delete(), "cannot delete temporary file: %s", schemaFile);
        }
    }

    private String getAvroCreateTableSql(String tableName, String schemaFile)
    {
        return format("CREATE TABLE %s.%s.%s (\n" +
                        "   dummy_col varchar,\n" +
                        "   another_dummy_col varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   avro_schema_url = '%s',\n" +
                        "   format = 'AVRO'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                schemaFile);
    }

    private static File createAvroSchemaFile()
            throws Exception
    {
        File schemaFile = File.createTempFile("avro_single_column-", ".avsc");
        String schema = "{\n" +
                "  \"namespace\": \"io.prestosql.test\",\n" +
                "  \"name\": \"single_column\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"fields\": [\n" +
                "    { \"name\":\"string_col\", \"type\":\"string\" }\n" +
                "]}";
        asCharSink(schemaFile, UTF_8).write(schema);
        return schemaFile;
    }

    @Test
    public void testCreateOrcTableWithSchemaUrl()
            throws Exception
    {
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.%s.test_orc (\n" +
                        "   dummy_col varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   avro_schema_url = 'dummy.avsc',\n" +
                        "   format = 'ORC'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get());

        assertQueryFails(createTableSql, "Cannot specify avro_schema_url table property for storage format: ORC");
    }

    @Test
    public void testCtasFailsWithAvroSchemaUrl()
            throws Exception
    {
        @Language("SQL") String ctasSqlWithoutData = "CREATE TABLE create_avro\n" +
                "WITH (avro_schema_url = 'dummy_schema')\n" +
                "AS SELECT 'dummy_value' as dummy_col WITH NO DATA";

        assertQueryFails(ctasSqlWithoutData, "CREATE TABLE AS not supported when Avro schema url is set");

        @Language("SQL") String ctasSql = "CREATE TABLE create_avro\n" +
                "WITH (avro_schema_url = 'dummy_schema')\n" +
                "AS SELECT * FROM (VALUES('a')) t (a)";

        assertQueryFails(ctasSql, "CREATE TABLE AS not supported when Avro schema url is set");
    }

    @Test
    public void testBucketedTablesFailWithAvroSchemaUrl()
            throws Exception
    {
        @Language("SQL") String createSql = "CREATE TABLE create_avro (dummy VARCHAR)\n" +
                "WITH (avro_schema_url = 'dummy_schema',\n" +
                "      bucket_count = 2, bucketed_by=ARRAY['dummy'])";

        assertQueryFails(createSql, "Bucketing/Partitioning columns not supported when Avro schema url is set");
    }

    @Test
    public void testPartitionedTablesFailWithAvroSchemaUrl()
            throws Exception
    {
        @Language("SQL") String createSql = "CREATE TABLE create_avro (dummy VARCHAR)\n" +
                "WITH (avro_schema_url = 'dummy_schema',\n" +
                "      partitioned_by=ARRAY['dummy'])";

        assertQueryFails(createSql, "Bucketing/Partitioning columns not supported when Avro schema url is set");
    }

    @Test
    public void testPrunePartitionFailure()
    {
        assertUpdate("CREATE TABLE test_prune_failure\n" +
                "WITH (partitioned_by = ARRAY['p']) AS\n" +
                "SELECT 123 x, 'abc' p", 1);

        assertQueryReturnsEmptyResult("" +
                "SELECT * FROM test_prune_failure\n" +
                "WHERE x < 0 AND cast(p AS int) > 0");

        assertUpdate("DROP TABLE test_prune_failure");
    }

    @Test
    public void testTemporaryStagingDirectorySessionProperties()
    {
        String tableName = "test_temporary_staging_directory_session_properties";
        assertUpdate(format("CREATE TABLE %s(i int)", tableName));

        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "temporary_staging_directory_enabled", "false")
                .build();

        HiveInsertTableHandle hiveInsertTableHandle = getHiveInsertTableHandle(session, tableName);
        assertEquals(hiveInsertTableHandle.getLocationHandle().getWritePath(), hiveInsertTableHandle.getLocationHandle().getTargetPath());

        session = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "temporary_staging_directory_enabled", "true")
                .build();

        hiveInsertTableHandle = getHiveInsertTableHandle(session, tableName);
        assertNotEquals(hiveInsertTableHandle.getLocationHandle().getWritePath(), hiveInsertTableHandle.getLocationHandle().getTargetPath());
        // Since staging directory is getting created inside table path
        assertTrue(hiveInsertTableHandle.getLocationHandle().getWritePath().toString().startsWith(hiveInsertTableHandle.getLocationHandle().getTargetPath().toString()));

        assertUpdate("DROP TABLE " + tableName);
    }

    private HiveInsertTableHandle getHiveInsertTableHandle(Session session, String tableName)
    {
        getQueryRunner().getMetadata().cleanupQuery(session);

        Metadata metadata = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getMetadata();
        return transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .execute(session, transactionSession -> {
                    QualifiedObjectName objectName = new QualifiedObjectName(catalog, TPCH_SCHEMA, tableName);
                    Optional<TableHandle> handle = metadata.getTableHandle(transactionSession, objectName);
                    InsertTableHandle insertTableHandle = metadata.beginInsert(transactionSession, handle.get(), false);
                    HiveInsertTableHandle hiveInsertTableHandle = (HiveInsertTableHandle) insertTableHandle.getConnectorHandle();

                    metadata.finishInsert(transactionSession, insertTableHandle, ImmutableList.of(), ImmutableList.of());
                    return hiveInsertTableHandle;
                });
    }

    @Test
    public void testSelectWithNoColumns()
    {
        testWithAllStorageFormats(this::testSelectWithNoColumns);
    }

    private void testSelectWithNoColumns(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_select_with_no_columns";
        @Language("SQL") String createTable = format(
                "CREATE TABLE %s (col0) WITH (format = '%s') AS VALUES 5, 6, 7",
                tableName,
                storageFormat);
        assertUpdate(session, createTable, 3);
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));

        assertQuery("SELECT 1 FROM " + tableName, "VALUES 1, 1, 1");
        assertQuery("SELECT count(*) FROM " + tableName, "SELECT 3");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUnsupportedCsvTable()
    {
        assertQueryFails(
                "CREATE TABLE create_unsupported_csv(i INT, bound VARCHAR(10), unbound VARCHAR, dummy VARCHAR) WITH (format = 'CSV')",
                "\\QHive CSV storage format only supports VARCHAR (unbounded). Unsupported columns: i integer, bound varchar(10)\\E");
    }

    private Session getParallelWriteSession()
    {
        return Session.builder(getSession())
                .setSystemProperty("task_writer_count", "4")
                .build();
    }

    private void assertOneNotNullResult(@Language("SQL") String query)
    {
        MaterializedResult results = getQueryRunner().execute(getSession(), query).toTestTypes();
        assertEquals(results.getRowCount(), 1);
        assertEquals(results.getMaterializedRows().get(0).getFieldCount(), 1);
        assertNotNull(results.getMaterializedRows().get(0).getField(0));
    }

    private Type canonicalizeType(Type type)
    {
        HiveType hiveType = HiveType.toHiveType(typeTranslator, type);
        return TYPE_MANAGER.getType(hiveType.getTypeSignature());
    }

    private String canonicalizeTypeName(String type)
    {
        TypeSignature typeSignature = TypeSignature.parseTypeSignature(type);
        return canonicalizeType(TYPE_MANAGER.getType(typeSignature)).toString();
    }

    private void assertColumnType(TableMetadata tableMetadata, String columnName, Type expectedType)
    {
        assertEquals(tableMetadata.getColumn(columnName).getType(), canonicalizeType(expectedType));
    }

    private void assertConstraints(@Language("SQL") String query, Set<ColumnConstraint> expected)
    {
        MaterializedResult result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) " + query);
        Set<ColumnConstraint> constraints = jsonCodec(IoPlan.class).fromJson((String) getOnlyElement(result.getOnlyColumnAsSet()))
                .getInputTableColumnInfos().stream()
                .findFirst().get()
                .getColumnConstraints();

        assertEquals(constraints, expected);
    }

    private void verifyPartition(boolean hasPartition, TableMetadata tableMetadata, List<String> partitionKeys)
    {
        Object partitionByProperty = tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY);
        if (hasPartition) {
            assertEquals(partitionByProperty, partitionKeys);
            for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                boolean partitionKey = partitionKeys.contains(columnMetadata.getName());
                assertEquals(columnMetadata.getExtraInfo(), columnExtraInfo(partitionKey));
            }
        }
        else {
            assertNull(partitionByProperty);
        }
    }

    private void rollback()
    {
        throw new RollbackException();
    }

    private static class RollbackException
            extends RuntimeException
    {
    }

    private static ConnectorSession getConnectorSession(Session session)
    {
        return session.toConnectorSession(new CatalogName(session.getCatalog().get()));
    }

    @Test
    public void testEmptyBucketedTable()
    {
        // go through all storage formats to make sure the empty buckets are correctly created
        testWithAllStorageFormats(this::testEmptyBucketedTable);
    }

    private void testEmptyBucketedTable(Session session, HiveStorageFormat storageFormat)
    {
        testEmptyBucketedTable(session, storageFormat, true);
        testEmptyBucketedTable(session, storageFormat, false);
    }

    private void testEmptyBucketedTable(Session session, HiveStorageFormat storageFormat, boolean createEmpty)
    {
        String tableName = "test_empty_bucketed_table";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(bucket_key VARCHAR, col_1 VARCHAR, col2 VARCHAR) " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "bucketed_by = ARRAY[ 'bucket_key' ], " +
                "bucket_count = 11 " +
                ") ";

        assertUpdate(createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertNull(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("bucket_key"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);

        assertEquals(computeActual("SELECT * from " + tableName).getRowCount(), 0);

        // make sure that we will get one file per bucket regardless of writer count configured
        Session parallelWriter = Session.builder(getParallelWriteSession())
                .setCatalogSessionProperty(catalog, "create_empty_bucket_files", String.valueOf(createEmpty))
                .build();
        assertUpdate(parallelWriter, "INSERT INTO " + tableName + " VALUES ('a0', 'b0', 'c0')", 1);
        assertUpdate(parallelWriter, "INSERT INTO " + tableName + " VALUES ('a1', 'b1', 'c1')", 1);

        assertQuery("SELECT * from " + tableName, "VALUES ('a0', 'b0', 'c0'), ('a1', 'b1', 'c1')");

        assertUpdate(session, "DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testBucketedTable()
    {
        // go through all storage formats to make sure the empty buckets are correctly created
        testWithAllStorageFormats(this::testBucketedTable);
    }

    private void testBucketedTable(Session session, HiveStorageFormat storageFormat)
    {
        testBucketedTable(session, storageFormat, true);
        testBucketedTable(session, storageFormat, false);
    }

    private void testBucketedTable(Session session, HiveStorageFormat storageFormat, boolean createEmpty)
    {
        String tableName = "test_bucketed_table";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "bucketed_by = ARRAY[ 'bucket_key' ], " +
                "bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT * " +
                "FROM (" +
                "VALUES " +
                "  (VARCHAR 'a', VARCHAR 'b', VARCHAR 'c'), " +
                "  ('aa', 'bb', 'cc'), " +
                "  ('aaa', 'bbb', 'ccc')" +
                ") t (bucket_key, col_1, col_2)";

        // make sure that we will get one file per bucket regardless of writer count configured
        Session parallelWriter = Session.builder(getParallelWriteSession())
                .setCatalogSessionProperty(catalog, "create_empty_bucket_files", String.valueOf(createEmpty))
                .build();
        assertUpdate(parallelWriter, createTable, 3);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertNull(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("bucket_key"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);

        assertQuery("SELECT * from " + tableName, "VALUES ('a', 'b', 'c'), ('aa', 'bb', 'cc'), ('aaa', 'bbb', 'ccc')");

        assertUpdate(parallelWriter, "INSERT INTO " + tableName + " VALUES ('a0', 'b0', 'c0')", 1);
        assertUpdate(parallelWriter, "INSERT INTO " + tableName + " VALUES ('a1', 'b1', 'c1')", 1);

        assertQuery("SELECT * from " + tableName, "VALUES ('a', 'b', 'c'), ('aa', 'bb', 'cc'), ('aaa', 'bbb', 'ccc'), ('a0', 'b0', 'c0'), ('a1', 'b1', 'c1')");

        assertUpdate(session, "DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testInsertTwiceToSamePartitionedBucket()
    {
        String tableName = "test_insert_twice_to_same_partitioned_bucket";
        createPartitionedBucketedTable(tableName, HiveStorageFormat.RCBINARY);

        String insert = "INSERT INTO " + tableName +
                " VALUES (1, 1, 'first_comment', 'F'), (2, 2, 'second_comment', 'G')";
        assertUpdate(insert, 2);
        assertUpdate(insert, 2);

        assertQuery(
                "SELECT custkey, custkey2, comment, orderstatus FROM " + tableName + " ORDER BY custkey",
                "VALUES (1, 1, 'first_comment', 'F'), (1, 1, 'first_comment', 'F'), (2, 2, 'second_comment', 'G'), (2, 2, 'second_comment', 'G')");
        assertQuery(
                "SELECT custkey, custkey2, comment, orderstatus FROM " + tableName + " WHERE custkey = 1 and custkey2 = 1",
                "VALUES (1, 1, 'first_comment', 'F'), (1, 1, 'first_comment', 'F')");
        assertUpdate("DROP TABLE " + tableName);
    }

    private void testWithAllStorageFormats(BiConsumer<Session, HiveStorageFormat> test)
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            testWithStorageFormat(storageFormat, test);
        }
    }

    private static void testWithStorageFormat(TestingHiveStorageFormat storageFormat, BiConsumer<Session, HiveStorageFormat> test)
    {
        requireNonNull(storageFormat, "storageFormat is null");
        requireNonNull(test, "test is null");
        Session session = storageFormat.getSession();
        try {
            test.accept(session, storageFormat.getFormat());
        }
        catch (Exception | AssertionError e) {
            fail(format("Failure for format %s with properties %s", storageFormat.getFormat(), session.getConnectorProperties()), e);
        }
    }

    private List<TestingHiveStorageFormat> getAllTestingHiveStorageFormat()
    {
        Session session = getSession();
        ImmutableList.Builder<TestingHiveStorageFormat> formats = ImmutableList.builder();
        for (HiveStorageFormat hiveStorageFormat : HiveStorageFormat.values()) {
            if (hiveStorageFormat == HiveStorageFormat.CSV || hiveStorageFormat == HiveStorageFormat.MULTIDELIMIT) {
                // CSV supports only unbounded VARCHAR type
                // MULTIDELIMIT is supported only when field.delim property is specified
                continue;
            }
            formats.add(new TestingHiveStorageFormat(session, hiveStorageFormat));
        }
        return formats.build();
    }

    /*
     * Expected output is the original CREATE TABLE query
     * While actual output has additional data like location and external table properties
     * Verify that all the lines of expected result are present in actual result
     */
    private void assertShowCreateTableOutput(Object actual, String expected)
    {
        List<String> expectedLines = Stream.of(
                expected.split("\n"))
                .map(line -> line.lastIndexOf(',') == (line.length() - 1) ? line.substring(0, line.length() - 1) : line)
                .collect(Collectors.toList());

        List<String> absentLines = expectedLines.stream().filter(line -> !actual.toString().contains(line)).collect(Collectors.toList());

        assertTrue(absentLines.isEmpty(), format("Expected %s\nFound %s\nMissing lines in output %s", expected, actual, absentLines));
    }

    private static class TestingHiveStorageFormat
    {
        private final Session session;
        private final HiveStorageFormat format;

        TestingHiveStorageFormat(Session session, HiveStorageFormat format)
        {
            this.session = requireNonNull(session, "session is null");
            this.format = requireNonNull(format, "format is null");
        }

        public Session getSession()
        {
            return session;
        }

        public HiveStorageFormat getFormat()
        {
            return format;
        }
    }

    @Test
    public void testAutoVacuum()
    {
        assertUpdate(autoVacuumSession, "CREATE TABLE auto_vacuum_test_table1 (a int) with (format='orc', transactional=true)");

        TableMetadata tableMetadata = getTableMetadata(autoVacuumSession.getCatalog().get(), autoVacuumSession.getSchema().get(),
                "auto_vacuum_test_table1");

        for (int i = 0; i <= 10; i++) {
            String query = format("INSERT INTO auto_vacuum_test_table1 VALUES(%d), (%d)", i, i * 2);
            assertUpdate(autoVacuumSession, query, 2);
        }

        String tablePath = String.valueOf(tableMetadata.getMetadata().getProperties().get("location"));

        checkBaseDirectoryExists(tablePath, true);

        assertUpdate(autoVacuumSession, "DROP TABLE auto_vacuum_test_table1");
    }

    @Test
    public void testAutoVacuumOnPartitionTable()
    {
        assertUpdate(autoVacuumSession, "CREATE TABLE auto_vacuum_test_table2 (a int, b int)" +
                " with (format='orc', transactional=true, partitioned_by=Array['b'])");

        TableMetadata tableMetadata = getTableMetadata(autoVacuumSession.getCatalog().get(), autoVacuumSession.getSchema().get(),
                "auto_vacuum_test_table2");

        for (int i = 0; i <= 10; i++) {
            String query = format("INSERT INTO auto_vacuum_test_table2 VALUES(%d, 1), (%d, 2)", i, i * 2);
            assertUpdate(autoVacuumSession, query, 2);
        }

        String tablePath = String.valueOf(tableMetadata.getMetadata().getProperties().get("location"));

        checkBaseDirectoryExists(tablePath + "/b=1", true);
        checkBaseDirectoryExists(tablePath + "/b=2", false);

        assertUpdate(autoVacuumSession, "DROP TABLE auto_vacuum_test_table2");
    }

    private void checkBaseDirectoryExists(String path, boolean delayRequired)
    {
        try {
            // Since auto-vacuum runs asynchronously
            if (delayRequired) {
                TimeUnit.SECONDS.sleep(80);
            }
        }
        catch (InterruptedException e) {
            // Ignore
        }
        if (path.startsWith("file:")) {
            path = path.replace("file:", "");
        }
        String[] actualDirectoryList = new File(path).list(new FilenameFilter()
        {
            @Override
            public boolean accept(File file, String s)
            {
                return s.startsWith("base");
            }
        });

        assertEquals(actualDirectoryList.length, 1);
    }

    @Test
    public void testUnsupportedFunctions()
    {
        testWithAllStorageFormats(this::testUnsupportedFunctions);
    }

    private void testUnsupportedFunctions(Session session, HiveStorageFormat storageFormat)
    {
        Session session1 = Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                .build();
        Session session2 = Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                .setCatalogSessionProperty(session.getCatalog().get(), "orc_disjunct_predicate_pushdown_enabled", "false")
                .build();
        try {
            assertUpdate(session1, "CREATE TABLE test_unsupport (a int, b int) with (transactional=true, format='orc')");
            assertUpdate(session1, "INSERT INTO test_unsupport VALUES (1, 2),(11,22)", 2);
            assertEquals(computeActual(session1, "SELECT * from test_unsupport").getRowCount(), 02L);
            assertUpdate(session1, "UPDATE test_unsupport set a=111 where a=1", 01L);
            assertUpdate(session1, "DELETE from test_unsupport where a=111", 01L);

            assertUpdate(session1, "CREATE TABLE map_test with (format='orc') AS SELECT MAP(ARRAY[0,1], ARRAY[2,NULL]) AS col", 1);
            assertQuery(session1, "SELECT col[0] FROM map_test", "SELECT 2");
            assertUpdate(session1, "CREATE TABLE array_test AS SELECT ARRAY[1, 2, NULL] AS col", 1);
            assertQuery(session1, "SELECT col[2] FROM array_test", "SELECT 2");
            assertUpdate(session1, "CREATE TABLE alldtype (id1 int, id4 double, id5 float, id6 decimal(5,2), id7 varchar(10), id8 char(10)) with (format='orc')");
            assertUpdate(session1, "INSERT Into alldtype values(1,4.5,5.6,6.7,'rajeev','male')", 1);
            assertQuery(session1, "select * from alldtype where id1=1", "SELECT 1,4.5,5.6,6.7,'rajeev','male'");
            assertQuery(session1, "select count(1) from alldtype where id4=4.5", "SELECT 1");
            assertUpdate(session1, "CREATE TABLE part2key (id1 int, id2 int, id3 int) with (format='orc', partitioned_by=ARRAY['id2','id3'])");
            assertUpdate(session1, "INSERT Into part2key values(1,2,3)", 1);
            assertQuery(session1, "select * from part2key where id2=2 and id3=3", "SELECT 1,2,3");
            assertQuery(session2, "select * from part2key where id2=2 and id3=3", "SELECT 1,2,3");
            assertUpdate(session1, "CREATE TABLE multiin (id1 int, id2 int, id3 varchar(10)) with (format='orc')");
            assertUpdate(session1, "INSERT Into multiin values(1,2,'abc'), (11,22,'xyz'), (111,222,'abcd'), (1111,2222,'zxy')", 4);
            assertQuery(session1, "select * from multiin where id3 in ('abc', 'xyz', 'abcd', 'zxy') order by id1", "SELECT * from (values(1,2,'abc'), (11,22,'xyz'), (111,222,'abcd'), (1111,2222,'zxy'))");
            assertQuery(session1, "select * from multiin where id3 in ('abc', 'yzx', 'adbc', 'abcde')", "SELECT 1,2,'abc'");
            assertUpdate(session1, "create table inperftest(id1 float, id2 double, id3 decimal(19,2), id4 int) with (format='orc')");
            assertUpdate(session1, "insert into inperftest values(1.2,2.2,3.2,4), (11.22, 22.22, 33.22, 44),(111.33,222.33, 333.33, 444)", 3);
            assertQuery(session1, "select * from inperftest where id1 in (1.2,5.3,11.22, 111.33)", "select * from (values(1.2, 2.2 , 3.20, 4), (11.22, 22.22, 33.22, 44), (111.33,222.33, 333.33, 444))");
            assertQuery(session1, "select * from inperftest where id1 in (1.2,5.3)", "select * from (values(1.2, 2.2 , 3.20, 4))");
            assertQuery(session1, "select * from inperftest where id2 in (2.2,5.3,22.22, 222.33)", "select * from (values(1.2, 2.2 , 3.20, 4), (11.22, 22.22, 33.22, 44), (111.33,222.33, 333.33, 444))");
            assertQuery(session1, "select * from inperftest where id2 in (2.2,5.3)", "select * from (values(1.2, 2.2 , 3.20, 4))");
            assertQuery(session1, "select * from inperftest where id3 in (3.2,5.3,33.22, 333.331, 333.33)", "select * from (values(1.2, 2.2 , 3.20, 4), (11.22, 22.22, 33.22, 44), (111.33,222.33, 333.33, 444))");
            assertQuery(session1, "select * from inperftest where id3 in (3.2,5.3, 33.21)", "select * from (values(1.2, 2.2 , 3.20, 4))");
            assertQuery(session1, "select * from inperftest where id3 in (3.2,5.3, 33.221)", "select * from (values(1.2, 2.2 , 3.20, 4))");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_unsupport");
            assertUpdate("DROP TABLE IF EXISTS part2key");
            assertUpdate("DROP TABLE IF EXISTS alldtype");
            assertUpdate("DROP TABLE IF EXISTS map_test");
            assertUpdate("DROP TABLE IF EXISTS array_test");
            assertUpdate("DROP TABLE IF EXISTS multiin");
            assertUpdate("DROP TABLE IF EXISTS inperftest");
        }
    }

    @Test
    public void testNonEqualDynamicFilter()
    {
        Session session = getSession();
        Session sessionTest = Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                .setCatalogSessionProperty(session.getCatalog().get(), "dynamic_filtering_filter_rows_threshold", "20000")
                .setSystemProperty("dynamic_filtering_max_per_driver_value_count", "100000")
                .setSystemProperty("enable_dynamic_filtering", "true")
                .setSystemProperty("optimize_dynamic_filter_generation", "false")
                .build();

        Session sessionCtrl = Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().get(), "orc_predicate_pushdown_enabled", "false")
                .setSystemProperty("enable_dynamic_filtering", "false")
                .build();

        String query = "SELECT COUNT(*) FROM " +
                "(SELECT orderkey FROM lineitem WHERE orderkey < 1000) a " +
                "JOIN " +
                "(SELECT orderkey FROM orders) b " +
                "ON NOT (a.orderkey <= b.orderkey)";

        MaterializedResult expected = computeActual(query);
        MaterializedResult expected2 = computeActual(sessionCtrl, query);
        MaterializedResult resultDynamicFilter = computeActual(sessionTest, query);

        assertEquals(expected.getMaterializedRows(), resultDynamicFilter.getMaterializedRows());
        assertEquals(expected2.getMaterializedRows(), resultDynamicFilter.getMaterializedRows());

        System.out.println(">>>>>>>>> result " + resultDynamicFilter);
    }

    @Test
    public void testPushdownWithNullRows()
    {
        Session session = getSession();
        Session session1 = Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                .build();
        Session session2 = Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                .setCatalogSessionProperty(session.getCatalog().get(), "orc_disjunct_predicate_pushdown_enabled", "false")
                .build();

        String[] types = {"double", "decimal(7,2)", "decimal(38,7)", "integer", "bigint", "string", "boolean"};
        for (String type : types) {
            testPushdownNullForType(session1, session2, type);
            testPushdownGetAllNULLsForType(session1, session2, type);
        }
    }

    private void testPushdownNullForType(Session sessionWithOr, Session sessionWithoutOR, String type)
    {
        try {
            assertUpdate(sessionWithOr, "CREATE TABLE test_predicate_or_NULL (a " + type + ", b " + type + ", c int) with (transactional=false, format='orc')");
            assertUpdate(sessionWithOr, "INSERT INTO test_predicate_or_NULL VALUES " +
                    "(cast(0 as " + type + "), cast(0 as " + type + "),0)," +
                    "(cast(1 as " + type + "), NULL, 1)," +
                    "(NULL,cast(2 as " + type + "), 2)," +
                    "(NULL,NULL,3)," +
                    "(cast(4 as " + type + "), cast(4 as " + type + "),4)", 5);

            List<String> queries = new ArrayList<>();
            queries.add("SELECT * FROM test_predicate_or_NULL WHERE " +
                    "c BETWEEN 0 AND 5 AND (a BETWEEN cast(0 as " + type + ") AND cast(5 as " + type + ") or b BETWEEN cast(0 as " + type + ") AND cast(5 as " + type + ")) " +
                    "ORDER BY a,b,c");
            queries.add("SELECT * FROM test_predicate_or_NULL WHERE " +
                    "c BETWEEN 0 AND 5 " +
                    "AND (" +
                        "a BETWEEN cast(0 as " + type + ") and cast(5 as " + type + ") " +
                        "OR b BETWEEN cast(0 as " + type + ") and cast(5 as " + type + ") " +
                        "OR a IS NULL) " +
                    "ORDER BY a,b,c");
            queries.add("SELECT * FROM test_predicate_or_NULL WHERE " +
                    "c BETWEEN 0 AND 5 " +
                    "AND (" +
                        "a BETWEEN cast(0 as " + type + ") and cast(5 as " + type + ") " +
                        "OR b BETWEEN cast(0 as " + type + ") and cast(5 as " + type + ") " +
                        "OR a IS NULL " +
                        "OR b IS NULL" +
                    ") ORDER BY a,b,c");
            queries.add("SELECT * FROM test_predicate_or_NULL WHERE " +
                    "c BETWEEN 0 AND 5 " +
                    "AND (" +
                        "a BETWEEN cast(0 as " + type + ") and cast(5 as " + type + ") " +
                        "OR b BETWEEN cast(0 as " + type + ") and cast(1 as " + type + ") " +
                        "OR a IS NOT NULL " +
                        "OR a BETWEEN cast(3 as " + type + ") and cast(5 as " + type + ") " +
                    ") ORDER BY a,b,c");

            MaterializedResult expected;
            MaterializedResult resultPushdownOr;
            MaterializedResult resultPushdown;
            for (String query : queries) {
                expected = computeActual(query);
                resultPushdownOr = computeActual(sessionWithOr, query);
                resultPushdown = computeActual(sessionWithoutOR, query);

                assertEquals(expected.getMaterializedRows(), resultPushdown.getMaterializedRows());
                assertEquals(expected.getMaterializedRows(), resultPushdownOr.getMaterializedRows());
                System.out.println("Type(" + type + ")\n-------------\n" + resultPushdown.getMaterializedRows());
            }
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_predicate_or_NULL");
        }
    }

    private void testPushdownGetAllNULLsForType(Session sessionWithOr, Session sessionWithoutOR, String type)
    {
        try {
            assertUpdate(sessionWithOr, "CREATE TABLE test_predicate_or_NULL_tmp (a " + type + ", b " + type + ", c int) with (transactional=false, format='orc')");
            assertUpdate(sessionWithOr, "INSERT INTO test_predicate_or_NULL_tmp VALUES " +
                            "(cast(0 as " + type + "),NULL,0)," +
                            "(NULL,NULL,1)," +
                            "(cast(2 as " + type + "),NULL,2)," +
                            "(NULL,NULL,3)," +
                            "(cast(4 as " + type + "),NULL,4)," +
                    "(NULL,NULL,NULL)",
                    6);
            assertUpdate(sessionWithOr, "INSERT INTO test_predicate_or_NULL_tmp SELECT * from test_predicate_or_NULL_tmp", 6); /* 12 rows */
            assertUpdate(sessionWithOr, "INSERT INTO test_predicate_or_NULL_tmp SELECT * from test_predicate_or_NULL_tmp", 12); /* 24 rows */
            assertUpdate(sessionWithOr, "INSERT INTO test_predicate_or_NULL_tmp SELECT * from test_predicate_or_NULL_tmp", 24); /* 48 rows */
            assertUpdate(sessionWithOr, "INSERT INTO test_predicate_or_NULL_tmp SELECT * from test_predicate_or_NULL_tmp", 48); /* 96 rows */
            assertUpdate(sessionWithOr, "INSERT INTO test_predicate_or_NULL_tmp SELECT * from test_predicate_or_NULL_tmp", 96); /* 192 rows */
            assertUpdate(sessionWithOr, "INSERT INTO test_predicate_or_NULL_tmp SELECT * from test_predicate_or_NULL_tmp", 192); /* 384 rows */
            assertUpdate(sessionWithOr, "INSERT INTO test_predicate_or_NULL_tmp SELECT * from test_predicate_or_NULL_tmp", 384); /* 768 rows */
            assertUpdate(sessionWithOr, "INSERT INTO test_predicate_or_NULL_tmp SELECT * from test_predicate_or_NULL_tmp", 768); /* 1536 rows */

            assertUpdate(sessionWithOr, "CREATE TABLE test_predicate_or_NULL WITH (transactional=false, format='orc')" +
                    " AS SELECT * FROM test_predicate_or_NULL_tmp ORDER BY a, b, c",
                    1536);

            List<String> queries = new ArrayList<>();
            queries.add("SELECT a FROM test_predicate_or_NULL WHERE c = 2 ORDER BY 1");
            queries.add("SELECT a FROM test_predicate_or_NULL WHERE c >= 1 and c <= 3 ORDER BY 1");
            queries.add("SELECT a FROM test_predicate_or_NULL WHERE c IN (1,3,5) ORDER BY 1");
            queries.add("SELECT a FROM test_predicate_or_NULL WHERE c IN (0,2,4) ORDER BY 1");
            queries.add("SELECT a FROM test_predicate_or_NULL WHERE c IS NULL ORDER BY 1");

            queries.add("SELECT a FROM test_predicate_or_NULL WHERE b IS NULL ORDER BY 1");
            queries.add("SELECT a,c FROM test_predicate_or_NULL WHERE b IS NULL ORDER BY 1,2");

            queries.add("SELECT b FROM test_predicate_or_NULL WHERE c = 2 ORDER BY 1");
            queries.add("SELECT b FROM test_predicate_or_NULL WHERE c >= 1 and c <= 3 ORDER BY 1");
            queries.add("SELECT b FROM test_predicate_or_NULL WHERE c IN (1,3,5) ORDER BY 1");
            queries.add("SELECT B FROM test_predicate_or_NULL WHERE c IN (0,2,4) ORDER BY 1");
            queries.add("SELECT b FROM test_predicate_or_NULL WHERE c IS NULL ORDER BY 1");

            queries.add("SELECT a,b FROM test_predicate_or_NULL WHERE c = 2 ORDER BY 1,2");
            queries.add("SELECT b,a FROM test_predicate_or_NULL WHERE c >= 1 and c <= 3 ORDER BY 1,2");
            queries.add("SELECT a,b FROM test_predicate_or_NULL WHERE c IN (1,3,5) ORDER BY 1,2");
            queries.add("SELECT b,a FROM test_predicate_or_NULL WHERE c IN (0,2,4) ORDER BY 1,2");
            queries.add("SELECT a,b,a FROM test_predicate_or_NULL WHERE c IS NULL ORDER BY 1,2");

            MaterializedResult expected;
            MaterializedResult resultPushdownOr;
            MaterializedResult resultPushdown;
            for (String query : queries) {
                expected = computeActual(query);
                resultPushdownOr = computeActual(sessionWithOr, query);
                resultPushdown = computeActual(sessionWithoutOR, query);
                System.out.println("Query [ " + query + " ]");

                assertEquals(expected.getMaterializedRows(), resultPushdown.getMaterializedRows());
                assertEquals(expected.getMaterializedRows(), resultPushdownOr.getMaterializedRows());
                System.out.println("Type(" + type + ")\n-------------\n" + resultPushdown.getMaterializedRows().size());
            }
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_predicate_or_NULL_tmp");
            assertUpdate("DROP TABLE IF EXISTS test_predicate_or_NULL");
        }
    }

    @Test
    public void testUpdateAndDeleteForBooleanColumn()
    {
        assertUpdate("DROP TABLE IF EXISTS tab_bkt_009");
        assertUpdate(autoVacuumSession, "CREATE TABLE tab_bkt_009 (aa tinyint, bb smallint, cc int, " +
                "dd bigint, ee boolean, ff real, gg double, hh varchar(10), ii varbinary, jj timestamp, kk decimal,ll decimal(10, 8),mm date,nn char(6)) " +
                "with (bucket_count=2, bucketed_by=array ['dd'], format='orc', transactional=true)");

        assertUpdate("insert into tab_bkt_009 values (tinyint'21', smallint'31', 810, 11111, boolean'0', 111.111," +
                "111111111.111111, 'hello_111', varbinary'/', timestamp'2019-09-11 01:00:00'," +
                "51, 11.11, date '2019-09-11', 'work_1')", 1);
        assertUpdate("insert into tab_bkt_009 values (tinyint'22', smallint'32', 820, 22222, boolean'0', 222.222," +
                "222222222.222222, 'hello_222', varbinary'/', timestamp'2019-09-14 02:00:00', 52, 22.22," +
                "date '2019-09-14', 'work_2')", 1);
        assertUpdate("update tab_bkt_009 set bb=smallint'10' where ee=boolean'0'", 2);

        assertUpdate("insert into tab_bkt_009 values (tinyint'23', smallint'33', 830, 999930, boolean'0', 3.3, " +
                "3.03,'hello_3', varbinary'/', timestamp'2019-09-13 15:00:03', 53, 30.33, date '2019-09-13', 'work_3')", 1);
        assertUpdate("insert into tab_bkt_009 values (tinyint'24', smallint'34', 840, 999940, boolean'1', 4.4, " +
                "4.04,'hello_4', varbinary'/', timestamp'2019-09-14 15:00:04', 54, 40.34, date '2019-09-14', 'work_4')", 1);
        assertUpdate("insert into tab_bkt_009 values (tinyint'26', smallint'36', 860, 999960, boolean'0', 6.6, " +
                "6.06,'hello_6', varbinary'/', timestamp'2019-09-16 15:00:06', 56, 60.36, date '2019-09-16', 'work_6')", 1);
        assertUpdate("delete from tab_bkt_009 where mm=date'2019-09-14'", 2);
        assertUpdate("delete from tab_bkt_009 where mm=date'2019-09-16'", 1);

        assertUpdate(String.format("DROP TABLE tab_bkt_009"));
    }

    @Test
    public void testVacuumForBooleanColumn()
    {
        assertUpdate("DROP TABLE IF EXISTS tab_bkt_010");
        assertUpdate(autoVacuumSession, "CREATE TABLE tab_bkt_010 (aa tinyint, bb smallint, cc int, " +
                "dd bigint, ee boolean, ff real, gg double, hh varchar(10), ii varbinary, jj timestamp, kk decimal,ll decimal(10, 8),mm date,nn char(6)) " +
                "with (bucket_count=2, bucketed_by=array ['dd'], format='orc', transactional=true)");

        assertUpdate("insert into tab_bkt_010 values (tinyint'23', smallint'33', 830, 999930, boolean'0', 3.3, 3.03," +
                "'hello_3', varbinary'/', timestamp'2019-09-13 15:00:03', 53, 30.33, date '2019-09-13', 'work_3')", 1);
        assertUpdate("insert into tab_bkt_010 values (tinyint'24', smallint'34', 840, 999940, boolean'1', 4.4, 4.04," +
                "'hello_4', varbinary'/', timestamp'2019-09-14 15:00:04', 54, 40.34, date '2019-09-14', 'work_4')", 1);
        assertUpdate("insert into tab_bkt_010 values (tinyint'23', smallint'33', 830, 999930, boolean'0', 3.3, 3.03," +
                "'hello_3', varbinary'/', timestamp'2019-09-13 15:00:03', 53, 30.33, date '2019-09-13', 'work_3')", 1);
        assertUpdate("insert into tab_bkt_010 values (tinyint'24', smallint'34', 840, 999940, boolean'1', 4.4, 4.04," +
                "'hello_4', varbinary'/', timestamp'2019-09-14 15:00:04', 54, 40.34, date '2019-09-14', 'work_4')", 1);
        assertUpdate(String.format("VACUUM TABLE tab_bkt_010 AND WAIT"), 4);
        assertUpdate("delete from tab_bkt_010 where mm=date'2019-09-14'", 2);

        assertUpdate(String.format("DROP TABLE tab_bkt_010"));
    }

    @Test
    public void testCteReuse()
    {
        MaterializedResult result = getQueryRunner().execute("with customer_total_return " +
                " as (select lineitem.orderkey, sum(totalprice) as finalprice " +
                " from   lineitem, " +
                "orders " +
                " where  lineitem.orderkey=orders.orderkey  " +
                " group  by lineitem.orderkey) " +
                "select ctr1.orderkey " +
                "from   customer_total_return ctr1, orders " +
                "where  ctr1.finalprice < (select Avg(finalprice) * 1.2 " +
                "from   customer_total_return ctr2 " +
                "where ctr2.orderkey=ctr1.orderkey) " +
                "and ctr1.orderkey=orders.orderkey limit 100");
        assertEquals(result.getRowCount(), 100);

        result = getQueryRunner().execute("with ss as (select * from orders), sd as (select * from ss) " +
                " select * from ss,sd where ss.orderkey = sd.orderkey");
        assertEquals(result.getRowCount(), 15000);
    }

    private void setUpNodes()
    {
        ImmutableList.Builder<InternalNode> nodeBuilder = ImmutableList.builder();
        nodeBuilder.add(new InternalNode("other1", URI.create("http://10.0.0.1:11"), io.prestosql.client.NodeVersion.UNKNOWN, false));
        nodeBuilder.add(new InternalNode("other2", URI.create("http://10.0.0.1:12"), io.prestosql.client.NodeVersion.UNKNOWN, false));
        nodeBuilder.add(new InternalNode("other3", URI.create("http://10.0.0.1:13"), NodeVersion.UNKNOWN, false));
        ImmutableList<InternalNode> nodes = nodeBuilder.build();
        nodeManager.addNode(CONNECTOR_ID, nodes);
    }

    @Test
    public void testRuseExchangeGroupSplitsMatchingBetweenProducerConsumer()
    {
        setUpNodes();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(new FinalizerService());
        StageId stageId = new StageId(new QueryId("query"), 0);
        UUID uuid = UUID.randomUUID();

        PlanFragment testFragmentProducer = createTableScanPlanFragment("build", ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_PRODUCER, uuid, 1);

        PlanNodeId tableScanNodeId = new PlanNodeId("plan_id");
        StageExecutionPlan producerStageExecutionPlan = new StageExecutionPlan(
                testFragmentProducer,
                ImmutableMap.of(tableScanNodeId, new ConnectorAwareSplitSource(CONNECTOR_ID, createFixedSplitSource(0, TestingSplit::createRemoteSplit))),
                ImmutableList.of(),
                ImmutableMap.of(tableScanNodeId, new TableInfo(new QualifiedObjectName("test", TEST_SCHEMA, "test"), TupleDomain.all())));

        SqlStageExecution producerStage = createSqlStageExecution(
                stageId,
                new TestSqlTaskManager.MockLocationFactory().createStageLocation(stageId),
                producerStageExecutionPlan.getFragment(),
                producerStageExecutionPlan.getTables(),
                new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor),
                TEST_SESSION_REUSE,
                true,
                nodeTaskMap,
                remoteTaskExecutor,
                new NoOpFailureDetector(),
                new SplitSchedulerStats(),
                new DynamicFilterService(new LocalStateStoreProvider(
                        new SeedStoreManager(new FileSystemClientManager()))),
                new QuerySnapshotManager(stageId.getQueryId(), NOOP_SNAPSHOT_UTILS, TEST_SESSION));

        Set<Split> splits = createAndGetSplits(10);
        Multimap<InternalNode, Split> producerAssignment = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values()), Optional.of(producerStage)).getAssignments();
        PlanFragment testFragmentConsumer = createTableScanPlanFragment("build", ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_CONSUMER, uuid, 1);
        StageExecutionPlan consumerStageExecutionPlan = new StageExecutionPlan(
                testFragmentConsumer,
                ImmutableMap.of(tableScanNodeId, new ConnectorAwareSplitSource(CONNECTOR_ID, createFixedSplitSource(0, TestingSplit::createRemoteSplit))),
                ImmutableList.of(),
                ImmutableMap.of(tableScanNodeId, new TableInfo(new QualifiedObjectName("test", TEST_SCHEMA, "test"), TupleDomain.all())));

        SqlStageExecution stage = createSqlStageExecution(
                stageId,
                new TestSqlTaskManager.MockLocationFactory().createStageLocation(stageId),
                consumerStageExecutionPlan.getFragment(),
                consumerStageExecutionPlan.getTables(),
                new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor),
                TEST_SESSION_REUSE,
                true,
                nodeTaskMap,
                remoteTaskExecutor,
                new NoOpFailureDetector(),
                new SplitSchedulerStats(),
                new DynamicFilterService(new LocalStateStoreProvider(
                        new SeedStoreManager(new FileSystemClientManager()))),
                new QuerySnapshotManager(stageId.getQueryId(), NOOP_SNAPSHOT_UTILS, TEST_SESSION));
        Multimap<InternalNode, Split> consumerAssignment = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values()), Optional.of(stage)).getAssignments();

        assertEquals(consumerAssignment.size(), consumerAssignment.size());
        for (InternalNode node : consumerAssignment.keySet()) {
            List<Split> splitList = new ArrayList<>();
            List<Split> splitList2 = new ArrayList<>();
            boolean b = producerAssignment.containsEntry(node, consumerAssignment.get(node));
            Collection<Split> producerSplits = producerAssignment.get(node);
            Collection<Split> consumerSplits = producerAssignment.get(node);
            producerSplits.forEach(s -> splitList.add(s));
            List<Split> splitList1 = splitList.get(0).getSplits();
            consumerSplits.forEach(s -> splitList2.add(s));
            int i = 0;
            for (Split split3 : splitList1) {
                SplitKey splitKey1 = new SplitKey(split3, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);
                SplitKey splitKey2 = new SplitKey(splitList1.get(i), TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);
                boolean f = splitKey1.equals(splitKey2);
                assertEquals(true, f);
                i++;
            }
        }
    }

    @Test
    public void testRuseExchangeSplitsGroupNotMatchingBetweenProducerConsumer()
    {
        setUpNodes();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(new FinalizerService());
        StageId stageId = new StageId(new QueryId("query"), 0);
        UUID uuid = UUID.randomUUID();

        PlanFragment testFragmentProducer = createTableScanPlanFragment("build", ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_PRODUCER, uuid, 1);

        PlanNodeId tableScanNodeId = new PlanNodeId("plan_id");
        StageExecutionPlan producerStageExecutionPlan = new StageExecutionPlan(
                testFragmentProducer,
                ImmutableMap.of(tableScanNodeId, new ConnectorAwareSplitSource(CONNECTOR_ID, createFixedSplitSource(0, TestingSplit::createRemoteSplit))),
                ImmutableList.of(),
                ImmutableMap.of(tableScanNodeId, new TableInfo(new QualifiedObjectName("test", TEST_SCHEMA, "test"), TupleDomain.all())));

        SqlStageExecution producerStage = createSqlStageExecution(
                stageId,
                new TestSqlTaskManager.MockLocationFactory().createStageLocation(stageId),
                producerStageExecutionPlan.getFragment(),
                producerStageExecutionPlan.getTables(),
                new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor),
                TEST_SESSION_REUSE,
                true,
                nodeTaskMap,
                remoteTaskExecutor,
                new NoOpFailureDetector(),
                new SplitSchedulerStats(),
                new DynamicFilterService(new LocalStateStoreProvider(
                        new SeedStoreManager(new FileSystemClientManager()))),
                new QuerySnapshotManager(stageId.getQueryId(), NOOP_SNAPSHOT_UTILS, TEST_SESSION));

        Set<Split> producerSplits = createAndGetSplits(10);
        Multimap<InternalNode, Split> producerAssignment = nodeSelector.computeAssignments(producerSplits, ImmutableList.copyOf(taskMap.values()), Optional.of(producerStage)).getAssignments();
        PlanFragment testFragmentConsumer = createTableScanPlanFragment("build", ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_CONSUMER, uuid, 1);
        StageExecutionPlan consumerStageExecutionPlan = new StageExecutionPlan(
                testFragmentConsumer,
                ImmutableMap.of(tableScanNodeId, new ConnectorAwareSplitSource(CONNECTOR_ID, createFixedSplitSource(0, TestingSplit::createRemoteSplit))),
                ImmutableList.of(),
                ImmutableMap.of(tableScanNodeId, new TableInfo(new QualifiedObjectName("test", TEST_SCHEMA, "test"), TupleDomain.all())));

        SqlStageExecution stage = createSqlStageExecution(
                stageId,
                new TestSqlTaskManager.MockLocationFactory().createStageLocation(stageId),
                consumerStageExecutionPlan.getFragment(),
                consumerStageExecutionPlan.getTables(),
                new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor),
                TEST_SESSION_REUSE,
                true,
                nodeTaskMap,
                remoteTaskExecutor,
                new NoOpFailureDetector(),
                new SplitSchedulerStats(),
                new DynamicFilterService(new LocalStateStoreProvider(
                        new SeedStoreManager(new FileSystemClientManager()))),
                new QuerySnapshotManager(stageId.getQueryId(), NOOP_SNAPSHOT_UTILS, TEST_SESSION));
        Set<Split> consumerSplits = createAndGetSplits(50);

        try {
            Multimap<InternalNode, Split> consumerAssignment = nodeSelector.computeAssignments(consumerSplits, ImmutableList.copyOf(taskMap.values()), Optional.of(stage)).getAssignments();
        }
        catch (PrestoException e) {
            assertEquals("Producer & consumer splits are not same", e.getMessage());
            return;
        }
        assertEquals(false, true);
    }

    private void initSortBasedAggregation()
    {
        synchronized (TestHiveIntegrationSmokeTest.this) {
            if (null == testSessionSort) {
                this.testSessionSort = Session.builder(getSession())
                        .setSystemProperty("sort_based_aggregation_enabled", "true")
                        .build();

                this.testSessionSortPrcntDrv50 = Session.builder(getSession())
                        .setSystemProperty("sort_based_aggregation_enabled", "true")
                        .setSystemProperty("prcnt_drivers_for_partial_aggr", "33")
                        .build();

                this.testSessionSortPrcntDrv25 = Session.builder(getSession())
                        .setSystemProperty("sort_based_aggregation_enabled", "true")
                        .setSystemProperty("prcnt_drivers_for_partial_aggr", "25")
                        .build();

                this.testSessionSortPrcntDrv40 = Session.builder(getSession())
                        .setSystemProperty("sort_based_aggregation_enabled", "true")
                        .setSystemProperty("prcnt_drivers_for_partial_aggr", "25")
                        .build();
            }
        }
    }

    @Test
    public void sortAggSingleSort()
    {
        initSortBasedAggregation();
        assertUpdate("drop table if exists unsorttable");
        assertUpdate("drop table if exists sorttable");
        computeActual("create table unsorttable (orderkey int, year int) WITH (transactional = true , " +
                "format = 'ORC', partitioned_by = ARRAY[ 'year' ] )");
        assertUpdate("insert into unsorttable values (1,2011)", 1);
        assertUpdate("insert into unsorttable values (2,2012)", 1);
        assertUpdate("insert into unsorttable values (2,2012)", 1);
        assertUpdate("insert into unsorttable values (2,2012)", 1);
        assertUpdate("insert into unsorttable values (3,2013)", 1);
        assertUpdate("insert into unsorttable values (3,2013)", 1);
        assertUpdate("insert into unsorttable values (3,2014)", 1);

        computeActual("create table sorttable  with(transactional = false, " +
                "format = 'ORC',  bucketed_by=array['year'], bucket_count=1, sorted_by = ARRAY['year'])  as select * from unsorttable order by year");

        MaterializedResult sortResult = computeActual(testSessionSort, "select avg(orderkey), count(year)," +
                "year from sorttable  group by year order by year");

        MaterializedResult hashResult = computeActual("select avg(orderkey), count(year)," +
                "year from sorttable  group by year order by year");
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        sortResult = computeActual(testSessionSortPrcntDrv50, "select avg(orderkey), count(year), year from sorttable  group by year order by year");
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        assertUpdate("DROP TABLE sorttable");
        assertUpdate("DROP TABLE unsorttable");
    }

    @Test
    public void sortAggSingleSortNoAggregation()
    {
        initSortBasedAggregation();
        assertUpdate("drop table if exists unsorttable1");
        assertUpdate("drop table if exists sorttable1");
        computeActual("create table unsorttable1 (orderkey int, year bigint) WITH (transactional = true , " +
                "format = 'ORC', partitioned_by = ARRAY[ 'year'] )");
        assertUpdate("insert into unsorttable1 values (1,2011)", 1);
        assertUpdate("insert into unsorttable1 values (2,2012)", 1);
        assertUpdate("insert into unsorttable1 values (2,2012)", 1);
        assertUpdate("insert into unsorttable1 values (2,2012)", 1);
        assertUpdate("insert into unsorttable1 values (3,2013)", 1);
        assertUpdate("insert into unsorttable1 values (3,2013)", 1);
        assertUpdate("insert into unsorttable1 values (3,2014)", 1);

        computeActual("create table sorttable1  with(transactional = false, " +
                "format = 'ORC',  bucketed_by=array['year'], bucket_count=1, sorted_by = ARRAY['year'])  as select * from unsorttable1 order by year");

        MaterializedResult sortResult = computeActual(testSessionSort, "select year from sorttable1 group by year order by year");
        MaterializedResult hashResult = computeActual("select year from sorttable1 group by year order by year");
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        sortResult = computeActual(testSessionSortPrcntDrv50, "select year from sorttable1 group by year order by year");
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        assertUpdate("DROP TABLE sorttable1");
        assertUpdate("DROP TABLE unsorttable1");
    }

    @Test
    public void sortAggBigint()
    {
        initSortBasedAggregation();
        assertUpdate("drop table if exists unsorttable2");
        assertUpdate("drop table if exists sorttable2");
        computeActual("create table unsorttable2 (orderkey int, year bigint) WITH (transactional = true , " +
                "format = 'ORC', partitioned_by = ARRAY[ 'year'] )");
        assertUpdate("insert into unsorttable2 values (1,null)", 1);
        assertUpdate("insert into unsorttable2 values (2,null)", 1);
        assertUpdate("insert into unsorttable2 values (2,2012)", 1);
        assertUpdate("insert into unsorttable2 values (2,2012)", 1);
        assertUpdate("insert into unsorttable2 values (3,2013)", 1);
        assertUpdate("insert into unsorttable2 values (3,2013)", 1);
        assertUpdate("insert into unsorttable2 values (3,2014)", 1);

        computeActual("create table sorttable2  with(transactional = false, " +
                "format = 'ORC',  bucketed_by=array['year'], bucket_count=10, sorted_by = ARRAY['year'])  as select * from unsorttable2 order by year");

        assertUpdate("set session sort_based_aggregation_enabled=true");
        MaterializedResult sortResult = computeActual(testSessionSort, "select avg(orderkey), count(year), year from sorttable2 group by year order by year");
        MaterializedResult hashResult = computeActual("select avg(orderkey), count(year), year from sorttable2 group by year order by year");
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        sortResult = computeActual(testSessionSortPrcntDrv50, "select avg(orderkey), count(year), year from sorttable2 group by year order by year");
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        assertUpdate("DROP TABLE sorttable2");
        assertUpdate("DROP TABLE unsorttable2");
    }

    @Test
    public void sortAggMultipleSort()
    {
        initSortBasedAggregation();
        assertUpdate("drop table if exists unsorttable3");
        assertUpdate("drop table if exists sorttable3");
        computeActual("create table unsorttable3 (number int, orderkey double, year double) WITH (transactional = true , " +
                "format = 'ORC')");
        assertUpdate("insert into unsorttable3 values (1,11,2011)", 1);
        assertUpdate("insert into unsorttable3 values (2,22,2012)", 1);
        assertUpdate("insert into unsorttable3 values (3,33,2012)", 1);
        assertUpdate("insert into unsorttable3 values (4,33,2012)", 1);
        assertUpdate("insert into unsorttable3 values (4,44,2012)", 1);
        assertUpdate("insert into unsorttable3 values (5,55,2013)", 1);
        assertUpdate("insert into unsorttable3 values (6,66,2013)", 1);
        assertUpdate("insert into unsorttable3 values (7,77,2014)", 1);

        computeActual("create table sorttable3  with(transactional = false, " +
                "format = 'ORC',  bucketed_by=array['orderkey', 'year'], bucket_count=2, sorted_by = ARRAY['orderkey', 'year'])  as select * from unsorttable3 order by orderkey,year");

        MaterializedResult sortResult = computeActual(testSessionSort, "select avg(orderkey), count(year)," +
                "year from sorttable3  group by orderkey,year order by orderkey,year");

        MaterializedResult hashResult = computeActual("select avg(orderkey), count(year)," +
                "year from sorttable3  group by orderkey,year order by orderkey,year");

        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        sortResult = computeActual(testSessionSort, "select avg(orderkey), count(year)," +
                "year from sorttable3 group by year order by year");

        hashResult = computeActual("select avg(orderkey), count(year)," +
                "year from sorttable3 group by year order by year");

        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        sortResult = computeActual(testSessionSortPrcntDrv50, "select avg(orderkey), count(year), year from sorttable3 group by year order by year");
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        assertUpdate("DROP TABLE sorttable3");
        assertUpdate("DROP TABLE unsorttable3");
    }

    @Test
    public void sortAggDateType()
    {
        initSortBasedAggregation();
        assertUpdate("drop table if exists unsorttable4");
        assertUpdate("drop table if exists sorttable4");
        computeActual("create table unsorttable4 (number int, orderkey decimal(10,4), year date) WITH (transactional = true , " +
                "format = 'ORC')");
        assertUpdate("insert into unsorttable4 values (1,11.1,date '2011-07-20')", 1);
        assertUpdate("insert into unsorttable4 values (2,22.2,date '2012-07-20')", 1);
        assertUpdate("insert into unsorttable4 values (3,33.3,date '2013-07-20')", 1);
        assertUpdate("insert into unsorttable4 values (4,33.3,date '2013-07-20')", 1);
        assertUpdate("insert into unsorttable4 values (5,55.5,date '2013-07-20')", 1);
        assertUpdate("insert into unsorttable4 values (6,66.6,date '2014-07-20')", 1);
        assertUpdate("insert into unsorttable4 values (7,77.7,date '2015-07-20')", 1);

        computeActual("create table sorttable4  with(transactional = false, " +
                "format = 'ORC',  bucketed_by=array['year'], bucket_count=2, sorted_by = ARRAY['year'])  as select * from unsorttable4 order by year");
        String query = "select avg(orderkey), count(year), year from sorttable4  group by year order by year";
        MaterializedResult sortResult = computeActual(testSessionSort, query);
        MaterializedResult hashResult = computeActual(query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv50, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv40, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        assertUpdate("DROP TABLE sorttable4");
        assertUpdate("DROP TABLE unsorttable4");
    }

    @Test
    public void sortAggVarchar()
    {
        initSortBasedAggregation();
        assertUpdate("drop table if exists unsorttable5");
        assertUpdate("drop table if exists sorttable5");
        computeActual("create table unsorttable5 (number int, orderkey decimal(10,4), year varchar) WITH (transactional = true , " +
                "format = 'ORC')");
        assertUpdate("insert into unsorttable5 values (1,11.1, '2011-07-20')", 1);
        assertUpdate("insert into unsorttable5 values (2,22.2, '2012-07-20')", 1);
        assertUpdate("insert into unsorttable5 values (3,33.3, '2013-07-20')", 1);
        assertUpdate("insert into unsorttable5 values (4,33.3, '2013-07-20')", 1);
        assertUpdate("insert into unsorttable5 values (5,55.5, '2013-07-20')", 1);
        assertUpdate("insert into unsorttable5 values (6,66.6, '2014-07-20')", 1);
        assertUpdate("insert into unsorttable5 values (7,77.7, '2015-07-20')", 1);

        computeActual("create table sorttable5  with(transactional = false, " +
                "format = 'ORC',  bucketed_by=array['year'], bucket_count=2, sorted_by = ARRAY['year'])  as select * from unsorttable5 order by year");

        MaterializedResult sortResult = computeActual(testSessionSort, "select avg(orderkey), count(year)," +
                "year from sorttable5  group by year order by year");

        MaterializedResult hashResult = computeActual("select avg(orderkey), count(year)," +
                "year from sorttable5  group by year order by year");

        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        sortResult = computeActual(testSessionSortPrcntDrv50, "select avg(orderkey), count(year), year from sorttable5  group by year order by year");
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        assertUpdate("DROP TABLE sorttable5");
        assertUpdate("DROP TABLE unsorttable5");
    }

    @Test
    public void sortAggSmallint()
    {
        initSortBasedAggregation();
        assertUpdate("drop table if exists unsorttable6");
        assertUpdate("drop table if exists sorttable6");
        computeActual("create table unsorttable6 (orderkey int, year smallint) WITH (transactional = true , " +
                "format = 'ORC', partitioned_by = ARRAY[ 'year' ] )");
        assertUpdate("insert into unsorttable6 values (1,smallint '2011')", 1);
        assertUpdate("insert into unsorttable6 values (2,smallint '2012')", 1);
        assertUpdate("insert into unsorttable6 values (2,smallint '2012')", 1);
        assertUpdate("insert into unsorttable6 values (2,smallint '2012')", 1);
        assertUpdate("insert into unsorttable6 values (3,smallint '2013')", 1);
        assertUpdate("insert into unsorttable6 values (3,smallint '2014')", 1);
        assertUpdate("insert into unsorttable6 values (3,smallint '2015')", 1);

        computeActual("create table sorttable6  with(transactional = false, " +
                "format = 'ORC',  bucketed_by=array['year'], bucket_count=1, sorted_by = ARRAY['year'])  as select * from unsorttable6 order by year");

        MaterializedResult sortResult = computeActual(testSessionSort, "select avg(orderkey), count(year)," +
                "year from sorttable6  group by year order by year");

        MaterializedResult hashResult = computeActual("select avg(orderkey), count(year)," +
                "year from sorttable6  group by year order by year");

        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        sortResult = computeActual(testSessionSortPrcntDrv50, "select avg(orderkey), count(year), year from sorttable6  group by year order by year");
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        assertUpdate("DROP TABLE sorttable6");
        assertUpdate("DROP TABLE unsorttable6");
    }

    @Test
    public void sortAggBoolean()
    {
        initSortBasedAggregation();
        assertUpdate("drop table if exists unsorttable7");
        assertUpdate("drop table if exists sorttable7");
        computeActual("create table unsorttable7 (orderkey int, year int, iscurrentemployee boolean ) WITH (transactional = true , " +
                "format = 'ORC')");
        assertUpdate("insert into unsorttable7 values (1,2011, true)", 1);
        assertUpdate("insert into unsorttable7 values (2,2012, true)", 1);
        assertUpdate("insert into unsorttable7 values (2,2012, true)", 1);
        assertUpdate("insert into unsorttable7 values (2,2012, false)", 1);
        assertUpdate("insert into unsorttable7 values (3,2013, false)", 1);
        assertUpdate("insert into unsorttable7 values (3,2013, true)", 1);
        assertUpdate("insert into unsorttable7 values (3,2014, false)", 1);

        computeActual("create table sorttable7  with(transactional = false, " +
                "format = 'ORC',  bucketed_by=array['iscurrentemployee'], bucket_count=1, sorted_by = ARRAY['iscurrentemployee'])" +
                "  as select * from unsorttable7 order by iscurrentemployee");

        MaterializedResult sortResult = computeActual(testSessionSort, "select avg(orderkey), count(year)," +
                "iscurrentemployee from sorttable7  group by iscurrentemployee order by iscurrentemployee");

        MaterializedResult hashResult = computeActual("select avg(orderkey), count(year)," +
                "iscurrentemployee from sorttable7  group by iscurrentemployee order by iscurrentemployee");

        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        sortResult = computeActual(testSessionSortPrcntDrv50, "select avg(orderkey), count(year), iscurrentemployee from sorttable7  group by iscurrentemployee order by iscurrentemployee");
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        assertUpdate("DROP TABLE sorttable7");
        assertUpdate("DROP TABLE unsorttable7");
    }

    @Test
    public void sortAggSplitWithMultiplePagesBigint()
    {
        initSortBasedAggregation();
        // this Test case we will insert many rows , so that single split will yield many pages, groub & sort by bigint

        assertUpdate("drop table if exists unsorttable8");
        assertUpdate("drop table if exists sorttable8");
        computeActual("create table unsorttable8 (orderkey int, year bigint) WITH (transactional = false , " +
                "format = 'ORC')");

        String str = generateNumberOfRowsForTwoColumns(2500, 10);

        assertUpdate("insert into unsorttable8 values " + str, 2510);

        computeActual("create table sorttable8  with(transactional = false, " +
                "format = 'ORC',  bucketed_by=array['year'], bucket_count=1, sorted_by = ARRAY['year'])  as select * from unsorttable8 order by year");

        MaterializedResult sortResult = computeActual(testSessionSort, "select avg(orderkey), count(year)," +
                "year from sorttable8  group by year order by year");

        MaterializedResult hashResult = computeActual("select avg(orderkey), count(year)," +
                "year from sorttable8  group by year order by year");

        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        sortResult = computeActual(testSessionSortPrcntDrv50, "select avg(orderkey), count(year)," +
                "year from sorttable8  group by year order by year");
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        sortResult = computeActual(testSessionSortPrcntDrv25, "select avg(orderkey), count(year)," +
                "year from sorttable8  group by year order by year");
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        assertUpdate("DROP TABLE sorttable8");
        assertUpdate("DROP TABLE unsorttable8");
    }

    @Test
    public void sortAggSplitWithMultiplePagesBigintMultiSort()
    {
        // this Test case we will insert many rows , so that single split will yield many pages, groub & sort by bigint
        assertUpdate("drop table if exists unsorttable9");
        assertUpdate("drop table if exists sorttable9");
        computeActual("create table unsorttable9 (code int, orderkey int, year bigint) WITH (transactional = false , " +
                "format = 'ORC')");

        String str = generateNumberOfRowsForThreeColumns(2500);

        assertUpdate("insert into unsorttable9 values " + str, 5000);

        computeActual("create table sorttable9  with(transactional = false, " +
                "format = 'ORC',  bucketed_by=array['year'], bucket_count=10, sorted_by = ARRAY['year', 'orderkey'])  as select * from unsorttable9");

        MaterializedResult sortResult = computeActual(testSessionSort, "select avg(orderkey), count(year)," +
                "year from sorttable9  group by year order by year");

        MaterializedResult hashResult = computeActual("select avg(orderkey), count(year)," +
                "year from sorttable9  group by year order by year");

        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        sortResult = computeActual(testSessionSortPrcntDrv50, "select avg(orderkey), count(year), year from sorttable9  group by year order by year");
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        sortResult = computeActual(testSessionSortPrcntDrv25, "select avg(orderkey), count(year), year from sorttable9  group by year order by year");
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        assertUpdate("DROP TABLE sorttable9");
        assertUpdate("DROP TABLE unsorttable9");
    }

    @Test
    public void sortAggSplitWithMultiplePagesInt()
    {
        initSortBasedAggregation();
        // this Test case we will insert many rows , so that single split will yield many pages, groub & sort by int
        assertUpdate("drop table if exists unsorttable10");
        assertUpdate("drop table if exists sorttable10");
        computeActual("create table unsorttable10 (orderkey int, year int) WITH (transactional = false , " +
                "format = 'ORC')");

        String str = generateNumberOfRowsForTwoColumns(2500, 5);

        assertUpdate("insert into unsorttable10 values " + str, 2505);

        computeActual("create table sorttable10  with(transactional = false, " +
                "format = 'ORC',  bucketed_by=array['year'], bucket_count=1, sorted_by = ARRAY['year'])  as select * from unsorttable10 order by year");

        MaterializedResult sortResult = computeActual(testSessionSort, "select count(orderkey), count(year)," +
                "year from sorttable10  group by year order by year");

        MaterializedResult hashResult = computeActual("select count(orderkey), count(year), " +
                "year from sorttable10  group by year order by year");

        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        sortResult = computeActual(testSessionSortPrcntDrv25, "select count(orderkey), count(year)," +
            "year from sorttable10  group by year order by year");
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        sortResult = computeActual(testSessionSortPrcntDrv50, "select count(orderkey), count(year)," +
                "year from sorttable10  group by year order by year");
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        assertUpdate("DROP TABLE sorttable10");
        assertUpdate("DROP TABLE unsorttable10");
    }

    @Test
    public void sortAggNullAndZero()
    {
        initSortBasedAggregation();
        assertUpdate("drop table if exists unsorttable11");
        assertUpdate("drop table if exists sorttable11");
        computeActual("create table unsorttable11 (number int, orderkey int, year int) WITH (transactional = true , " +
                "format = 'ORC')");
        assertUpdate("insert into unsorttable11 values (1, null, null)", 1);
        assertUpdate("insert into unsorttable11 values (1, null, null)", 1);
        assertUpdate("insert into unsorttable11 values (2, 0, null)", 1);
        assertUpdate("insert into unsorttable11 values (2, 0, null)", 1);
        assertUpdate("insert into unsorttable11 values (4, null, 0)", 1);
        assertUpdate("insert into unsorttable11 values (5, null, 0)", 1);
        assertUpdate("insert into unsorttable11 values (6, 0, 33)", 1);
        assertUpdate("insert into unsorttable11 values (7, 0, 33)", 1);
        assertUpdate("insert into unsorttable11 values (8, 33, 0)", 1);
        assertUpdate("insert into unsorttable11 values (9, 33, 0)", 1);
        assertUpdate("insert into unsorttable11 values (10, 33, null)", 1);
        assertUpdate("insert into unsorttable11 values (11, 33, null)", 1);
        assertUpdate("insert into unsorttable11 values (12, 33, null)", 1);
        assertUpdate("insert into unsorttable11 values (13, null, 33)", 1);
        assertUpdate("insert into unsorttable11 values (13, null, 33)", 1);
        assertUpdate("insert into unsorttable11 values (12, null, 33)", 1);
        assertUpdate("insert into unsorttable11 values (14, 33, 66)", 1);
        assertUpdate("insert into unsorttable11 values (15, 55, 77 )", 1);
        assertUpdate("insert into unsorttable11 values (16, 66, 88)", 1);
        assertUpdate("insert into unsorttable11 values (17, 77, 99)", 1);

        computeActual("create table sorttable11  with(transactional = false, " +
                "format = 'ORC',  bucketed_by=array['year', 'orderkey'], bucket_count=1, sorted_by = ARRAY['year', 'orderkey'])  as select * from unsorttable11 order by year");
        String query = "select sum (number), avg(orderkey  ), count(year)," +
                "year from sorttable11  group by year order by year";

        MaterializedResult sortResult = computeActual(testSessionSort, query);
        MaterializedResult hashResult = computeActual(query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv50, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        query = "select sum (number), avg(CASE WHEN orderkey IS NULL THEN 0 ELSE orderkey END), count(CASE WHEN year IS NULL THEN 0 ELSE year END)," +
                "year from sorttable11  group by year, orderkey order by year, orderkey";

        sortResult = computeActual(testSessionSort, query);
        hashResult = computeActual(query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv50, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        assertUpdate("DROP TABLE sorttable11");
        assertUpdate("DROP TABLE unsorttable11");
    }

    private String generateNumberOfRowsForTwoColumns(int numberOfRows, int numberOfNullRows)
    {
        String str = "";
        String str1;
        for (int i = 0; i < numberOfRows; i++) {
            str1 = " ( " + (i + 200) + " , " + i + " ) ";
            str = str.concat(str1);
            if (i != numberOfRows - 1) {
                str = str.concat(",");
            }
        }

        if (numberOfNullRows != 0) {
            str = str.concat(",");
        }
        for (int i = 0; i < numberOfNullRows; i++) {
            str1 = " ( " + (i + 200) + " , null  ) ";
            str = str.concat(str1);
            if (i != numberOfNullRows - 1) {
                str = str.concat(",");
            }
        }
        return str;
    }

    private String generateNumberOfRowsForThreeColumns(int numberOfRows)
    {
        String str = "";
        String str1;
        for (int i = 0; i < numberOfRows; i++) {
            //str1 = " ( " + (i + 200) + " , " + i + " ) ";
            str1 = " ( " + (i + 300) + " , " + (i + 200) + " , " + i + " ), ";
            str = str.concat(str1);
            str1 = " ( " + (i + 600) + " , " + (i + 500) + " , " + i + " ) ";
            str = str.concat(str1);
            if (i != numberOfRows - 1) {
                str = str.concat(",");
            }
        }
        return str;
    }

    private Set<Split> createAndGetSplits(long start)
    {
        HiveConfig config = new HiveConfig();
        config.setHiveStorageFormat(HiveStorageFormat.ORC);
        config.setHiveCompressionCodec(NONE);
        Properties splitProperties = new Properties();
        splitProperties.setProperty(FILE_INPUT_FORMAT, config.getHiveStorageFormat().getInputFormat());
        splitProperties.setProperty(SERIALIZATION_LIB, config.getHiveStorageFormat().getSerDe());
        splitProperties.setProperty("columns", Joiner.on(',').join(TestHivePageSink.getColumnHandles().stream().map(HiveColumnHandle::getName).collect(toList())));
        splitProperties.setProperty("columns.types", Joiner.on(',').join(TestHivePageSink.getColumnHandles().stream().map(HiveColumnHandle::getHiveType).map(hiveType -> hiveType.getHiveTypeName().toString()).collect(toList())));
        List<ConnectorSplit> connectorSplits1 = new ArrayList<>();

        for (long j = start; j < start + 30; j += 10) {
            List<HiveSplit> hiveSplitList = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                HiveSplit hiveSplit = new HiveSplit(
                        TEST_SCHEMA,
                        TEST_TABLE,
                        "",
                        "file:///",
                        i + j,
                        100 + i + j,
                        100 + i + j,
                        0,
                        splitProperties,
                        ImmutableList.of(),
                        ImmutableList.of(),
                        OptionalInt.empty(),
                        false,
                        ImmutableMap.of(),
                        Optional.empty(),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        ImmutableMap.of());
                hiveSplitList.add(hiveSplit);
            }

            HiveSplitWrapper split2 = HiveSplitWrapper.wrap(hiveSplitList, OptionalInt.empty());
            connectorSplits1.add(split2);
        }

        ImmutableList.Builder<Split> result = ImmutableList.builder();
        for (ConnectorSplit connectorSplit : connectorSplits1) {
            result.add(new Split(CONNECTOR_ID, connectorSplit, Lifespan.taskWide()));
        }
        List<Split> splitList = result.build();
        Set<Split> set = splitList.stream().collect(Collectors.toSet());
        return set;
    }

    @Test
    public void sortAggBasicAggreTestOnTpch()
    {
        initSortBasedAggregation();
        assertUpdate("drop table if exists sortLineitem");
        assertUpdate("drop table if exists orders_orderkey_totalprice");

        computeActual("create table sortLineitem  with(transactional = false, " +
                "format = 'ORC',  bucketed_by=array['orderkey', 'partkey'], bucket_count=4, sorted_by = ARRAY['orderkey', 'partkey'])" +
                "  as select * from tpch.tiny.lineitem");

        String query = "select count(partkey), count(orderkey), orderkey from sortLineitem  group by orderkey, partkey order by orderkey, partkey";
        MaterializedResult sortResult = computeActual(testSessionSort, query);

        MaterializedResult hashResult = computeActual(query);
        assertEquals(sortResult.toString(), hashResult.toString());

        computeActual("create table orders_orderkey_totalprice  with(transactional = false, " +
                "format = 'ORC',  bucketed_by=array['orderkey', 'totalprice'], bucket_count=4, sorted_by = ARRAY['orderkey', 'totalprice'])" +
                "  as select * from tpch.tiny.orders");

        sortResult = computeActual(testSessionSortPrcntDrv50, "select count(totalprice), count(orderkey)," +
                "orderkey from orders_orderkey_totalprice  group by orderkey, totalprice order by orderkey, totalprice");

        hashResult = computeActual("select count(totalprice), count(orderkey)," +
                "orderkey from orders_orderkey_totalprice group by orderkey, totalprice order by orderkey, totalprice");
        assertEquals(sortResult.toString(), hashResult.toString());

        assertUpdate("DROP TABLE sortLineitem");
        assertUpdate("DROP TABLE orders_orderkey_totalprice");
    }

    @Test
    public void sortAggInnerJoin()
    {
        initSortBasedAggregation();
        assertUpdate("drop table if exists lineitemSortBy_orderkey_inner");
        assertUpdate("drop table if exists ordersSortBy_orderkey_inner");
        computeActual("create table lineitemSortBy_orderkey_inner  with(transactional = false, " +
                "format = 'ORC',  bucketed_by=array['orderkey'], bucket_count=1, sorted_by = ARRAY['orderkey'])" +
                "  as select * from tpch.tiny.lineitem");

        computeActual("create table ordersSortBy_orderkey_inner  with(transactional = false, " +
                "format = 'ORC',  bucketed_by=array['orderkey'], bucket_count=1, sorted_by = ARRAY['orderkey'])" +
                "  as select * from tpch.tiny.orders");
        assertUpdate("set session sort_based_aggregation_enabled=true");
        String query = "select avg(lineitemSortBy_orderkey_inner.orderkey),lineitemSortBy_orderkey_inner.orderkey from lineitemSortBy_orderkey_inner " +
                "INNER JOIN ordersSortBy_orderkey_inner ON lineitemSortBy_orderkey_inner.orderkey = ordersSortBy_orderkey_inner.orderkey " +
                "group by lineitemSortBy_orderkey_inner.orderkey " +
                "order by lineitemSortBy_orderkey_inner.orderkey";

        MaterializedResult sortResult = computeActual(testSessionSort, query);
        MaterializedResult hashResult = computeActual(query);
        assertEquals(sortResult.toString(), hashResult.toString());
        sortResult = computeActual(testSessionSortPrcntDrv50, query);
        assertEquals(sortResult.toString(), hashResult.toString());
        sortResult = computeActual(testSessionSortPrcntDrv25, query);
        assertEquals(sortResult.toString(), hashResult.toString());

        assertUpdate("DROP TABLE lineitemSortBy_orderkey_inner");
        assertUpdate("DROP TABLE ordersSortBy_orderkey_inner");
    }

    @Test
    public void sortAggLeftJoin()
    {
        initSortBasedAggregation();
        assertUpdate("drop table if exists lineitemSortBy_orderkey_left");
        assertUpdate("drop table if exists ordersSortBy_orderkey_left");
        computeActual("create table lineitemSortBy_orderkey_left  with(transactional = false, " +
                "format = 'ORC',  bucketed_by=array['orderkey'], bucket_count=1, sorted_by = ARRAY['orderkey'])" +
                "  as select * from tpch.tiny.lineitem");

        computeActual("create table ordersSortBy_orderkey_left  with(transactional = false, " +
                "format = 'ORC',  bucketed_by=array['orderkey'], bucket_count=1, sorted_by = ARRAY['orderkey'])" +
                "  as select * from tpch.tiny.orders");
        assertUpdate("set session sort_based_aggregation_enabled=true");
        String query = "select avg(lineitemSortBy_orderkey_left.orderkey),lineitemSortBy_orderkey_left.orderkey from lineitemSortBy_orderkey_left " +
                "LEFT JOIN " +
                "ordersSortBy_orderkey_left ON lineitemSortBy_orderkey_left.orderkey = ordersSortBy_orderkey_left.orderkey " +
                "group by lineitemSortBy_orderkey_left.orderkey " +
                "order by lineitemSortBy_orderkey_left.orderkey";

        MaterializedResult sortResult = computeActual(testSessionSort, query);
        MaterializedResult hashResult = computeActual(query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv50, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv25, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv40, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        assertUpdate("DROP TABLE lineitemSortBy_orderkey_left");
        assertUpdate("DROP TABLE ordersSortBy_orderkey_left");
    }

    @Test
    public void sortAggRightJoin()
    {
        initSortBasedAggregation();
        assertUpdate("drop table if exists lineitemSortBy_orderkey_right");

        computeActual("create table lineitemSortBy_orderkey_right  with(transactional = false, " +
                "format = 'ORC',  bucketed_by=array['orderkey'], bucket_count=1, sorted_by = ARRAY['orderkey'])" +
                "  as select * from tpch.tiny.lineitem");
        assertUpdate("set session sort_based_aggregation_enabled=true");
        String query = "select count(lineitemSortBy_orderkey_right.orderkey), lineitemSortBy_orderkey_right.orderkey from lineitemSortBy_orderkey_right " +
                "RIGHT JOIN " +
                "tpch.tiny.orders ON lineitemSortBy_orderkey_right.orderkey = tpch.tiny.orders.orderkey " +
                "group by " +
                "lineitemSortBy_orderkey_right.orderkey " +
                "order by " +
                "lineitemSortBy_orderkey_right.orderkey";

        MaterializedResult sortResult = computeActual(testSessionSort, query);
        MaterializedResult hashResult = computeActual(query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv50, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv25, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv40, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        assertUpdate("DROP TABLE lineitemSortBy_orderkey_right");
    }

    @Test
    public void sortAggInnerLeftJoin()
    {
        initSortBasedAggregation();
        computeActual("create table lineitem_orderkey_partkey_InnerLeftJoin  with(transactional = false, " +
                "format = 'ORC',  bucketed_by=array['orderkey', 'partkey'], bucket_count=1, sorted_by = ARRAY['orderkey', 'partkey'])" +
                "  as select * from tpch.tiny.lineitem");

        computeActual("create table shortlineitem_InnerLeftJoin  with(transactional = false, format = 'ORC') as select * from tpch.tiny.lineitem limit 10000");
        String query = "select avg(lineitem_orderkey_partkey_InnerLeftJoin.orderkey), lineitem_orderkey_partkey_InnerLeftJoin.orderkey " +
                "from " +
                "lineitem_orderkey_partkey_InnerLeftJoin " +
                "INNER JOIN  " +
                "shortlineitem_InnerLeftJoin ON lineitem_orderkey_partkey_InnerLeftJoin.orderkey = shortlineitem_InnerLeftJoin.orderkey " +
                "Left JOIN " +
                " tpch.tiny.orders ON lineitem_orderkey_partkey_InnerLeftJoin.orderkey = tpch.tiny.orders.orderkey " +
                "group by lineitem_orderkey_partkey_InnerLeftJoin.orderkey, lineitem_orderkey_partkey_InnerLeftJoin.partkey " +
                "order by lineitem_orderkey_partkey_InnerLeftJoin.orderkey, lineitem_orderkey_partkey_InnerLeftJoin.partkey";

        MaterializedResult sortResult = computeActual(testSessionSort, query);
        MaterializedResult hashResult = computeActual(query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv50, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv40, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        assertUpdate("DROP TABLE lineitem_orderkey_partkey_InnerLeftJoin");
        assertUpdate("DROP TABLE shortlineitem_InnerLeftJoin");
    }

    @Test
    public void sortAggInnerRightJoin()
    {
        initSortBasedAggregation();
        computeActual("create table lineitem_orderkey_partkey_innerRight  with(transactional = false, " +
                "format = 'ORC',  bucketed_by=array['orderkey', 'partkey'], bucket_count=4, sorted_by = ARRAY['orderkey', 'partkey'])" +
                "  as select * from tpch.tiny.lineitem");

        computeActual("create table shortlineitem_InnerRightJoin  with(transactional = false, format = 'ORC') as select * from tpch.tiny.lineitem limit 10000");
        String query = "select avg(lineitem_orderkey_partkey_innerRight.orderkey), lineitem_orderkey_partkey_innerRight.orderkey " +
                "from lineitem_orderkey_partkey_innerRight " +
                "INNER JOIN " +
                "shortlineitem_InnerRightJoin ON lineitem_orderkey_partkey_innerRight.orderkey = shortlineitem_InnerRightJoin.orderkey " +
                "RIGHT JOIN " +
                "tpch.tiny.orders ON lineitem_orderkey_partkey_innerRight.orderkey = tpch.tiny.orders.orderkey " +
                "group by " +
                "lineitem_orderkey_partkey_innerRight.orderkey, lineitem_orderkey_partkey_innerRight.partkey " +
                "order by " +
                "lineitem_orderkey_partkey_innerRight.orderkey, lineitem_orderkey_partkey_innerRight.partkey";

        MaterializedResult sortResult = computeActual(testSessionSort, query);
        MaterializedResult hashResult = computeActual(query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv50, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv40, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());

        assertUpdate("DROP TABLE lineitem_orderkey_partkey_innerRight");
        assertUpdate("DROP TABLE shortlineitem_InnerRightJoin");
    }

    @Test
    public void testCachedPlanTableValidation()
    {
        assertUpdate("CREATE TABLE table_plan_cache_001 (id int)");
        assertUpdate("INSERT INTO table_plan_cache_001 VALUES(1)", 1);
        assertUpdate("INSERT INTO table_plan_cache_001 VALUES(2)", 1);
        assertUpdate("INSERT INTO table_plan_cache_001 VALUES(3)", 1);
        MaterializedResult result = computeActual("SELECT * from table_plan_cache_001 where id = 1");
        assertEquals(result.getRowCount(), 1);
        assertUpdate("DROP TABLE table_plan_cache_001");
        assertUpdate("CREATE TABLE table_plan_cache_001 (id int) with (transactional=true)");
        assertUpdate("INSERT INTO table_plan_cache_001 VALUES(1)", 1);
        assertUpdate("INSERT INTO table_plan_cache_001 VALUES(2)", 1);
        assertUpdate("INSERT INTO table_plan_cache_001 VALUES(3)", 1);
        result = computeActual("SELECT * from table_plan_cache_001 where id = 1");
        assertEquals(result.getRowCount(), 1);
        assertUpdate("DROP TABLE table_plan_cache_001");
    }

    @Test
    public void sortAggPartitionBucketCount1()
    {
        initSortBasedAggregation();
        computeActual("create table lineitem_orderkey_partkey_partition  with(transactional = false, " +
                "format = 'ORC', partitioned_by = ARRAY['comment'], bucketed_by=array['orderkey'], bucket_count=1, sorted_by = ARRAY['orderkey', 'partkey'])" +
                "  as select * from tpch.tiny.lineitem limit 100");

        String query = "select avg(lineitem_orderkey_partkey_partition.orderkey), lineitem_orderkey_partkey_partition.orderkey " +
                "from lineitem_orderkey_partkey_partition " +
                "group by " +
                "comment, orderkey, partkey " +
                "order by " +
                "comment, orderkey, partkey ";

        MaterializedResult sortResult = computeActual(testSessionSort, query);
        MaterializedResult hashResult = computeActual(query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv50, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv40, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        assertUpdate("DROP TABLE lineitem_orderkey_partkey_partition");
    }

    @Test
    public void sortAggPartitionBucketCount2()
    {
        initSortBasedAggregation();
        computeActual("create table lineitem_orderkey_partkey_partition2  with(transactional = false, " +
                "format = 'ORC', partitioned_by = ARRAY['comment'], bucketed_by=array['orderkey',  'partkey'], bucket_count=2, sorted_by = ARRAY['orderkey', 'partkey'])" +
                "  as select * from tpch.tiny.lineitem limit 100");

        String query = "select avg(lineitem_orderkey_partkey_partition2.orderkey), lineitem_orderkey_partkey_partition2.orderkey " +
                "from lineitem_orderkey_partkey_partition2 " +
                "group by comment, orderkey, partkey " +
                "order by comment, orderkey, partkey ";

        MaterializedResult sortResult = computeActual(testSessionSort, query);
        MaterializedResult hashResult = computeActual(query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv50, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv40, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        assertUpdate("DROP TABLE lineitem_orderkey_partkey_partition2");
    }

    @Test
    public void sortAggPartitionBucketCount1With2BucketColumns()
    {
        initSortBasedAggregation();
        computeActual("create table lineitem_orderkey_partkey_partition3  with(transactional = false, " +
                "format = 'ORC', partitioned_by = ARRAY['comment'], bucketed_by=array['orderkey',  'partkey'], bucket_count=1, sorted_by = ARRAY['orderkey', 'partkey'])" +
                "  as select * from tpch.tiny.lineitem limit 100");

        String query = "select avg(lineitem_orderkey_partkey_partition3.orderkey), lineitem_orderkey_partkey_partition3.orderkey " +
                "from lineitem_orderkey_partkey_partition3 " +
                "group by comment, orderkey, partkey " +
                "order by comment, orderkey, partkey ";

        MaterializedResult sortResult = computeActual(testSessionSort, query);
        MaterializedResult hashResult = computeActual(query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv50, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv40, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        assertUpdate("DROP TABLE lineitem_orderkey_partkey_partition3");
    }

    @Test
    public void sortAggPartition2BucketCount1With2BucketColumns()
    {
        initSortBasedAggregation();
        computeActual("create table lineitem_orderkey_partkey_partition4  with(transactional = false, " +
                "format = 'ORC', partitioned_by = ARRAY['shipmode', 'comment'], bucketed_by=array['orderkey',  'partkey'], bucket_count=1, sorted_by = ARRAY['orderkey', 'partkey'])" +
                "  as select * from tpch.tiny.lineitem limit 100");

        String query = "select avg(lineitem_orderkey_partkey_partition4.orderkey), lineitem_orderkey_partkey_partition4.orderkey " +
                "from lineitem_orderkey_partkey_partition4 " +
                "group by shipmode, comment, orderkey, partkey " +
                "order by shipmode, comment, orderkey, partkey ";

        MaterializedResult sortResult = computeActual(testSessionSort, query);
        MaterializedResult hashResult = computeActual(query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv50, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv40, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        assertUpdate("DROP TABLE lineitem_orderkey_partkey_partition4");
    }

    @Test
    public void sortAggSemiJoin()
    {
        initSortBasedAggregation();

        computeActual("create table lineitem_orderkey_partkey_SemiJoin  with(transactional = false, " +
                "format = 'ORC', bucketed_by=array['orderkey', 'partkey'], bucket_count=1, sorted_by = ARRAY['orderkey', 'partkey'])" +
                "  as select * from tpch.tiny.lineitem");

        computeActual("create table lineitem_semiJoin with(transactional = false, format = 'ORC')" +
                "  as select * from tpch.tiny.lineitem where partkey is not null");

        String query = "select avg(lineitem_orderkey_partkey_SemiJoin.orderkey), lineitem_orderkey_partkey_SemiJoin.orderkey " +
                " from lineitem_orderkey_partkey_SemiJoin " +
                " where orderkey " +
                " in (select orderkey from lineitem_semiJoin) " +
                " group by orderkey, partkey " +
                " order by orderkey, partkey ";

        MaterializedResult sortResult = computeActual(testSessionSort, query);
        MaterializedResult hashResult = computeActual(query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv50, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv40, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        assertUpdate("DROP TABLE lineitem_orderkey_partkey_SemiJoin");
        assertUpdate("DROP TABLE lineitem_semiJoin");
    }

    @Test
    public void SortAggreDistinct()
    {
        initSortBasedAggregation();
        computeActual("create table lineitem_orderkey_partkey_Distinct  with(transactional = false, " +
                "format = 'ORC', bucketed_by=array['orderkey', 'partkey'], bucket_count=1, sorted_by = ARRAY['orderkey', 'partkey'])" +
                "  as select * from tpch.tiny.lineitem");

        String query = "select sum(distinct(partkey)), orderkey from lineitem_orderkey_partkey_Distinct " +
                "group by orderkey order by orderkey";

        MaterializedResult sortResult = computeActual(testSessionSort, query);
        MaterializedResult hashResult = computeActual(query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv50, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv40, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        assertUpdate("DROP TABLE lineitem_orderkey_partkey_Distinct");
    }

    @Test
    public void testFailRefreshMetaCache()
    {
        assertQueryFails("REFRESH META CACHE FOR abc", "Catalog does not exist:abc");
        assertQueryFails("REFRESH META CACHE FOR abc.def", "Catalog does not exist:abc.def");
    }

    @Test
    public void testCachedPlanForTablesWithSameName()
    {
        String table = "tab2";
        String schema = "default";
        assertUpdate(String.format("CREATE SCHEMA IF NOT EXISTS %s", schema));
        assertUpdate(String.format("CREATE TABLE %s.%s (a int, b int, c int) with (partitioned_by = ARRAY['c'])", schema, table));
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (1, 1, 1)", schema, table), 1);
        assertQuery(String.format("SELECT * FROM %s.%s", schema, table), "VALUES (1, 1, 1)");
        assertUpdate(String.format("DROP TABLE %s.%s", schema, table));
        assertUpdate(String.format("CREATE TABLE %s.%s (a int, b int, c int)", schema, table));
        assertUpdate(String.format("INSERT INTO %s.%s VALUES (1, 1, 1)", schema, table), 1);
        assertQuery(String.format("SELECT * FROM %s.%s", schema, table), "VALUES (1, 1, 1)");
        assertUpdate(String.format("DROP TABLE %s.%s", schema, table));
    }

    @Test
    public void UnSortAggrePartitionBucketCount1()
    {
        initSortBasedAggregation();
        computeActual("create table lineitem_partition_shipmode_comment_bucket1  with(transactional = false, " +
                "format = 'ORC', partitioned_by = ARRAY['shipmode', 'comment'], bucketed_by=array['orderkey',  'partkey'], bucket_count=1)" +
                "  as select * from tpch.tiny.lineitem limit 100");

        String query = "select lineitem_partition_shipmode_comment_bucket1.shipmode, lineitem_partition_shipmode_comment_bucket1.comment " +
                "from lineitem_partition_shipmode_comment_bucket1 " +
                "group by shipmode, comment " +
                "order by shipmode, comment ";

        MaterializedResult sortResult = computeActual(testSessionSort, query);
        MaterializedResult hashResult = computeActual(query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv50, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv40, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        assertUpdate("DROP TABLE lineitem_partition_shipmode_comment_bucket1");
    }

    @Test
    public void SortAggreGroupOnlyPartitionColumns()
    {
        initSortBasedAggregation();
        computeActual("create table lineitem_sort_partition_shipmode_comment_bucket1  with(transactional = false, " +
                "format = 'ORC', partitioned_by = ARRAY['shipmode', 'comment'], bucketed_by=array['orderkey',  'partkey'], bucket_count=1, sorted_by = ARRAY['orderkey', 'partkey'])" +
                "  as select * from tpch.tiny.lineitem limit 100");

        String query = "select lineitem_sort_partition_shipmode_comment_bucket1.shipmode, lineitem_sort_partition_shipmode_comment_bucket1.comment " +
                "from lineitem_sort_partition_shipmode_comment_bucket1 " +
                "group by shipmode, comment " +
                "order by shipmode, comment ";

        MaterializedResult sortResult = computeActual(testSessionSort, query);
        MaterializedResult hashResult = computeActual(query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv50, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        sortResult = computeActual(testSessionSortPrcntDrv40, query);
        assertEquals(sortResult.getMaterializedRows(), hashResult.getMaterializedRows());
        assertUpdate("DROP TABLE lineitem_sort_partition_shipmode_comment_bucket1");
    }

    @Test
    public void testAcidFormatColumnNameConflict()
    {
        assertUpdate(String.format("CREATE TABLE test_acid_columnname_conflict (originalTransaction int, currentTransaction int," +
                " rowId int, bucket int, row int )"
                + "with (transactional=true, format='orc')"));

        assertUpdate(String.format("INSERT INTO test_acid_columnname_conflict VALUES (1, 2, 3, 4, 5)"), 1);

        assertQuery("SELECT * FROM test_acid_columnname_conflict", "VALUES (1, 2, 3, 4, 5)");

        assertUpdate(String.format("DROP TABLE test_acid_columnname_conflict"));
    }

    @Test
    public void testMultiDelimitFormat()
    {
        assertUpdate(String.format("CREATE TABLE multi_delimit(id int, row int," +
                " class int )"
                + "with (format='MULTIDELIMIT', \"field.delim\"='#')"));
        assertUpdate(String.format("INSERT INTO multi_delimit VALUES (1, 2, 3)"), 1);

        assertQuery("SELECT * FROM multi_delimit", "VALUES (1, 2, 3)");

        assertUpdate(String.format("DROP TABLE multi_delimit"));
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testMultiDelimitFormatWithoutFieldDelim()
    {
        assertUpdate(String.format("CREATE TABLE multi_delimit_without_delim(id int, row int," +
                " class int )"
                + "with (format='MULTIDELIMIT')"));
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testFieldDelimForORCFormat()
    {
        assertUpdate(String.format("CREATE TABLE multi_delimit_without_delim(id int, row int," +
                " class int )"
                + "with (format='ORC', \"field.delim\"='#')"));
    }

    @Test
    public void testShowViews()
    {
        List<String> views = new ArrayList<>(Arrays.asList("v1", "v2", "a1"));
        List<String> viewsStartingWith = new ArrayList<>(Arrays.asList("v1", "v2"));
        List<String> viewsEndingWith = new ArrayList<>(Arrays.asList("v1", "a1"));
        assertUpdate(String.format("CREATE TABLE table1 (id int, row int)"));
        assertUpdate(String.format("INSERT INTO table1 VALUES(1, 10), (2, 20)"), 2);
        assertUpdate(String.format("CREATE TABLE table2 (id int, row int)"));
        assertUpdate(String.format("INSERT INTO table2 VALUES(1, 10), (2, 20)"), 2);
        assertUpdate(String.format("CREATE TABLE table3 (id int, row int)"));
        assertUpdate(String.format("INSERT INTO table3 VALUES(1, 10), (2, 20)"), 2);
        assertUpdate(String.format("CREATE VIEW v1 AS SELECT * FROM table1"));
        assertUpdate(String.format("CREATE VIEW v2 AS SELECT * FROM table2"));
        assertUpdate(String.format("CREATE VIEW a1 AS SELECT * FROM table3"));
        assertTrue(computeActual("SHOW VIEWS").getOnlyColumn().collect(Collectors.toList()).containsAll(views));
        assertTrue(computeActual("SHOW VIEWS LIKE \'v*\'").getOnlyColumn().collect(Collectors.toList()).containsAll(viewsStartingWith));
        assertTrue(computeActual("SHOW VIEWS LIKE \'v%\'").getOnlyColumn().collect(Collectors.toList()).containsAll(viewsStartingWith));
        assertTrue(computeActual("SHOW VIEWS LIKE \'*1\'").getOnlyColumn().collect(Collectors.toList()).containsAll(viewsEndingWith));
        assertTrue(computeActual("SHOW VIEWS LIKE \'%1\'").getOnlyColumn().collect(Collectors.toList()).containsAll(viewsEndingWith));
        assertUpdate(String.format("DROP VIEW v1"));
        assertUpdate(String.format("DROP VIEW v2"));
        assertUpdate(String.format("DROP VIEW a1"));
        assertUpdate(String.format("DROP TABLE table1"));
        assertUpdate(String.format("DROP TABLE table2"));
        assertUpdate(String.format("DROP TABLE table3"));
    }

    @Test
    public void testShowViewsFrom()
    {
        List<String> viewsFromFirstSchema = new ArrayList<>(Arrays.asList("v1", "a1"));
        List<String> viewsFromSecondSchema = new ArrayList<>(Arrays.asList("v2"));
        assertUpdate(String.format("CREATE SCHEMA schema1"));
        assertUpdate(String.format("CREATE SCHEMA schema2"));
        assertUpdate(String.format("CREATE TABLE schema1.table1 (id int, row int)"));
        assertUpdate(String.format("INSERT INTO schema1.table1 VALUES(1, 10), (2, 20)"), 2);
        assertUpdate(String.format("CREATE TABLE schema1.table2 (id int, row int)"));
        assertUpdate(String.format("INSERT INTO schema1.table2 VALUES(1, 10), (2, 20)"), 2);
        assertUpdate(String.format("CREATE TABLE schema2.table3 (id int, row int)"));
        assertUpdate(String.format("INSERT INTO schema2.table3 VALUES(1, 10), (2, 20)"), 2);
        assertUpdate(String.format("CREATE VIEW schema1.v1 AS SELECT * FROM schema1.table1"));
        assertUpdate(String.format("CREATE VIEW schema1.a1 AS SELECT * FROM schema1.table2"));
        assertUpdate(String.format("CREATE VIEW schema2.v2 AS SELECT * FROM schema2.table3"));
        assertTrue(computeActual("SHOW VIEWS FROM schema1").getOnlyColumn().collect(Collectors.toList()).containsAll(viewsFromFirstSchema));
        assertTrue(computeActual("SHOW VIEWS IN schema1").getOnlyColumn().collect(Collectors.toList()).containsAll(viewsFromFirstSchema));
        assertTrue(computeActual("SHOW VIEWS FROM schema2").getOnlyColumn().collect(Collectors.toList()).containsAll(viewsFromSecondSchema));
        assertTrue(computeActual("SHOW VIEWS IN schema2").getOnlyColumn().collect(Collectors.toList()).containsAll(viewsFromSecondSchema));
        assertUpdate(String.format("DROP VIEW schema1.v1"));
        assertUpdate(String.format("DROP VIEW schema2.v2"));
        assertUpdate(String.format("DROP VIEW schema1.a1"));
        assertUpdate(String.format("DROP TABLE schema1.table1"));
        assertUpdate(String.format("DROP TABLE schema1.table2"));
        assertUpdate(String.format("DROP TABLE schema2.table3"));
        assertUpdate(String.format("DROP SCHEMA schema1"));
        assertUpdate(String.format("DROP SCHEMA schema2"));
    }

    @Test
    public void testAlterTableRenameColumn()
    {
        assertUpdate(String.format("CREATE TABLE alter_table_1(id int, name string, age int)"));
        assertUpdate(String.format("CREATE TABLE alter_table_2(id int, name string, age int) with (transactional=true)"));
        assertUpdate(String.format("CREATE TABLE alter_table_3(id int, name string, age int) with (partitioned_by=ARRAY['age'])"));
        assertUpdate(String.format("INSERT INTO alter_table_1 VALUES (1, 'table_1', 10)"), 1);
        assertUpdate(String.format("INSERT INTO alter_table_2 VALUES (1, 'table_2', 10)"), 1);
        assertUpdate(String.format("INSERT INTO alter_table_3 VALUES (1, 'table_3', 10)"), 1);
        assertQuery("SELECT * FROM alter_table_1", "VALUES (1, 'table_1', 10)");
        assertQuery("SELECT * FROM alter_table_2", "VALUES (1, 'table_2', 10)");
        assertQuery("SELECT * FROM alter_table_3", "VALUES (1, 'table_3', 10)");
        assertUpdate(String.format("ALTER TABLE alter_table_1 RENAME COLUMN id to id_new"));
        assertUpdate(String.format("ALTER TABLE alter_table_2 RENAME COLUMN id to id_new"));
        assertUpdate(String.format("ALTER TABLE alter_table_3 RENAME COLUMN id to id_new"));
        assertQuery("SELECT * FROM alter_table_1", "VALUES (1, 'table_1', 10)");
        assertQuery("SELECT * FROM alter_table_2", "VALUES (1, 'table_2', 10)");
        assertQuery("SELECT * FROM alter_table_3", "VALUES (1, 'table_3', 10)");
        assertUpdate(String.format("DROP TABLE alter_table_1"));
        assertUpdate(String.format("DROP TABLE alter_table_2"));
        assertUpdate(String.format("DROP TABLE alter_table_3"));
    }

    @Test
    public void testAlterTableAddColumn()
    {
        assertUpdate(String.format("CREATE TABLE alter_table_4(id int, name string, age int)"));
        assertUpdate(String.format("CREATE TABLE alter_table_5(id int, name string, age int) with (transactional=true)"));
        assertUpdate(String.format("CREATE TABLE alter_table_6(id int, name string, age int) with (partitioned_by=ARRAY['age'])"));
        assertUpdate(String.format("INSERT INTO alter_table_4 VALUES (1, 'table_1', 10)"), 1);
        assertUpdate(String.format("INSERT INTO alter_table_5 VALUES (1, 'table_2', 10)"), 1);
        assertUpdate(String.format("INSERT INTO alter_table_6 VALUES (1, 'table_3', 10)"), 1);
        assertQuery("SELECT * FROM alter_table_4", "VALUES (1, 'table_1', 10)");
        assertQuery("SELECT * FROM alter_table_5", "VALUES (1, 'table_2', 10)");
        assertQuery("SELECT * FROM alter_table_6", "VALUES (1, 'table_3', 10)");
        assertUpdate(String.format("ALTER TABLE alter_table_4 ADD COLUMN new_column int"));
        assertUpdate(String.format("ALTER TABLE alter_table_5 ADD COLUMN new_column int"));
        assertUpdate(String.format("ALTER TABLE alter_table_6 ADD COLUMN new_column int"));
        assertUpdate(String.format("INSERT INTO alter_table_4 VALUES (2, 'table_1', 20, 200)"), 1);
        assertUpdate(String.format("INSERT INTO alter_table_5 VALUES (2, 'table_2', 20, 200)"), 1);
        assertUpdate(String.format("INSERT INTO alter_table_6 VALUES (2, 'table_3', 200, 20), (3, 'table_3', 300, 10)"), 2);
        assertQuery("SELECT * FROM alter_table_4", "VALUES (1, 'table_1', 10, NULL), (2, 'table_1', 20, 200)");
        assertQuery("SELECT * FROM alter_table_5", "VALUES (1, 'table_2', 10, NULL), (2, 'table_2', 20, 200)");
        assertQuery("SELECT * FROM alter_table_6", "VALUES (1, 'table_3', NULL, 10), (2, 'table_3', 200, 20), (3, 'table_3', NULL, 10)");
        assertUpdate(String.format("DROP TABLE alter_table_4"));
        assertUpdate(String.format("DROP TABLE alter_table_5"));
        assertUpdate(String.format("DROP TABLE alter_table_6"));
    }

    @Test
    public void testAlterTableDropColumn()
    {
        assertUpdate(String.format("CREATE TABLE alter_table_7(id int, name int, age int)"));
        assertUpdate(String.format("CREATE TABLE alter_table_8(id int, name int, age int) with (transactional=true)"));
        assertUpdate(String.format("CREATE TABLE alter_table_9(id int, name int, age int) with (partitioned_by=ARRAY['age'])"));
        assertUpdate(String.format("INSERT INTO alter_table_7 VALUES (1, 10, 100)"), 1);
        assertUpdate(String.format("INSERT INTO alter_table_8 VALUES (1, 10, 100)"), 1);
        assertUpdate(String.format("INSERT INTO alter_table_9 VALUES (1, 10, 100)"), 1);
        assertQuery("SELECT * FROM alter_table_7", "VALUES (1, 10, 100)");
        assertQuery("SELECT * FROM alter_table_8", "VALUES (1, 10, 100)");
        assertQuery("SELECT * FROM alter_table_9", "VALUES (1, 10, 100)");
        assertUpdate(String.format("ALTER TABLE alter_table_7 DROP COLUMN id"));
        assertUpdate(String.format("ALTER TABLE alter_table_8 DROP COLUMN id"));
        assertUpdate(String.format("ALTER TABLE alter_table_9 DROP COLUMN id"));
        assertQuery("SELECT * FROM alter_table_7", "VALUES (1, 10)");
        assertQuery("SELECT * FROM alter_table_8", "VALUES (1, 10)");
        assertQuery("SELECT * FROM alter_table_9", "VALUES (1, 100)");
        assertUpdate(String.format("DROP TABLE alter_table_7"));
        assertUpdate(String.format("DROP TABLE alter_table_8"));
        assertUpdate(String.format("DROP TABLE alter_table_9"));
    }

    @Test
    public void testEscapeCharacter()
    {
        testWithAllStorageFormats(this::testEscapeCharacter);
    }

    private void testEscapeCharacter(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = format("test_escape_character_%s", storageFormat);
        assertUpdate(session, format("CREATE TABLE %s (id varchar) WITH (format = '%s')", tableName, storageFormat));
        assertUpdate(session, format("INSERT INTO %s VALUES ('\0')", tableName), 1);

        MaterializedResult result = getQueryRunner().execute(session, format("SELECT * FROM %s", tableName));
        assertEquals(result.getRowCount(), 1);
        MaterializedRow actualRow = result.getMaterializedRows().get(0);
        assertEquals(actualRow.getField(0), String.valueOf('\0'));

        assertUpdate(session, format("DROP TABLE %s", tableName));
    }

    @Test
    public void testReadFromTableWithStructDataTypeColumns()
    {
        assertUpdate("CREATE SCHEMA testReadSchema");
        assertUpdate("CREATE TABLE testReadSchema.testReadStruct1 (orderkey array(varchar), orderstatus row(big int), lables map(varchar, integer))  WITH (format='orc',transactional=true)");
        assertUpdate("INSERT INTO testReadSchema.testReadStruct1 VALUES (ARRAY ['YOUNG', 'FASION', 'STYLE'], row(111), MAP(ARRAY ['type', 'grand'], ARRAY [1, 2]))", 1);
        assertEquals(computeActual("SELECT * FROM testReadSchema.testReadStruct1").getRowCount(), 1);
        assertUpdate("CREATE TABLE testReadSchema.testReadStruct2 (orderkey array(varchar), orderstatus row(big int, big1 int), lables map(varchar, integer))  WITH (format='orc',transactional=true)");
        assertUpdate("INSERT INTO testReadSchema.testReadStruct2 VALUES (ARRAY ['YOUNG', 'FASION', 'STYLE'], row(111,222), MAP(ARRAY ['type', 'grand'], ARRAY[1, 2]))", 1);
        assertEquals(computeActual("SELECT * FROM testReadSchema.testReadStruct2").getRowCount(), 1);
        assertUpdate("CREATE TABLE testReadSchema.testReadStruct3 (orderkey array(varchar), orderstatus row(big int, big1 int), lables map(varchar, array(int)), orderstatus1 row(big2 int))  WITH (format='orc',transactional=true)");
        assertUpdate("INSERT INTO testReadSchema.testReadStruct3 VALUES (ARRAY ['YOUNG', 'FASION', 'STYLE'], row(111,222), MAP(ARRAY ['type', 'grand'], ARRAY [ARRAY[1, 2], ARRAY[1,2]]), row(333))", 1);
        assertEquals(computeActual("SELECT * FROM testReadSchema.testReadStruct3").getRowCount(), 1);
        assertUpdate("CREATE TABLE testReadSchema.testReadStruct4 (orderkey array(varchar), orderstatus row(big int, big1 int), lables map(varchar, array(int)), orderstatus1 row(big2 int))  WITH (format='orc',transactional=true)");
        assertUpdate("INSERT INTO testReadSchema.testReadStruct4 VALUES (ARRAY ['YOUNG', 'FASION', 'STYLE'], row(111,222), MAP(ARRAY ['type', 'grand'], ARRAY [ARRAY[1, 2], ARRAY[1,2]]), row(111))", 1);
        assertEquals(computeActual("SELECT * FROM testReadSchema.testReadStruct4").getRowCount(), 1);
        assertUpdate("CREATE TABLE testReadSchema.testReadStruct5 (orderkey array(varchar), orderstatus row(big row(big1 int)), lables map(varchar, array(int)), orderstatus1 row(big2 int))  WITH (format='orc',transactional=true)");
        assertUpdate("INSERT INTO testReadSchema.testReadStruct5 VALUES (ARRAY ['YOUNG', 'FASION', 'STYLE'], row(row(111)), MAP(ARRAY ['type', 'grand'], ARRAY [ARRAY[1, 2], ARRAY[1,2]]), row(111))", 1);
        assertEquals(computeActual("SELECT * FROM testReadSchema.testReadStruct5").getRowCount(), 1);
        assertUpdate("DROP TABLE testReadSchema.testReadStruct1");
        assertUpdate("DROP TABLE testReadSchema.testReadStruct2");
        assertUpdate("DROP TABLE testReadSchema.testReadStruct3");
        assertUpdate("DROP TABLE testReadSchema.testReadStruct4");
        assertUpdate("DROP TABLE testReadSchema.testReadStruct5");
        assertUpdate("DROP SCHEMA testReadSchema");
    }

    @Test
    public void testAlterTableWithStructDataTypeColumns()
    {
        assertUpdate("CREATE SCHEMA testReadSchema1");
        assertUpdate("CREATE TABLE testReadSchema1.testReadStruct6 (orderkey array(varchar), orderstatus array(row(big int)), lables map(varchar, array(int)), orderstatus1 row(big2 int))  WITH (format='orc',transactional=true)");
        assertUpdate("INSERT INTO testReadSchema1.testReadStruct6 VALUES (ARRAY ['YOUNG', 'FASION', 'STYLE'], ARRAY[row(111), row(222)], MAP(ARRAY ['type', 'grand'], ARRAY [ARRAY[1, 2], ARRAY[1,2]]), row(111))", 1);
        assertEquals(computeActual("SELECT * FROM testReadSchema1.testReadStruct6").getRowCount(), 1);
        assertUpdate("ALTER TABLE testReadSchema1.testReadStruct6 RENAME COLUMN orderstatus to new_orderstatus");
        assertEquals(computeActual("SELECT * FROM testReadSchema1.testReadStruct6").getRowCount(), 1);
        assertUpdate("DROP TABLE testReadSchema1.testReadStruct6");
        assertUpdate("DROP SCHEMA testReadSchema1");
    }

    @Test
    public void testUpdateWithStructDataTypeColumns()
    {
        assertUpdate("CREATE SCHEMA testReadSchema2");
        assertUpdate("CREATE TABLE testReadSchema2.testReadStruct7 (orderkey array(varchar), orderstatus array(row(big int)), lables map(varchar, array(int)), orderstatus1 row(big2 int))  WITH (format='orc',transactional=true)");
        assertUpdate("INSERT INTO testReadSchema2.testReadStruct7 VALUES (ARRAY ['YOUNG', 'FASION', 'STYLE'], ARRAY[row(111), row(222)], MAP(ARRAY ['type', 'grand'], ARRAY [ARRAY[1, 2], ARRAY[1,2]]), row(111))", 1);
        assertEquals(computeActual("SELECT * FROM testReadSchema2.testReadStruct7").getRowCount(), 1);
        assertUpdate("UPDATE testReadSchema2.testReadStruct7 set orderkey = ARRAY['UPDATE','UPDATE','UPDATE']", 1);
        assertEquals(computeActual("SELECT * FROM testReadSchema2.testReadStruct7").getRowCount(), 1);
        assertUpdate("DROP TABLE testReadSchema2.testReadStruct7");
        assertUpdate("DROP SCHEMA testReadSchema2");
    }

    @Test
    public void testReadSingleColumnStructDataTypeColumns()
    {
        assertUpdate("CREATE SCHEMA testReadSchema3");
        assertUpdate("CREATE TABLE testReadSchema3.testReadStruct8 (orderkey array(varchar), orderstatus array(row(big int)), lables map(varchar, array(int)), orderstatus1 row(big2 int))  WITH (format='orc',transactional=true)");
        assertUpdate("INSERT INTO testReadSchema3.testReadStruct8 VALUES (ARRAY ['YOUNG', 'FASION', 'STYLE'], ARRAY[row(111), row(222)], MAP(ARRAY ['type', 'grand'], ARRAY [ARRAY[1, 2], ARRAY[1,2]]), row(111))", 1);
        assertEquals(computeActual("SELECT orderkey FROM testReadSchema3.testReadStruct8").getRowCount(), 1);
        assertEquals(computeActual("SELECT orderstatus FROM testReadSchema3.testReadStruct8").getRowCount(), 1);
        assertEquals(computeActual("SELECT lables FROM testReadSchema3.testReadStruct8").getRowCount(), 1);
        assertUpdate("DROP TABLE testReadSchema3.testReadStruct8");
        assertUpdate("DROP SCHEMA testReadSchema3");
    }

    @Test
    public void testReadFromTablesWithPrimitiveAndStructDataTypeColumns()
    {
        assertUpdate("CREATE SCHEMA testReadSchema4");
        assertUpdate("CREATE TABLE testReadSchema4.testReadStruct9 (id int, orderkey array(varchar), name string, orderstatus array(row(big int)), lables map(varchar, array(int)), orderstatus1 row(big2 int), age int, salary double)  WITH (format='orc',transactional=true)");
        assertUpdate("INSERT INTO testReadSchema4.testReadStruct9 VALUES (1, ARRAY ['YOUNG', 'FASION', 'STYLE'], 'name1', ARRAY[row(111), row(222)], MAP(ARRAY ['type', 'grand'], ARRAY [ARRAY[1, 2], ARRAY[1,2]]), row(111), 30, 2000.5)", 1);
        assertEquals(computeActual("SELECT id FROM testReadSchema4.testReadStruct9").getRowCount(), 1);
        assertEquals(computeActual("SELECT name FROM testReadSchema4.testReadStruct9").getRowCount(), 1);
        assertEquals(computeActual("SELECT orderkey FROM testReadSchema4.testReadStruct9").getRowCount(), 1);
        assertEquals(computeActual("SELECT orderstatus FROM testReadSchema4.testReadStruct9").getRowCount(), 1);
        assertEquals(computeActual("SELECT lables FROM testReadSchema4.testReadStruct9").getRowCount(), 1);
        assertEquals(computeActual("SELECT age FROM testReadSchema4.testReadStruct9").getRowCount(), 1);
        assertEquals(computeActual("SELECT salary FROM testReadSchema4.testReadStruct9").getRowCount(), 1);
        assertEquals(computeActual("SELECT id, name, age, salary FROM testReadSchema4.testReadStruct9").getRowCount(), 1);
        assertEquals(computeActual("SELECT orderkey, orderstatus, lables, orderstatus1 FROM testReadSchema4.testReadStruct9").getRowCount(), 1);
        assertEquals(computeActual("SELECT id, orderkey, name, orderstatus FROM testReadSchema4.testReadStruct9").getRowCount(), 1);
        assertEquals(computeActual("SELECT salary, age, lables, orderstatus FROM testReadSchema4.testReadStruct9").getRowCount(), 1);
        assertEquals(computeActual("SELECT orderkey, name, lables, orderstatus1 FROM testReadSchema4.testReadStruct9").getRowCount(), 1);
        assertEquals(computeActual("SELECT * FROM testReadSchema4.testReadStruct9").getRowCount(), 1);
        assertUpdate("DROP TABLE testReadSchema4.testReadStruct9");
        assertUpdate("DROP SCHEMA testReadSchema4");
    }

    @Test
    public void testAlterTableDropPartition()
    {
        assertUpdate("CREATE SCHEMA dropPartition0");
        assertUpdate("CREATE TABLE dropPartition0.test_drop_partition_table(rownum int, ii varbinary, aa tinyint, bb smallint, cc int, dd bigint, ee boolean, ff real, gg double, hh varchar(10), kk decimal, ll decimal(10, 8), nn char(5), oo timestamp, pp date)" +
                " WITH (partitioned_by=ARRAY['aa','bb','cc','dd','ee','ff','gg','hh', 'kk', 'll', 'nn', 'oo', 'pp'])");
        assertUpdate("INSERT INTO dropPartition0.test_drop_partition_table VALUES (2,varbinary'abcd',tinyint'22', smallint'33', 88, 99999, boolean'0', 3.3, 2.02,'hello', 56, 32.3, 'work', timestamp'2019-09-19 15:00:00', date'2019-10-30')", 1);
        assertEquals(computeActual("SELECT * FROM dropPartition0.test_drop_partition_table").getRowCount(), 1);
        assertUpdate("ALTER TABLE dropPartition0.test_drop_partition_table DROP PARTITION (aa=22,bb=33,cc=88,dd=99999,ee=false,ff=3.3,gg=2.02,hh='hello',kk=56,ll=32.3,nn='work', oo=timestamp'2019-09-19 15:00:00', pp=date'2019-10-30')");
        assertEquals(computeActual("SELECT * FROM dropPartition0.test_drop_partition_table").getRowCount(), 0);
        assertUpdate("DROP TABLE dropPartition0.test_drop_partition_table");
        assertUpdate("DROP SCHEMA dropPartition0");
    }

    @Test
    public void testAlterTableDropPartitionWithSubsetOfPartitions()
    {
        assertUpdate("CREATE SCHEMA dropPartition1");
        assertUpdate("CREATE TABLE dropPartition1.test_drop_partition_table1(rownum int, ii varbinary, aa tinyint, bb smallint, cc int, dd bigint, ee boolean, ff real, gg double, hh varchar(10), kk decimal, ll decimal(10, 8), nn char(5), oo timestamp, pp date)" +
                " WITH (partitioned_by=ARRAY['aa','bb','cc','dd','ee','ff','gg','hh', 'kk', 'll', 'nn', 'oo', 'pp'])");
        assertUpdate("INSERT INTO dropPartition1.test_drop_partition_table1 VALUES (2,varbinary'abcd',tinyint'22', smallint'33', 88, 99999, boolean'0', 3.3, 2.02, 'hello', 56, 32.3, 'work', timestamp'2019-09-19 15:00:00', date'2019-10-30')", 1);
        assertEquals(computeActual("SELECT * FROM dropPartition1.test_drop_partition_table1").getRowCount(), 1);
        assertUpdate("ALTER TABLE dropPartition1.test_drop_partition_table1 DROP PARTITION (oo=timestamp'2019-09-19 15:00:00')");
        assertEquals(computeActual("SELECT * FROM dropPartition1.test_drop_partition_table1").getRowCount(), 0);
        assertUpdate("DROP TABLE dropPartition1.test_drop_partition_table1");
        assertUpdate("DROP SCHEMA dropPartition1");
    }

    @Test
    public void testAlterTableDropPartitionWithMultiPartitions()
    {
        assertUpdate("CREATE SCHEMA dropPartition2");
        assertUpdate("CREATE TABLE dropPartition2.test_drop_partition_table2(rownum int, ii varbinary, cc int, dd bigint)" +
                " WITH (partitioned_by=ARRAY['cc','dd'])");
        assertUpdate("INSERT INTO dropPartition2.test_drop_partition_table2 VALUES (2,varbinary'abcd', 88, 88888)", 1);
        assertUpdate("INSERT INTO dropPartition2.test_drop_partition_table2 VALUES (2,varbinary'abcd', 99, 99999)", 1);
        assertEquals(computeActual("SELECT * FROM dropPartition2.test_drop_partition_table2").getRowCount(), 2);
        assertUpdate("ALTER TABLE dropPartition2.test_drop_partition_table2 DROP PARTITION (cc=88,dd=88888), PARTITION (cc=99,dd=99999)");
        assertEquals(computeActual("SELECT * FROM dropPartition2.test_drop_partition_table2").getRowCount(), 0);
        assertUpdate("DROP TABLE dropPartition2.test_drop_partition_table2");
        assertUpdate("DROP SCHEMA dropPartition2");
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testAlterTableDropPartitionPassingNonPartitionColumns()
    {
        assertUpdate("CREATE SCHEMA dropPartition3");
        assertUpdate("CREATE TABLE dropPartition3.test_drop_partition_table3(rownum int, ii varbinary, aa tinyint, bb smallint, cc int, dd bigint, ee boolean, ff real, gg double, hh varchar(10), kk decimal, ll decimal(10, 8), nn char(5))" +
                " WITH (partitioned_by=ARRAY['aa','bb','cc','dd','ee','ff','gg','hh', 'kk', 'll', 'nn'])");
        assertUpdate("INSERT INTO dropPartition3.test_drop_partition_table3 VALUES (2,varbinary'abcd',tinyint'22', smallint'33', 88, 99999, boolean'0', 3.3, 2.02,'hello', 56, 32.3, 'work')", 1);
        assertEquals(computeActual("SELECT * FROM dropPartition3.test_drop_partition_table3").getRowCount(), 1);
        assertUpdate("ALTER TABLE dropPartition3.test_drop_partition_table3 DROP PARTITION (rownum=22,bb=33,cc=88,dd=99999)");
        assertEquals(computeActual("SELECT * FROM dropPartition3.test_drop_partition_table3").getRowCount(), 0);
        assertUpdate("DROP TABLE dropPartition3.test_drop_partition_table3");
        assertUpdate("DROP SCHEMA dropPartition3");
    }
}
