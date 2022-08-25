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
package io.hetu.core.plugin.iceberg.metastore.thrift;

import com.google.common.collect.ImmutableMultimap;
import io.prestosql.plugin.hive.HiveBasicStatistics;
import io.prestosql.plugin.hive.HiveBucketProperty;
import io.prestosql.plugin.hive.HiveBucketing;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HivePartition;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.HiveUpdateProcessor;
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.acid.AcidOperation;
import io.prestosql.plugin.hive.acid.AcidTransaction;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.AcidTransactionOwner;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.HivePrincipal;
import io.prestosql.plugin.hive.metastore.HivePrivilegeInfo;
import io.prestosql.plugin.hive.metastore.Partition;
import io.prestosql.plugin.hive.metastore.PartitionWithStatistics;
import io.prestosql.plugin.hive.metastore.PrincipalPrivileges;
import io.prestosql.plugin.hive.metastore.SortingColumn;
import io.prestosql.plugin.hive.metastore.Storage;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastore;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class BridgingHiveMetastoreTest
{
    @Mock
    private ThriftMetastore mockDelegate;

    private BridgingHiveMetastore bridgingHiveMetastoreUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        bridgingHiveMetastoreUnderTest = new BridgingHiveMetastore(mockDelegate, new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())));
    }

    @Test
    public void testGetDatabase()
    {
        // Setup
        final Optional<Database> expectedResult = Optional.of(
                new Database("newDatabaseName", Optional.of("value"), "ownerName", PrincipalType.USER,
                        Optional.of("value"), new HashMap<>()));

        // Configure ThriftMetastore.getDatabase(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Database> database = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Database("newDatabaseName", "description", "locationUri",
                        new HashMap<>()));
        when(mockDelegate.getDatabase("databaseName")).thenReturn(database);

        // Run the test
        final Optional<Database> result = bridgingHiveMetastoreUnderTest.getDatabase("databaseName");

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetDatabase_ThriftMetastoreReturnsAbsent()
    {
        // Setup
        when(mockDelegate.getDatabase("databaseName")).thenReturn(Optional.empty());

        // Run the test
        final Optional<Database> result = bridgingHiveMetastoreUnderTest.getDatabase("databaseName");

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testGetAllDatabases()
    {
        // Setup
        when(mockDelegate.getAllDatabases()).thenReturn(Arrays.asList("value"));

        // Run the test
        final List<String> result = bridgingHiveMetastoreUnderTest.getAllDatabases();

        // Verify the results
        assertEquals(Arrays.asList("value"), result);
    }

    @Test
    public void testGetAllDatabases_ThriftMetastoreReturnsNoItems()
    {
        // Setup
        when(mockDelegate.getAllDatabases()).thenReturn(Collections.emptyList());

        // Run the test
        final List<String> result = bridgingHiveMetastoreUnderTest.getAllDatabases();

        // Verify the results
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void testGetSupportedColumnStatistics()
    {
        // Setup
        final Type type = null;
        when(mockDelegate.getSupportedColumnStatistics(any(Type.class))).thenReturn(new HashSet<>(
                Arrays.asList(ColumnStatisticType.MIN_VALUE)));

        // Run the test
        final Set<ColumnStatisticType> result = bridgingHiveMetastoreUnderTest.getSupportedColumnStatistics(type);

        // Verify the results
        assertEquals(new HashSet<>(Arrays.asList(ColumnStatisticType.MIN_VALUE)), result);
    }

    @Test
    public void testGetSupportedColumnStatistics_ThriftMetastoreReturnsNoItems()
    {
        // Setup
        final Type type = null;
        when(mockDelegate.getSupportedColumnStatistics(any(Type.class))).thenReturn(Collections.emptySet());

        // Run the test
        final Set<ColumnStatisticType> result = bridgingHiveMetastoreUnderTest.getSupportedColumnStatistics(type);

        // Verify the results
        assertEquals(Collections.emptySet(), result);
    }

    @Test
    public void testUpdateTableStatistics1()
    {
        // Setup
        final AcidTransaction transaction = new AcidTransaction(AcidOperation.NONE, 0L, 0L,
                Optional.of(new HiveUpdateProcessor(
                        Arrays.asList(new HiveColumnHandle("name", HIVE_STRING,
                                new TypeSignature("base", TypeSignatureParameter.of(0L)), 0,
                                HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false)), Arrays.asList(
                                        new HiveColumnHandle("name", HIVE_STRING,
                                                new TypeSignature("base", TypeSignatureParameter.of(0L)), 0,
                                HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false)))));
        final Function<PartitionStatistics, PartitionStatistics> update = val -> {
            return new PartitionStatistics(new HiveBasicStatistics(0L, 0L, 0L, 0L), new HashMap<>());
        };

        // Run the test
        bridgingHiveMetastoreUnderTest.updateTableStatistics("databaseName", "tableName", transaction, update);

        // Verify the results
        verify(mockDelegate).updateTableStatistics(eq(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()))), eq("databaseName"), eq("tableName"),
                any(AcidTransaction.class), any(Function.class));
    }

    @Test
    public void testUpdateTableStatistics2()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Function<PartitionStatistics, PartitionStatistics> update = val -> {
            return new PartitionStatistics(new HiveBasicStatistics(0L, 0L, 0L, 0L), new HashMap<>());
        };

        // Run the test
        bridgingHiveMetastoreUnderTest.updateTableStatistics(identity, "databaseName", "tableName", update);

        // Verify the results
        verify(mockDelegate).updateTableStatistics(eq(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()))), eq("databaseName"), eq("tableName"),
                any(Function.class));
    }

    @Test
    public void testUpdatePartitionStatistics1()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Function<PartitionStatistics, PartitionStatistics> update = val -> {
            return new PartitionStatistics(new HiveBasicStatistics(0L, 0L, 0L, 0L), new HashMap<>());
        };

        // Run the test
        bridgingHiveMetastoreUnderTest.updatePartitionStatistics(identity, "databaseName", "tableName", "partitionName",
                update);

        // Verify the results
        verify(mockDelegate).updatePartitionStatistics(eq(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()))), eq("databaseName"), eq("tableName"),
                eq("partitionName"),
                any(Function.class));
    }

    @Test
    public void testUpdatePartitionStatistics2()
    {
        // Setup
        final Table table = new Table("newDatabaseName", "newTableName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("col", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));
        final Map<String, Function<PartitionStatistics, PartitionStatistics>> updates = new HashMap<>();

        // Run the test
        bridgingHiveMetastoreUnderTest.updatePartitionStatistics(table, updates);
    }

    @Test
    public void testGetTablesWithParameter()
    {
        // Setup
        when(mockDelegate.getTablesWithParameter("databaseName", "parameterKey", "parameterValue"))
                .thenReturn(Arrays.asList("value"));

        // Run the test
        final List<String> result = bridgingHiveMetastoreUnderTest.getTablesWithParameter("databaseName",
                "parameterKey", "parameterValue");

        // Verify the results
        assertEquals(Arrays.asList("value"), result);
    }

    @Test
    public void testGetTablesWithParameter_ThriftMetastoreReturnsNoItems()
    {
        // Setup
        when(mockDelegate.getTablesWithParameter("databaseName", "parameterKey", "parameterValue"))
                .thenReturn(Collections.emptyList());

        // Run the test
        final List<String> result = bridgingHiveMetastoreUnderTest.getTablesWithParameter("databaseName",
                "parameterKey", "parameterValue");

        // Verify the results
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void testGetAllViews()
    {
        // Setup
        when(mockDelegate.getAllViews("databaseName")).thenReturn(Optional.of(Arrays.asList("value")));

        // Run the test
        final Optional<List<String>> result = bridgingHiveMetastoreUnderTest.getAllViews("databaseName");

        // Verify the results
        assertEquals(Optional.of(Arrays.asList("value")), result);
    }

    @Test
    public void testGetAllViews_ThriftMetastoreReturnsAbsent()
    {
        // Setup
        when(mockDelegate.getAllViews("databaseName")).thenReturn(Optional.empty());

        // Run the test
        final Optional<List<String>> result = bridgingHiveMetastoreUnderTest.getAllViews("databaseName");

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testGetAllViews_ThriftMetastoreReturnsNoItems()
    {
        // Setup
        when(mockDelegate.getAllViews("databaseName")).thenReturn(Optional.of(Collections.emptyList()));

        // Run the test
        final Optional<List<String>> result = bridgingHiveMetastoreUnderTest.getAllViews("databaseName");

        // Verify the results
        assertEquals(Optional.of(Collections.emptyList()), result);
    }

    @Test
    public void testCreateDatabase()
    {
        // Setup
        final HiveIdentity hiveIdentity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Database database = new Database("newDatabaseName", Optional.of("value"), "ownerName", PrincipalType.USER,
                Optional.of("value"), new HashMap<>());

        // Run the test
        bridgingHiveMetastoreUnderTest.createDatabase(hiveIdentity, database);

        // Verify the results
        verify(mockDelegate).createDatabase(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())),
                new org.apache.hadoop.hive.metastore.api.Database("newDatabaseName", "description", "locationUri",
                        new HashMap<>()));
    }

    @Test
    public void testDropDatabase1()
    {
        // Setup
        // Run the test
        bridgingHiveMetastoreUnderTest.dropDatabase("databaseName", false);

        // Verify the results
        verify(mockDelegate).dropDatabase(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName");
    }

    @Test
    public void testRenameDatabase()
    {
        // Setup
        final HiveIdentity hiveIdentity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));

        // Configure ThriftMetastore.getDatabase(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Database> database = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Database("newDatabaseName", "description", "locationUri",
                        new HashMap<>()));
        when(mockDelegate.getDatabase("databaseName")).thenReturn(database);

        // Run the test
        bridgingHiveMetastoreUnderTest.renameDatabase(hiveIdentity, "databaseName", "newDatabaseName");

        // Verify the results
        verify(mockDelegate).alterDatabase(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName",
                new org.apache.hadoop.hive.metastore.api.Database("newDatabaseName", "description", "locationUri",
                        new HashMap<>()));
    }

    @Test
    public void testRenameDatabase_ThriftMetastoreGetDatabaseReturnsAbsent()
    {
        // Setup
        final HiveIdentity hiveIdentity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        when(mockDelegate.getDatabase("databaseName")).thenReturn(Optional.empty());

        // Run the test
        bridgingHiveMetastoreUnderTest.renameDatabase(hiveIdentity, "databaseName", "newDatabaseName");

        // Verify the results
        verify(mockDelegate).alterDatabase(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName",
                new org.apache.hadoop.hive.metastore.api.Database("newDatabaseName", "description", "locationUri",
                        new HashMap<>()));
    }

    @Test
    public void testSetDatabaseOwner()
    {
        // Setup
        final HivePrincipal principal = new HivePrincipal(PrincipalType.USER, "name");

        // Configure ThriftMetastore.getDatabase(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Database> database = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Database("newDatabaseName", "description", "locationUri",
                        new HashMap<>()));
        when(mockDelegate.getDatabase("databaseName")).thenReturn(database);

        // Run the test
        bridgingHiveMetastoreUnderTest.setDatabaseOwner("databaseName", principal);

        // Verify the results
        verify(mockDelegate).alterDatabase(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName",
                new org.apache.hadoop.hive.metastore.api.Database("newDatabaseName", "description", "locationUri",
                        new HashMap<>()));
    }

    @Test
    public void testSetDatabaseOwner_ThriftMetastoreGetDatabaseReturnsAbsent()
    {
        // Setup
        final HivePrincipal principal = new HivePrincipal(PrincipalType.USER, "name");
        when(mockDelegate.getDatabase("databaseName")).thenReturn(Optional.empty());

        // Run the test
        bridgingHiveMetastoreUnderTest.setDatabaseOwner("databaseName", principal);

        // Verify the results
        verify(mockDelegate).alterDatabase(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName",
                new org.apache.hadoop.hive.metastore.api.Database("newDatabaseName", "description", "locationUri",
                        new HashMap<>()));
    }

    @Test
    public void testCreateTable1()
    {
        // Setup
        final Table table = new Table("newDatabaseName", "newTableName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("col", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));
        final PrincipalPrivileges principalPrivileges = new PrincipalPrivileges(
                ImmutableMultimap.of("value", new HivePrivilegeInfo(
                        HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(PrincipalType.USER, "name"),
                        new HivePrincipal(
                                PrincipalType.USER, "name"))), ImmutableMultimap.of("value",
                                        new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(
                        PrincipalType.USER, "name"), new HivePrincipal(PrincipalType.USER, "name"))));

        // Run the test
        bridgingHiveMetastoreUnderTest.createTable(table, principalPrivileges);
    }

    @Test
    public void testRenameTable_ThriftMetastoreGetTableReturnsAbsent()
    {
        // Setup
        final HiveIdentity hiveIdentity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(Optional.empty());

        // Run the test
        bridgingHiveMetastoreUnderTest.renameTable(hiveIdentity, "databaseName", "tableName", "newDatabaseName",
                "newTableName");

        // Verify the results
        verify(mockDelegate).alterTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
    }

    @Test
    public void testCommentTable()
    {
        // Setup
        final HiveIdentity hiveIdentity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));

        // Configure ThriftMetastore.getTable(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Table> table = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(table);

        // Run the test
        bridgingHiveMetastoreUnderTest.commentTable(hiveIdentity, "databaseName", "tableName", Optional.of("value"));

        // Verify the results
        verify(mockDelegate).alterTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
    }

    @Test
    public void testCommentTable_ThriftMetastoreGetTableReturnsAbsent()
    {
        // Setup
        final HiveIdentity hiveIdentity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(Optional.empty());

        // Run the test
        bridgingHiveMetastoreUnderTest.commentTable(hiveIdentity, "databaseName", "tableName", Optional.of("value"));

        // Verify the results
        verify(mockDelegate).alterTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
    }

    @Test
    public void testSetTableOwner()
    {
        // Setup
        final HivePrincipal principal = new HivePrincipal(PrincipalType.USER, "name");

        // Configure ThriftMetastore.getTable(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Table> table = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(table);

        // Run the test
        bridgingHiveMetastoreUnderTest.setTableOwner("databaseName", "tableName", principal);
    }

    @Test
    public void testCommentColumn()
    {
        // Setup
        // Configure ThriftMetastore.getTable(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Table> table = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(table);

        // Run the test
        bridgingHiveMetastoreUnderTest.commentColumn("databaseName", "tableName", "columnName", Optional.of("value"));

        // Verify the results
        verify(mockDelegate).alterTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
    }

    @Test
    public void testCommentColumn_ThriftMetastoreGetTableReturnsAbsent()
    {
        // Setup
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(Optional.empty());

        // Run the test
        bridgingHiveMetastoreUnderTest.commentColumn("databaseName", "tableName", "columnName", Optional.of("value"));

        // Verify the results
        verify(mockDelegate).alterTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
    }

    @Test
    public void testAddColumn()
    {
        // Setup
        final HiveIdentity hiveIdentity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final HiveType columnType = HIVE_STRING;

        // Configure ThriftMetastore.getTable(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Table> table = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(table);

        // Run the test
        bridgingHiveMetastoreUnderTest.addColumn(hiveIdentity, "databaseName", "tableName", "columnName", columnType,
                "columnComment");
    }

    @Test
    public void testRenameColumn()
    {
        // Setup
        final HiveIdentity hiveIdentity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));

        // Configure ThriftMetastore.getTable(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Table> table = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(table);

        // Run the test
        bridgingHiveMetastoreUnderTest.renameColumn(hiveIdentity, "databaseName", "tableName", "oldColumnName",
                "columnName");

        // Verify the results
        verify(mockDelegate).alterTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
    }

    @Test
    public void testRenameColumn_ThriftMetastoreGetTableReturnsAbsent()
    {
        // Setup
        final HiveIdentity hiveIdentity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(Optional.empty());

        // Run the test
        bridgingHiveMetastoreUnderTest.renameColumn(hiveIdentity, "databaseName", "tableName", "oldColumnName",
                "columnName");

        // Verify the results
        verify(mockDelegate).alterTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
    }

    @Test
    public void testDropColumn()
    {
        // Setup
        final HiveIdentity hiveIdentity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));

        // Configure ThriftMetastore.getTable(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Table> table = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(table);

        // Run the test
        bridgingHiveMetastoreUnderTest.dropColumn(hiveIdentity, "databaseName", "tableName", "columnName");

        // Verify the results
        verify(mockDelegate).alterTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
    }

    @Test
    public void testDropColumn_ThriftMetastoreGetTableReturnsAbsent()
    {
        // Setup
        final HiveIdentity hiveIdentity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(Optional.empty());

        // Run the test
        bridgingHiveMetastoreUnderTest.dropColumn(hiveIdentity, "databaseName", "tableName", "columnName");

        // Verify the results
        verify(mockDelegate).alterTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
    }

    @Test
    public void testGetPartition()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Optional<Partition> expectedResult = Optional.of(
                new Partition("databaseName", "tableName", Arrays.asList("value"), new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("col", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                        new HashMap<>()));

        // Configure ThriftMetastore.getPartition(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Partition> partition = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Partition(
                        Arrays.asList("value"), "dbName", "tableName", 0, 0, new StorageDescriptor(
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), "location",
                        "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                        Arrays.asList(new Order("col", 0)), new HashMap<>()), new HashMap<>()));
        when(mockDelegate.getPartition(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                Arrays.asList("value"))).thenReturn(partition);

        // Configure ThriftMetastore.getTable(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Table> table = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(table);

        // Configure ThriftMetastore.getFields(...).
        final Optional<List<FieldSchema>> fieldSchemas = Optional.of(
                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")));
        when(mockDelegate.getFields(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(fieldSchemas);

        // Run the test
        final Optional<Partition> result = bridgingHiveMetastoreUnderTest.getPartition(identity, "databaseName",
                "tableName",
                Arrays.asList("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetPartition_ThriftMetastoreGetPartitionReturnsAbsent()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Optional<Partition> expectedResult = Optional.of(
                new Partition("databaseName", "tableName", Arrays.asList("value"), new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("col", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                        new HashMap<>()));
        when(mockDelegate.getPartition(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                Arrays.asList("value"))).thenReturn(Optional.empty());

        // Configure ThriftMetastore.getTable(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Table> table = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(table);

        // Configure ThriftMetastore.getFields(...).
        final Optional<List<FieldSchema>> fieldSchemas = Optional.of(
                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")));
        when(mockDelegate.getFields(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(fieldSchemas);

        // Run the test
        final Optional<Partition> result = bridgingHiveMetastoreUnderTest.getPartition(identity, "databaseName",
                "tableName",
                Arrays.asList("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetPartition_ThriftMetastoreGetTableReturnsAbsent()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Optional<Partition> expectedResult = Optional.of(
                new Partition("databaseName", "tableName", Arrays.asList("value"), new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("col", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                        new HashMap<>()));

        // Configure ThriftMetastore.getPartition(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Partition> partition = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Partition(
                        Arrays.asList("value"), "dbName", "tableName", 0, 0, new StorageDescriptor(
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), "location",
                        "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                        Arrays.asList(new Order("col", 0)), new HashMap<>()), new HashMap<>()));
        when(mockDelegate.getPartition(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                Arrays.asList("value"))).thenReturn(partition);

        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(Optional.empty());

        // Configure ThriftMetastore.getFields(...).
        final Optional<List<FieldSchema>> fieldSchemas = Optional.of(
                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")));
        when(mockDelegate.getFields(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(fieldSchemas);

        // Run the test
        final Optional<Partition> result = bridgingHiveMetastoreUnderTest.getPartition(identity, "databaseName",
                "tableName",
                Arrays.asList("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetPartition_ThriftMetastoreGetFieldsReturnsAbsent()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Optional<Partition> expectedResult = Optional.of(
                new Partition("databaseName", "tableName", Arrays.asList("value"), new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("col", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                        new HashMap<>()));

        // Configure ThriftMetastore.getPartition(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Partition> partition = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Partition(
                        Arrays.asList("value"), "dbName", "tableName", 0, 0, new StorageDescriptor(
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), "location",
                        "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                        Arrays.asList(new Order("col", 0)), new HashMap<>()), new HashMap<>()));
        when(mockDelegate.getPartition(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                Arrays.asList("value"))).thenReturn(partition);

        // Configure ThriftMetastore.getTable(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Table> table = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(table);

        when(mockDelegate.getFields(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(Optional.empty());

        // Run the test
        final Optional<Partition> result = bridgingHiveMetastoreUnderTest.getPartition(identity, "databaseName",
                "tableName",
                Arrays.asList("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetPartition_ThriftMetastoreGetFieldsReturnsNoItems()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Optional<Partition> expectedResult = Optional.of(
                new Partition("databaseName", "tableName", Arrays.asList("value"), new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("col", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                        new HashMap<>()));

        // Configure ThriftMetastore.getPartition(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Partition> partition = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Partition(
                        Arrays.asList("value"), "dbName", "tableName", 0, 0, new StorageDescriptor(
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), "location",
                        "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                        Arrays.asList(new Order("col", 0)), new HashMap<>()), new HashMap<>()));
        when(mockDelegate.getPartition(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                Arrays.asList("value"))).thenReturn(partition);

        // Configure ThriftMetastore.getTable(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Table> table = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(table);

        when(mockDelegate.getFields(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(Optional.of(
                Collections.emptyList()));

        // Run the test
        final Optional<Partition> result = bridgingHiveMetastoreUnderTest.getPartition(identity, "databaseName",
                "tableName",
                Arrays.asList("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetPartitionNamesByFilter()
    {
        // Setup
        final TupleDomain<String> partitionKeysFilter = TupleDomain.withColumnDomains(new HashMap<>());
        when(mockDelegate.getPartitionNamesByFilter(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                Arrays.asList("value"), TupleDomain.withColumnDomains(new HashMap<>()))).thenReturn(Optional.of(
                Arrays.asList("value")));

        // Run the test
        final Optional<List<String>> result = bridgingHiveMetastoreUnderTest.getPartitionNamesByFilter("databaseName",
                "tableName",
                Arrays.asList("value"), partitionKeysFilter);

        // Verify the results
        assertEquals(Optional.of(Arrays.asList("value")), result);
    }

    @Test
    public void testGetPartitionNamesByFilter_ThriftMetastoreReturnsAbsent()
    {
        // Setup
        final TupleDomain<String> partitionKeysFilter = TupleDomain.withColumnDomains(new HashMap<>());
        when(mockDelegate.getPartitionNamesByFilter(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                Arrays.asList("value"),
                TupleDomain.withColumnDomains(new HashMap<>()))).thenReturn(Optional.empty());

        // Run the test
        final Optional<List<String>> result = bridgingHiveMetastoreUnderTest.getPartitionNamesByFilter("databaseName",
                "tableName",
                Arrays.asList("value"), partitionKeysFilter);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testGetPartitionNamesByFilter_ThriftMetastoreReturnsNoItems()
    {
        // Setup
        final TupleDomain<String> partitionKeysFilter = TupleDomain.withColumnDomains(new HashMap<>());
        when(mockDelegate.getPartitionNamesByFilter(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                Arrays.asList("value"),
                TupleDomain.withColumnDomains(new HashMap<>()))).thenReturn(Optional.of(Collections.emptyList()));

        // Run the test
        final Optional<List<String>> result = bridgingHiveMetastoreUnderTest.getPartitionNamesByFilter("databaseName",
                "tableName",
                Arrays.asList("value"), partitionKeysFilter);

        // Verify the results
        assertEquals(Optional.of(Collections.emptyList()), result);
    }

    @Test
    public void testGetPartitionsByNames()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Map<String, Optional<Partition>> expectedResult = new HashMap<>();

        // Configure ThriftMetastore.getPartitionsByNames(...).
        final List<org.apache.hadoop.hive.metastore.api.Partition> partitions = Arrays.asList(
                new org.apache.hadoop.hive.metastore.api.Partition(
                        Arrays.asList("value"), "dbName", "tableName", 0, 0, new StorageDescriptor(
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), "location",
                        "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                        Arrays.asList(new Order("col", 0)), new HashMap<>()), new HashMap<>()));
        when(mockDelegate.getPartitionsByNames(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                Arrays.asList("value"))).thenReturn(partitions);

        // Configure ThriftMetastore.getTable(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Table> table = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(table);

        // Configure ThriftMetastore.getFields(...).
        final Optional<List<FieldSchema>> fieldSchemas = Optional.of(
                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")));
        when(mockDelegate.getFields(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(fieldSchemas);

        // Run the test
        final Map<String, Optional<Partition>> result = bridgingHiveMetastoreUnderTest.getPartitionsByNames(identity,
                "databaseName", "tableName",
                Arrays.asList("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetPartitionsByNames_ThriftMetastoreGetPartitionsByNamesReturnsNoItems()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Map<String, Optional<Partition>> expectedResult = new HashMap<>();
        when(mockDelegate.getPartitionsByNames(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                Arrays.asList("value"))).thenReturn(Collections.emptyList());

        // Configure ThriftMetastore.getTable(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Table> table = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(table);

        // Configure ThriftMetastore.getFields(...).
        final Optional<List<FieldSchema>> fieldSchemas = Optional.of(
                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")));
        when(mockDelegate.getFields(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(fieldSchemas);

        // Run the test
        final Map<String, Optional<Partition>> result = bridgingHiveMetastoreUnderTest.getPartitionsByNames(identity,
                "databaseName", "tableName",
                Arrays.asList("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetPartitionsByNames_ThriftMetastoreGetTableReturnsAbsent()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Map<String, Optional<Partition>> expectedResult = new HashMap<>();

        // Configure ThriftMetastore.getPartitionsByNames(...).
        final List<org.apache.hadoop.hive.metastore.api.Partition> partitions = Arrays.asList(
                new org.apache.hadoop.hive.metastore.api.Partition(
                        Arrays.asList("value"), "dbName", "tableName", 0, 0, new StorageDescriptor(
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), "location",
                        "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                        Arrays.asList(new Order("col", 0)), new HashMap<>()), new HashMap<>()));
        when(mockDelegate.getPartitionsByNames(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                Arrays.asList("value"))).thenReturn(partitions);

        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(Optional.empty());

        // Configure ThriftMetastore.getFields(...).
        final Optional<List<FieldSchema>> fieldSchemas = Optional.of(
                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")));
        when(mockDelegate.getFields(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(fieldSchemas);

        // Run the test
        final Map<String, Optional<Partition>> result = bridgingHiveMetastoreUnderTest.getPartitionsByNames(identity,
                "databaseName", "tableName",
                Arrays.asList("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetPartitionsByNames_ThriftMetastoreGetFieldsReturnsAbsent()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Map<String, Optional<Partition>> expectedResult = new HashMap<>();

        // Configure ThriftMetastore.getPartitionsByNames(...).
        final List<org.apache.hadoop.hive.metastore.api.Partition> partitions = Arrays.asList(
                new org.apache.hadoop.hive.metastore.api.Partition(
                        Arrays.asList("value"), "dbName", "tableName", 0, 0, new StorageDescriptor(
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), "location",
                        "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                        Arrays.asList(new Order("col", 0)), new HashMap<>()), new HashMap<>()));
        when(mockDelegate.getPartitionsByNames(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                Arrays.asList("value"))).thenReturn(partitions);

        // Configure ThriftMetastore.getTable(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Table> table = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(table);

        when(mockDelegate.getFields(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(Optional.empty());

        // Run the test
        final Map<String, Optional<Partition>> result = bridgingHiveMetastoreUnderTest.getPartitionsByNames(identity,
                "databaseName", "tableName",
                Arrays.asList("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetPartitionsByNames_ThriftMetastoreGetFieldsReturnsNoItems()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Map<String, Optional<Partition>> expectedResult = new HashMap<>();

        // Configure ThriftMetastore.getPartitionsByNames(...).
        final List<org.apache.hadoop.hive.metastore.api.Partition> partitions = Arrays.asList(
                new org.apache.hadoop.hive.metastore.api.Partition(
                        Arrays.asList("value"), "dbName", "tableName", 0, 0, new StorageDescriptor(
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), "location",
                        "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                        Arrays.asList(new Order("col", 0)), new HashMap<>()), new HashMap<>()));
        when(mockDelegate.getPartitionsByNames(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                Arrays.asList("value"))).thenReturn(partitions);

        // Configure ThriftMetastore.getTable(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Table> table = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(table);

        when(mockDelegate.getFields(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(Optional.of(
                Collections.emptyList()));

        // Run the test
        final Map<String, Optional<Partition>> result = bridgingHiveMetastoreUnderTest.getPartitionsByNames(identity,
                "databaseName", "tableName",
                Arrays.asList("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testAddPartitions()
    {
        // Setup
        final HiveIdentity hiveIdentity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final List<PartitionWithStatistics> partitions = Arrays.asList(new PartitionWithStatistics(
                new Partition("databaseName", "tableName", Arrays.asList("value"), new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("col", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                        new HashMap<>()), "partitionName",
                new PartitionStatistics(new HiveBasicStatistics(0L, 0L, 0L, 0L), new HashMap<>()), false));

        // Run the test
        bridgingHiveMetastoreUnderTest.addPartitions(hiveIdentity, "databaseName", "tableName", partitions);
    }

    @Test
    public void testDropPartition()
    {
        // Setup
        final HiveIdentity hiveIdentity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));

        // Run the test
        bridgingHiveMetastoreUnderTest.dropPartition(hiveIdentity, "databaseName", "tableName", Arrays.asList("value"),
                false);

        // Verify the results
        verify(mockDelegate).dropPartition(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName", Arrays.asList("value"),
                false);
    }

    @Test
    public void testAlterPartition()
    {
        // Setup
        final HiveIdentity hiveIdentity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final PartitionWithStatistics partition = new PartitionWithStatistics(
                new Partition("databaseName", "tableName", Arrays.asList("value"), new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("col", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                        new HashMap<>()), "partitionName",
                new PartitionStatistics(new HiveBasicStatistics(0L, 0L, 0L, 0L), new HashMap<>()), false);

        // Run the test
        bridgingHiveMetastoreUnderTest.alterPartition(hiveIdentity, "databaseName", "tableName", partition);

        // Verify the results
        verify(mockDelegate).alterPartition(eq(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()))), eq("databaseName"), eq("tableName"),
                any(PartitionWithStatistics.class));
    }

    @Test
    public void testCreateRole()
    {
        // Setup
        // Run the test
        bridgingHiveMetastoreUnderTest.createRole("role", "grantor");

        // Verify the results
        verify(mockDelegate).createRole("role", "grantor");
    }

    @Test
    public void testDropRole()
    {
        // Setup
        // Run the test
        bridgingHiveMetastoreUnderTest.dropRole("role");

        // Verify the results
        verify(mockDelegate).dropRole("role");
    }

    @Test
    public void testListRoles()
    {
        // Setup
        when(mockDelegate.listRoles()).thenReturn(new HashSet<>(Arrays.asList("value")));

        // Run the test
        final Set<String> result = bridgingHiveMetastoreUnderTest.listRoles();

        // Verify the results
        assertEquals(new HashSet<>(Arrays.asList("value")), result);
    }

    @Test
    public void testListRoles_ThriftMetastoreReturnsNoItems()
    {
        // Setup
        when(mockDelegate.listRoles()).thenReturn(Collections.emptySet());

        // Run the test
        final Set<String> result = bridgingHiveMetastoreUnderTest.listRoles();

        // Verify the results
        assertEquals(Collections.emptySet(), result);
    }

    @Test
    public void testGrantRoles()
    {
        // Setup
        final Set<HivePrincipal> grantees = new HashSet<>(
                Arrays.asList(new HivePrincipal(PrincipalType.USER, "name")));
        final HivePrincipal grantor = new HivePrincipal(PrincipalType.USER, "name");

        // Run the test
        bridgingHiveMetastoreUnderTest.grantRoles(new HashSet<>(Arrays.asList("value")), grantees, false, grantor);

        // Verify the results
        verify(mockDelegate).grantRoles(new HashSet<>(Arrays.asList("value")), new HashSet<>(
                        Arrays.asList(new HivePrincipal(PrincipalType.USER, "name"))), false,
                new HivePrincipal(PrincipalType.USER, "name"));
    }

    @Test
    public void testRevokeRoles()
    {
        // Setup
        final Set<HivePrincipal> grantees = new HashSet<>(
                Arrays.asList(new HivePrincipal(PrincipalType.USER, "name")));
        final HivePrincipal grantor = new HivePrincipal(PrincipalType.USER, "name");

        // Run the test
        bridgingHiveMetastoreUnderTest.revokeRoles(new HashSet<>(Arrays.asList("value")), grantees, false, grantor);

        // Verify the results
        verify(mockDelegate).revokeRoles(new HashSet<>(Arrays.asList("value")), new HashSet<>(
                        Arrays.asList(new HivePrincipal(PrincipalType.USER, "name"))), false,
                new HivePrincipal(PrincipalType.USER, "name"));
    }

    @Test
    public void testListGrantedPrincipals()
    {
        // Setup
        final Set<RoleGrant> expectedResult = new HashSet<>(
                Arrays.asList(new RoleGrant(new PrestoPrincipal(PrincipalType.USER, "name"), "roleName", false)));

        // Configure ThriftMetastore.listGrantedPrincipals(...).
        final Set<RoleGrant> roleGrants = new HashSet<>(
                Arrays.asList(new RoleGrant(new PrestoPrincipal(PrincipalType.USER, "name"), "roleName", false)));
        when(mockDelegate.listGrantedPrincipals("role")).thenReturn(roleGrants);

        // Run the test
        final Set<RoleGrant> result = bridgingHiveMetastoreUnderTest.listGrantedPrincipals("role");

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListGrantedPrincipals_ThriftMetastoreReturnsNoItems()
    {
        // Setup
        when(mockDelegate.listGrantedPrincipals("role")).thenReturn(Collections.emptySet());

        // Run the test
        final Set<RoleGrant> result = bridgingHiveMetastoreUnderTest.listGrantedPrincipals("role");

        // Verify the results
        assertEquals(Collections.emptySet(), result);
    }

    @Test
    public void testListRoleGrants()
    {
        // Setup
        final HivePrincipal principal = new HivePrincipal(PrincipalType.USER, "name");
        final Set<RoleGrant> expectedResult = new HashSet<>(
                Arrays.asList(new RoleGrant(new PrestoPrincipal(PrincipalType.USER, "name"), "roleName", false)));

        // Configure ThriftMetastore.listRoleGrants(...).
        final Set<RoleGrant> roleGrants = new HashSet<>(
                Arrays.asList(new RoleGrant(new PrestoPrincipal(PrincipalType.USER, "name"), "roleName", false)));
        when(mockDelegate.listRoleGrants(new HivePrincipal(PrincipalType.USER, "name"))).thenReturn(roleGrants);

        // Run the test
        final Set<RoleGrant> result = bridgingHiveMetastoreUnderTest.listRoleGrants(principal);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListRoleGrants_ThriftMetastoreReturnsNoItems()
    {
        // Setup
        final HivePrincipal principal = new HivePrincipal(PrincipalType.USER, "name");
        when(mockDelegate.listRoleGrants(new HivePrincipal(PrincipalType.USER, "name")))
                .thenReturn(Collections.emptySet());

        // Run the test
        final Set<RoleGrant> result = bridgingHiveMetastoreUnderTest.listRoleGrants(principal);

        // Verify the results
        assertEquals(Collections.emptySet(), result);
    }

    @Test
    public void testGrantTablePrivileges1()
    {
        // Setup
        final HivePrincipal grantee = new HivePrincipal(PrincipalType.USER, "name");
        final HivePrincipal grantor = new HivePrincipal(PrincipalType.USER, "name");

        // Run the test
        bridgingHiveMetastoreUnderTest.grantTablePrivileges("databaseName", "tableName", "tableOwner", grantee, grantor,
                new HashSet<>(
                        Arrays.asList(HivePrivilegeInfo.HivePrivilege.SELECT)), false);

        // Verify the results
        verify(mockDelegate).grantTablePrivileges("databaseName", "tableName", "tableOwner", new HivePrincipal(
                PrincipalType.USER, "name"), new HivePrincipal(PrincipalType.USER, "name"), new HashSet<>(
                Arrays.asList(HivePrivilegeInfo.HivePrivilege.SELECT)), false);
    }

    @Test
    public void testRevokeTablePrivileges1()
    {
        // Setup
        final HivePrincipal grantee = new HivePrincipal(PrincipalType.USER, "name");
        final HivePrincipal grantor = new HivePrincipal(PrincipalType.USER, "name");

        // Run the test
        bridgingHiveMetastoreUnderTest.revokeTablePrivileges("databaseName", "tableName", "tableOwner", grantee,
                grantor, new HashSet<>(
                        Arrays.asList(HivePrivilegeInfo.HivePrivilege.SELECT)), false);

        // Verify the results
        verify(mockDelegate).revokeTablePrivileges("databaseName", "tableName", "tableOwner", new HivePrincipal(
                PrincipalType.USER, "name"), new HivePrincipal(PrincipalType.USER, "name"), new HashSet<>(
                Arrays.asList(HivePrivilegeInfo.HivePrivilege.SELECT)), false);
    }

    @Test
    public void testListTablePrivileges1()
    {
        // Setup
        final Optional<HivePrincipal> principal = Optional.of(new HivePrincipal(PrincipalType.USER, "name"));
        final Set<HivePrivilegeInfo> expectedResult = new HashSet<>(Arrays.asList(new HivePrivilegeInfo(
                HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(PrincipalType.USER, "name"),
                new HivePrincipal(
                        PrincipalType.USER, "name"))));

        // Configure ThriftMetastore.listTablePrivileges(...).
        final Set<HivePrivilegeInfo> hivePrivilegeInfos = new HashSet<>(Arrays.asList(new HivePrivilegeInfo(
                HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(PrincipalType.USER, "name"),
                new HivePrincipal(
                        PrincipalType.USER, "name"))));
        when(mockDelegate.listTablePrivileges("databaseName", "tableName", Optional.of("value"),
                Optional.of(new HivePrincipal(PrincipalType.USER, "name")))).thenReturn(hivePrivilegeInfos);

        // Run the test
        final Set<HivePrivilegeInfo> result = bridgingHiveMetastoreUnderTest.listTablePrivileges("databaseName",
                "tableName",
                Optional.of("value"), principal);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListTablePrivileges1_ThriftMetastoreReturnsNoItems()
    {
        // Setup
        final Optional<HivePrincipal> principal = Optional.of(new HivePrincipal(PrincipalType.USER, "name"));
        when(mockDelegate.listTablePrivileges("databaseName", "tableName", Optional.of("value"),
                Optional.of(new HivePrincipal(PrincipalType.USER, "name")))).thenReturn(Collections.emptySet());

        // Run the test
        final Set<HivePrivilegeInfo> result = bridgingHiveMetastoreUnderTest.listTablePrivileges("databaseName",
                "tableName",
                Optional.of("value"), principal);

        // Verify the results
        assertEquals(Collections.emptySet(), result);
    }

    @Test
    public void testGetConfigValue()
    {
        // Setup
        when(mockDelegate.getConfigValue("name")).thenReturn(Optional.of("value"));

        // Run the test
        final Optional<String> result = bridgingHiveMetastoreUnderTest.getConfigValue("name");

        // Verify the results
        assertEquals(Optional.of("value"), result);
    }

    @Test
    public void testGetConfigValue_ThriftMetastoreReturnsAbsent()
    {
        // Setup
        when(mockDelegate.getConfigValue("name")).thenReturn(Optional.empty());

        // Run the test
        final Optional<String> result = bridgingHiveMetastoreUnderTest.getConfigValue("name");

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testOpenTransaction()
    {
        // Setup
        final AcidTransactionOwner transactionOwner = new AcidTransactionOwner("owner");
        when(mockDelegate.openTransaction(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), new AcidTransactionOwner("owner"))).thenReturn(0L);

        // Run the test
        final long result = bridgingHiveMetastoreUnderTest.openTransaction(transactionOwner);

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testCommitTransaction()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));

        // Run the test
        bridgingHiveMetastoreUnderTest.commitTransaction(identity, 0L);

        // Verify the results
        verify(mockDelegate).commitTransaction(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), 0L);
    }

    @Test
    public void testAbortTransaction()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));

        // Run the test
        bridgingHiveMetastoreUnderTest.abortTransaction(identity, 0L);

        // Verify the results
        verify(mockDelegate).abortTransaction(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), 0L);
    }

    @Test
    public void testSendTransactionHeartbeat()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));

        // Run the test
        bridgingHiveMetastoreUnderTest.sendTransactionHeartbeat(identity, 0L);

        // Verify the results
        verify(mockDelegate).sendTransactionHeartbeat(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), 0L);
    }

    @Test
    public void testAcquireSharedReadLock()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final List<SchemaTableName> fullTables = Arrays.asList(new SchemaTableName("databaseName", "tableName"));
        final List<HivePartition> partitions = Arrays.asList(
                new HivePartition(new SchemaTableName("databaseName", "tableName"), "partitionId", new HashMap<>()));

        // Run the test
        bridgingHiveMetastoreUnderTest.acquireSharedReadLock(identity, "queryId", 0L, fullTables, partitions);

        // Verify the results
        verify(mockDelegate).acquireSharedReadLock(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "queryId", 0L,
                Arrays.asList(new SchemaTableName("databaseName", "tableName")),
                Arrays.asList(new HivePartition(new SchemaTableName("databaseName", "tableName"), "partitionId",
                        new HashMap<>())));
    }

    @Test
    public void testGetValidWriteIds()
    {
        // Setup
        final List<SchemaTableName> tables = Arrays.asList(new SchemaTableName("databaseName", "tableName"));
        when(mockDelegate.getValidWriteIds(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())),
                Arrays.asList(new SchemaTableName("databaseName", "tableName")), 0L)).thenReturn("result");

        // Run the test
        final String result = bridgingHiveMetastoreUnderTest.getValidWriteIds(tables, 0L);

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testAllocateWriteId()
    {
        // Setup
        when(mockDelegate.allocateWriteId(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "dbName", "tableName", 0L)).thenReturn(0L);

        // Run the test
        final long result = bridgingHiveMetastoreUnderTest.allocateWriteId("dbName", "tableName", 0L);

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testAcquireTableWriteLock()
    {
        // Setup
        final AcidTransactionOwner transactionOwner = new AcidTransactionOwner("owner");

        // Run the test
        bridgingHiveMetastoreUnderTest.acquireTableWriteLock(transactionOwner, "queryId", 0L, "dbName", "tableName",
                DataOperationType.SELECT, false);

        // Verify the results
        verify(mockDelegate).acquireTableWriteLock(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), new AcidTransactionOwner("owner"), "queryId",
                0L, "dbName", "tableName",
                DataOperationType.SELECT, false);
    }

    @Test
    public void testUpdateTableWriteId()
    {
        // Setup
        final OptionalLong rowCountChange = OptionalLong.of(0);

        // Run the test
        bridgingHiveMetastoreUnderTest.updateTableWriteId("dbName", "tableName", 0L, 0L, rowCountChange);

        // Verify the results
        verify(mockDelegate).updateTableWriteId(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "dbName", "tableName", 0L, 0L,
                OptionalLong.of(0));
    }

    @Test
    public void testAlterPartitions()
    {
        // Setup
        final List<Partition> partitions = Arrays.asList(
                new Partition("databaseName", "tableName", Arrays.asList("value"), new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("col", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                        new HashMap<>()));

        // Run the test
        bridgingHiveMetastoreUnderTest.alterPartitions("dbName", "tableName", partitions, 0L);

        // Verify the results
        verify(mockDelegate).alterPartitions(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "dbName", "tableName",
                Arrays.asList(new org.apache.hadoop.hive.metastore.api.Partition(Arrays.asList("value"), "dbName",
                        "tableName", 0, 0, new StorageDescriptor(
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), "location",
                        "inputFormat", "outputFormat", false, 0,
                                        new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                        Arrays.asList(new Order("col", 0)), new HashMap<>()), new HashMap<>())), 0L);
    }

    @Test
    public void testAddDynamicPartitions()
    {
        // Setup
        // Run the test
        bridgingHiveMetastoreUnderTest.addDynamicPartitions("dbName", "tableName", Arrays.asList("value"), 0L, 0L,
                AcidOperation.NONE);

        // Verify the results
        verify(mockDelegate).addDynamicPartitions(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "dbName", "tableName", Arrays.asList("value"),
                0L, 0L,
                AcidOperation.NONE);
    }

    @Test
    public void testAlterTransactionalTable()
    {
        // Setup
        final Table table = new Table("newDatabaseName", "newTableName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("col", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));
        final PrincipalPrivileges principalPrivileges = new PrincipalPrivileges(
                ImmutableMultimap.of("value", new HivePrivilegeInfo(
                        HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(PrincipalType.USER, "name"),
                        new HivePrincipal(
                                PrincipalType.USER, "name"))), ImmutableMultimap.of("value",
                                        new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(
                        PrincipalType.USER, "name"), new HivePrincipal(PrincipalType.USER, "name"))));

        // Run the test
        bridgingHiveMetastoreUnderTest.alterTransactionalTable(table, 0L, 0L, principalPrivileges);

        // Verify the results
        verify(mockDelegate).alterTransactionalTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())),
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"), 0L, 0L);
    }

    @Test
    public void testGetTable()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Optional<Table> expectedResult = Optional.of(
                new Table("newDatabaseName", "newTableName", "owner", "tableType", new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("col", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                        Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                        new HashMap<>(), Optional.of("value"), Optional.of("value")));

        // Configure ThriftMetastore.getTable(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Table> table = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(table);

        // Configure ThriftMetastore.getFields(...).
        final Optional<List<FieldSchema>> fieldSchemas = Optional.of(
                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")));
        when(mockDelegate.getFields(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(fieldSchemas);

        // Run the test
        final Optional<Table> result = bridgingHiveMetastoreUnderTest.getTable(identity, "databaseName", "tableName");

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetTable_ThriftMetastoreGetTableReturnsAbsent()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Optional<Table> expectedResult = Optional.of(
                new Table("newDatabaseName", "newTableName", "owner", "tableType", new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("col", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                        Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                        new HashMap<>(), Optional.of("value"), Optional.of("value")));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(Optional.empty());

        // Configure ThriftMetastore.getFields(...).
        final Optional<List<FieldSchema>> fieldSchemas = Optional.of(
                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")));
        when(mockDelegate.getFields(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(fieldSchemas);

        // Run the test
        final Optional<Table> result = bridgingHiveMetastoreUnderTest.getTable(identity, "databaseName", "tableName");

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetTable_ThriftMetastoreGetFieldsReturnsAbsent()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Optional<Table> expectedResult = Optional.of(
                new Table("newDatabaseName", "newTableName", "owner", "tableType", new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("col", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                        Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                        new HashMap<>(), Optional.of("value"), Optional.of("value")));

        // Configure ThriftMetastore.getTable(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Table> table = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(table);

        when(mockDelegate.getFields(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(Optional.empty());

        // Run the test
        final Optional<Table> result = bridgingHiveMetastoreUnderTest.getTable(identity, "databaseName", "tableName");

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetTable_ThriftMetastoreGetFieldsReturnsNoItems()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Optional<Table> expectedResult = Optional.of(
                new Table("newDatabaseName", "newTableName", "owner", "tableType", new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("col", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                        Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                        new HashMap<>(), Optional.of("value"), Optional.of("value")));

        // Configure ThriftMetastore.getTable(...).
        final Optional<org.apache.hadoop.hive.metastore.api.Table> table = Optional.of(
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
        when(mockDelegate.getTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(table);

        when(mockDelegate.getFields(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName")).thenReturn(Optional.of(
                Collections.emptyList()));

        // Run the test
        final Optional<Table> result = bridgingHiveMetastoreUnderTest.getTable(identity, "databaseName", "tableName");

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetTableStatistics()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Table table = new Table("newDatabaseName", "newTableName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("col", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));
        final PartitionStatistics expectedResult = new PartitionStatistics(new HiveBasicStatistics(0L, 0L, 0L, 0L),
                new HashMap<>());

        // Configure ThriftMetastore.getTableStatistics(...).
        final PartitionStatistics partitionStatistics = new PartitionStatistics(new HiveBasicStatistics(0L, 0L, 0L, 0L),
                new HashMap<>());
        when(mockDelegate.getTableStatistics(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())),
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"))).thenReturn(partitionStatistics);

        // Run the test
        final PartitionStatistics result = bridgingHiveMetastoreUnderTest.getTableStatistics(identity, table);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetPartitionStatistics()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Table table = new Table("newDatabaseName", "newTableName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("col", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));
        final List<Partition> partitions = Arrays.asList(
                new Partition("databaseName", "tableName", Arrays.asList("value"), new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("col", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                        new HashMap<>()));
        final Map<String, PartitionStatistics> expectedResult = new HashMap<>();
        when(mockDelegate.getPartitionStatistics(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())),
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"),
                Arrays.asList(new org.apache.hadoop.hive.metastore.api.Partition(Arrays.asList("value"), "dbName",
                        "tableName", 0, 0, new StorageDescriptor(
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), "location",
                        "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                        Arrays.asList(new Order("col", 0)), new HashMap<>()), new HashMap<>()))))
                .thenReturn(new HashMap<>());

        // Run the test
        final Map<String, PartitionStatistics> result = bridgingHiveMetastoreUnderTest.getPartitionStatistics(identity,
                table, partitions);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testUpdatePartitionsStatistics()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Map<String, Function<PartitionStatistics, PartitionStatistics>> partNamesUpdateFunctionMap = new HashMap<>();

        // Run the test
        bridgingHiveMetastoreUnderTest.updatePartitionsStatistics(identity, "databaseName", "tableName",
                partNamesUpdateFunctionMap);

        // Verify the results
    }

    @Test
    public void testGetAllTables()
    {
        // Setup
        when(mockDelegate.getAllTables("databaseName")).thenReturn(Optional.of(Arrays.asList("value")));

        // Run the test
        final Optional<List<String>> result = bridgingHiveMetastoreUnderTest.getAllTables("databaseName");

        // Verify the results
        assertEquals(Optional.of(Arrays.asList("value")), result);
    }

    @Test
    public void testGetAllTables_ThriftMetastoreReturnsAbsent()
    {
        // Setup
        when(mockDelegate.getAllTables("databaseName")).thenReturn(Optional.empty());

        // Run the test
        final Optional<List<String>> result = bridgingHiveMetastoreUnderTest.getAllTables("databaseName");

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testGetAllTables_ThriftMetastoreReturnsNoItems()
    {
        // Setup
        when(mockDelegate.getAllTables("databaseName")).thenReturn(Optional.of(Collections.emptyList()));

        // Run the test
        final Optional<List<String>> result = bridgingHiveMetastoreUnderTest.getAllTables("databaseName");

        // Verify the results
        assertEquals(Optional.of(Collections.emptyList()), result);
    }

    @Test
    public void testDropDatabase2()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));

        // Run the test
        bridgingHiveMetastoreUnderTest.dropDatabase(identity, "databaseName");

        // Verify the results
        verify(mockDelegate).dropDatabase(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName");
    }

    @Test
    public void testCreateTable2()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Table table = new Table("newDatabaseName", "newTableName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("col", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));
        final PrincipalPrivileges principalPrivileges = new PrincipalPrivileges(
                ImmutableMultimap.of("value", new HivePrivilegeInfo(
                        HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(PrincipalType.USER, "name"),
                        new HivePrincipal(
                                PrincipalType.USER, "name"))), ImmutableMultimap.of("value",
                                        new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(
                        PrincipalType.USER, "name"), new HivePrincipal(PrincipalType.USER, "name"))));

        // Run the test
        bridgingHiveMetastoreUnderTest.createTable(identity, table, principalPrivileges);

        // Verify the results
        verify(mockDelegate).createTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())),
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
    }

    @Test
    public void testDropTable()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));

        // Run the test
        bridgingHiveMetastoreUnderTest.dropTable(identity, "databaseName", "tableName", false);

        // Verify the results
        verify(mockDelegate).dropTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName", false);
    }

    @Test
    public void testReplaceTable1()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty()));
        final Table newTable = new Table("newDatabaseName", "newTableName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("col", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));
        final PrincipalPrivileges principalPrivileges = new PrincipalPrivileges(
                ImmutableMultimap.of("value", new HivePrivilegeInfo(
                        HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(PrincipalType.USER, "name"),
                        new HivePrincipal(
                                PrincipalType.USER, "name"))), ImmutableMultimap.of("value",
                                        new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(
                        PrincipalType.USER, "name"), new HivePrincipal(PrincipalType.USER, "name"))));

        // Run the test
        bridgingHiveMetastoreUnderTest.replaceTable(identity, "databaseName", "tableName", newTable,
                principalPrivileges);

        // Verify the results
        verify(mockDelegate).alterTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
    }

    @Test
    public void testReplaceTable2()
    {
        // Setup
        final Table newTable = new Table("newDatabaseName", "newTableName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("col", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                Arrays.asList(new Column("columnName", HIVE_STRING, Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));
        final PrincipalPrivileges principalPrivileges = new PrincipalPrivileges(
                ImmutableMultimap.of("value", new HivePrivilegeInfo(
                        HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(PrincipalType.USER, "name"),
                        new HivePrincipal(
                                PrincipalType.USER, "name"))), ImmutableMultimap.of("value",
                                        new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(
                        PrincipalType.USER, "name"), new HivePrincipal(PrincipalType.USER, "name"))));

        // Run the test
        bridgingHiveMetastoreUnderTest.replaceTable("databaseName", "tableName", newTable, principalPrivileges);

        // Verify the results
        verify(mockDelegate).alterTable(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName",
                new org.apache.hadoop.hive.metastore.api.Table("newTableName", "newDatabaseName", "owner", 0, 0, 0,
                        new StorageDescriptor(
                                Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")),
                                "location", "inputFormat", "outputFormat", false, 0,
                                new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                        Arrays.asList(new FieldSchema("columnName", "HIVE_STRING", "columnComment")), new HashMap<>(),
                        "viewOriginalText", "viewExpandedText", "tableType"));
    }

    @Test
    public void testGetPartitionNames()
    {
        assertEquals(Optional.of(Arrays.asList("value")),
                bridgingHiveMetastoreUnderTest.getPartitionNames(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName"));
        assertEquals(Optional.empty(),
                bridgingHiveMetastoreUnderTest.getPartitionNames(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName"));
        assertEquals(Optional.of(Collections.emptyList()),
                bridgingHiveMetastoreUnderTest.getPartitionNames(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName", "tableName"));
    }

    @Test
    public void testGetPartitionNamesByParts()
    {
        assertEquals(Optional.of(Arrays.asList("value")),
                bridgingHiveMetastoreUnderTest.getPartitionNamesByParts(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName",
                        "tableName",
                        Arrays.asList("value")));
        assertEquals(Optional.empty(),
                bridgingHiveMetastoreUnderTest.getPartitionNamesByParts(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName",
                        "tableName",
                        Arrays.asList("value")));
        assertEquals(Optional.of(Collections.emptyList()),
                bridgingHiveMetastoreUnderTest.getPartitionNamesByParts(new HiveIdentity(new ConnectorIdentity("str", Optional.empty(), Optional.empty())), "databaseName",
                        "tableName",
                        Arrays.asList("value")));
    }

    @Test
    public void testGrantTablePrivileges2()
    {
        // Setup
        final HivePrincipal grantee = new HivePrincipal(PrincipalType.USER, "name");
        final Set<HivePrivilegeInfo> privileges = new HashSet<>(
                Arrays.asList(new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(
                        PrincipalType.USER, "name"), new HivePrincipal(PrincipalType.USER, "name"))));

        // Run the test
        bridgingHiveMetastoreUnderTest.grantTablePrivileges("databaseName", "tableName", grantee, privileges);

        // Verify the results
        verify(mockDelegate).grantTablePrivileges("databaseName", "tableName",
                new HivePrincipal(PrincipalType.USER, "name"), new HashSet<>(
                        Arrays.asList(new HivePrivilegeInfo(
                                HivePrivilegeInfo.HivePrivilege.SELECT, false,
                                new HivePrincipal(PrincipalType.USER, "name"), new HivePrincipal(
                                PrincipalType.USER, "name")))));
    }

    @Test
    public void testRevokeTablePrivileges2()
    {
        // Setup
        final HivePrincipal grantee = new HivePrincipal(PrincipalType.USER, "name");
        final Set<HivePrivilegeInfo> privileges = new HashSet<>(
                Arrays.asList(new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(
                        PrincipalType.USER, "name"), new HivePrincipal(PrincipalType.USER, "name"))));

        // Run the test
        bridgingHiveMetastoreUnderTest.revokeTablePrivileges("databaseName", "tableName", grantee, privileges);

        // Verify the results
        verify(mockDelegate).revokeTablePrivileges("databaseName", "tableName",
                new HivePrincipal(PrincipalType.USER, "name"), new HashSet<>(
                        Arrays.asList(new HivePrivilegeInfo(
                                HivePrivilegeInfo.HivePrivilege.SELECT, false,
                                new HivePrincipal(PrincipalType.USER, "name"), new HivePrincipal(
                                PrincipalType.USER, "name")))));
    }

    @Test
    public void testListTablePrivileges2()
    {
        // Setup
        final HivePrincipal principal = new HivePrincipal(PrincipalType.USER, "name");
        final Set<HivePrivilegeInfo> expectedResult = new HashSet<>(Arrays.asList(new HivePrivilegeInfo(
                HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(PrincipalType.USER, "name"),
                new HivePrincipal(
                        PrincipalType.USER, "name"))));

        // Configure ThriftMetastore.listTablePrivileges(...).
        final Set<HivePrivilegeInfo> hivePrivilegeInfos = new HashSet<>(Arrays.asList(new HivePrivilegeInfo(
                HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(PrincipalType.USER, "name"),
                new HivePrincipal(
                        PrincipalType.USER, "name"))));
        when(mockDelegate.listTablePrivileges("databaseName", "tableName",
                new HivePrincipal(PrincipalType.USER, "name"))).thenReturn(hivePrivilegeInfos);

        // Run the test
        final Set<HivePrivilegeInfo> result = bridgingHiveMetastoreUnderTest.listTablePrivileges("databaseName",
                "tableName", principal);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListTablePrivileges2_ThriftMetastoreReturnsNoItems()
    {
        // Setup
        final HivePrincipal principal = new HivePrincipal(PrincipalType.USER, "name");
        when(mockDelegate.listTablePrivileges("databaseName", "tableName",
                new HivePrincipal(PrincipalType.USER, "name"))).thenReturn(Collections.emptySet());

        // Run the test
        final Set<HivePrivilegeInfo> result = bridgingHiveMetastoreUnderTest.listTablePrivileges("databaseName",
                "tableName", principal);

        // Verify the results
        assertEquals(Collections.emptySet(), result);
    }

    @Test
    public void testIsImpersonationEnabled()
    {
        assertTrue(bridgingHiveMetastoreUnderTest.isImpersonationEnabled());
    }
}
