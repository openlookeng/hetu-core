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
package io.prestosql.plugin.hive.metastore.file;

import com.google.common.collect.ImmutableMultimap;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveBasicStatistics;
import io.prestosql.plugin.hive.HiveBucketProperty;
import io.prestosql.plugin.hive.HiveBucketing;
import io.prestosql.plugin.hive.HivePartition;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.VacuumCleanerTest;
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
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class FileHiveMetastoreTest
{
    @Mock
    private HdfsEnvironment mockHdfsEnvironment;

    private FileHiveMetastore fileHiveMetastoreUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        fileHiveMetastoreUnderTest = new FileHiveMetastore(mockHdfsEnvironment, "catalogDirectory", "metastoreUser");
    }

    @Test
    public void testCreateDatabase() throws Exception
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final Database database = new Database("databaseName", Optional.of(null), "ownerName", PrincipalType.USER,
                Optional.of("value"), new HashMap<>());
        Database.Builder builder = new Database.Builder(database);

        // Run the test
        fileHiveMetastoreUnderTest.createDatabase(identity, database);

        // Verify the results
    }

    @Test
    public void testDropDatabase1()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());

        // Run the test
        fileHiveMetastoreUnderTest.dropDatabase(identity, "databaseName");

        // Verify the results
    }

    @Test
    public void testDropDatabase2()
    {
        // Setup
        // Run the test
        fileHiveMetastoreUnderTest.dropDatabase("databaseName", false);

        // Verify the results
    }

    @Test
    public void testRenameDatabase() throws Exception

    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());

        // Run the test
        fileHiveMetastoreUnderTest.renameDatabase(identity, "databaseName", "databaseName");

        // Verify the results
    }

    @Test
    public void testSetDatabaseOwner()
    {
        // Setup
        final HivePrincipal principal = new HivePrincipal(PrincipalType.USER, "name");

        // Run the test
        fileHiveMetastoreUnderTest.setDatabaseOwner("databaseName", principal);

        // Verify the results
    }

    @Test
    public void testGetDatabase() throws Exception
    {
        // Setup
        final Optional<Database> expectedResult = Optional.of(
                new Database("databaseName", Optional.of("value"), "ownerName", PrincipalType.USER,
                        Optional.of("value"), new HashMap<>()));

        // Run the test
        final Optional<Database> result = fileHiveMetastoreUnderTest.getDatabase("databaseName");

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetAllDatabases()
    {
        // Setup
        // Run the test
        final List<String> result = fileHiveMetastoreUnderTest.getAllDatabases();

        // Verify the results
        assertEquals(Arrays.asList("value"), result);
    }

    @Test
    public void testCreateTable1() throws Exception
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final Table table = new Table("databaseName", "databaseName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));
        final PrincipalPrivileges principalPrivileges = new PrincipalPrivileges(
                ImmutableMultimap.of("value", new HivePrivilegeInfo(
                        HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(PrincipalType.USER, "name"),
                        new HivePrincipal(
                                PrincipalType.USER, "name"))), ImmutableMultimap.of("value",
                                        new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(
                        PrincipalType.USER, "name"), new HivePrincipal(PrincipalType.USER, "name"))));

        // Configure HdfsEnvironment.getFileSystem(...).
        final FileSystem spyFileSystem = spy(FileSystem.get(new Configuration(false)));
        when(mockHdfsEnvironment.getFileSystem(any(HdfsEnvironment.HdfsContext.class),
                eq(new Path("scheme", "authority", "path")))).thenReturn(spyFileSystem);

        // Run the test
        fileHiveMetastoreUnderTest.createTable(identity, table, principalPrivileges);

        // Verify the results
        verify(spyFileSystem).close();
    }

    @Test
    public void testCreateTable1_HdfsEnvironmentThrowsIOException() throws Exception
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final Table table = new Table("databaseName", "databaseName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));
        final PrincipalPrivileges principalPrivileges = new PrincipalPrivileges(
                ImmutableMultimap.of("value", new HivePrivilegeInfo(
                        HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(PrincipalType.USER, "name"),
                        new HivePrincipal(
                                PrincipalType.USER, "name"))), ImmutableMultimap.of("value",
                                        new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(
                        PrincipalType.USER, "name"), new HivePrincipal(PrincipalType.USER, "name"))));
        when(mockHdfsEnvironment.getFileSystem(any(HdfsEnvironment.HdfsContext.class),
                eq(new Path("scheme", "authority", "path"))))
                .thenThrow(IOException.class);

        // Run the test
        fileHiveMetastoreUnderTest.createTable(identity, table, principalPrivileges);

        // Verify the results
    }

    @Test
    public void testGetTable1()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final Optional<Table> expectedResult = Optional.of(
                new Table("databaseName", "databaseName", "owner", "tableType", new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                        Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                        new HashMap<>(), Optional.of("value"), Optional.of("value")));

        // Run the test
        final Optional<Table> result = fileHiveMetastoreUnderTest.getTable(identity, "databaseName", "databaseName");

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetTable2()
    {
        // Setup
        final Optional<Table> expectedResult = Optional.of(
                new Table("databaseName", "databaseName", "owner", "tableType", new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                        Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                        new HashMap<>(), Optional.of("value"), Optional.of("value")));

        // Run the test
        final Optional<Table> result = fileHiveMetastoreUnderTest.getTable("databaseName", "databaseName");

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetSupportedColumnStatistics()
    {
        // Setup
        final Type type = null;

        // Run the test
        final Set<ColumnStatisticType> result = fileHiveMetastoreUnderTest.getSupportedColumnStatistics(type);

        // Verify the results
        assertEquals(new HashSet<>(Arrays.asList(ColumnStatisticType.MIN_VALUE)), result);
    }

    @Test
    public void testGetTableStatistics1()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final Table table = new Table("databaseName", "databaseName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));
        final PartitionStatistics expectedResult = new PartitionStatistics(new HiveBasicStatistics(0L, 0L, 0L, 0L),
                new HashMap<>());

        // Run the test
        final PartitionStatistics result = fileHiveMetastoreUnderTest.getTableStatistics(identity, table);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetPartitionStatistics1()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final Table table = new Table("databaseName", "databaseName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));
        final List<Partition> partitions = Arrays.asList(
                new Partition("databaseName", "tableName", Arrays.asList("value"), new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                        new HashMap<>()));
        final Map<String, PartitionStatistics> expectedResult = new HashMap<>();

        // Run the test
        final Map<String, PartitionStatistics> result = fileHiveMetastoreUnderTest.getPartitionStatistics(identity,
                table, partitions);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testUpdateTableStatistics() throws Exception
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final Function<PartitionStatistics, PartitionStatistics> update = val -> {
            return new PartitionStatistics(new HiveBasicStatistics(0L, 0L, 0L, 0L), new HashMap<>());
        };

        // Run the test
        fileHiveMetastoreUnderTest.updateTableStatistics(identity, "databaseName", "databaseName", update);

        // Verify the results
    }

    @Test
    public void testUpdatePartitionStatistics() throws Exception
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final Function<PartitionStatistics, PartitionStatistics> update = val -> {
            return new PartitionStatistics(new HiveBasicStatistics(0L, 0L, 0L, 0L), new HashMap<>());
        };

        // Run the test
        fileHiveMetastoreUnderTest.updatePartitionStatistics(identity, "databaseName", "databaseName", "partitionName",
                update);

        // Verify the results
    }

    @Test
    public void testUpdatePartitionsStatistics() throws Exception
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final Map<String, Function<PartitionStatistics, PartitionStatistics>> partNamesUpdateFunctionMap = new HashMap<>();

        // Run the test
        fileHiveMetastoreUnderTest.updatePartitionsStatistics(identity, "databaseName", "databaseName",
                partNamesUpdateFunctionMap);

        // Verify the results
    }

    @Test
    public void testCreateTable2() throws Exception
    {
        // Setup
        final Table table = new Table("databaseName", "databaseName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));
        final PrincipalPrivileges principalPrivileges = new PrincipalPrivileges(
                ImmutableMultimap.of("value", new HivePrivilegeInfo(
                        HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(PrincipalType.USER, "name"),
                        new HivePrincipal(
                                PrincipalType.USER, "name"))), ImmutableMultimap.of("value",
                                         new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(
                        PrincipalType.USER, "name"), new HivePrincipal(PrincipalType.USER, "name"))));

        // Configure HdfsEnvironment.getFileSystem(...).
        final FileSystem spyFileSystem = spy(FileSystem.get(new Configuration(false)));
        when(mockHdfsEnvironment.getFileSystem(any(HdfsEnvironment.HdfsContext.class),
                eq(new Path("scheme", "authority", "path")))).thenReturn(spyFileSystem);

        // Run the test
        fileHiveMetastoreUnderTest.createTable(table, principalPrivileges);

        // Verify the results
        verify(spyFileSystem).close();
    }

    @Test
    public void testCreateTable2_HdfsEnvironmentThrowsIOException() throws Exception
    {
        // Setup
        final Table table = new Table("databaseName", "databaseName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));
        final PrincipalPrivileges principalPrivileges = new PrincipalPrivileges(
                ImmutableMultimap.of("value", new HivePrivilegeInfo(
                        HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(PrincipalType.USER, "name"),
                        new HivePrincipal(
                                PrincipalType.USER, "name"))), ImmutableMultimap.of("value",
                                         new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(
                        PrincipalType.USER, "name"), new HivePrincipal(PrincipalType.USER, "name"))));
        when(mockHdfsEnvironment.getFileSystem(any(HdfsEnvironment.HdfsContext.class),
                eq(new Path("scheme", "authority", "path"))))
                .thenThrow(IOException.class);

        // Run the test
        fileHiveMetastoreUnderTest.createTable(table, principalPrivileges);

        // Verify the results
    }

    @Test
    public void testGetAllTables()
    {
        // Setup
        // Run the test
        final Optional<List<String>> result = fileHiveMetastoreUnderTest.getAllTables("databaseName");

        // Verify the results
        assertEquals(Optional.of(Arrays.asList("value")), result);
    }

    @Test
    public void testGetTablesWithParameter()
    {
        // Setup
        // Run the test
        final List<String> result = fileHiveMetastoreUnderTest.getTablesWithParameter("databaseName", "parameterKey",
                "parameterValue");

        // Verify the results
        assertEquals(Arrays.asList("value"), result);
    }

    @Test
    public void testGetAllViews()
    {
        // Setup
        // Run the test
        final Optional<List<String>> result = fileHiveMetastoreUnderTest.getAllViews("databaseName");

        // Verify the results
        assertEquals(Optional.of(Arrays.asList("value")), result);
    }

    @Test
    public void testDropTable() throws Exception
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());

        // Run the test
        fileHiveMetastoreUnderTest.dropTable(identity, "databaseName", "databaseName", false);

        // Verify the results
    }

    @Test
    public void testReplaceTable1()
    {
        // Setup
        final Table newTable = new Table("databaseName", "databaseName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));
        final PrincipalPrivileges principalPrivileges = new PrincipalPrivileges(
                ImmutableMultimap.of("value", new HivePrivilegeInfo(
                        HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(PrincipalType.USER, "name"),
                        new HivePrincipal(
                                PrincipalType.USER, "name"))), ImmutableMultimap.of("value",
                                         new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(
                        PrincipalType.USER, "name"), new HivePrincipal(PrincipalType.USER, "name"))));

        // Run the test
        fileHiveMetastoreUnderTest.replaceTable("databaseName", "databaseName", newTable, principalPrivileges);

        // Verify the results
    }

    @Test
    public void testReplaceTable2()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final Table newTable = new Table("databaseName", "databaseName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));
        final PrincipalPrivileges principalPrivileges = new PrincipalPrivileges(
                ImmutableMultimap.of("value", new HivePrivilegeInfo(
                        HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(PrincipalType.USER, "name"),
                        new HivePrincipal(
                                PrincipalType.USER, "name"))), ImmutableMultimap.of("value",
                                         new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(
                        PrincipalType.USER, "name"), new HivePrincipal(PrincipalType.USER, "name"))));

        // Run the test
        fileHiveMetastoreUnderTest.replaceTable(identity, "databaseName", "databaseName", newTable,
                principalPrivileges);

        // Verify the results
    }

    @Test
    public void testRenameTable() throws Exception
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());

        // Run the test
        fileHiveMetastoreUnderTest.renameTable(identity, "databaseName", "databaseName", "databaseName",
                "databaseName");

        // Verify the results
    }

    @Test
    public void testCommentColumn()
    {
        // Setup
        // Run the test
        fileHiveMetastoreUnderTest.commentColumn("databaseName", "databaseName", "columnName", Optional.of("value"));

        // Verify the results
    }

    @Test
    public void testCommentTable() throws Exception
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());

        // Run the test
        fileHiveMetastoreUnderTest.commentTable(identity, "databaseName", "databaseName", Optional.of("value"));

        // Verify the results
    }

    @Test
    public void testAddColumn() throws Exception
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final HiveType columnType = HiveType.valueOf("hiveTypeName");

        // Run the test
        fileHiveMetastoreUnderTest.addColumn(identity, "databaseName", "databaseName", "columnName", columnType,
                "columnComment");

        // Verify the results
    }

    @Test
    public void testRenameColumn() throws Exception
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());

        // Run the test
        fileHiveMetastoreUnderTest.renameColumn(identity, "databaseName", "databaseName", "columnName", "columnName");

        // Verify the results
    }

    @Test
    public void testDropColumn() throws Exception
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());

        // Run the test
        fileHiveMetastoreUnderTest.dropColumn(identity, "databaseName", "databaseName", "columnName");

        // Verify the results
    }

    @Test
    public void testAddPartitions() throws Exception
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final List<PartitionWithStatistics> partitions = Arrays.asList(new PartitionWithStatistics(
                new Partition("databaseName", "tableName", Arrays.asList("value"), new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                        new HashMap<>()), "partitionName",
                new PartitionStatistics(new HiveBasicStatistics(0L, 0L, 0L, 0L), new HashMap<>()), false));

        // Configure HdfsEnvironment.getFileSystem(...).
        final FileSystem spyFileSystem = spy(FileSystem.get(new Configuration(false)));
        when(mockHdfsEnvironment.getFileSystem(any(HdfsEnvironment.HdfsContext.class),
                eq(new Path("scheme", "authority", "path")))).thenReturn(spyFileSystem);

        // Run the test
        fileHiveMetastoreUnderTest.addPartitions(identity, "databaseName", "databaseName", partitions);

        // Verify the results
        verify(spyFileSystem).close();
    }

    @Test
    public void testAddPartitions_HdfsEnvironmentThrowsIOException() throws Exception
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final List<PartitionWithStatistics> partitions = Arrays.asList(new PartitionWithStatistics(
                new Partition("databaseName", "tableName", Arrays.asList("value"), new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                        new HashMap<>()), "partitionName",
                new PartitionStatistics(new HiveBasicStatistics(0L, 0L, 0L, 0L), new HashMap<>()), false));
        when(mockHdfsEnvironment.getFileSystem(any(HdfsEnvironment.HdfsContext.class),
                eq(new Path("scheme", "authority", "path"))))
                .thenThrow(IOException.class);

        // Run the test
        fileHiveMetastoreUnderTest.addPartitions(identity, "databaseName", "databaseName", partitions);

        // Verify the results
    }

    @Test
    public void testDropPartition()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());

        // Run the test
        fileHiveMetastoreUnderTest.dropPartition(identity, "databaseName", "databaseName", Arrays.asList("value"),
                false);

        // Verify the results
    }

    @Test
    public void testAlterPartition() throws Exception
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final PartitionWithStatistics partitionWithStatistics = new PartitionWithStatistics(
                new Partition("databaseName", "tableName", Arrays.asList("value"), new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                        new HashMap<>()), "partitionName",
                new PartitionStatistics(new HiveBasicStatistics(0L, 0L, 0L, 0L), new HashMap<>()), false);

        // Configure HdfsEnvironment.getFileSystem(...).
        final FileSystem spyFileSystem = spy(FileSystem.get(new Configuration(false)));
        when(mockHdfsEnvironment.getFileSystem(any(HdfsEnvironment.HdfsContext.class),
                eq(new Path("scheme", "authority", "path")))).thenReturn(spyFileSystem);

        // Run the test
        fileHiveMetastoreUnderTest.alterPartition(identity, "databaseName", "databaseName", partitionWithStatistics);

        // Verify the results
        verify(spyFileSystem).close();
    }

    @Test
    public void testAlterPartition_HdfsEnvironmentThrowsIOException() throws Exception
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final PartitionWithStatistics partitionWithStatistics = new PartitionWithStatistics(
                new Partition("databaseName", "tableName", Arrays.asList("value"), new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                        new HashMap<>()), "partitionName",
                new PartitionStatistics(new HiveBasicStatistics(0L, 0L, 0L, 0L), new HashMap<>()), false);
        when(mockHdfsEnvironment.getFileSystem(any(HdfsEnvironment.HdfsContext.class),
                eq(new Path("scheme", "authority", "path"))))
                .thenThrow(IOException.class);

        // Run the test
        fileHiveMetastoreUnderTest.alterPartition(identity, "databaseName", "databaseName", partitionWithStatistics);

        // Verify the results
    }

    @Test
    public void testCreateRole() throws Exception
    {
        // Setup
        // Run the test
        fileHiveMetastoreUnderTest.createRole("role", "grantor");

        // Verify the results
    }

    @Test
    public void testDropRole()
    {
        // Setup
        // Run the test
        fileHiveMetastoreUnderTest.dropRole("role");

        // Verify the results
    }

    @Test
    public void testListRoles()
    {
        // Setup
        // Run the test
        final Set<String> result = fileHiveMetastoreUnderTest.listRoles();

        // Verify the results
        assertEquals(new HashSet<>(Arrays.asList("value")), result);
    }

    @Test
    public void testGrantRoles() throws Exception
    {
        // Setup
        final Set<HivePrincipal> grantees = new HashSet<>(
                Arrays.asList(new HivePrincipal(PrincipalType.USER, "name")));
        final HivePrincipal grantor = new HivePrincipal(PrincipalType.USER, "name");

        // Run the test
        fileHiveMetastoreUnderTest.grantRoles(new HashSet<>(Arrays.asList("value")), grantees, false, grantor);

        // Verify the results
    }

    @Test
    public void testRevokeRoles()
    {
        // Setup
        final Set<HivePrincipal> grantees = new HashSet<>(
                Arrays.asList(new HivePrincipal(PrincipalType.USER, "name")));
        final HivePrincipal grantor = new HivePrincipal(PrincipalType.USER, "name");

        // Run the test
        fileHiveMetastoreUnderTest.revokeRoles(new HashSet<>(Arrays.asList("value")), grantees, false, grantor);

        // Verify the results
    }

    @Test
    public void testListRoleGrants()
    {
        // Setup
        final HivePrincipal principal = new HivePrincipal(PrincipalType.USER, "name");
        final Set<RoleGrant> expectedResult = new HashSet<>(
                Arrays.asList(new RoleGrant(new PrestoPrincipal(PrincipalType.USER, "name"), "role", false)));

        // Run the test
        final Set<RoleGrant> result = fileHiveMetastoreUnderTest.listRoleGrants(principal);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetPartitionNames()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());

        // Run the test
        final Optional<List<String>> result = fileHiveMetastoreUnderTest.getPartitionNames(identity, "databaseName",
                "databaseName");

        // Verify the results
        assertEquals(Optional.of(Arrays.asList("value")), result);
    }

    @Test
    public void testGetPartition() throws Exception
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final Optional<Partition> expectedResult = Optional.of(
                new Partition("databaseName", "tableName", Arrays.asList("value"), new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                        new HashMap<>()));

        // Run the test
        final Optional<Partition> result = fileHiveMetastoreUnderTest.getPartition(identity, "databaseName",
                "databaseName",
                Arrays.asList("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetPartitionNamesByParts()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());

        // Run the test
        final Optional<List<String>> result = fileHiveMetastoreUnderTest.getPartitionNamesByParts(identity,
                "databaseName", "databaseName",
                Arrays.asList("value"));

        // Verify the results
        assertEquals(Optional.of(Arrays.asList("value")), result);
    }

    @Test
    public void testGetPartitionsByNames() throws Exception
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final Map<String, Optional<Partition>> expectedResult = new HashMap<>();

        // Run the test
        final Map<String, Optional<Partition>> result = fileHiveMetastoreUnderTest.getPartitionsByNames(identity,
                "databaseName", "databaseName",
                Arrays.asList("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListTablePrivileges()
    {
        // Setup
        final HivePrincipal principal = new HivePrincipal(PrincipalType.USER, "name");
        final Set<HivePrivilegeInfo> expectedResult = new HashSet<>(Arrays.asList(new HivePrivilegeInfo(
                HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(PrincipalType.USER, "name"),
                new HivePrincipal(
                        PrincipalType.USER, "name"))));

        // Run the test
        final Set<HivePrivilegeInfo> result = fileHiveMetastoreUnderTest.listTablePrivileges("databaseName",
                "databaseName", principal);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGrantTablePrivileges() throws Exception
    {
        // Setup
        final HivePrincipal grantee = new HivePrincipal(PrincipalType.USER, "name");
        final Set<HivePrivilegeInfo> privileges = new HashSet<>(
                Arrays.asList(new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(
                        PrincipalType.USER, "name"), new HivePrincipal(PrincipalType.USER, "name"))));

        // Run the test
        fileHiveMetastoreUnderTest.grantTablePrivileges("databaseName", "databaseName", grantee, privileges);

        // Verify the results
    }

    @Test
    public void testRevokeTablePrivileges()
    {
        // Setup
        final HivePrincipal grantee = new HivePrincipal(PrincipalType.USER, "name");
        final Set<HivePrivilegeInfo> privileges = new HashSet<>(
                Arrays.asList(new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(
                        PrincipalType.USER, "name"), new HivePrincipal(PrincipalType.USER, "name"))));

        // Run the test
        fileHiveMetastoreUnderTest.revokeTablePrivileges("databaseName", "databaseName", grantee, privileges);

        // Verify the results
    }

    @Test
    public void testIsImpersonationEnabled()
    {
        assertTrue(fileHiveMetastoreUnderTest.isImpersonationEnabled());
    }

    @Test
    public void testOpenTransaction() throws Exception
    {
        assertEquals(0L, fileHiveMetastoreUnderTest.openTransaction(new HiveIdentity(new VacuumCleanerTest.ConnectorSession())));
    }

    @Test
    public void testCommitTransaction()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());

        // Run the test
        fileHiveMetastoreUnderTest.commitTransaction(identity, 0L);

        // Verify the results
    }

    @Test
    public void testAbortTransaction()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());

        // Run the test
        fileHiveMetastoreUnderTest.abortTransaction(identity, 0L);

        // Verify the results
    }

    @Test
    public void testSendTransactionHeartbeat()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());

        // Run the test
        fileHiveMetastoreUnderTest.sendTransactionHeartbeat(identity, 0L);

        // Verify the results
    }

    @Test
    public void testAcquireSharedReadLock()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final List<SchemaTableName> fullTables = Arrays.asList(new SchemaTableName("databaseName", "databaseName"));
        final List<HivePartition> partitions = Arrays.asList(
                new HivePartition(new SchemaTableName("databaseName", "databaseName"), "partitionId", new HashMap<>()));

        // Run the test
        fileHiveMetastoreUnderTest.acquireSharedReadLock(identity, "queryId", 0L, fullTables, partitions);

        // Verify the results
    }

    @Test
    public void testAcquireLock()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final List<SchemaTableName> fullTables = Arrays.asList(new SchemaTableName("databaseName", "databaseName"));
        final List<HivePartition> partitions = Arrays.asList(
                new HivePartition(new SchemaTableName("databaseName", "databaseName"), "partitionId", new HashMap<>()));

        // Run the test
        fileHiveMetastoreUnderTest.acquireLock(identity, "queryId", 0L, fullTables, partitions,
                DataOperationType.SELECT);

        // Verify the results
    }

    @Test
    public void testGetValidWriteIds()
    {
        // Setup
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final List<SchemaTableName> tables = Arrays.asList(new SchemaTableName("databaseName", "databaseName"));

        // Run the test
        final String result = fileHiveMetastoreUnderTest.getValidWriteIds(identity, tables, 0L, false);

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testShowLocks()
    {
        // Setup
        final ShowLocksRequest rqst = new ShowLocksRequest();
        final ShowLocksResponse expectedResult = new ShowLocksResponse(
                Arrays.asList(
                        new ShowLocksResponseElement(0L, "dbname", LockState.ACQUIRED, LockType.SHARED_READ, 0L, "user",
                                "hostname")));

        // Run the test
        final ShowLocksResponse result = fileHiveMetastoreUnderTest.showLocks(rqst);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetTableWriteId() throws Exception
    {
        assertEquals(0L, fileHiveMetastoreUnderTest.getTableWriteId("dbName", "tableName", 0L));
    }

    @Test
    public void testCreateTestingFileHiveMetastore()
    {
        // Setup
        final File catalogDirectory = new File("filename.txt");

        // Run the test
        final FileHiveMetastore result = FileHiveMetastore.createTestingFileHiveMetastore(catalogDirectory);
        assertEquals(Optional.of(new Database("databaseName", Optional.of("value"), "ownerName", PrincipalType.USER,
                Optional.of("value"), new HashMap<>())), result.getDatabase("databaseName"));
        assertEquals(Arrays.asList("value"), result.getAllDatabases());
        final HiveIdentity identity = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        assertEquals(
                Optional.of(new Table("databaseName", "databaseName", "owner", "tableType", new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                        Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                        new HashMap<>(), Optional.of("value"), Optional.of("value"))),
                result.getTable(identity, "databaseName", "databaseName"));
        assertEquals(
                Optional.of(new Table("databaseName", "databaseName", "owner", "tableType", new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                        Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                        new HashMap<>(), Optional.of("value"), Optional.of("value"))),
                result.getTable("databaseName", "databaseName"));
        final Type type = null;
        assertEquals(new HashSet<>(Arrays.asList(ColumnStatisticType.MIN_VALUE)),
                result.getSupportedColumnStatistics(type));
        final HiveIdentity identity1 = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final Table table = new Table("databaseName", "databaseName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));
        assertEquals(new PartitionStatistics(new HiveBasicStatistics(0L, 0L, 0L, 0L), new HashMap<>()),
                result.getTableStatistics(identity1, table));
        final HiveIdentity identity2 = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final Table table1 = new Table("databaseName", "databaseName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));
        final List<Partition> partitions = Arrays.asList(
                new Partition("databaseName", "tableName", Arrays.asList("value"), new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                        new HashMap<>()));
        assertEquals(new HashMap<>(), result.getPartitionStatistics(identity2, table1, partitions));
        assertEquals(Optional.of(Arrays.asList("value")), result.getAllTables("databaseName"));
        assertEquals(Arrays.asList("value"),
                result.getTablesWithParameter("databaseName", "parameterKey", "parameterValue"));
        assertEquals(Optional.of(Arrays.asList("value")), result.getAllViews("databaseName"));
        assertEquals(new HashSet<>(Arrays.asList("value")), result.listRoles());
        final HivePrincipal principal = new HivePrincipal(PrincipalType.USER, "name");
        assertEquals(new HashSet<>(
                        Arrays.asList(new RoleGrant(new PrestoPrincipal(PrincipalType.USER, "name"), "role", false))),
                result.listRoleGrants(principal));
        final HiveIdentity identity3 = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        assertEquals(Optional.of(Arrays.asList("value")),
                result.getPartitionNames(identity3, "databaseName", "databaseName"));
        final HiveIdentity identity4 = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        assertEquals(Optional.of(new Partition("databaseName", "tableName", Arrays.asList("value"), new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "catalogDirectory",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("columnName", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                new HashMap<>())), result.getPartition(identity4, "databaseName", "databaseName",
                Arrays.asList("value")));
        final HiveIdentity identity5 = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        assertEquals(Optional.of(Arrays.asList("value")),
                result.getPartitionNamesByParts(identity5, "databaseName", "databaseName",
                        Arrays.asList("value")));
        final HiveIdentity identity6 = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        assertEquals(new HashMap<>(), result.getPartitionsByNames(identity6, "databaseName", "databaseName",
                Arrays.asList("value")));
        final HivePrincipal principal1 = new HivePrincipal(PrincipalType.USER, "name");
        assertEquals(new HashSet<>(
                        Arrays.asList(new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(
                                PrincipalType.USER, "name"), new HivePrincipal(PrincipalType.USER, "name")))),
                result.listTablePrivileges("databaseName", "databaseName", principal1));
        assertTrue(result.isImpersonationEnabled());
        assertEquals(0L, result.openTransaction(new HiveIdentity(new VacuumCleanerTest.ConnectorSession())));
        final HiveIdentity identity7 = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        final List<SchemaTableName> tables = Arrays.asList(new SchemaTableName("databaseName", "databaseName"));
        assertEquals("result", result.getValidWriteIds(identity7, tables, 0L, false));
        final ShowLocksRequest rqst = new ShowLocksRequest();
        assertEquals(new ShowLocksResponse(
                Arrays.asList(
                        new ShowLocksResponseElement(0L, "dbname", LockState.ACQUIRED, LockType.SHARED_READ, 0L, "user",
                                "hostname"))), result.showLocks(rqst));
        assertEquals(0L, result.getTableWriteId("dbName", "tableName", 0L));
        assertEquals(Optional.of("value"), result.getConfigValue("name"));
        final HivePrincipal principal2 = new HivePrincipal(PrincipalType.USER, "name");
        assertEquals(new HashSet<>(
                        Arrays.asList(new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(
                                PrincipalType.USER, "name"), new HivePrincipal(PrincipalType.USER, "name")))),
                result.listColumnPrivileges("databaseName", "tableName", "columnName", principal2));
        final HivePrincipal principal3 = new HivePrincipal(PrincipalType.USER, "name");
        assertEquals(new HashSet<>(
                        Arrays.asList(new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(
                                PrincipalType.USER, "name"), new HivePrincipal(PrincipalType.USER, "name")))),
                result.listSchemaPrivileges("databaseName", "tableName", principal3));
        assertEquals(Optional.of(Arrays.asList("value")), result.getPartitionNamesByFilter("databaseName", "tableName",
                Arrays.asList("value"), TupleDomain.withColumnDomains(new HashMap<>())));
        assertEquals(new HashSet<>(
                        Arrays.asList(new RoleGrant(new PrestoPrincipal(PrincipalType.USER, "name"), "role", false))),
                result.listGrantedPrincipals("role"));
        final Optional<HivePrincipal> principal4 = Optional.of(new HivePrincipal(PrincipalType.USER, "name"));
        assertEquals(new HashSet<>(
                        Arrays.asList(new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(
                                PrincipalType.USER, "name"), new HivePrincipal(PrincipalType.USER, "name")))),
                result.listTablePrivileges("databaseName", "tableName",
                        Optional.of("value"), principal4));
        assertEquals(0L, result.openTransaction(new AcidTransactionOwner("owner")));
        assertEquals("result", result.getValidWriteIds(
                Arrays.asList(new SchemaTableName("databaseName", "databaseName")), 0L));
        assertEquals(0L, result.allocateWriteId("dbName", "tableName", 0L));
    }
}
