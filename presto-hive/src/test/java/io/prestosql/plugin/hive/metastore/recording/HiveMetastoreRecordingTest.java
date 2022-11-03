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
package io.prestosql.plugin.hive.metastore.recording;

import io.airlift.json.JsonCodec;
import io.prestosql.plugin.hive.HiveBasicStatistics;
import io.prestosql.plugin.hive.HiveBucketProperty;
import io.prestosql.plugin.hive.HiveBucketing;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.RecordingMetastoreConfig;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.HivePartitionName;
import io.prestosql.plugin.hive.metastore.HivePrincipal;
import io.prestosql.plugin.hive.metastore.HivePrivilegeInfo;
import io.prestosql.plugin.hive.metastore.HiveTableName;
import io.prestosql.plugin.hive.metastore.Partition;
import io.prestosql.plugin.hive.metastore.PartitionFilter;
import io.prestosql.plugin.hive.metastore.SortingColumn;
import io.prestosql.plugin.hive.metastore.Storage;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.metastore.TablesWithParameterCacheKey;
import io.prestosql.plugin.hive.metastore.UserTableKey;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.statistics.ColumnStatisticType;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class HiveMetastoreRecordingTest
{
    @Mock
    private RecordingMetastoreConfig mockConfig;
    @Mock
    private JsonCodec<HiveMetastoreRecording.Recording> mockRecordingCodec;

    private HiveMetastoreRecording hiveMetastoreRecordingUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        RecordingMetastoreConfig path = new RecordingMetastoreConfig().setRecordingPath("path");
        hiveMetastoreRecordingUnderTest = new HiveMetastoreRecording(path, mockRecordingCodec);
    }

    @Test
    public void testLoadRecording() throws Exception
    {
        hiveMetastoreRecordingUnderTest.loadRecording();

        // Verify the results
    }

    @Test
    public void testGetDatabase() throws Exception
    {
        // Setup
        final Supplier<Optional<Database>> valueSupplier = () -> Optional.of(
                new Database("databaseName", Optional.of("value"), "ownerName", PrincipalType.USER,
                        Optional.of("value"), new HashMap<>()));
        final Optional<Database> expectedResult = Optional.of(
                new Database("databaseName", Optional.of("value"), "ownerName", PrincipalType.USER,
                        Optional.of("value"), new HashMap<>()));

        // Run the test
        final Optional<Database> result = hiveMetastoreRecordingUnderTest.getDatabase("databaseName", valueSupplier);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetAllDatabases()
    {
        // Setup
        // Run the test
        final List<String> result = hiveMetastoreRecordingUnderTest.getAllDatabases(() -> Arrays.asList("value"));

        // Verify the results
        assertEquals(Arrays.asList("value"), result);
    }

    @Test
    public void testGetTable() throws Exception
    {
        // Setup
        final HiveTableName hiveTableName = new HiveTableName("databaseName", "tableName");
        final Supplier<Optional<Table>> valueSupplier = () -> Optional.of(
                new Table("databaseName", "tableName", "owner", "tableType", new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("name", HiveType.HIVE_BYTE, Optional.of("value"))),
                        Arrays.asList(new Column("name", HiveType.HIVE_BYTE, Optional.of("value"))),
                        new HashMap<>(), Optional.of("value"), Optional.of("value")));
        final Optional<Table> expectedResult = Optional.of(
                new Table("databaseName", "tableName", "owner", "tableType", new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("name", HiveType.HIVE_BYTE, Optional.of("value"))),
                        Arrays.asList(new Column("name", HiveType.HIVE_BYTE, Optional.of("value"))),
                        new HashMap<>(), Optional.of("value"), Optional.of("value")));

        // Run the test
        final Optional<Table> result = hiveMetastoreRecordingUnderTest.getTable(hiveTableName, valueSupplier);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetSupportedColumnStatistics()
    {
        assertEquals(new HashSet<>(Arrays.asList(ColumnStatisticType.MIN_VALUE)),
                hiveMetastoreRecordingUnderTest.getSupportedColumnStatistics("type", () -> new HashSet<>(
                        Arrays.asList(ColumnStatisticType.MIN_VALUE))));
        assertEquals(
                Collections.emptySet(),
                hiveMetastoreRecordingUnderTest.getSupportedColumnStatistics("type", () -> new HashSet<>(
                        Arrays.asList(ColumnStatisticType.MIN_VALUE))));
    }

    @Test
    public void testGetTableStatistics() throws Exception
    {
        // Setup
        final HiveTableName hiveTableName = new HiveTableName("databaseName", "tableName");
        final Supplier<PartitionStatistics> valueSupplier = () -> new PartitionStatistics(
                new HiveBasicStatistics(0L, 0L, 0L, 0L), new HashMap<>());
        final PartitionStatistics expectedResult = new PartitionStatistics(new HiveBasicStatistics(0L, 0L, 0L, 0L),
                new HashMap<>());

        // Run the test
        final PartitionStatistics result = hiveMetastoreRecordingUnderTest.getTableStatistics(hiveTableName,
                valueSupplier);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetPartitionStatistics()
    {
        // Setup
        final Set<HivePartitionName> partitionNames = new HashSet<>(
                Arrays.asList(
                        new HivePartitionName(new HiveTableName("databaseName", "tableName"), Arrays.asList("value"),
                                Optional.of("value"))));
        final Supplier<Map<String, PartitionStatistics>> valueSupplier = () -> new HashMap<>();
        final Map<String, PartitionStatistics> expectedResult = new HashMap<>();

        // Run the test
        final Map<String, PartitionStatistics> result = hiveMetastoreRecordingUnderTest.getPartitionStatistics(
                partitionNames, valueSupplier);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetAllTables()
    {
        assertEquals(Arrays.asList("value"),
                hiveMetastoreRecordingUnderTest.getAllTables("databaseName", () -> Optional.of(Arrays.asList("value"))));
        assertEquals(Collections.emptyList(),
                hiveMetastoreRecordingUnderTest.getAllTables("databaseName", () -> Optional.of(Arrays.asList("value"))));
    }

    @Test
    public void testGetTablesWithParameter()
    {
        // Setup
        final TablesWithParameterCacheKey tablesWithParameterCacheKey = new TablesWithParameterCacheKey("databaseName",
                "parameterKey", "parameterValue");

        // Run the test
        final List<String> result = hiveMetastoreRecordingUnderTest.getTablesWithParameter(tablesWithParameterCacheKey,
                () -> Arrays.asList("value"));

        // Verify the results
        assertEquals(Arrays.asList("value"), result);
    }

    @Test
    public void testGetAllViews()
    {
        assertEquals(Arrays.asList("value"),
                hiveMetastoreRecordingUnderTest.getAllViews("databaseName", () -> Optional.of(Arrays.asList("value"))));
        assertEquals(Collections.emptyList(),
                hiveMetastoreRecordingUnderTest.getAllViews("databaseName", () -> Optional.of(Arrays.asList("value"))));
    }

    @Test
    public void testGetPartition() throws Exception
    {
        // Setup
        final HivePartitionName hivePartitionName = new HivePartitionName(
                new HiveTableName("databaseName", "tableName"), Arrays.asList("value"), Optional.of("value"));
        final Supplier<Optional<Partition>> valueSupplier = () -> Optional.of(
                new Partition("databaseName", "tableName", Arrays.asList("value"), new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("name", HiveType.HIVE_BYTE, Optional.of("value"))),
                        new HashMap<>()));
        final Optional<Partition> expectedResult = Optional.of(
                new Partition("databaseName", "tableName", Arrays.asList("value"), new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("name", HiveType.HIVE_BYTE, Optional.of("value"))),
                        new HashMap<>()));

        // Run the test
        final Optional<Partition> result = hiveMetastoreRecordingUnderTest.getPartition(hivePartitionName,
                valueSupplier);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetPartitionNamesByFilter()
    {
        // Setup
        final PartitionFilter partitionFilter = new PartitionFilter(new HiveTableName("databaseName", "tableName"),
                Arrays.asList("value"));

        // Run the test
        final Optional<List<String>> result = hiveMetastoreRecordingUnderTest.getPartitionNamesByFilter(partitionFilter,
                () -> Optional.of(
                        Arrays.asList("value")));

        // Verify the results
        assertEquals(Optional.of(Arrays.asList("value")), result);
    }

    @Test
    public void testGetPartitionsByNames() throws Exception
    {
        // Setup
        final Set<HivePartitionName> partitionNames = new HashSet<>(
                Arrays.asList(
                        new HivePartitionName(new HiveTableName("databaseName", "tableName"), Arrays.asList("value"),
                                Optional.of("value"))));
        final Supplier<Map<String, Optional<Partition>>> valueSupplier = () -> new HashMap<>();
        final Map<String, Optional<Partition>> expectedResult = new HashMap<>();

        // Run the test
        final Map<String, Optional<Partition>> result = hiveMetastoreRecordingUnderTest.getPartitionsByNames(
                partitionNames, valueSupplier);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListTablePrivileges()
    {
        // Setup
        final UserTableKey userTableKey = new UserTableKey(new HivePrincipal(PrincipalType.USER, "name"), "database",
                "table", "column");
        final Supplier<Set<HivePrivilegeInfo>> valueSupplier = () -> new HashSet<>(Arrays.asList(new HivePrivilegeInfo(
                HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(PrincipalType.USER, "name"),
                new HivePrincipal(
                        PrincipalType.USER, "name"))));
        final Set<HivePrivilegeInfo> expectedResult = new HashSet<>(Arrays.asList(new HivePrivilegeInfo(
                HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(PrincipalType.USER, "name"),
                new HivePrincipal(
                        PrincipalType.USER, "name"))));

        // Run the test
        final Set<HivePrivilegeInfo> result = hiveMetastoreRecordingUnderTest.listTablePrivileges(userTableKey,
                valueSupplier);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListRoles()
    {
        // Setup
        // Run the test
        final Set<String> result = hiveMetastoreRecordingUnderTest.listRoles(
                () -> new HashSet<>(Arrays.asList("value")));

        // Verify the results
        assertEquals(new HashSet<>(Arrays.asList("value")), result);
    }

    @Test
    public void testListGrantedPrincipals()
    {
        // Setup
        final Supplier<Set<RoleGrant>> valueSupplier = () -> new HashSet<>(
                Arrays.asList(new RoleGrant(new PrestoPrincipal(PrincipalType.USER, "name"), "roleName", false)));
        final Set<RoleGrant> expectedResult = new HashSet<>(
                Arrays.asList(new RoleGrant(new PrestoPrincipal(PrincipalType.USER, "name"), "roleName", false)));

        // Run the test
        final Set<RoleGrant> result = hiveMetastoreRecordingUnderTest.listGrantedPrincipals("role", valueSupplier);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListRoleGrants()
    {
        // Setup
        final HivePrincipal principal = new HivePrincipal(PrincipalType.USER, "name");
        final Supplier<Set<RoleGrant>> valueSupplier = () -> new HashSet<>(
                Arrays.asList(new RoleGrant(new PrestoPrincipal(PrincipalType.USER, "name"), "roleName", false)));
        final Set<RoleGrant> expectedResult = new HashSet<>(
                Arrays.asList(new RoleGrant(new PrestoPrincipal(PrincipalType.USER, "name"), "roleName", false)));

        // Run the test
        final Set<RoleGrant> result = hiveMetastoreRecordingUnderTest.listRoleGrants(principal, valueSupplier);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testWriteRecording() throws Exception
    {
        // Setup
        when(mockRecordingCodec.toJsonBytes(any(HiveMetastoreRecording.Recording.class)))
                .thenReturn("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        hiveMetastoreRecordingUnderTest.writeRecording();

        // Verify the results
    }

    @Test
    public void testWriteRecording_JsonCodecThrowsIllegalArgumentException() throws Exception
    {
        // Setup
        when(mockRecordingCodec.toJsonBytes(any(HiveMetastoreRecording.Recording.class)))
                .thenThrow(IllegalArgumentException.class);

        // Run the test
        hiveMetastoreRecordingUnderTest.writeRecording();

        // Verify the results
    }
}
