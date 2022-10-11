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
package io.prestosql.plugin.hive.metastore.thrift;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class ThriftHiveMetastoreClientTest
{
    @Mock
    private TTransport mockTransport;

    private ThriftHiveMetastoreClient thriftHiveMetastoreClientUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        thriftHiveMetastoreClientUnderTest = new ThriftHiveMetastoreClient(mockTransport, "hostname");
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        thriftHiveMetastoreClientUnderTest.close();
    }

    @Test
    public void testGetAllDatabases() throws Exception
    {
        // Setup
        // Run the test
        final List<String> result = thriftHiveMetastoreClientUnderTest.getAllDatabases();
    }

    @Test
    public void testGetAllDatabases_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.getAllDatabases());
    }

    @Test
    public void testGetDatabase() throws Exception
    {
        // Setup
        final Database expectedResult = new Database("name", "description", "locationUri", new HashMap<>());

        // Run the test
        final Database result = thriftHiveMetastoreClientUnderTest.getDatabase("dbName");

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetDatabase_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.getDatabase("dbName"));
    }

    @Test
    public void testGetAllTables() throws Exception
    {
        // Setup
        // Run the test
        final List<String> result = thriftHiveMetastoreClientUnderTest.getAllTables("databaseName");

        // Verify the results
        assertEquals(Arrays.asList("value"), result);
    }

    @Test
    public void testGetAllTables_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.getAllTables("databaseName"));
    }

    @Test
    public void testGetTableNamesByFilter() throws Exception
    {
        // Setup
        // Run the test
        final List<String> result = thriftHiveMetastoreClientUnderTest.getTableNamesByFilter("databaseName", "filter");

        // Verify the results
        assertEquals(Arrays.asList("value"), result);
    }

    @Test
    public void testGetTableNamesByFilter_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class,
                () -> thriftHiveMetastoreClientUnderTest.getTableNamesByFilter("databaseName", "filter"));
    }

    @Test
    public void testCreateDatabase() throws Exception
    {
        // Setup
        final Database database = new Database("name", "description", "locationUri", new HashMap<>());

        // Run the test
        thriftHiveMetastoreClientUnderTest.createDatabase(database);

        // Verify the results
    }

    @Test
    public void testCreateDatabase_ThrowsTException()
    {
        // Setup
        final Database database = new Database("name", "description", "locationUri", new HashMap<>());

        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.createDatabase(database));
    }

    @Test
    public void testDropDatabase() throws Exception
    {
        // Setup
        // Run the test
        thriftHiveMetastoreClientUnderTest.dropDatabase("databaseName", false, false);

        // Verify the results
    }

    @Test
    public void testDropDatabase_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(
                TException.class, () -> thriftHiveMetastoreClientUnderTest.dropDatabase("databaseName", false, false));
    }

    @Test
    public void testAlterDatabase() throws Exception
    {
        // Setup
        final Database database = new Database("name", "description", "locationUri", new HashMap<>());

        // Run the test
        thriftHiveMetastoreClientUnderTest.alterDatabase("databaseName", database);

        // Verify the results
    }

    @Test
    public void testAlterDatabase_ThrowsTException()
    {
        // Setup
        final Database database = new Database("name", "description", "locationUri", new HashMap<>());

        // Run the test
        assertThrows(
                TException.class, () -> thriftHiveMetastoreClientUnderTest.alterDatabase("databaseName", database));
    }

    @Test
    public void testCreateTable() throws Exception
    {
        // Setup
        final Table table = new Table("tableName", "dbName", "owner", 0, 0, 0, new StorageDescriptor(
                Arrays.asList(new FieldSchema("name", "type", "comment")), "location", "inputFormat", "outputFormat",
                false, 0, new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                Arrays.asList(new FieldSchema("name", "type", "comment")), new HashMap<>(), "viewOriginalText",
                "viewExpandedText", "tableType");

        // Run the test
        thriftHiveMetastoreClientUnderTest.createTable(table);

        // Verify the results
    }

    @Test
    public void testCreateTable_ThrowsTException()
    {
        // Setup
        final Table table = new Table("tableName", "dbName", "owner", 0, 0, 0, new StorageDescriptor(
                Arrays.asList(new FieldSchema("name", "type", "comment")), "location", "inputFormat", "outputFormat",
                false, 0, new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                Arrays.asList(new FieldSchema("name", "type", "comment")), new HashMap<>(), "viewOriginalText",
                "viewExpandedText", "tableType");

        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.createTable(table));
    }

    @Test
    public void testDropTable() throws Exception
    {
        // Setup
        // Run the test
        thriftHiveMetastoreClientUnderTest.dropTable("databaseName", "name", false);

        // Verify the results
    }

    @Test
    public void testDropTable_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(
                TException.class, () -> thriftHiveMetastoreClientUnderTest.dropTable("databaseName", "name", false));
    }

    @Test
    public void testAlterTable() throws Exception
    {
        // Setup
        final Table newTable = new Table("tableName", "dbName", "owner", 0, 0, 0, new StorageDescriptor(
                Arrays.asList(new FieldSchema("name", "type", "comment")), "location", "inputFormat", "outputFormat",
                false, 0, new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                Arrays.asList(new FieldSchema("name", "type", "comment")), new HashMap<>(), "viewOriginalText",
                "viewExpandedText", "tableType");

        // Run the test
        thriftHiveMetastoreClientUnderTest.alterTable("databaseName", "tableName", newTable);

        // Verify the results
    }

    @Test
    public void testAlterTable_ThrowsTException()
    {
        // Setup
        final Table newTable = new Table("tableName", "dbName", "owner", 0, 0, 0, new StorageDescriptor(
                Arrays.asList(new FieldSchema("name", "type", "comment")), "location", "inputFormat", "outputFormat",
                false, 0, new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                Arrays.asList(new FieldSchema("name", "type", "comment")), new HashMap<>(), "viewOriginalText",
                "viewExpandedText", "tableType");

        // Run the test
        assertThrows(
                TException.class,
                () -> thriftHiveMetastoreClientUnderTest.alterTable("databaseName", "tableName", newTable));
    }

    @Test
    public void testGetTable() throws Exception
    {
        // Setup
        final Table expectedResult = new Table("tableName", "dbName", "owner", 0, 0, 0, new StorageDescriptor(
                Arrays.asList(new FieldSchema("name", "type", "comment")), "location", "inputFormat", "outputFormat",
                false, 0, new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                Arrays.asList(new FieldSchema("name", "type", "comment")), new HashMap<>(), "viewOriginalText",
                "viewExpandedText", "tableType");

        // Run the test
        final Table result = thriftHiveMetastoreClientUnderTest.getTable("databaseName", "tableName");

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetTable_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.getTable("databaseName", "tableName"));
    }

    @Test
    public void testGetTableWithCapabilities() throws Exception
    {
        // Setup
        final Table expectedResult = new Table("tableName", "dbName", "owner", 0, 0, 0, new StorageDescriptor(
                Arrays.asList(new FieldSchema("name", "type", "comment")), "location", "inputFormat", "outputFormat",
                false, 0, new SerDeInfo("name", "serializationLib", new HashMap<>()), Arrays.asList("value"),
                Arrays.asList(new Order("col", 0)), new HashMap<>()),
                Arrays.asList(new FieldSchema("name", "type", "comment")), new HashMap<>(), "viewOriginalText",
                "viewExpandedText", "tableType");

        // Run the test
        final Table result = thriftHiveMetastoreClientUnderTest.getTableWithCapabilities("databaseName", "tableName");

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetTableWithCapabilities_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class,
                () -> thriftHiveMetastoreClientUnderTest.getTableWithCapabilities("databaseName", "tableName"));
    }

    @Test
    public void testGetFields() throws Exception
    {
        // Setup
        final List<FieldSchema> expectedResult = Arrays.asList(new FieldSchema("name", "type", "comment"));

        // Run the test
        final List<FieldSchema> result = thriftHiveMetastoreClientUnderTest.getFields("databaseName", "tableName");

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetFields_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.getFields("databaseName", "tableName"));
    }

    @Test
    public void testGetTableColumnStatistics() throws Exception
    {
        // Setup
        final List<ColumnStatisticsObj> expectedResult = Arrays.asList(
                new ColumnStatisticsObj("colName", "colType", new ColumnStatisticsData(
                        ColumnStatisticsData._Fields.BOOLEAN_STATS, "value")));

        // Run the test
        final List<ColumnStatisticsObj> result = thriftHiveMetastoreClientUnderTest.getTableColumnStatistics(
                "databaseName", "tableName",
                Arrays.asList("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetTableColumnStatistics_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class,
                () -> thriftHiveMetastoreClientUnderTest.getTableColumnStatistics("databaseName", "tableName",
                        Arrays.asList("value")));
    }

    @Test
    public void testSetTableColumnStatistics() throws Exception
    {
        // Setup
        final List<ColumnStatisticsObj> statistics = Arrays.asList(
                new ColumnStatisticsObj("colName", "colType", new ColumnStatisticsData(
                        ColumnStatisticsData._Fields.BOOLEAN_STATS, "value")));

        // Run the test
        thriftHiveMetastoreClientUnderTest.setTableColumnStatistics("databaseName", "tableName", statistics);

        // Verify the results
    }

    @Test
    public void testSetTableColumnStatistics_ThrowsTException()
    {
        // Setup
        final List<ColumnStatisticsObj> statistics = Arrays.asList(
                new ColumnStatisticsObj("colName", "colType", new ColumnStatisticsData(
                        ColumnStatisticsData._Fields.BOOLEAN_STATS, "value")));

        // Run the test
        assertThrows(TException.class,
                () -> thriftHiveMetastoreClientUnderTest.setTableColumnStatistics("databaseName", "tableName",
                        statistics));
    }

    @Test
    public void testDeleteTableColumnStatistics() throws Exception
    {
        // Setup
        // Run the test
        thriftHiveMetastoreClientUnderTest.deleteTableColumnStatistics("databaseName", "tableName", "columnName");

        // Verify the results
    }

    @Test
    public void testDeleteTableColumnStatistics_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class,
                () -> thriftHiveMetastoreClientUnderTest.deleteTableColumnStatistics("databaseName", "tableName",
                        "columnName"));
    }

    @Test
    public void testGetPartitionColumnStatistics() throws Exception
    {
        // Setup
        final Map<String, List<ColumnStatisticsObj>> expectedResult = new HashMap<>();

        // Run the test
        final Map<String, List<ColumnStatisticsObj>> result = thriftHiveMetastoreClientUnderTest.getPartitionColumnStatistics(
                "databaseName", "tableName",
                Arrays.asList("value"), Arrays.asList("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetPartitionColumnStatistics_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class,
                () -> thriftHiveMetastoreClientUnderTest.getPartitionColumnStatistics("databaseName", "tableName",
                        Arrays.asList("value"), Arrays.asList("value")));
    }

    @Test
    public void testSetPartitionColumnStatistics() throws Exception
    {
        // Setup
        final List<ColumnStatisticsObj> statistics = Arrays.asList(
                new ColumnStatisticsObj("colName", "colType", new ColumnStatisticsData(
                        ColumnStatisticsData._Fields.BOOLEAN_STATS, "value")));

        // Run the test
        thriftHiveMetastoreClientUnderTest.setPartitionColumnStatistics("databaseName", "tableName", "partitionName",
                statistics);

        // Verify the results
    }

    @Test
    public void testSetPartitionColumnStatistics_ThrowsTException()
    {
        // Setup
        final List<ColumnStatisticsObj> statistics = Arrays.asList(
                new ColumnStatisticsObj("colName", "colType", new ColumnStatisticsData(
                        ColumnStatisticsData._Fields.BOOLEAN_STATS, "value")));

        // Run the test
        assertThrows(TException.class,
                () -> thriftHiveMetastoreClientUnderTest.setPartitionColumnStatistics("databaseName", "tableName",
                        "partitionName", statistics));
    }

    @Test
    public void testDeletePartitionColumnStatistics() throws Exception
    {
        // Setup
        // Run the test
        thriftHiveMetastoreClientUnderTest.deletePartitionColumnStatistics("databaseName", "tableName", "partitionName",
                "columnName");

        // Verify the results
    }

    @Test
    public void testDeletePartitionColumnStatistics_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class,
                () -> thriftHiveMetastoreClientUnderTest.deletePartitionColumnStatistics("databaseName", "tableName",
                        "partitionName", "columnName"));
    }

    @Test
    public void testGetPartitionNames() throws Exception
    {
        // Setup
        // Run the test
        final List<String> result = thriftHiveMetastoreClientUnderTest.getPartitionNames("databaseName", "tableName");

        // Verify the results
        assertEquals(Arrays.asList("value"), result);
    }

    @Test
    public void testGetPartitionNames_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class,
                () -> thriftHiveMetastoreClientUnderTest.getPartitionNames("databaseName", "tableName"));
    }

    @Test
    public void testGetPartitionNamesFiltered() throws Exception
    {
        // Setup
        // Run the test
        final List<String> result = thriftHiveMetastoreClientUnderTest.getPartitionNamesFiltered("databaseName",
                "tableName",
                Arrays.asList("value"));

        // Verify the results
        assertEquals(Arrays.asList("value"), result);
    }

    @Test
    public void testGetPartitionNamesFiltered_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class,
                () -> thriftHiveMetastoreClientUnderTest.getPartitionNamesFiltered("databaseName", "tableName",
                        Arrays.asList("value")));
    }

    @Test
    public void testAddPartitions() throws Exception
    {
        // Setup
        final List<Partition> newPartitions = Arrays.asList(
                new Partition(Arrays.asList("value"), "dbName", "tableName", 0, 0, new StorageDescriptor(
                        Arrays.asList(new FieldSchema("name", "type", "comment")), "location", "inputFormat",
                        "outputFormat", false, 0, new SerDeInfo("name", "serializationLib", new HashMap<>()),
                        Arrays.asList("value"), Arrays.asList(new Order("col", 0)), new HashMap<>()), new HashMap<>()));

        // Run the test
        final int result = thriftHiveMetastoreClientUnderTest.addPartitions(newPartitions);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testAddPartitions_ThrowsTException()
    {
        // Setup
        final List<Partition> newPartitions = Arrays.asList(
                new Partition(Arrays.asList("value"), "dbName", "tableName", 0, 0, new StorageDescriptor(
                        Arrays.asList(new FieldSchema("name", "type", "comment")), "location", "inputFormat",
                        "outputFormat", false, 0, new SerDeInfo("name", "serializationLib", new HashMap<>()),
                        Arrays.asList("value"), Arrays.asList(new Order("col", 0)), new HashMap<>()), new HashMap<>()));

        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.addPartitions(newPartitions));
    }

    @Test
    public void testDropPartition() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = thriftHiveMetastoreClientUnderTest.dropPartition("databaseName", "tableName",
                Arrays.asList("value"), false);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testDropPartition_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(
                TException.class, () -> thriftHiveMetastoreClientUnderTest.dropPartition("databaseName", "tableName",
                        Arrays.asList("value"), false));
    }

    @Test
    public void testAlterPartition() throws Exception
    {
        // Setup
        final Partition partition = new Partition(Arrays.asList("value"), "dbName", "tableName", 0, 0,
                new StorageDescriptor(
                        Arrays.asList(new FieldSchema("name", "type", "comment")), "location", "inputFormat",
                        "outputFormat", false, 0, new SerDeInfo("name", "serializationLib", new HashMap<>()),
                        Arrays.asList("value"), Arrays.asList(new Order("col", 0)), new HashMap<>()), new HashMap<>());

        // Run the test
        thriftHiveMetastoreClientUnderTest.alterPartition("databaseName", "tableName", partition);

        // Verify the results
    }

    @Test
    public void testAlterPartition_ThrowsTException()
    {
        // Setup
        final Partition partition = new Partition(Arrays.asList("value"), "dbName", "tableName", 0, 0,
                new StorageDescriptor(
                        Arrays.asList(new FieldSchema("name", "type", "comment")), "location", "inputFormat",
                        "outputFormat", false, 0, new SerDeInfo("name", "serializationLib", new HashMap<>()),
                        Arrays.asList("value"), Arrays.asList(new Order("col", 0)), new HashMap<>()), new HashMap<>());

        // Run the test
        assertThrows(
                TException.class,
                () -> thriftHiveMetastoreClientUnderTest.alterPartition("databaseName", "tableName", partition));
    }

    @Test
    public void testAlterPartitions() throws Exception
    {
        // Setup
        final List<Partition> partitions = Arrays.asList(
                new Partition(Arrays.asList("value"), "dbName", "tableName", 0, 0, new StorageDescriptor(
                        Arrays.asList(new FieldSchema("name", "type", "comment")), "location", "inputFormat",
                        "outputFormat", false, 0, new SerDeInfo("name", "serializationLib", new HashMap<>()),
                        Arrays.asList("value"), Arrays.asList(new Order("col", 0)), new HashMap<>()), new HashMap<>()));

        // Run the test
        thriftHiveMetastoreClientUnderTest.alterPartitions("databaseName", "tableName", partitions);

        // Verify the results
    }

    @Test
    public void testAlterPartitions_ThrowsTException()
    {
        // Setup
        final List<Partition> partitions = Arrays.asList(
                new Partition(Arrays.asList("value"), "dbName", "tableName", 0, 0, new StorageDescriptor(
                        Arrays.asList(new FieldSchema("name", "type", "comment")), "location", "inputFormat",
                        "outputFormat", false, 0, new SerDeInfo("name", "serializationLib", new HashMap<>()),
                        Arrays.asList("value"), Arrays.asList(new Order("col", 0)), new HashMap<>()), new HashMap<>()));

        // Run the test
        assertThrows(TException.class,
                () -> thriftHiveMetastoreClientUnderTest.alterPartitions("databaseName", "tableName", partitions));
    }

    @Test
    public void testGetPartition() throws Exception
    {
        // Setup
        final Partition expectedResult = new Partition(Arrays.asList("value"), "dbName", "tableName", 0, 0,
                new StorageDescriptor(
                        Arrays.asList(new FieldSchema("name", "type", "comment")), "location", "inputFormat",
                        "outputFormat", false, 0, new SerDeInfo("name", "serializationLib", new HashMap<>()),
                        Arrays.asList("value"), Arrays.asList(new Order("col", 0)), new HashMap<>()), new HashMap<>());

        // Run the test
        final Partition result = thriftHiveMetastoreClientUnderTest.getPartition("databaseName", "tableName",
                Arrays.asList("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetPartition_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(
                TException.class, () -> thriftHiveMetastoreClientUnderTest.getPartition("databaseName", "tableName",
                        Arrays.asList("value")));
    }

    @Test
    public void testGetPartitionsByNames() throws Exception
    {
        // Setup
        final List<Partition> expectedResult = Arrays.asList(
                new Partition(Arrays.asList("value"), "dbName", "tableName", 0, 0, new StorageDescriptor(
                        Arrays.asList(new FieldSchema("name", "type", "comment")), "location", "inputFormat",
                        "outputFormat", false, 0, new SerDeInfo("name", "serializationLib", new HashMap<>()),
                        Arrays.asList("value"), Arrays.asList(new Order("col", 0)), new HashMap<>()), new HashMap<>()));

        // Run the test
        final List<Partition> result = thriftHiveMetastoreClientUnderTest.getPartitionsByNames("databaseName",
                "tableName",
                Arrays.asList("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetPartitionsByNames_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class,
                () -> thriftHiveMetastoreClientUnderTest.getPartitionsByNames("databaseName", "tableName",
                        Arrays.asList("value")));
    }

    @Test
    public void testListRoles() throws Exception
    {
        // Setup
        final List<Role> expectedResult = Arrays.asList(new Role("roleName", 0, "grantor"));

        // Run the test
        final List<Role> result = thriftHiveMetastoreClientUnderTest.listRoles("principalName", PrincipalType.USER);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListRoles_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(
                TException.class,
                () -> thriftHiveMetastoreClientUnderTest.listRoles("principalName", PrincipalType.USER));
    }

    @Test
    public void testListPrivileges() throws Exception
    {
        // Setup
        final HiveObjectRef hiveObjectRef = new HiveObjectRef(HiveObjectType.GLOBAL, "dbName", "objectName",
                Arrays.asList("value"), "columnName");
        final List<HiveObjectPrivilege> expectedResult = Arrays.asList(new HiveObjectPrivilege(new HiveObjectRef(
                HiveObjectType.GLOBAL, "dbName", "objectName", Arrays.asList("value"), "columnName"), "principalName",
                PrincipalType.USER, new PrivilegeGrantInfo("privilege", 0, "grantor", PrincipalType.USER, false),
                "authorizer"));

        // Run the test
        final List<HiveObjectPrivilege> result = thriftHiveMetastoreClientUnderTest.listPrivileges("principalName",
                PrincipalType.USER, hiveObjectRef);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListPrivileges_ThrowsTException()
    {
        // Setup
        final HiveObjectRef hiveObjectRef = new HiveObjectRef(HiveObjectType.GLOBAL, "dbName", "objectName",
                Arrays.asList("value"), "columnName");

        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.listPrivileges("principalName",
                PrincipalType.USER, hiveObjectRef));
    }

    @Test
    public void testGetRoleNames() throws Exception
    {
        // Setup
        // Run the test
        final List<String> result = thriftHiveMetastoreClientUnderTest.getRoleNames();

        // Verify the results
        assertEquals(Arrays.asList("value"), result);
    }

    @Test
    public void testGetRoleNames_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.getRoleNames());
    }

    @Test
    public void testCreateRole() throws Exception
    {
        // Setup
        // Run the test
        thriftHiveMetastoreClientUnderTest.createRole("roleName", "grantor");

        // Verify the results
    }

    @Test
    public void testCreateRole_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.createRole("roleName", "grantor"));
    }

    @Test
    public void testDropRole() throws Exception
    {
        // Setup
        // Run the test
        thriftHiveMetastoreClientUnderTest.dropRole("role");

        // Verify the results
    }

    @Test
    public void testDropRole_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.dropRole("role"));
    }

    @Test
    public void testGrantPrivileges() throws Exception
    {
        // Setup
        final PrivilegeBag privilegeBag = new PrivilegeBag(
                Arrays.asList(new HiveObjectPrivilege(
                        new HiveObjectRef(HiveObjectType.GLOBAL, "dbName", "objectName", Arrays.asList("value"),
                                "columnName"), "principalName", PrincipalType.USER,
                        new PrivilegeGrantInfo("privilege", 0, "grantor", PrincipalType.USER, false), "authorizer")));

        // Run the test
        final boolean result = thriftHiveMetastoreClientUnderTest.grantPrivileges(privilegeBag);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testGrantPrivileges_ThrowsTException()
    {
        // Setup
        final PrivilegeBag privilegeBag = new PrivilegeBag(
                Arrays.asList(new HiveObjectPrivilege(
                        new HiveObjectRef(HiveObjectType.GLOBAL, "dbName", "objectName", Arrays.asList("value"),
                                "columnName"), "principalName", PrincipalType.USER,
                        new PrivilegeGrantInfo("privilege", 0, "grantor", PrincipalType.USER, false), "authorizer")));

        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.grantPrivileges(privilegeBag));
    }

    @Test
    public void testRevokePrivileges() throws Exception
    {
        // Setup
        final PrivilegeBag privilegeBag = new PrivilegeBag(
                Arrays.asList(new HiveObjectPrivilege(
                        new HiveObjectRef(HiveObjectType.GLOBAL, "dbName", "objectName", Arrays.asList("value"),
                                "columnName"), "principalName", PrincipalType.USER,
                        new PrivilegeGrantInfo("privilege", 0, "grantor", PrincipalType.USER, false), "authorizer")));

        // Run the test
        final boolean result = thriftHiveMetastoreClientUnderTest.revokePrivileges(privilegeBag);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testRevokePrivileges_ThrowsTException()
    {
        // Setup
        final PrivilegeBag privilegeBag = new PrivilegeBag(
                Arrays.asList(new HiveObjectPrivilege(
                        new HiveObjectRef(HiveObjectType.GLOBAL, "dbName", "objectName", Arrays.asList("value"),
                                "columnName"), "principalName", PrincipalType.USER,
                        new PrivilegeGrantInfo("privilege", 0, "grantor", PrincipalType.USER, false), "authorizer")));

        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.revokePrivileges(privilegeBag));
    }

    @Test
    public void testGrantRole() throws Exception
    {
        // Setup
        // Run the test
        thriftHiveMetastoreClientUnderTest.grantRole("role", "granteeName", PrincipalType.USER, "grantorName",
                PrincipalType.USER, false);

        // Verify the results
    }

    @Test
    public void testGrantRole_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.grantRole("role", "granteeName",
                PrincipalType.USER, "grantorName", PrincipalType.USER, false));
    }

    @Test
    public void testRevokeRole() throws Exception
    {
        // Setup
        // Run the test
        thriftHiveMetastoreClientUnderTest.revokeRole("role", "granteeName", PrincipalType.USER, false);

        // Verify the results
    }

    @Test
    public void testRevokeRole_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.revokeRole("role", "granteeName",
                PrincipalType.USER, false));
    }

    @Test
    public void testListRoleGrants() throws Exception
    {
        // Setup
        final List<RolePrincipalGrant> expectedResult = Arrays.asList(
                new RolePrincipalGrant("roleName", "principalName", PrincipalType.USER, false, 0, "grantorName",
                        PrincipalType.USER));

        // Run the test
        final List<RolePrincipalGrant> result = thriftHiveMetastoreClientUnderTest.listRoleGrants("granteeName",
                PrincipalType.USER);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListRoleGrants_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.listRoleGrants("granteeName",
                PrincipalType.USER));
    }

    @Test
    public void testSetUGI() throws Exception
    {
        // Setup
        // Run the test
        thriftHiveMetastoreClientUnderTest.setUGI("userName");

        // Verify the results
    }

    @Test
    public void testSetUGI_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.setUGI("userName"));
    }

    @Test
    public void testOpenTransaction() throws Exception
    {
        // Setup
        // Run the test
        final long result = thriftHiveMetastoreClientUnderTest.openTransaction("user");

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testOpenTransaction_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.openTransaction("user"));
    }

    @Test
    public void testCommitTransaction() throws Exception
    {
        // Setup
        // Run the test
        thriftHiveMetastoreClientUnderTest.commitTransaction(0L);

        // Verify the results
    }

    @Test
    public void testCommitTransaction_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.commitTransaction(0L));
    }

    @Test
    public void testAbortTransaction() throws Exception
    {
        // Setup
        // Run the test
        thriftHiveMetastoreClientUnderTest.abortTransaction(0L);

        // Verify the results
    }

    @Test
    public void testAbortTransaction_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.abortTransaction(0L));
    }

    @Test
    public void testSendTransactionHeartbeat() throws Exception
    {
        // Setup
        // Run the test
        thriftHiveMetastoreClientUnderTest.sendTransactionHeartbeat(0L);

        // Verify the results
    }

    @Test
    public void testSendTransactionHeartbeat_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.sendTransactionHeartbeat(0L));
    }

    @Test
    public void testAcquireLock() throws Exception

    {
        // Setup
        final LockRequest lockRequest = new LockRequest(
                Arrays.asList(new LockComponent(LockType.SHARED_READ, LockLevel.DB, "dbname")), "user", "hostname");
        final LockResponse expectedResult = new LockResponse(0L, LockState.ACQUIRED);

        // Run the test
        final LockResponse result = thriftHiveMetastoreClientUnderTest.acquireLock(lockRequest);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testAcquireLock_ThrowsTException()
    {
        // Setup
        final LockRequest lockRequest = new LockRequest(
                Arrays.asList(new LockComponent(LockType.SHARED_READ, LockLevel.DB, "dbname")), "user", "hostname");

        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.acquireLock(lockRequest));
    }

    @Test
    public void testCheckLock() throws Exception
    {
        // Setup
        final LockResponse expectedResult = new LockResponse(0L, LockState.ACQUIRED);

        // Run the test
        final LockResponse result = thriftHiveMetastoreClientUnderTest.checkLock(0L);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCheckLock_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.checkLock(0L));
    }

    @Test
    public void testGetValidWriteIds() throws Exception
    {
        // Setup
        // Run the test
        final String result = thriftHiveMetastoreClientUnderTest.getValidWriteIds(Arrays.asList("value"), 0L, false);

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testGetValidWriteIds_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class,
                () -> thriftHiveMetastoreClientUnderTest.getValidWriteIds(Arrays.asList("value"), 0L, false));
    }

    @Test
    public void testShowLocks() throws Exception
    {
        // Setup
        final ShowLocksRequest rqst = new ShowLocksRequest();
        final ShowLocksResponse expectedResult = new ShowLocksResponse(
                Arrays.asList(
                        new ShowLocksResponseElement(0L, "dbname", LockState.ACQUIRED, LockType.SHARED_READ, 0L, "user",
                                "hostname")));

        // Run the test
        final ShowLocksResponse result = thriftHiveMetastoreClientUnderTest.showLocks(rqst);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testShowLocks_ThrowsTException()
    {
        // Setup
        final ShowLocksRequest rqst = new ShowLocksRequest();

        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.showLocks(rqst));
    }

    @Test
    public void testGet_config_value() throws Exception
    {
        // Setup
        // Run the test
        final String result = thriftHiveMetastoreClientUnderTest.get_config_value("name", "defaultValue");

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testGet_config_value_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(
                TException.class, () -> thriftHiveMetastoreClientUnderTest.get_config_value("name", "defaultValue"));
    }

    @Test
    public void testGetDelegationToken() throws Exception
    {
        // Setup
        // Run the test
        final String result = thriftHiveMetastoreClientUnderTest.getDelegationToken("userName");

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testGetDelegationToken_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.getDelegationToken("userName"));
    }

    @Test
    public void testGetTableWriteId() throws Exception
    {
        // Setup
        // Run the test
        final long result = thriftHiveMetastoreClientUnderTest.getTableWriteId("dbName", "tableName", 0L);

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetTableWriteId_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(
                TException.class, () -> thriftHiveMetastoreClientUnderTest.getTableWriteId("dbName", "tableName", 0L));
    }

    @Test
    public void testUnlock() throws Exception
    {
        // Setup
        // Run the test
        thriftHiveMetastoreClientUnderTest.unlock(0L);

        // Verify the results
    }

    @Test
    public void testUnlock_ThrowsTException()
    {
        // Setup
        // Run the test
        assertThrows(TException.class, () -> thriftHiveMetastoreClientUnderTest.unlock(0L));
    }
}
