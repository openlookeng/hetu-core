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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.ObjectPair;
import org.apache.thrift.TException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hive.metastore.api.PrincipalType.ROLE;
import static org.apache.hadoop.hive.metastore.api.PrincipalType.USER;

public class MockThriftMetastoreClient
        implements ThriftMetastoreClient
{
    public static final String TEST_DATABASE = "testdb";
    public static final String BAD_DATABASE = "baddb";
    public static final String TEST_TABLE = "testtbl";
    public static final String TEST_TABLE_UP_NAME = "testtblup";
    public static final String TEST_PARTITION1 = "key=testpartition1";
    public static final String TEST_PARTITION2 = "key=testpartition2";
    public static final String TEST_PARTITION_UP1 = "key=testpartitionup1";
    public static final String TEST_PARTITION_UP2 = "key=testpartitionup2";
    public static final List<String> TEST_PARTITION_VALUES1 = ImmutableList.of("testpartition1");
    public static final List<String> TEST_PARTITION_VALUES2 = ImmutableList.of("testpartition2");
    public static final List<String> TEST_PARTITION_UP_VALUES1 = ImmutableList.of("testpartitionup1");
    public static final List<String> TEST_PARTITION_UP_VALUES2 = ImmutableList.of("testpartitionup2");
    public static final List<String> TEST_ROLES = ImmutableList.of("testrole");
    public static final List<RolePrincipalGrant> TEST_ROLE_GRANTS = ImmutableList.of(
            new RolePrincipalGrant("role1", "user", USER, false, 0, "grantor1", USER),
            new RolePrincipalGrant("role2", "role1", ROLE, true, 0, "grantor2", ROLE));

    private static final StorageDescriptor DEFAULT_STORAGE_DESCRIPTOR =
            new StorageDescriptor(ImmutableList.of(), "", null, null, false, 0, new SerDeInfo(TEST_TABLE, null, ImmutableMap.of()), null, null, ImmutableMap.of());
    private static final StorageDescriptor UPDATE_STORAGE_DESCRIPTOR =
            new StorageDescriptor(ImmutableList.of(new FieldSchema("t_bigint", "int", null)), "", null, null, false, 0, new SerDeInfo(TEST_TABLE_UP_NAME, null, ImmutableMap.of()), null, null, ImmutableMap.of());

    private static final Map<String, String> parameters = new HashMap<String, String>(){{
            put("numFiles", "4");
            put("numRows", "10");
        }};
    public static final Table TEST_TABLE_UP = new Table(TEST_TABLE_UP_NAME,
            TEST_DATABASE,
            "user",
            0,
            0,
            0,
            UPDATE_STORAGE_DESCRIPTOR,
            ImmutableList.of(
                    new FieldSchema("key", "string", null)),
            parameters,
            "view original text",
            "view extended text",
            "MANAGED_TABLE");
    private static Map<String, List<ColumnStatisticsObj>> partitionColumnStatistics = new HashMap<String, List<ColumnStatisticsObj>>(){{
            put(TEST_PARTITION_UP1, ImmutableList.of(new ColumnStatisticsObj("t_bigint", "int", new ColumnStatisticsData(ColumnStatisticsData._Fields.LONG_STATS, new LongColumnStatsData(0, 4)))));
            put(TEST_PARTITION_UP2, ImmutableList.of(new ColumnStatisticsObj("t_bigint", "int", new ColumnStatisticsData(ColumnStatisticsData._Fields.LONG_STATS, new LongColumnStatsData(0, 4)))));
        }};
    private static Partition testPartitionUp1 = new Partition(TEST_PARTITION_UP_VALUES1, TEST_DATABASE, TEST_TABLE_UP_NAME, 0, 0, UPDATE_STORAGE_DESCRIPTOR, parameters);
    private static Partition testPartitionUp2 = new Partition(TEST_PARTITION_UP_VALUES2, TEST_DATABASE, TEST_TABLE_UP_NAME, 0, 0, UPDATE_STORAGE_DESCRIPTOR, parameters);

    private final AtomicInteger accessCount = new AtomicInteger();
    private final AtomicInteger alterPartitionsCount = new AtomicInteger();
    private boolean throwException;

    private String hostAddress;

    public void setThrowException(boolean throwException)
    {
        this.throwException = throwException;
    }

    public int getAccessCount()
    {
        return accessCount.get();
    }

    public int getAlterPartitionCount()
    {
        return alterPartitionsCount.get();
    }

    public void setHostAddress(String hostAddress)
    {
        this.hostAddress = hostAddress;
    }

    public String getHostAddress()
    {
        return hostAddress;
    }

    @Override
    public List<String> getAllDatabases()
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new IllegalStateException();
        }
        return ImmutableList.of(TEST_DATABASE);
    }

    @Override
    public List<String> getAllTables(String dbName)
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!dbName.equals(TEST_DATABASE)) {
            return ImmutableList.of(); // As specified by Hive specification
        }
        return ImmutableList.of(TEST_TABLE);
    }

    @Override
    public Database getDatabase(String name)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!name.equals(TEST_DATABASE)) {
            throw new NoSuchObjectException();
        }
        return new Database(TEST_DATABASE, null, null, null);
    }

    @Override
    public Table getTable(String dbName, String tableName)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!dbName.equals(TEST_DATABASE) || !ImmutableList.of(TEST_TABLE, TEST_TABLE_UP_NAME).contains(tableName)) {
            throw new NoSuchObjectException();
        }
        if (tableName.equals(TEST_TABLE_UP_NAME)) {
            return TEST_TABLE_UP;
        }
        else {
            return new Table(
                    TEST_TABLE,
                    TEST_DATABASE,
                    "",
                    0,
                    0,
                    0,
                    DEFAULT_STORAGE_DESCRIPTOR,
                    ImmutableList.of(new FieldSchema("key", "string", null)),
                    null,
                    "",
                    "",
                    TableType.MANAGED_TABLE.name());
        }
    }

    @Override
    public Table getTableWithCapabilities(String databaseName, String tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<FieldSchema> getFields(String databaseName, String tableName)
            throws TException
    {
        return ImmutableList.of(new FieldSchema("key", "string", null));
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String databaseName, String tableName, List<String> columnNames)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTableColumnStatistics(String databaseName, String tableName, List<ColumnStatisticsObj> statistics)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteTableColumnStatistics(String databaseName, String tableName, String columnName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String databaseName, String tableName, List<String> partitionNames, List<String> columnNames)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!databaseName.equals(TEST_DATABASE) || !tableName.equals(TEST_TABLE_UP_NAME) || !ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2, TEST_PARTITION_UP1, TEST_PARTITION_UP2).containsAll(partitionNames)) {
            throw new NoSuchObjectException();
        }
        return partitionColumnStatistics;
    }

    @Override
    public void setPartitionColumnStatistics(String databaseName, String tableName, String partitionName, List<ColumnStatisticsObj> statistics)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!databaseName.equals(TEST_DATABASE) || !ImmutableList.of(TEST_TABLE, TEST_TABLE_UP_NAME).contains(tableName) || !ImmutableList.of(TEST_PARTITION_UP1, TEST_PARTITION_UP2).contains(partitionName)) {
            throw new NoSuchObjectException();
        }
        if (partitionColumnStatistics.containsKey(partitionName)) {
            partitionColumnStatistics.replace(partitionName, statistics);
        }
        else {
            partitionColumnStatistics.put(partitionName, statistics);
        }
    }

    @Override
    public void deletePartitionColumnStatistics(String databaseName, String tableName, String partitionName, String columnName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getTableNamesByFilter(String databaseName, String filter)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getPartitionNames(String dbName, String tableName)
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!dbName.equals(TEST_DATABASE) || !tableName.equals(TEST_TABLE)) {
            return ImmutableList.of();
        }
        return ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2);
    }

    @Override
    public List<String> getPartitionNamesFiltered(String dbName, String tableName, List<String> partValues)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!dbName.equals(TEST_DATABASE) || !tableName.equals(TEST_TABLE)) {
            throw new NoSuchObjectException();
        }
        return ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2);
    }

    @Override
    public Partition getPartition(String dbName, String tableName, List<String> partitionValues)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!dbName.equals(TEST_DATABASE) || !tableName.equals(TEST_TABLE) || !ImmutableSet.of(TEST_PARTITION_VALUES1, TEST_PARTITION_VALUES2).contains(partitionValues)) {
            throw new NoSuchObjectException();
        }
        return new Partition(null, TEST_DATABASE, TEST_TABLE, 0, 0, DEFAULT_STORAGE_DESCRIPTOR, ImmutableMap.of());
    }

    @Override
    public List<Partition> getPartitionsByNames(String dbName, String tableName, List<String> names)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!dbName.equals(TEST_DATABASE) || !ImmutableList.of(TEST_TABLE, TEST_TABLE_UP_NAME).contains(tableName) || !ImmutableSet.of(TEST_PARTITION1, TEST_PARTITION2, TEST_PARTITION_UP1, TEST_PARTITION_UP2).containsAll(names)) {
            throw new NoSuchObjectException();
        }
        if (names.containsAll(ImmutableList.of(TEST_PARTITION_UP1, TEST_PARTITION_UP2))) {
            return ImmutableList.of(testPartitionUp1, testPartitionUp2);
        }
        else {
            return Lists.transform(names, name -> {
                try {
                    return new Partition(ImmutableList.copyOf(Warehouse.getPartValuesFromPartName(name)), TEST_DATABASE, TEST_TABLE, 0, 0, DEFAULT_STORAGE_DESCRIPTOR, ImmutableMap.of());
                }
                catch (MetaException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @Override
    public void createDatabase(Database database)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData, boolean cascade)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterDatabase(String databaseName, Database database)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(Table table)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(String databaseName, String name, boolean deleteData)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTable(String databaseName, String tableName, Table newTable)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int addPartitions(List<Partition> newPartitions)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean dropPartition(String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(String databaseName, String tableName, Partition partition)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitions(String databaseName, String tableName, List<Partition> partitions)
            throws TException
    {
        accessCount.incrementAndGet();
        alterPartitionsCount.incrementAndGet();
        if (throwException) {
            throw new IllegalStateException();
        }
        if (!databaseName.equals(TEST_DATABASE) || !tableName.equals(TEST_TABLE_UP_NAME)) {
            throw new NoSuchObjectException();
        }
        testPartitionUp1 = partitions.get(0);
        testPartitionUp2 = partitions.get(1);
    }

    @Override
    public List<Role> listRoles(String principalName, PrincipalType principalType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<HiveObjectPrivilege> listPrivileges(String principalName, PrincipalType principalType, HiveObjectRef hiveObjectRef)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getRoleNames()
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new IllegalStateException();
        }
        return TEST_ROLES;
    }

    @Override
    public void createRole(String role, String grantor)
            throws TException
    {
        // No-op
    }

    @Override
    public void dropRole(String role)
            throws TException
    {
        // No-op
    }

    @Override
    public boolean grantPrivileges(PrivilegeBag privilegeBag)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean revokePrivileges(PrivilegeBag privilegeBag)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantRole(String role, String granteeName, PrincipalType granteeType, String grantorName, PrincipalType grantorType, boolean grantOption)
            throws TException
    {
        // No-op
    }

    @Override
    public void revokeRole(String role, String granteeName, PrincipalType granteeType, boolean grantOption)
            throws TException
    {
        // No-op
    }

    @Override
    public List<RolePrincipalGrant> listRoleGrants(String name, PrincipalType principalType)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new IllegalStateException();
        }
        return TEST_ROLE_GRANTS;
    }

    @Override
    public void close()
    {
        // No-op
    }

    @Override
    public void setUGI(String userName)
    {
        // No-op
    }

    @Override
    public long openTransaction(String user)
            throws TException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitTransaction(long transactionId)
            throws TException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abortTransaction(long transactionId)
            throws TException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sendTransactionHeartbeat(long transactionId)
            throws TException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public LockResponse acquireLock(LockRequest lockRequest)
            throws TException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public LockResponse checkLock(long lockId)
            throws TException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getValidWriteIds(List<String> tableList, long currentTransactionId, boolean isVacuum)
            throws TException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTableWriteId(String dbName, String tableName, long transactionId)
            throws TException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ShowLocksResponse showLocks(ShowLocksRequest rqst)
            throws TException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String get_config_value(String name, String defaultValue)
            throws TException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDelegationToken(String userName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartitionByRequest(String databaseName, String tableName, List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData, boolean ifExists)
            throws TException
    {
        throw new UnsupportedOperationException();
    }
}
