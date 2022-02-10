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
import io.airlift.log.Logger;
import io.prestosql.plugin.hive.util.LoggingInvocationHandler;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.ClientCapabilities;
import org.apache.hadoop.hive.metastore.api.ClientCapability;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropPartitionsExpr;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokeRoleRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokeRoleResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokeType;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeRequest;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.RequestPartsSpec;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableStatsRequest;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.ObjectPair;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.reflect.Reflection.newProxy;
import static java.util.Objects.requireNonNull;

public class ThriftHiveMetastoreClient
        implements ThriftMetastoreClient
{
    private static final Logger log = Logger.get(ThriftHiveMetastoreClient.class);

    private static final LoggingInvocationHandler.ParameterNamesProvider PARAMETER_NAMES_PROVIDER = new LoggingInvocationHandler.AirliftParameterNamesProvider(ThriftHiveMetastore.Iface.class, ThriftHiveMetastore.Client.class);
    private static final String CATALOG = "hive";

    private final TTransport transport;
    private final ThriftHiveMetastore.Iface client;
    private final String hostname;

    public ThriftHiveMetastoreClient(TTransport transport, String hostname)
    {
        this.transport = requireNonNull(transport, "transport is null");
        ThriftHiveMetastore.Client client = new ThriftHiveMetastore.Client(new TBinaryProtocol(transport));
        if (log.isDebugEnabled()) {
            this.client = newProxy(ThriftHiveMetastore.Iface.class, new LoggingInvocationHandler(client, PARAMETER_NAMES_PROVIDER, log::debug));
        }
        else {
            this.client = client;
        }
        this.hostname = requireNonNull(hostname, "hostname is null");
    }

    public ThriftHiveMetastore.Iface getClient()
    {
        return client;
    }

    @Override
    public void close()
    {
        transport.close();
    }

    @Override
    public List<String> getAllDatabases()
            throws TException
    {
        return client.get_all_databases();
    }

    @Override
    public Database getDatabase(String dbName)
            throws TException
    {
        return client.get_database(dbName);
    }

    @Override
    public List<String> getAllTables(String databaseName)
            throws TException
    {
        return client.get_all_tables(databaseName);
    }

    @Override
    public List<String> getTableNamesByFilter(String databaseName, String filter)
            throws TException
    {
        return client.get_table_names_by_filter(databaseName, filter, (short) -1);
    }

    @Override
    public void createDatabase(Database database)
            throws TException
    {
        client.create_database(database);
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData, boolean cascade)
            throws TException
    {
        client.drop_database(databaseName, deleteData, cascade);
    }

    @Override
    public void alterDatabase(String databaseName, Database database)
            throws TException
    {
        client.alter_database(databaseName, database);
    }

    @Override
    public void createTable(Table table)
            throws TException
    {
        client.create_table(table);
    }

    @Override
    public void dropTable(String databaseName, String name, boolean deleteData)
            throws TException
    {
        client.drop_table(databaseName, name, deleteData);
    }

    @Override
    public void alterTable(String databaseName, String tableName, Table newTable)
            throws TException
    {
        client.alter_table(databaseName, tableName, newTable);
    }

    @Override
    public Table getTable(String databaseName, String tableName)
            throws TException
    {
        return client.get_table(databaseName, tableName);
    }

    @Override
    public Table getTableWithCapabilities(String databaseName, String tableName)
            throws TException
    {
        GetTableRequest request = new GetTableRequest();
        request.setDbName(databaseName);
        request.setTblName(tableName);
        request.setCapabilities(new ClientCapabilities(ImmutableList.of(ClientCapability.INSERT_ONLY_TABLES)));
        return client.get_table_req(request).getTable();
    }

    @Override
    public List<FieldSchema> getFields(String databaseName, String tableName)
            throws TException
    {
        return client.get_fields(databaseName, tableName);
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String databaseName, String tableName, List<String> columnNames)
            throws TException
    {
        TableStatsRequest tableStatsRequest = new TableStatsRequest(databaseName, tableName, columnNames);
        return client.get_table_statistics_req(tableStatsRequest).getTableStats();
    }

    @Override
    public void setTableColumnStatistics(String databaseName, String tableName, List<ColumnStatisticsObj> statistics)
            throws TException
    {
        ColumnStatisticsDesc statisticsDescription = new ColumnStatisticsDesc(true, databaseName, tableName);
        ColumnStatistics request = new ColumnStatistics(statisticsDescription, statistics);
        client.update_table_column_statistics(request);
    }

    @Override
    public void deleteTableColumnStatistics(String databaseName, String tableName, String columnName)
            throws TException
    {
        client.delete_table_column_statistics(databaseName, tableName, columnName);
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String databaseName, String tableName, List<String> partitionNames, List<String> columnNames)
            throws TException
    {
        PartitionsStatsRequest partitionsStatsRequest = new PartitionsStatsRequest(databaseName, tableName, columnNames, partitionNames);
        return client.get_partitions_statistics_req(partitionsStatsRequest).getPartStats();
    }

    @Override
    public void setPartitionColumnStatistics(String databaseName, String tableName, String partitionName, List<ColumnStatisticsObj> statistics)
            throws TException
    {
        ColumnStatisticsDesc statisticsDescription = new ColumnStatisticsDesc(false, databaseName, tableName);
        statisticsDescription.setPartName(partitionName);
        ColumnStatistics request = new ColumnStatistics(statisticsDescription, statistics);
        client.update_partition_column_statistics(request);
    }

    @Override
    public void deletePartitionColumnStatistics(String databaseName, String tableName, String partitionName, String columnName)
            throws TException
    {
        client.delete_partition_column_statistics(databaseName, tableName, partitionName, columnName);
    }

    @Override
    public List<String> getPartitionNames(String databaseName, String tableName)
            throws TException
    {
        return client.get_partition_names(databaseName, tableName, (short) -1);
    }

    @Override
    public List<String> getPartitionNamesFiltered(String databaseName, String tableName, List<String> partitionValues)
            throws TException
    {
        return client.get_partition_names_ps(databaseName, tableName, partitionValues, (short) -1);
    }

    @Override
    public int addPartitions(List<Partition> newPartitions)
            throws TException
    {
        return client.add_partitions(newPartitions);
    }

    @Override
    public boolean dropPartition(String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
            throws TException
    {
        return client.drop_partition(databaseName, tableName, partitionValues, deleteData);
    }

    @Override
    public void dropPartitionByRequest(String databaseName, String tableName, List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData, boolean ifExists)
            throws TException
    {
        List<DropPartitionsExpr> expressions = new ArrayList<>(partExprs.size());
        for (ObjectPair<Integer, byte[]> partExpr : partExprs) {
            DropPartitionsExpr dpe = new DropPartitionsExpr();
            dpe.setExpr(partExpr.getSecond());
            dpe.setPartArchiveLevel(partExpr.getFirst());
            expressions.add(dpe);
        }
        DropPartitionsRequest dropPartitionsRequest = new DropPartitionsRequest(databaseName, tableName, RequestPartsSpec.exprs(expressions));
        dropPartitionsRequest.setCatName(CATALOG);
        dropPartitionsRequest.setDeleteData(deleteData);
        dropPartitionsRequest.setNeedResult(true);
        dropPartitionsRequest.setIfExists(ifExists);
        client.drop_partitions_req(dropPartitionsRequest).getPartitions();
    }

    @Override
    public void alterPartition(String databaseName, String tableName, Partition partition)
            throws TException
    {
        client.alter_partition(databaseName, tableName, partition);
    }

    @Override
    public void alterPartitions(String databaseName, String tableName, List<Partition> partitions)
            throws TException
    {
        client.alter_partitions(databaseName, tableName, partitions);
    }

    @Override
    public Partition getPartition(String databaseName, String tableName, List<String> partitionValues)
            throws TException
    {
        return client.get_partition(databaseName, tableName, partitionValues);
    }

    @Override
    public List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
            throws TException
    {
        return client.get_partitions_by_names(databaseName, tableName, partitionNames);
    }

    @Override
    public List<Role> listRoles(String principalName, PrincipalType principalType)
            throws TException
    {
        return client.list_roles(principalName, principalType);
    }

    @Override
    public List<HiveObjectPrivilege> listPrivileges(String principalName, PrincipalType principalType, HiveObjectRef hiveObjectRef)
            throws TException
    {
        return client.list_privileges(principalName, principalType, hiveObjectRef);
    }

    @Override
    public List<String> getRoleNames()
            throws TException
    {
        return client.get_role_names();
    }

    @Override
    public void createRole(String roleName, String grantor)
            throws TException
    {
        Role role = new Role(roleName, 0, grantor);
        client.create_role(role);
    }

    @Override
    public void dropRole(String role)
            throws TException
    {
        client.drop_role(role);
    }

    @Override
    public boolean grantPrivileges(PrivilegeBag privilegeBag)
            throws TException
    {
        return client.grant_privileges(privilegeBag);
    }

    @Override
    public boolean revokePrivileges(PrivilegeBag privilegeBag)
            throws TException
    {
        return client.revoke_privileges(privilegeBag);
    }

    @Override
    public void grantRole(String role, String granteeName, PrincipalType granteeType, String grantorName, PrincipalType grantorType, boolean grantOption)
            throws TException
    {
        List<RolePrincipalGrant> grants = listRoleGrants(granteeName, granteeType);
        for (RolePrincipalGrant grant : grants) {
            if (grant.getRoleName().equals(role)) {
                if (grant.isGrantOption() == grantOption) {
                    return;
                }
                if (!grant.isGrantOption() && grantOption) {
                    revokeRole(role, granteeName, granteeType, false);
                    break;
                }
            }
        }
        createGrant(role, granteeName, granteeType, grantorName, grantorType, grantOption);
    }

    private void createGrant(String role, String granteeName, PrincipalType granteeType, String grantorName, PrincipalType grantorType, boolean grantOption)
            throws TException
    {
        GrantRevokeRoleRequest request = new GrantRevokeRoleRequest();
        request.setRequestType(GrantRevokeType.GRANT);
        request.setRoleName(role);
        request.setPrincipalName(granteeName);
        request.setPrincipalType(granteeType);
        request.setGrantor(grantorName);
        request.setGrantorType(grantorType);
        request.setGrantOption(grantOption);
        GrantRevokeRoleResponse response = client.grant_revoke_role(request);
        if (!response.isSetSuccess()) {
            throw new MetaException("GrantRevokeResponse missing success field");
        }
    }

    @Override
    public void revokeRole(String role, String granteeName, PrincipalType granteeType, boolean grantOption)
            throws TException
    {
        List<RolePrincipalGrant> grants = listRoleGrants(granteeName, granteeType);
        RolePrincipalGrant currentGrant = null;
        for (RolePrincipalGrant grant : grants) {
            if (grant.getRoleName().equals(role)) {
                currentGrant = grant;
                break;
            }
        }

        if (currentGrant == null) {
            return;
        }

        if (!currentGrant.isGrantOption() && grantOption) {
            return;
        }

        removeGrant(role, granteeName, granteeType, grantOption);
    }

    private void removeGrant(String role, String granteeName, PrincipalType granteeType, boolean grantOption)
            throws TException
    {
        GrantRevokeRoleRequest request = new GrantRevokeRoleRequest();
        request.setRequestType(GrantRevokeType.REVOKE);
        request.setRoleName(role);
        request.setPrincipalName(granteeName);
        request.setPrincipalType(granteeType);
        request.setGrantOption(grantOption);
        GrantRevokeRoleResponse response = client.grant_revoke_role(request);
        if (!response.isSetSuccess()) {
            throw new MetaException("GrantRevokeResponse missing success field");
        }
    }

    @Override
    public List<RolePrincipalGrant> listRoleGrants(String principalName, PrincipalType principalType)
            throws TException
    {
        GetRoleGrantsForPrincipalRequest request = new GetRoleGrantsForPrincipalRequest(principalName, principalType);
        GetRoleGrantsForPrincipalResponse resp = client.get_role_grants_for_principal(request);
        return ImmutableList.copyOf(resp.getPrincipalGrants());
    }

    @Override
    public void setUGI(String userName)
            throws TException
    {
        client.set_ugi(userName, new ArrayList<>());
    }

    @Override
    public long openTransaction(String user)
            throws TException
    {
        OpenTxnRequest request = new OpenTxnRequest(1, user, hostname);
        return client.open_txns(request).getTxn_ids().get(0);
    }

    @Override
    public void commitTransaction(long transactionId)
            throws TException
    {
        client.commit_txn(new CommitTxnRequest(transactionId));
    }

    @Override
    public void abortTransaction(long transactionId)
            throws TException
    {
        client.abort_txn(new AbortTxnRequest(transactionId));
    }

    @Override
    public void sendTransactionHeartbeat(long transactionId)
            throws TException
    {
        HeartbeatTxnRangeRequest rqst = new HeartbeatTxnRangeRequest(transactionId, transactionId);
        client.heartbeat_txn_range(rqst);
    }

    @Override
    public LockResponse acquireLock(LockRequest lockRequest)
            throws TException
    {
        return client.lock(lockRequest);
    }

    @Override
    public LockResponse checkLock(long lockId)
            throws TException
    {
        return client.check_lock(new CheckLockRequest(lockId));
    }

    @Override
    public String getValidWriteIds(List<String> tableList, long currentTransactionId, boolean isVacuum)
            throws TException
    {
        // Pass currentTxn as 0L to get the recent snapshot of valid transactions in Hive
        // Do not pass currentTransactionId instead as it will break Hive's listing of delta directories if major compaction
        // deletes deleta directories for valid transactions that existed at the time transaction is opened
        ValidTxnList validTransactions = TxnUtils.createValidReadTxnList(client.get_open_txns(), 0L);
        GetValidWriteIdsRequest request = new GetValidWriteIdsRequest(tableList, validTransactions.toString());
        List<TableValidWriteIds> tblValidWriteIds = client.get_valid_write_ids(request).getTblValidWriteIds();
        if (isVacuum) {
            return createValidTxnWriteIdListForVacuum(currentTransactionId, tblValidWriteIds).toString();
        }
        return TxnUtils.createValidTxnWriteIdList(
                currentTransactionId,
                tblValidWriteIds)
                .toString();
    }

    /**
     * Creates the validTxnWriteIdList which consists of all valid commits less than minOpenWriteId
     */
    private static ValidTxnWriteIdList createValidTxnWriteIdListForVacuum(Long currentTxnId, List<TableValidWriteIds> validIds)
    {
        ValidTxnWriteIdList validTxnWriteIdList = new ValidTxnWriteIdList(currentTxnId);
        for (TableValidWriteIds tableWriteIds : validIds) {
            validTxnWriteIdList.addTableValidWriteIdList(TxnUtils.createValidCompactWriteIdList(tableWriteIds));
        }
        return validTxnWriteIdList;
    }

    @Override
    public ShowLocksResponse showLocks(ShowLocksRequest rqst)
            throws TException
    {
        return client.show_locks(rqst);
    }

    @Override
    public String get_config_value(String name, String defaultValue)
            throws TException
    {
        return client.get_config_value(name, defaultValue);
    }

    @Override
    public String getDelegationToken(String userName)
            throws TException
    {
        return client.get_delegation_token(userName, userName);
    }

    @Override
    public long getTableWriteId(String dbName, String tableName, long transactionId)
            throws TException
    {
        AllocateTableWriteIdsRequest allocateTableWriteIdsRequest = new AllocateTableWriteIdsRequest();
        allocateTableWriteIdsRequest.setDbName(dbName);
        allocateTableWriteIdsRequest.setTableName(tableName);
        allocateTableWriteIdsRequest.addToTxnIds(transactionId);

        final AllocateTableWriteIdsResponse allocateTableWriteIdsResponse = client.allocate_table_write_ids(allocateTableWriteIdsRequest);
        long txnToWriteId = allocateTableWriteIdsResponse.getTxnToWriteIds().stream().filter(e -> e.getTxnId() == transactionId).collect(toImmutableList()).get(0).getWriteId();
        return txnToWriteId;
    }
}
