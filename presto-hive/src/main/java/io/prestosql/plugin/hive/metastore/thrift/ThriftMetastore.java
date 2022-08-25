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

import io.prestosql.plugin.hive.HiveErrorCode;
import io.prestosql.plugin.hive.HivePartition;
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.acid.AcidOperation;
import io.prestosql.plugin.hive.acid.AcidTransaction;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.AcidTransactionOwner;
import io.prestosql.plugin.hive.metastore.HivePrincipal;
import io.prestosql.plugin.hive.metastore.HivePrivilegeInfo;
import io.prestosql.plugin.hive.metastore.PartitionWithStatistics;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

public interface ThriftMetastore
{
    void createDatabase(HiveIdentity identity, Database database);

    void dropDatabase(HiveIdentity identity, String databaseName);

    void alterDatabase(HiveIdentity identity, String databaseName, Database database);

    void createTable(HiveIdentity identity, Table table);

    void dropTable(HiveIdentity identity, String databaseName, String tableName, boolean deleteData);

    void alterTable(HiveIdentity identity, String databaseName, String tableName, Table table);

    List<String> getAllDatabases();

    Optional<List<String>> getAllTables(String databaseName);

    Optional<List<String>> getAllViews(String databaseName);

    Optional<Database> getDatabase(String databaseName);

    void addPartitions(HiveIdentity identity, String databaseName, String tableName, List<PartitionWithStatistics> partitions);

    void dropPartition(HiveIdentity identity, String databaseName, String tableName, List<String> parts, boolean deleteData);

    void alterPartition(HiveIdentity identity, String databaseName, String tableName, PartitionWithStatistics partition);

    Optional<List<String>> getPartitionNames(HiveIdentity identity, String databaseName, String tableName);

    Optional<List<String>> getPartitionNamesByParts(HiveIdentity identity, String databaseName, String tableName, List<String> parts);

    Optional<Partition> getPartition(HiveIdentity identity, String databaseName, String tableName, List<String> partitionValues);

    List<Partition> getPartitionsByNames(HiveIdentity identity, String databaseName, String tableName, List<String> partitionNames);

    Optional<Table> getTable(HiveIdentity identity, String databaseName, String tableName);

    Set<ColumnStatisticType> getSupportedColumnStatistics(Type type);

    PartitionStatistics getTableStatistics(HiveIdentity identity, Table table);

    Map<String, PartitionStatistics> getPartitionStatistics(HiveIdentity identity, Table table, List<Partition> partitions);

    void updateTableStatistics(HiveIdentity identity, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update);

    void updatePartitionStatistics(HiveIdentity identity, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update);

    void updatePartitionsStatistics(HiveIdentity identity, String databaseName, String tableName, Map<String, Function<PartitionStatistics, PartitionStatistics>> partNamesUpdateFunctionMap);

    void createRole(String role, String grantor);

    void dropRole(String role);

    Set<String> listRoles();

    void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean withAdminOption, HivePrincipal grantor);

    void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOptionFor, HivePrincipal grantor);

    Set<RoleGrant> listRoleGrants(HivePrincipal principal);

    void grantTablePrivileges(String databaseName, String tableName, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges);

    void revokeTablePrivileges(String databaseName, String tableName, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges);

    Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, HivePrincipal principal);

    boolean isImpersonationEnabled();

    default Optional<List<FieldSchema>> getFields(HiveIdentity identity, String databaseName, String tableName)
    {
        Optional<Table> table = getTable(identity, databaseName, tableName);
        if (!table.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }

        if (table.get().getSd() == null) {
            throw new PrestoException(HiveErrorCode.HIVE_INVALID_METADATA, "Table is missing storage descriptor");
        }

        return Optional.of(table.get().getSd().getCols());
    }

    default long openTransaction(HiveIdentity identity)
    {
        throw new UnsupportedOperationException();
    }

    default void commitTransaction(HiveIdentity identity, long transactionId)
    {
        throw new UnsupportedOperationException();
    }

    default void abortTransaction(HiveIdentity identity, long transactionId)
    {
        throw new UnsupportedOperationException();
    }

    default void sendTransactionHeartbeat(HiveIdentity identity, long transactionId)
    {
        throw new UnsupportedOperationException();
    }

    default void acquireSharedReadLock(HiveIdentity identity, String queryId, long transactionId, List<SchemaTableName> fullTables, List<HivePartition> partitions)
    {
        throw new UnsupportedOperationException();
    }

    default void acquireLock(HiveIdentity identity, String queryId, long transactionId, List<SchemaTableName> fullTables, List<HivePartition> partitions, DataOperationType operationType)
    {
        throw new UnsupportedOperationException();
    }

    default String getValidWriteIds(HiveIdentity identity, List<SchemaTableName> tables, long currentTransactionId, boolean isVacuum)
    {
        throw new UnsupportedOperationException();
    }

    default ShowLocksResponse showLocks(ShowLocksRequest rqst)
    {
        throw new UnsupportedOperationException();
    }

    default long getTableWriteId(String dbName, String tableName, long transactionId)
    {
        throw new UnsupportedOperationException();
    }

    default Optional<String> getConfigValue(String name)
    {
        return Optional.empty();
    }

    default Set<HivePrivilegeInfo> listColumnPrivileges(String databaseName, String tableName, String columnName,
            HivePrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    default Set<HivePrivilegeInfo> listSchemaPrivileges(String databaseName, String tableName,
            HivePrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    default long acquireTableExclusiveLock(
            HiveIdentity identity,
            AcidTransactionOwner transactionOwner,
            String queryId,
            String dbName,
            String tableName)
    {
        throw new UnsupportedOperationException();
    }

    default void releaseTableLock(HiveIdentity identity, long lockId)
    {
        throw new UnsupportedOperationException();
    }

    default List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
    {
        return Collections.emptyList();
    }

    default void updateTableStatistics(HiveIdentity identity, String databaseName, String tableName, AcidTransaction transaction, Function<PartitionStatistics, PartitionStatistics> update)
    {
        throw new UnsupportedOperationException();
    }

    default void updatePartitionStatistics(HiveIdentity identity, Table table, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        throw new UnsupportedOperationException();
    }

    default void dropDatabase(HiveIdentity identity, String databaseName, boolean deleteData)
    {
        throw new UnsupportedOperationException();
    }

    default Optional<List<String>> getPartitionNamesByFilter(HiveIdentity identity, String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        return Optional.empty();
    }

    default Set<RoleGrant> listGrantedPrincipals(String role)
    {
        return Collections.emptySet();
    }

    default void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    default void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    default Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        return Collections.emptySet();
    }

    default long openTransaction(HiveIdentity identity, AcidTransactionOwner transactionOwner)
    {
        throw new UnsupportedOperationException();
    }

    default String getValidWriteIds(HiveIdentity identity, List<SchemaTableName> tables, long currentTransactionId)
    {
        throw new UnsupportedOperationException();
    }

    default long allocateWriteId(HiveIdentity identity, String dbName, String tableName, long transactionId)
    {
        throw new UnsupportedOperationException();
    }

    default void acquireTableWriteLock(
            HiveIdentity identity,
            AcidTransactionOwner transactionOwner,
            String queryId,
            long transactionId,
            String dbName,
            String tableName,
            DataOperationType operation,
            boolean isDynamicPartitionWrite)
    {
        throw new UnsupportedOperationException();
    }

    default void updateTableWriteId(HiveIdentity identity, String dbName, String tableName, long transactionId, long writeId, OptionalLong rowCountChange)
    {
        throw new UnsupportedOperationException();
    }

    default void alterPartitions(HiveIdentity identity, String dbName, String tableName, List<Partition> partitions, long writeId)
    {
        throw new UnsupportedOperationException();
    }

    default void addDynamicPartitions(HiveIdentity identity, String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, AcidOperation operation)
    {
        throw new UnsupportedOperationException();
    }

    default void alterTransactionalTable(HiveIdentity identity, Table table, long transactionId, long writeId)
    {
        throw new UnsupportedOperationException();
    }
}
