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
package io.prestosql.plugin.hive.metastore;

import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.hive.ForRecordingHiveMetastore;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.recording.HiveMetastoreRecording;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.type.Type;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.plugin.hive.metastore.HivePartitionName.hivePartitionName;
import static io.prestosql.plugin.hive.metastore.HiveTableName.hiveTableName;
import static io.prestosql.plugin.hive.metastore.MetastoreUtil.makePartitionName;
import static io.prestosql.plugin.hive.metastore.PartitionFilter.partitionFilter;
import static java.util.Objects.requireNonNull;

public class RecordingHiveMetastore
        implements HiveMetastore
{
    private final HiveMetastoreRecording recording;
    private final HiveMetastore delegate;

    @Inject
    public RecordingHiveMetastore(@ForRecordingHiveMetastore HiveMetastore delegate, HiveMetastoreRecording recording)
    {
        this.recording = requireNonNull(recording, "recording is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return recording.getDatabase(databaseName, () -> delegate.getDatabase(databaseName));
    }

    @Override
    public List<String> getAllDatabases()
    {
        return recording.getAllDatabases(delegate::getAllDatabases);
    }

    @Override
    public Optional<Table> getTable(HiveIdentity identity, String databaseName, String tableName)
    {
        return recording.getTable(hiveTableName(databaseName, tableName), () -> delegate.getTable(identity, databaseName, tableName));
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return recording.getSupportedColumnStatistics(type.getTypeSignature().toString(), () -> delegate.getSupportedColumnStatistics(type));
    }

    @Override
    public PartitionStatistics getTableStatistics(HiveIdentity identity, Table table)
    {
        return recording.getTableStatistics(
                hiveTableName(table.getDatabaseName(), table.getTableName()),
                () -> delegate.getTableStatistics(identity, table));
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(HiveIdentity identity, Table table, List<Partition> partitions)
    {
        return recording.getPartitionStatistics(
                partitions.stream()
                        .map(partition -> hivePartitionName(hiveTableName(table.getDatabaseName(), table.getTableName()), makePartitionName(table, partition)))
                        .collect(toImmutableSet()),
                () -> delegate.getPartitionStatistics(identity, table, partitions));
    }

    @Override
    public void updateTableStatistics(HiveIdentity identity, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        verifyRecordingMode();
        delegate.updateTableStatistics(identity, databaseName, tableName, update);
    }

    @Override
    public void updatePartitionStatistics(HiveIdentity identity, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        verifyRecordingMode();
        delegate.updatePartitionStatistics(identity, databaseName, tableName, partitionName, update);
    }

    @Override
    public void updatePartitionsStatistics(HiveIdentity identity, String databaseName, String tableName, Map<String, Function<PartitionStatistics, PartitionStatistics>> partNamesUpdateFunctionMap)
    {
        partNamesUpdateFunctionMap.entrySet().stream().forEach(e -> {
            updatePartitionStatistics(identity, databaseName, tableName, e.getKey(), e.getValue());
        });
    }

    @Override
    public Optional<List<String>> getAllTables(String databaseName)
    {
        return recording.getAllTables(databaseName, () -> delegate.getAllTables(databaseName));
    }

    @Override
    public Optional<List<String>> getAllViews(String databaseName)
    {
        return recording.getAllViews(databaseName, () -> delegate.getAllViews(databaseName));
    }

    @Override
    public void createDatabase(HiveIdentity identity, Database database)
    {
        verifyRecordingMode();
        delegate.createDatabase(identity, database);
    }

    @Override
    public void dropDatabase(HiveIdentity identity, String databaseName)
    {
        verifyRecordingMode();
        delegate.dropDatabase(identity, databaseName);
    }

    @Override
    public void renameDatabase(HiveIdentity identity, String databaseName, String newDatabaseName)
    {
        verifyRecordingMode();
        delegate.renameDatabase(identity, databaseName, newDatabaseName);
    }

    @Override
    public void createTable(HiveIdentity identity, Table table, PrincipalPrivileges principalPrivileges)
    {
        verifyRecordingMode();
        delegate.createTable(identity, table, principalPrivileges);
    }

    @Override
    public void dropTable(HiveIdentity identity, String databaseName, String tableName, boolean deleteData)
    {
        verifyRecordingMode();
        delegate.dropTable(identity, databaseName, tableName, deleteData);
    }

    @Override
    public void replaceTable(HiveIdentity identity, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        verifyRecordingMode();
        delegate.replaceTable(identity, databaseName, tableName, newTable, principalPrivileges);
    }

    @Override
    public void renameTable(HiveIdentity identity, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        verifyRecordingMode();
        delegate.renameTable(identity, databaseName, tableName, newDatabaseName, newTableName);
    }

    @Override
    public void commentTable(HiveIdentity identity, String databaseName, String tableName, Optional<String> comment)
    {
        verifyRecordingMode();
        delegate.commentTable(identity, databaseName, tableName, comment);
    }

    @Override
    public void addColumn(HiveIdentity identity, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        verifyRecordingMode();
        delegate.addColumn(identity, databaseName, tableName, columnName, columnType, columnComment);
    }

    @Override
    public void renameColumn(HiveIdentity identity, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        verifyRecordingMode();
        delegate.renameColumn(identity, databaseName, tableName, oldColumnName, newColumnName);
    }

    @Override
    public void dropColumn(HiveIdentity identity, String databaseName, String tableName, String columnName)
    {
        verifyRecordingMode();
        delegate.dropColumn(identity, databaseName, tableName, columnName);
    }

    @Override
    public Optional<Partition> getPartition(HiveIdentity identity, String databaseName, String tableName, List<String> partitionValues)
    {
        return recording.getPartition(
                hivePartitionName(databaseName, tableName, partitionValues),
                () -> delegate.getPartition(identity, databaseName, tableName, partitionValues));
    }

    @Override
    public Optional<List<String>> getPartitionNames(HiveIdentity identity, String databaseName, String tableName)
    {
        return recording.getPartitionNames(
                hiveTableName(databaseName, tableName),
                () -> delegate.getPartitionNames(identity, databaseName, tableName));
    }

    @Override
    public Optional<List<String>> getPartitionNamesByParts(HiveIdentity identity, String databaseName, String tableName, List<String> parts)
    {
        return recording.getPartitionNamesByFilter(
                partitionFilter(databaseName, tableName, parts),
                () -> delegate.getPartitionNamesByParts(identity, databaseName, tableName, parts));
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(HiveIdentity identity, String databaseName, String tableName, List<String> partitionNames)
    {
        return recording.getPartitionsByNames(
                partitionNames.stream()
                        .map(partitionName -> hivePartitionName(hiveTableName(databaseName, tableName), partitionName))
                        .collect(toImmutableSet()),
                () -> delegate.getPartitionsByNames(identity, databaseName, tableName, partitionNames));
    }

    @Override
    public void addPartitions(HiveIdentity identity, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        verifyRecordingMode();
        delegate.addPartitions(identity, databaseName, tableName, partitions);
    }

    @Override
    public void dropPartition(HiveIdentity identity, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        verifyRecordingMode();
        delegate.dropPartition(identity, databaseName, tableName, parts, deleteData);
    }

    @Override
    public void alterPartition(HiveIdentity identity, String databaseName, String tableName, PartitionWithStatistics partition)
    {
        verifyRecordingMode();
        delegate.alterPartition(identity, databaseName, tableName, partition);
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, HivePrincipal principal)
    {
        return recording.listTablePrivileges(
                new UserTableKey(principal, databaseName, tableName),
                () -> delegate.listTablePrivileges(databaseName, tableName, principal));
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        verifyRecordingMode();
        delegate.grantTablePrivileges(databaseName, tableName, grantee, privileges);
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        verifyRecordingMode();
        delegate.revokeTablePrivileges(databaseName, tableName, grantee, privileges);
    }

    private Set<HivePartitionName> getHivePartitionNames(String databaseName, String tableName, Set<String> partitionNames)
    {
        return partitionNames.stream()
                .map(partitionName -> HivePartitionName.hivePartitionName(databaseName, tableName, partitionName))
                .collect(ImmutableSet.toImmutableSet());
    }

    @Override
    public void createRole(String role, String grantor)
    {
        verifyRecordingMode();
        delegate.createRole(role, grantor);
    }

    @Override
    public void dropRole(String role)
    {
        verifyRecordingMode();
        delegate.dropRole(role);
    }

    @Override
    public Set<String> listRoles()
    {
        return recording.listRoles(delegate::listRoles);
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean withAdminOption, HivePrincipal grantor)
    {
        verifyRecordingMode();
        delegate.grantRoles(roles, grantees, withAdminOption, grantor);
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOptionFor, HivePrincipal grantor)
    {
        verifyRecordingMode();
        delegate.revokeRoles(roles, grantees, adminOptionFor, grantor);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return recording.listRoleGrants(
                principal,
                () -> delegate.listRoleGrants(principal));
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return delegate.isImpersonationEnabled();
    }

    private void verifyRecordingMode()
    {
        if (recording.isReplay()) {
            throw new IllegalStateException("Cannot perform Metastore updates in replay mode");
        }
    }
}
