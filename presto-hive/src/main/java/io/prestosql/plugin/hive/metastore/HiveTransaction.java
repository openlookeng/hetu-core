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

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.HiveACIDWriteType;
import io.prestosql.plugin.hive.HivePartition;
import io.prestosql.plugin.hive.HiveTableHandle;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.spi.connector.SchemaTableName;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.metastore.api.DataOperationType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

public class HiveTransaction
{
    private final HiveIdentity identity;
    private final long transactionId;
    private final ScheduledFuture<?> heartbeatTask;
    private final Map<String, AtomicBoolean> locksMap = new HashMap<>();
    private final Map<HivePartition, AtomicBoolean> partitionLocks = new HashMap<>();

    private final Map<SchemaTableName, ValidTxnWriteIdList> validHiveTransactionsForTable = new HashMap<>();

    public HiveTransaction(HiveIdentity identity, long transactionId, ScheduledFuture<?> heartbeatTask)
    {
        this.identity = requireNonNull(identity, "identity is null");
        this.transactionId = transactionId;
        this.heartbeatTask = requireNonNull(heartbeatTask, "heartbeatTask is null");
    }

    public HiveIdentity getIdentity()
    {
        return identity;
    }

    public long getTransactionId()
    {
        return transactionId;
    }

    public ScheduledFuture<?> getHeartbeatTask()
    {
        return heartbeatTask;
    }

    public ValidTxnWriteIdList getValidWriteIds(HiveMetastore metastore, HiveTableHandle tableHandle, String queryId, boolean isVacuum)
    {
        //If the update or delete would have locked exclusive lock, then there is no need to lock again.
        if (isSharedLockNeeded(tableHandle)) {
            // Different calls for same table might need to lock different partitions so acquire locks every time
            metastore.acquireSharedReadLock(
                    identity,
                    queryId,
                    transactionId,
                    !tableHandle.getPartitions().isPresent() ? ImmutableList.of(tableHandle.getSchemaTableName()) : ImmutableList.of(),
                    tableHandle.getPartitions().orElse(ImmutableList.of()));
        }

        // For repeatable reads within a query, use the same list of valid transactions for a table which have once been used
        return validHiveTransactionsForTable.computeIfAbsent(tableHandle.getSchemaTableName(), schemaTableName -> new ValidTxnWriteIdList(
                metastore.getValidWriteIds(
                        identity,
                        ImmutableList.of(schemaTableName),
                        transactionId,
                        isVacuum)));
    }

    //If the query is on the same table, on which update/delete happening separate lock not required.
    private synchronized boolean isSharedLockNeeded(HiveTableHandle tableHandle)
    {
        if (tableHandle.getPartitions().isPresent() && tableHandle.getPartitions().get().size() > 0) {
            List<HivePartition> hivePartitions = tableHandle.getPartitions().get();
            for (HivePartition partition : hivePartitions) {
                AtomicBoolean partitionLockFlag = partitionLocks.get(partition);
                if (partitionLockFlag == null || !partitionLockFlag.get()) {
                    //some partition lock not found
                    return true;
                }
            }
        }
        else {
            AtomicBoolean lockFlag = locksMap.get(tableHandle.getSchemaPrefixedTableName());
            if (lockFlag == null || !lockFlag.get()) {
                return true;
            }
        }
        return false;
    }

    private synchronized void setLockFlagForTable(HiveTableHandle tableHandle)
    {
        if (tableHandle.getPartitions().isPresent() && tableHandle.getPartitions().get().size() > 0) {
            List<HivePartition> hivePartitions = tableHandle.getPartitions().get();
            hivePartitions.stream().forEach(hivePartition -> {
                AtomicBoolean flag = partitionLocks.get(hivePartition);
                if (flag == null) {
                    flag = new AtomicBoolean(true);
                    partitionLocks.put(hivePartition, flag);
                }
                else {
                    flag.set(true);
                }
            });
        }
        else {
            AtomicBoolean flag = locksMap.get(tableHandle.getSchemaPrefixedTableName());
            if (flag == null) {
                flag = new AtomicBoolean(true);
                locksMap.put(tableHandle.getSchemaPrefixedTableName(), flag);
            }
            else {
                flag.set(true);
            }
        }
    }

    public Long getTableWriteId(HiveMetastore metastore, HiveTableHandle tableHandle, HiveACIDWriteType writeType, String queryId)
    {
        DataOperationType operationType = DataOperationType.INSERT;
        boolean semiSharedLock = false;
        switch (writeType) {
            case VACUUM:
            case INSERT:
                operationType = DataOperationType.INSERT;
                break;
            case INSERT_OVERWRITE:
            case UPDATE:
                operationType = DataOperationType.UPDATE;
                semiSharedLock = true;
                break;
            case DELETE:
                operationType = DataOperationType.DELETE;
                semiSharedLock = true;
        }
        metastore.acquireLock(
                identity,
                queryId,
                transactionId,
                !tableHandle.getPartitions().isPresent() ? ImmutableList.of(tableHandle.getSchemaTableName()) : ImmutableList.of(),
                tableHandle.getPartitions().orElse(ImmutableList.of()),
                operationType);
        if (semiSharedLock) {
            setLockFlagForTable(tableHandle);
        }
        return metastore.getTableWriteId(tableHandle.getSchemaName(), tableHandle.getTableName(), transactionId);
    }
}
