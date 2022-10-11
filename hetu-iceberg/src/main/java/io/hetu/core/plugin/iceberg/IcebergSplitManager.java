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
package io.hetu.core.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.prestosql.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;

import javax.inject.Inject;

import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.getDynamicFilteringWaitTimeout;
import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.getMinimumAssignedSplitWeight;
import static java.util.Objects.requireNonNull;

public class IcebergSplitManager
        implements ConnectorSplitManager
{
    public static final int ICEBERG_DOMAIN_COMPACTION_THRESHOLD = 1000;

    private final IcebergTransactionManager transactionManager;
    private final TypeManager typeManager;

    @Inject
    public IcebergSplitManager(IcebergTransactionManager transactionManager, TypeManager typeManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        return getSplits(transaction, session, table, splitSchedulingStrategy, DynamicFilter.empty, new Constraint(TupleDomain.all()));
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle handle,
            SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;

        if (!table.getSnapshotId().isPresent()) {
            if (table.isRecordScannedFiles()) {
                return new FixedSplitSource(ImmutableList.of(), ImmutableList.of());
            }
            return new FixedSplitSource(ImmutableList.of());
        }

        Table icebergTable = transactionManager.get(transaction, session.getIdentity()).getIcebergTable(session, table.getSchemaTableName());
        Duration dynamicFilteringWaitTimeout = getDynamicFilteringWaitTimeout(session);

        TableScan tableScan = icebergTable.newScan()
                .useSnapshot(table.getSnapshotId().get());
        IcebergSplitSource splitSource = new IcebergSplitSource(
                table,
                tableScan,
                table.getMaxScannedFileSize(),
                dynamicFilter,
                dynamicFilteringWaitTimeout,
                constraint,
                typeManager,
                table.isRecordScannedFiles(),
                getMinimumAssignedSplitWeight(session));

        return new ClassLoaderSafeConnectorSplitSource(splitSource, IcebergSplitManager.class.getClassLoader());
    }
}
