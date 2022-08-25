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

import io.airlift.json.JsonCodec;
import io.hetu.core.plugin.iceberg.procedure.IcebergOptimizeHandle;
import io.hetu.core.plugin.iceberg.procedure.IcebergTableExecuteHandle;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableExecuteHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SchemaTableName;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.io.LocationProvider;

import javax.inject.Inject;

import static io.hetu.core.plugin.iceberg.IcebergUtil.getLocationProvider;
import static java.util.Objects.requireNonNull;

public class IcebergPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final IcebergFileWriterFactory fileWriterFactory;
    private final PageIndexerFactory pageIndexerFactory;
    private final int maxOpenPartitions;

    @Inject
    public IcebergPageSinkProvider(
            HdfsEnvironment hdfsEnvironment,
            JsonCodec<CommitTaskData> jsonCodec,
            IcebergFileWriterFactory fileWriterFactory,
            PageIndexerFactory pageIndexerFactory,
            IcebergConfig config)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
        requireNonNull(config, "config is null");
        this.maxOpenPartitions = config.getMaxPartitionsPerWriter();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        return createPageSink(session, (IcebergWritableTableHandle) outputTableHandle);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        return createPageSink(session, (IcebergWritableTableHandle) insertTableHandle);
    }

    private ConnectorPageSink createPageSink(ConnectorSession session, IcebergWritableTableHandle tableHandle)
    {
        HdfsContext hdfsContext = new HdfsContext(session.getIdentity());
        Schema schema = SchemaParser.fromJson(tableHandle.getSchemaAsJson());
        PartitionSpec partitionSpec = PartitionSpecParser.fromJson(schema, tableHandle.getPartitionSpecAsJson());
        LocationProvider locationProvider = getLocationProvider(new SchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName()),
                tableHandle.getOutputPath(), tableHandle.getStorageProperties());
        return new IcebergPageSink(
                schema,
                partitionSpec,
                locationProvider,
                fileWriterFactory,
                pageIndexerFactory,
                hdfsEnvironment,
                hdfsContext,
                tableHandle.getInputColumns(),
                jsonCodec,
                session,
                tableHandle.getFileFormat(),
                tableHandle.getStorageProperties(),
                maxOpenPartitions);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        IcebergTableExecuteHandle executeHandle = (IcebergTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.getProcedureId()) {
            case OPTIMIZE:
                HdfsContext hdfsContext = new HdfsContext(session.getIdentity());
                IcebergOptimizeHandle optimizeHandle = (IcebergOptimizeHandle) executeHandle.getProcedureHandle();
                Schema schema = SchemaParser.fromJson(optimizeHandle.getSchemaAsJson());
                PartitionSpec partitionSpec = PartitionSpecParser.fromJson(schema, optimizeHandle.getPartitionSpecAsJson());
                LocationProvider locationProvider = getLocationProvider(executeHandle.getSchemaTableName(),
                        executeHandle.getTableLocation(), optimizeHandle.getTableStorageProperties());
                return new IcebergPageSink(
                        schema,
                        partitionSpec,
                        locationProvider,
                        fileWriterFactory,
                        pageIndexerFactory,
                        hdfsEnvironment,
                        hdfsContext,
                        optimizeHandle.getTableColumns(),
                        jsonCodec,
                        session,
                        optimizeHandle.getFileFormat(),
                        optimizeHandle.getTableStorageProperties(),
                        maxOpenPartitions);
            case EXPIRE_SNAPSHOTS:
            case REMOVE_ORPHAN_FILES:
                // handled via ConnectorMetadata.executeTableExecute
        }
        throw new IllegalArgumentException("Unknown procedure: " + executeHandle.getProcedureId());
    }
}
