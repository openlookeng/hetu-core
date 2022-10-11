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

import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.BucketFunction;
import io.prestosql.spi.connector.ConnectorBucketNodeMap;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPartitioningHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeOperators;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import javax.inject.Inject;

import java.util.List;
import java.util.function.ToIntFunction;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.hetu.core.plugin.iceberg.PartitionFields.parsePartitionFields;
import static io.hetu.core.plugin.iceberg.TypeConverter.toIcebergType;
import static io.prestosql.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static java.util.Objects.requireNonNull;

public class IcebergNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final TypeOperators typeOperators;
    private final NodeManager nodeManager;

    @Inject
    public IcebergNodePartitioningProvider(TypeManager typeManager, NodeManager nodeManager)
    {
        this.typeOperators = requireNonNull(typeManager, "typeManager is null").getTypeOperators();
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public ConnectorBucketNodeMap getBucketNodeMap(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        return createBucketNodeMap(nodeManager.getRequiredWorkerNodes().size());
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        return split -> {
            // Not currently used, likely because IcebergMetadata.getTableProperties currently does not expose partitioning.
            throw new UnsupportedOperationException();
        };
    }

    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount)
    {
        IcebergPartitioningHandle handle = (IcebergPartitioningHandle) partitioningHandle;
        Schema schema = toIcebergSchema(handle.getPartitioningColumns());
        return new IcebergBucketFunction(
                typeOperators,
                parsePartitionFields(schema, handle.getPartitioning()),
                handle.getPartitioningColumns(),
                bucketCount);
    }

    private static Schema toIcebergSchema(List<IcebergColumnHandle> columns)
    {
        List<Types.NestedField> icebergColumns = columns.stream()
                .map(column -> {
                    org.apache.iceberg.types.Type type = toIcebergType(column.getType());
                    return Types.NestedField.of(column.getId(), true, column.getName(), type);
                })
                .collect(toImmutableList());
        org.apache.iceberg.types.Type icebergSchema = Types.StructType.of(icebergColumns);
        return new Schema(icebergSchema.asStructType().fields());
    }
}
