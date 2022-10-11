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
import io.hetu.core.plugin.iceberg.util.IcebergTestUtil;
import io.prestosql.icebergutil.TestSchemaMetadata;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.BucketFunction;
import io.prestosql.spi.connector.ConnectorBucketNodeMap;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.testing.TestingTypeManager;
import io.prestosql.testing.TestingNodeManager;
import io.prestosql.testing.TestingTransactionHandle;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static org.mockito.Mockito.when;

public class IcebergNodePartitioningProviderTest
{
    private TypeManager mockTypeManager;
    private NodeManager mockNodeManager;
    private IcebergNodePartitioningProvider icebergNodePartitioningProviderUnderTest;
    private TestSchemaMetadata metadata;
    private ConnectorSession connectorSession;

    @BeforeMethod
    public void setUp() throws Exception
    {
        metadata = IcebergTestUtil.getMetadata();
        connectorSession = IcebergTestUtil.getConnectorSession(metadata);
        mockTypeManager = new TestingTypeManager();
        mockNodeManager = new TestingNodeManager();
        icebergNodePartitioningProviderUnderTest = new IcebergNodePartitioningProvider(mockTypeManager, mockNodeManager);
    }

    @Test
    public void testGetBucketNodeMap()
    {
        // Setup
        when(mockNodeManager.getRequiredWorkerNodes()).thenReturn(new HashSet<>());

        // Run the test
        final ConnectorBucketNodeMap result = icebergNodePartitioningProviderUnderTest.getBucketNodeMap(null, null,
                null);

        // Verify the results
    }

    @Test
    public void testGetBucketNodeMap_NodeManagerReturnsNoItems()
    {
        // Setup
        when(mockNodeManager.getRequiredWorkerNodes()).thenReturn(Collections.emptySet());

        // Run the test
        final ConnectorBucketNodeMap result = icebergNodePartitioningProviderUnderTest.getBucketNodeMap(null, null,
                null);

        // Verify the results
    }

    @Test
    public void testGetSplitBucketFunction()
    {
        // Setup
        // Run the test
        final ToIntFunction<ConnectorSplit> result = icebergNodePartitioningProviderUnderTest.getSplitBucketFunction(
                null, null, null);

        // Verify the results
    }

    @Test
    public void testGetBucketFunction()
    {
        IcebergColumnHandle icebergColumnHandle = new IcebergColumnHandle(
                new ColumnIdentity(1, "test?", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                BIGINT,
                ImmutableList.of(),
                BIGINT,
                Optional.empty());
        IcebergPartitioningHandle icebergPartitioningHandle = new IcebergPartitioningHandle(Arrays.asList("test?"), Arrays.asList(icebergColumnHandle));
        List<Type> collect = Stream.of(BooleanType.BOOLEAN).collect(Collectors.toList());
        TestingTransactionHandle testingTransactionHandle = new TestingTransactionHandle(UUID.randomUUID());
        BucketFunction result = icebergNodePartitioningProviderUnderTest.getBucketFunction(
                testingTransactionHandle,
                connectorSession,
                icebergPartitioningHandle,
                collect,
                10);
    }
}
