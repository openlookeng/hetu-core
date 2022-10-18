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
package io.prestosql.plugin.hive;

import io.prestosql.spi.connector.BucketFunction;
import io.prestosql.spi.connector.ConnectorBucketNodeMap;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.type.Type;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import java.util.function.ToIntFunction;

public class HiveNodePartitioningProviderTest
{
    private HiveNodePartitioningProvider hiveNodePartitioningProviderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        hiveNodePartitioningProviderUnderTest = new HiveNodePartitioningProvider();
    }

    @Test
    public void testGetBucketFunction()
    {
        // Setup
        final List<Type> partitionChannelTypes = Arrays.asList();

        HivePartitioningHandle hivePartitioningHandle = new HivePartitioningHandle(HiveBucketing.BucketingVersion.BUCKETING_V1, 0, Arrays.asList(HiveType.HIVE_DOUBLE), OptionalInt.empty());
        // Run the test
        final BucketFunction result = hiveNodePartitioningProviderUnderTest.getBucketFunction(
                new HiveTransactionHandle(true),
                new VacuumCleanerTest.ConnectorSession(),
                hivePartitioningHandle,
                partitionChannelTypes, 0);

        // Verify the results
    }

    @Test
    public void testGetBucketNodeMap()
    {
        // Setup
        HivePartitioningHandle hivePartitioningHandle = new HivePartitioningHandle(HiveBucketing.BucketingVersion.BUCKETING_V1, 1, Arrays.asList(HiveType.HIVE_DOUBLE), OptionalInt.empty());

        // Run the test
        final ConnectorBucketNodeMap result = hiveNodePartitioningProviderUnderTest.getBucketNodeMap(new HiveTransactionHandle(true),
                new VacuumCleanerTest.ConnectorSession(),
                hivePartitioningHandle);

        // Verify the results
    }

    @Test
    public void testGetSplitBucketFunction()
    {
        // Setup
        // Run the test
        HivePartitioningHandle hivePartitioningHandle = new HivePartitioningHandle(HiveBucketing.BucketingVersion.BUCKETING_V1, 0, Arrays.asList(HiveType.HIVE_DOUBLE), OptionalInt.empty());
        final ToIntFunction<ConnectorSplit> result = hiveNodePartitioningProviderUnderTest.getSplitBucketFunction(new HiveTransactionHandle(true),
                new VacuumCleanerTest.ConnectorSession(), hivePartitioningHandle);

        // Verify the results
    }

    @Test
    public void testListPartitionHandles()
    {
        // Setup

        // Run the test
        HivePartitioningHandle hivePartitioningHandle = new HivePartitioningHandle(HiveBucketing.BucketingVersion.BUCKETING_V1, 0, Arrays.asList(HiveType.HIVE_DOUBLE), OptionalInt.empty());
        final List<ConnectorPartitionHandle> result = hiveNodePartitioningProviderUnderTest.listPartitionHandles(new HiveTransactionHandle(true),
                new VacuumCleanerTest.ConnectorSession(), hivePartitioningHandle);
    }
}
