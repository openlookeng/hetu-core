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

import com.hazelcast.cache.impl.PreJoinCacheConfig;
import com.hazelcast.cache.impl.operation.AddCacheConfigOperationSupplier;
import io.airlift.json.JsonCodec;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.spi.connector.ConnectorSession;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.function.Supplier;

import static org.mockito.MockitoAnnotations.initMocks;

public class CreateEmptyPartitionProcedureTest
{
    @Mock
    private HiveMetastore mockMetastore;
    @Mock
    private LocationService mockLocationService;
    @Mock
    private JsonCodec<PartitionUpdate> mockPartitionUpdateCodec;

    private CreateEmptyPartitionProcedure createEmptyPartitionProcedureUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        Supplier<TransactionalMetadata> supplier = new Supplier<TransactionalMetadata>()
        {
            @Override
            public TransactionalMetadata get()
            {
                AddCacheConfigOperationSupplier addCacheConfigOperationSupplier = new AddCacheConfigOperationSupplier(new PreJoinCacheConfig());
                return (TransactionalMetadata) addCacheConfigOperationSupplier;
            }
        };
        createEmptyPartitionProcedureUnderTest = new CreateEmptyPartitionProcedure(() -> (TransactionalMetadata) supplier, mockMetastore,
                mockLocationService, mockPartitionUpdateCodec);
    }

    @Test
    public void testCreateEmptyPartition_JsonCodecThrowsIllegalArgumentException()
    {
        // Setup
        final ConnectorSession session = new VacuumCleanerTest.ConnectorSession();
        createEmptyPartitionProcedureUnderTest.createEmptyPartition(session, "schema", "table", Arrays.asList("value"),
                Arrays.asList("value"));

        // Verify the results
    }
}
