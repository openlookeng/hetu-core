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
package io.hetu.core.plugin.iceberg.metastore.thrift;

import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastore;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.security.SelectedRole;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertTrue;

public class BridgingHiveMetastoreFactoryTest
{
    @Mock
    private ThriftMetastore mockThriftMetastore;

    private BridgingHiveMetastoreFactory bridgingHiveMetastoreFactoryUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        bridgingHiveMetastoreFactoryUnderTest = new BridgingHiveMetastoreFactory(mockThriftMetastore);
    }

    @Test
    public void testIsImpersonationEnabled()
    {
        // Setup
        when(mockThriftMetastore.isImpersonationEnabled()).thenReturn(false);

        // Run the test
        final boolean result = bridgingHiveMetastoreFactoryUnderTest.isImpersonationEnabled();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testCreateMetastore()
    {
        // Setup
        final Optional<ConnectorIdentity> identity = Optional.of(new ConnectorIdentity("user", new HashSet<>(
                Arrays.asList("value")), Optional.empty(),
                Optional.of(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>()));

        // Run the test
        final HiveMetastore result = bridgingHiveMetastoreFactoryUnderTest.createMetastore(identity);

        // Verify the results
    }
}
