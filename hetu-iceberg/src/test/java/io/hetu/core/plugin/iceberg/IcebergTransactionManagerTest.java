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

import io.hetu.core.plugin.vdm.DefaultVdmTransactionHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.security.SelectedRole;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;

public class IcebergTransactionManagerTest
{
    @Mock
    private IcebergMetadataFactory mockMetadataFactory;
    @Mock
    private ClassLoader mockClassLoader;

    private IcebergTransactionManager icebergTransactionManagerUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        icebergTransactionManagerUnderTest = new IcebergTransactionManager(mockMetadataFactory, mockClassLoader);
        IcebergTransactionManager icebergTransactionManager = new IcebergTransactionManager(mockMetadataFactory);
    }

    @Test
    public void testBegin()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = new DefaultVdmTransactionHandle();

        // Run the test
        icebergTransactionManagerUnderTest.begin(transactionHandle);

        // Verify the results
    }

    @Test
    public void testGet()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = new DefaultVdmTransactionHandle();
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        icebergTransactionManagerUnderTest.get(transactionHandle, identity);

        // Verify the results
    }

    @Test
    public void testCommit()
    {
        // Setup
        final ConnectorTransactionHandle transaction = new DefaultVdmTransactionHandle();

        // Run the test
        icebergTransactionManagerUnderTest.commit(transaction);

        // Verify the results
    }

    @Test
    public void testRollback()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;

        // Run the test
        icebergTransactionManagerUnderTest.rollback(transaction);

        // Verify the results
    }
}
