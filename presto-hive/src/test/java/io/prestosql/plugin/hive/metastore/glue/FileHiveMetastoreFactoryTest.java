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
package io.prestosql.plugin.hive.metastore.glue;

import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.file.FileHiveMetastore;
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

public class FileHiveMetastoreFactoryTest
{
    @Mock
    private FileHiveMetastore mockMetastore;

    private FileHiveMetastoreFactory fileHiveMetastoreFactoryUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        fileHiveMetastoreFactoryUnderTest = new FileHiveMetastoreFactory(mockMetastore);
    }

    @Test
    public void testGetMetastore()
    {
        fileHiveMetastoreFactoryUnderTest.getMetastore();
    }

    @Test
    public void testIsImpersonationEnabled()
    {
        fileHiveMetastoreFactoryUnderTest.isImpersonationEnabled();
    }

    @Test
    public void testCreateMetastore() throws Exception
    {
        // Setup
        final Optional<ConnectorIdentity> identity = Optional.of(new ConnectorIdentity("user", new HashSet<>(
                Arrays.asList("value")), Optional.empty(),
                Optional.of(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>()));

        // Run the test
        final HiveMetastore result = fileHiveMetastoreFactoryUnderTest.createMetastore(identity);
    }
}
