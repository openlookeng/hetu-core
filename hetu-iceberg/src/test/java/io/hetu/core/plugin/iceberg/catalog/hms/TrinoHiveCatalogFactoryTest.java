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
package io.hetu.core.plugin.iceberg.catalog.hms;

import io.hetu.core.plugin.iceberg.IcebergConfig;
import io.hetu.core.plugin.iceberg.IcebergSecurityConfig;
import io.hetu.core.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.hetu.core.plugin.iceberg.catalog.TrinoCatalog;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.NodeVersion;
import io.prestosql.plugin.hive.metastore.HiveMetastoreFactory;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.security.SelectedRole;
import io.prestosql.spi.type.TypeManager;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class TrinoHiveCatalogFactoryTest
{
    @Mock
    private IcebergConfig mockConfig;
    @Mock
    private HiveMetastoreFactory mockMetastoreFactory;
    @Mock
    private HdfsEnvironment mockHdfsEnvironment;
    @Mock
    private TypeManager mockTypeManager;
    @Mock
    private IcebergTableOperationsProvider mockTableOperationsProvider;
    @Mock
    private IcebergSecurityConfig mockSecurityConfig;
    @Mock
    private HiveConfig mockHiveConfig;

    private TrinoHiveCatalogFactory trinoHiveCatalogFactoryUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        trinoHiveCatalogFactoryUnderTest = new TrinoHiveCatalogFactory(mockConfig, new CatalogName("catalogName"),
                mockMetastoreFactory, mockHdfsEnvironment, mockTypeManager, mockTableOperationsProvider,
                new NodeVersion("version"), mockSecurityConfig, mockHiveConfig);
    }

    @Test
    public void testCreate()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        when(mockMetastoreFactory.createMetastore(
                Optional.of(new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                        Optional.of(new SelectedRole(
                                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>())))).thenReturn(null);

        // Run the test
        final TrinoCatalog result = trinoHiveCatalogFactoryUnderTest.create(identity);

        // Verify the results
    }
}
