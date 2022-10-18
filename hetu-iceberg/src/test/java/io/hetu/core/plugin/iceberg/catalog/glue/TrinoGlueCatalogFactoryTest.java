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
package io.hetu.core.plugin.iceberg.catalog.glue;

import com.amazonaws.auth.AWSCredentialsProvider;
import io.hetu.core.plugin.iceberg.IcebergConfig;
import io.hetu.core.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.hetu.core.plugin.iceberg.catalog.TrinoCatalog;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.NodeVersion;
import io.prestosql.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.prestosql.plugin.hive.metastore.glue.GlueMetastoreStats;
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

public class TrinoGlueCatalogFactoryTest
{
    @Mock
    private HdfsEnvironment mockHdfsEnvironment;
    @Mock
    private IcebergTableOperationsProvider mockTableOperationsProvider;
    @Mock
    private GlueHiveMetastoreConfig mockGlueConfig;
    @Mock
    private AWSCredentialsProvider mockCredentialsProvider;
    @Mock
    private IcebergConfig mockIcebergConfig;
    @Mock
    private GlueMetastoreStats mockStats;

    private TrinoGlueCatalogFactory trinoGlueCatalogFactoryUnderTest;

    @BeforeMethod
    public void setUp()
    {
        initMocks(this);
        trinoGlueCatalogFactoryUnderTest = new TrinoGlueCatalogFactory(mockHdfsEnvironment, mockTableOperationsProvider,
                new NodeVersion("version"), mockGlueConfig, mockCredentialsProvider, mockIcebergConfig, mockStats);
    }

    @Test
    public void testCreate()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        final TrinoCatalog result = trinoGlueCatalogFactoryUnderTest.create(identity);

        // Verify the results
    }
}
