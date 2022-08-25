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
import io.hetu.core.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.prestosql.plugin.hive.HdfsEnvironment;
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

public class IcebergMetadataFactoryTest
{
    @Mock
    private TypeManager mockTypeManager;
    @Mock
    private JsonCodec<CommitTaskData> mockCommitTaskCodec;
    @Mock
    private TrinoCatalogFactory mockCatalogFactory;
    @Mock
    private HdfsEnvironment mockHdfsEnvironment;

    private IcebergMetadataFactory icebergMetadataFactoryUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        icebergMetadataFactoryUnderTest = new IcebergMetadataFactory(mockTypeManager, mockCommitTaskCodec,
                mockCatalogFactory, mockHdfsEnvironment);
    }

    @Test
    public void testCreate()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        when(mockCatalogFactory.create(
                new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                        Optional.of(new SelectedRole(
                                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>()))).thenReturn(null);

        // Run the test
        final IcebergMetadata result = icebergMetadataFactoryUnderTest.create(identity);

        // Verify the results
    }
}
