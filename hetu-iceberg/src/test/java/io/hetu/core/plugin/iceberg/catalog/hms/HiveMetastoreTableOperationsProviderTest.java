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

import io.hetu.core.plugin.iceberg.FileIoProvider;
import io.hetu.core.plugin.iceberg.catalog.IcebergTableOperations;
import io.hetu.core.plugin.iceberg.catalog.TrinoCatalog;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastore;
import io.prestosql.spi.connector.ConnectorSession;
import org.apache.iceberg.io.FileIO;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class HiveMetastoreTableOperationsProviderTest
{
    @Mock
    private FileIoProvider mockFileIoProvider;
    @Mock
    private ThriftMetastore mockThriftMetastore;

    private HiveMetastoreTableOperationsProvider hiveMetastoreTableOperationsProviderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        hiveMetastoreTableOperationsProviderUnderTest = new HiveMetastoreTableOperationsProvider(mockFileIoProvider,
                mockThriftMetastore);
    }

    @Test
    public void testCreateTableOperations()
    {
        // Setup
        final TrinoCatalog catalog = null;
        final ConnectorSession session = null;

        // Configure FileIoProvider.createFileIo(...).
        final FileIO mockFileIO = mock(FileIO.class);
        when(mockFileIoProvider.createFileIo(any(HdfsEnvironment.HdfsContext.class), eq("queryId")))
                .thenReturn(mockFileIO);

        // Run the test
        final IcebergTableOperations result = hiveMetastoreTableOperationsProviderUnderTest.createTableOperations(
                catalog, session, "database", "table",
                Optional.of("value"), Optional.of("value"));

        // Verify the results
        verify(mockFileIO).close();
    }
}
