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
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableExecuteHandle;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.MockitoAnnotations.initMocks;

public class IcebergPageSinkProviderTest
{
    @Mock
    private HdfsEnvironment mockHdfsEnvironment;
    @Mock
    private JsonCodec<CommitTaskData> mockJsonCodec;
    @Mock
    private IcebergFileWriterFactory mockFileWriterFactory;
    @Mock
    private PageIndexerFactory mockPageIndexerFactory;
    @Mock
    private IcebergConfig mockConfig;

    private IcebergPageSinkProvider icebergPageSinkProviderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        icebergPageSinkProviderUnderTest = new IcebergPageSinkProvider(mockHdfsEnvironment, mockJsonCodec,
                mockFileWriterFactory, mockPageIndexerFactory, mockConfig);
    }

    @Test
    public void testCreatePageSink1()
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorOutputTableHandle outputTableHandle = null;

        // Run the test
        final ConnectorPageSink result = icebergPageSinkProviderUnderTest.createPageSink(null, session,
                outputTableHandle);

        // Verify the results
    }

    @Test
    public void testCreatePageSink2()
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorInsertTableHandle insertTableHandle = null;

        // Run the test
        final ConnectorPageSink result = icebergPageSinkProviderUnderTest.createPageSink(null, session,
                insertTableHandle);

        // Verify the results
    }

    @Test
    public void testCreatePageSink4()
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableExecuteHandle tableExecuteHandle = null;

        // Run the test
        final ConnectorPageSink result = icebergPageSinkProviderUnderTest.createPageSink(null, session,
                tableExecuteHandle);

        // Verify the results
    }
}
