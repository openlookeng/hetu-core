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
package io.hetu.core.plugin.iceberg.delete;

import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.hetu.core.plugin.iceberg.CommitTaskData;
import io.hetu.core.plugin.iceberg.IcebergFileFormat;
import io.hetu.core.plugin.iceberg.IcebergFileWriterFactory;
import io.hetu.core.plugin.iceberg.PartitionData;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.type.TimeZoneKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.LocationProvider;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class IcebergPositionDeletePageSinkTest
{
    @Mock
    private PartitionSpec mockPartitionSpec;
    @Mock
    private LocationProvider mockLocationProvider;
    @Mock
    private IcebergFileWriterFactory mockFileWriterFactory;
    @Mock
    private HdfsEnvironment mockHdfsEnvironment;
    @Mock
    private JsonCodec<CommitTaskData> mockJsonCodec;
    @Mock
    private ConnectorSession mockSession;

    private IcebergPositionDeletePageSink icebergPositionDeletePageSinkUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        ConnectorSession connectorSession = new ConnectorSession()
        {
            @Override
            public String getQueryId()
            {
                return "queryId";
            }

            @Override
            public Optional<String> getSource()
            {
                return Optional.empty();
            }

            @Override
            public ConnectorIdentity getIdentity()
            {
                return new ConnectorIdentity("value", Optional.empty(), Optional.empty());
            }

            @Override
            public TimeZoneKey getTimeZoneKey()
            {
                return null;
            }

            @Override
            public Locale getLocale()
            {
                return null;
            }

            @Override
            public Optional<String> getTraceToken()
            {
                return Optional.empty();
            }

            @Override
            public long getStartTime()
            {
                return 0;
            }

            @Override
            public <T> T getProperty(String name, Class<T> type)
            {
                return null;
            }
        };
        icebergPositionDeletePageSinkUnderTest = new IcebergPositionDeletePageSink("dataFilePath", mockPartitionSpec,
                Optional.of(new PartitionData(new Object[]{"/tmp/huayupy"})), mockLocationProvider,
                mockFileWriterFactory, mockHdfsEnvironment,
                new HdfsEnvironment.HdfsContext(connectorSession, "schemaName", "tableName"), mockJsonCodec, mockSession,
                IcebergFileFormat.ORC, new HashMap<>(), 0L);
    }

    @Test
    public void testGetCompletedBytes()
    {
        // Setup
        // Run the test
        final long result = icebergPositionDeletePageSinkUnderTest.getCompletedBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetSystemMemoryUsage()
    {
        // Setup
        // Run the test
        final long result = icebergPositionDeletePageSinkUnderTest.getSystemMemoryUsage();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testAppendPage()
    {
        // Setup
        final Page page = new Page(0, new Properties(), null);

        // Run the test
        final CompletableFuture<?> result = icebergPositionDeletePageSinkUnderTest.appendPage(page);

        // Verify the results
    }

    @Test
    public void testFinish()
    {
        // Setup
        when(mockJsonCodec.toJsonBytes(any(CommitTaskData.class))).thenReturn("content".getBytes());

        // Run the test
        final CompletableFuture<Collection<Slice>> result = icebergPositionDeletePageSinkUnderTest.finish();

        // Verify the results
    }

    @Test
    public void testFinish_JsonCodecThrowsIllegalArgumentException()
    {
        // Setup
        when(mockJsonCodec.toJsonBytes(any(CommitTaskData.class))).thenThrow(IllegalArgumentException.class);

        // Run the test
        final CompletableFuture<Collection<Slice>> result = icebergPositionDeletePageSinkUnderTest.finish();

        // Verify the results
    }

    @Test
    public void testAbort()
    {
        // Setup
        // Run the test
        icebergPositionDeletePageSinkUnderTest.abort();

        // Verify the results
    }
}
