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

import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.authentication.GenericExceptionAction;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.type.TimeZoneKey;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.io.SeekableInputStream;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.Optional;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class HdfsInputFileTest
{
    @Mock
    private Path mockPath;
    @Mock
    private HdfsEnvironment mockEnvironment;

    private HdfsInputFile hdfsInputFileUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        hdfsInputFileUnderTest = new HdfsInputFile(mockPath, mockEnvironment,
                new HdfsEnvironment.HdfsContext(new ConnectorSession(), "schemaName", "tableName"));
    }

    public static class ConnectorSession
            implements io.prestosql.spi.connector.ConnectorSession
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
            return new ConnectorIdentity("str", Optional.empty(), Optional.empty());
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
    }

    @Test
    public void testGetLength()
    {
        // Setup
        // Run the test
        final long result = hdfsInputFileUnderTest.getLength();
    }

    @Test
    public void testNewStream() throws Exception
    {
        // Setup
        // Configure HdfsEnvironment.doAs(...).
        final SeekableInputStream mockSeekableInputStream = mock(SeekableInputStream.class);
        when(mockEnvironment.doAs(eq("user"), any(GenericExceptionAction.class))).thenReturn(mockSeekableInputStream);

        // Run the test
        final SeekableInputStream result = hdfsInputFileUnderTest.newStream();
    }

    @Test
    public void testNewStream_HdfsEnvironmentThrowsE()
    {
        // Setup
        when(mockEnvironment.doAs(eq("user"), any(GenericExceptionAction.class))).thenThrow(Exception.class);

        // Run the test
        final SeekableInputStream result = hdfsInputFileUnderTest.newStream();

        // Verify the results
    }

    @Test
    public void testLocation()
    {
        // Setup
        // Run the test
        final String result = hdfsInputFileUnderTest.location();
    }

    @Test
    public void testExists()
    {
        // Setup
        when(mockEnvironment.doAs(eq("user"), any(GenericExceptionAction.class))).thenReturn(false);

        // Run the test
        final boolean result = hdfsInputFileUnderTest.exists();
    }

    @Test
    public void testExists_HdfsEnvironmentThrowsE()
    {
        // Setup
        when(mockEnvironment.doAs(eq("user"), any(GenericExceptionAction.class))).thenThrow(Exception.class);

        // Run the test
        final boolean result = hdfsInputFileUnderTest.exists();
    }

    @Test
    public void testToString()
    {
        hdfsInputFileUnderTest.toString();
    }
}
