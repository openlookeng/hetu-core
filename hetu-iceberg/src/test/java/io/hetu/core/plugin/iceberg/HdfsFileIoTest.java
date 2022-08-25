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
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.type.TimeZoneKey;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;

public class HdfsFileIoTest
{
    @Mock
    private HdfsEnvironment mockEnvironment;

    private HdfsFileIo hdfsFileIoUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        ConnectorSession session = new ConnectorSession()
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
        };
        hdfsFileIoUnderTest = new HdfsFileIo(mockEnvironment,
                new HdfsEnvironment.HdfsContext(session, "schemaName", "tableName"));
    }

    @Test
    public void testNewInputFile()
    {
        // Setup
        // Run the test
        final InputFile result = hdfsFileIoUnderTest.newInputFile("path");

        // Verify the results
    }

    @Test
    public void testNewOutputFile()
    {
        // Setup
        // Run the test
        final OutputFile result = hdfsFileIoUnderTest.newOutputFile("path");

        // Verify the results
    }

    @Test
    public void testDeleteFile()
    {
        // Setup
        // Configure HdfsEnvironment.getFileSystem(...).
        // Run the test
        hdfsFileIoUnderTest.deleteFile("path");
    }
}
