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
package io.prestosql.plugin.hive.parquet;

import io.prestosql.parquet.writer.ParquetWriterOptions;
import io.prestosql.spi.Page;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Callable;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class ParquetFileWriterTest
{
    @Mock
    private Callable<Void> mockRollbackAction;
    @Mock
    private ParquetWriterOptions mockParquetWriterOptions;

    private ParquetFileWriter parquetFileWriterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        ParquetWriterOptions build = new ParquetWriterOptions.Builder().build();
        parquetFileWriterUnderTest = new ParquetFileWriter(
                new ByteArrayOutputStream(),
                mockRollbackAction,
                Arrays.asList(),
                new MessageType("name", Arrays.asList()),
                new HashMap<>(),
                build,
                new int[]{0},
                CompressionCodecName.UNCOMPRESSED,
                "trinoVersion");
    }

    @Test
    public void testGetWrittenBytes()
    {
        // Setup
        // Run the test
        final long result = parquetFileWriterUnderTest.getWrittenBytes();
    }

    @Test
    public void testGetSystemMemoryUsage()
    {
        // Setup
        // Run the test
        final long result = parquetFileWriterUnderTest.getSystemMemoryUsage();
    }

    @Test
    public void testAppendRows()
    {
        // Setup
        final Page dataPage = new Page(0, new Properties(), null);

        // Run the test
        parquetFileWriterUnderTest.appendRows(dataPage);

        // Verify the results
    }

    @Test
    public void testCommit() throws Exception
    {
        parquetFileWriterUnderTest.commit();
    }

    @Test
    public void testCommit_CallableThrowsException() throws Exception
    {
        // Setup
        when(mockRollbackAction.call()).thenThrow(Exception.class);

        // Run the test
        parquetFileWriterUnderTest.commit();

        // Verify the results
    }

    @Test
    public void testRollback() throws Exception
    {
        // Setup
        when(mockRollbackAction.call()).thenReturn(null);

        // Run the test
        parquetFileWriterUnderTest.rollback();

        // Verify the results
        verify(mockRollbackAction).call();
    }

    @Test
    public void testRollback_CallableThrowsException() throws Exception
    {
        // Setup
        when(mockRollbackAction.call()).thenThrow(Exception.class);

        // Run the test
        parquetFileWriterUnderTest.rollback();

        // Verify the results
    }

    @Test
    public void testGetValidationCpuNanos()
    {
        assertEquals(0L, parquetFileWriterUnderTest.getValidationCpuNanos());
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", parquetFileWriterUnderTest.toString());
    }
}
