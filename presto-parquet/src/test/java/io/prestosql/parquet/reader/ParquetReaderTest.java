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
package io.prestosql.parquet.reader;

import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.parquet.Field;
import io.prestosql.parquet.ParquetDataSource;
import io.prestosql.parquet.ParquetReaderOptions;
import io.prestosql.parquet.predicate.Predicate;
import io.prestosql.spi.block.Block;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.MessageColumnIO;
import org.joda.time.DateTimeZone;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class ParquetReaderTest
{
    @Mock
    private MessageColumnIO mockMessageColumnIO;
    @Mock
    private ParquetDataSource mockDataSource;
    @Mock
    private DateTimeZone mockTimeZone;
    @Mock
    private AggregatedMemoryContext mockMemoryContext;
    @Mock
    private ParquetReaderOptions mockOptions;
    @Mock
    private Predicate mockParquetPredicate;

    private ParquetReader parquetReaderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        parquetReaderUnderTest = new ParquetReader(Optional.of("value"), mockMessageColumnIO,
                Arrays.asList(new BlockMetaData()), Arrays.asList(0L), mockDataSource, mockTimeZone, mockMemoryContext,
                mockOptions, mockParquetPredicate,
                Arrays.asList(Optional.empty()));
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        parquetReaderUnderTest.close();

        // Verify the results
        verify(mockDataSource).close();
    }

    @Test
    public void testClose_ParquetDataSourceThrowsIOException() throws Exception
    {
        // Setup
        doThrow(IOException.class).when(mockDataSource).close();

        // Run the test
        assertThrows(IOException.class, () -> parquetReaderUnderTest.close());
    }

    @Test
    public void testGetPosition()
    {
        assertEquals(0L, parquetReaderUnderTest.getPosition());
    }

    @Test
    public void testNextBatch()
    {
        // Setup
        // Run the test
        final int result = parquetReaderUnderTest.nextBatch();

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testReadBlock() throws Exception
    {
        // Setup
        final Field field = null;

        // Run the test
        final Block result = parquetReaderUnderTest.readBlock(field);

        // Verify the results
        verify(mockDataSource).readFully(eq(0L), any(byte[].class));
    }

    @Test
    public void testReadBlock_ThrowsIOException()
    {
        // Setup
        final Field field = null;

        // Run the test
        assertThrows(IOException.class, () -> parquetReaderUnderTest.readBlock(field));
        verify(mockDataSource).readFully(eq(0L), any(byte[].class));
    }
}
