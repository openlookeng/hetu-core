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

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class ParquetColumnChunkTest
{
    @Mock
    private ColumnChunkDescriptor mockDescriptor;

    private ParquetColumnChunk parquetColumnChunkUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        parquetColumnChunkUnderTest = new ParquetColumnChunk(Optional.of("value"), mockDescriptor, "content".getBytes(StandardCharsets.UTF_8),
                0);
    }

    @Test
    public void testReadPageHeader() throws Exception
    {
        // Setup
        final PageHeader expectedResult = new PageHeader(PageType.DATA_PAGE, 0, 0);

        // Run the test
        final PageHeader result = parquetColumnChunkUnderTest.readPageHeader();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testReadPageHeader_ThrowsIOException()
    {
        // Setup
        // Run the test
        assertThrows(IOException.class, () -> parquetColumnChunkUnderTest.readPageHeader());
    }

    @Test
    public void testReadAllPages() throws Exception
    {
        // Setup
        // Configure ColumnChunkDescriptor.getColumnChunkMetaData(...).
        final ColumnChunkMetaData columnChunkMetaData = ColumnChunkMetaData.get(
                ColumnPath.fromDotString("path"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"),
                CompressionCodecName.UNCOMPRESSED, null, new HashSet<>(
                        Arrays.asList(Encoding.PLAIN)), Statistics.createStats(null), 0L, 0L, 0L, 0L, 0L);
        when(mockDescriptor.getColumnChunkMetaData()).thenReturn(columnChunkMetaData);

        // Configure ColumnChunkDescriptor.getColumnDescriptor(...).
        final ColumnDescriptor columnDescriptor = new ColumnDescriptor(new String[]{"path"},
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"), 0, 0);
        when(mockDescriptor.getColumnDescriptor()).thenReturn(columnDescriptor);

        // Run the test
        final PageReader result = parquetColumnChunkUnderTest.readAllPages();

        // Verify the results
    }

    @Test
    public void testReadAllPages_ThrowsIOException()
    {
        // Setup
        // Configure ColumnChunkDescriptor.getColumnChunkMetaData(...).
        final ColumnChunkMetaData columnChunkMetaData = ColumnChunkMetaData.get(
                ColumnPath.fromDotString("path"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"),
                CompressionCodecName.UNCOMPRESSED, null, new HashSet<>(
                        Arrays.asList(Encoding.PLAIN)), Statistics.createStats(null), 0L, 0L, 0L, 0L, 0L);
        when(mockDescriptor.getColumnChunkMetaData()).thenReturn(columnChunkMetaData);

        // Configure ColumnChunkDescriptor.getColumnDescriptor(...).
        final ColumnDescriptor columnDescriptor = new ColumnDescriptor(new String[]{"path"},
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"), 0, 0);
        when(mockDescriptor.getColumnDescriptor()).thenReturn(columnDescriptor);

        // Run the test
        assertThrows(IOException.class, () -> parquetColumnChunkUnderTest.readAllPages());
    }

    @Test
    public void testGetPosition()
    {
        assertEquals(0, parquetColumnChunkUnderTest.getPosition());
    }
}
