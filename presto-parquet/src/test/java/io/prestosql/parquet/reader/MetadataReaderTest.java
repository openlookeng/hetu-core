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

import io.prestosql.parquet.ParquetDataSource;
import org.apache.commons.io.input.BrokenInputStream;
import org.apache.commons.io.input.NullInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;

import static org.testng.Assert.assertThrows;

public class MetadataReaderTest
{
    @Test
    public void testReadFooter1() throws Exception
    {
        // Setup
        final ParquetDataSource dataSource = null;

        // Run the test
        final ParquetMetadata result = MetadataReader.readFooter(dataSource);

        // Verify the results
    }

    @Test
    public void testReadFooter1_ThrowsIOException()
    {
        // Setup
        final ParquetDataSource dataSource = null;

        // Run the test
        assertThrows(IOException.class, () -> MetadataReader.readFooter(dataSource));
    }

    @Test
    public void testReadFooter2() throws Exception
    {
        // Setup
        final FileSystem fileSystem = FileSystem.get(new Configuration(false));
        final Path file = new Path("scheme", "authority", "path");

        // Run the test
        final ParquetMetadata result = MetadataReader.readFooter(fileSystem, file, 0L);

        // Verify the results
    }

    @Test
    public void testReadFooter2_ThrowsIOException() throws Exception
    {
        // Setup
        final FileSystem fileSystem = FileSystem.get(new Configuration(false));
        final Path file = new Path("scheme", "authority", "path");

        // Run the test
        assertThrows(IOException.class, () -> MetadataReader.readFooter(fileSystem, file, 0L));
    }

    @Test
    public void testReadFooter3() throws Exception
    {
        // Setup
        final FSDataInputStream inputStream = new FSDataInputStream(new ByteArrayInputStream("content".getBytes()));
        final Path file = new Path("scheme", "authority", "path");

        // Run the test
        final ParquetMetadata result = MetadataReader.readFooter(inputStream, file, 0L);

        // Verify the results
    }

    @Test
    public void testReadFooter3_EmptyInputStream() throws Exception
    {
        // Setup
        final FSDataInputStream inputStream = new FSDataInputStream(new NullInputStream());
        final Path file = new Path("scheme", "authority", "path");

        // Run the test
        final ParquetMetadata result = MetadataReader.readFooter(inputStream, file, 0L);

        // Verify the results
    }

    @Test
    public void testReadFooter3_BrokenInputStream()
    {
        // Setup
        final FSDataInputStream inputStream = new FSDataInputStream(new BrokenInputStream());
        final Path file = new Path("scheme", "authority", "path");

        // Run the test
        assertThrows(IOException.class, () -> MetadataReader.readFooter(inputStream, file, 0L));
    }

    @Test
    public void testReadStats()
    {
        // Setup
        final Optional<Statistics> statisticsFromFile = Optional.of(new Statistics());
        final PrimitiveType type = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0,
                "name");

        // Run the test
        final org.apache.parquet.column.statistics.Statistics<?> result = MetadataReader.readStats(Optional.of("value"),
                statisticsFromFile, type);

        // Verify the results
    }
}
