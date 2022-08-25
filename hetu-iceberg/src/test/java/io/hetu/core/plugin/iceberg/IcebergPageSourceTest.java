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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.hetu.core.plugin.iceberg.delete.IcebergPositionDeletePageSink;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.NodeVersion;
import io.prestosql.plugin.hive.ReaderColumns;
import io.prestosql.plugin.hive.ReaderProjectionsAdapter;
import io.prestosql.plugin.hive.orc.OrcWriterConfig;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.metrics.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static io.prestosql.spi.type.IntegerType.INTEGER;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class IcebergPageSourceTest
{
    @Mock
    private Schema mockSchema;
    @Mock
    private ConnectorPageSource mockDelegate;

    private IcebergPageSource icebergPageSourceUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        IcebergColumnHandle icebergColumnHandle = new IcebergColumnHandle(
                new ColumnIdentity(1, "test", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                INTEGER,
                ImmutableList.of(),
                INTEGER,
                Optional.empty());
        IcebergColumnHandle icebergColumnHandles = new IcebergColumnHandle(
                new ColumnIdentity(1, "test", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                INTEGER,
                ImmutableList.of(),
                INTEGER,
                Optional.empty());
        icebergPageSourceUnderTest = new IcebergPageSource(
                mockSchema,
                Arrays.asList(icebergColumnHandle),
                Arrays.asList(icebergColumnHandle),
                Arrays.asList(icebergColumnHandle),
                mockDelegate,
                Optional.of(new ReaderProjectionsAdapter(
                        Arrays.asList(icebergColumnHandle), new ReaderColumns(Arrays.asList(icebergColumnHandle, icebergColumnHandles), Arrays.asList(0)),
                        column -> INTEGER,
                        (required, read) -> Arrays.asList(1)
                )), Optional.empty(), () -> new IcebergPositionDeletePageSink("dataFilePath", PartitionSpec.unpartitioned(),
                Optional.of(new PartitionData(new Object[]{"partitionValues"})), null,
                         new IcebergFileWriterFactory(new HdfsEnvironment(null, new HiveConfig(), null), null,
                                new NodeVersion("version"), new FileFormatDataSourceStats(), new OrcWriterConfig()),
                        new HdfsEnvironment(null, new HiveConfig(), null),
                         new HdfsEnvironment.HdfsContext(null, "schemaName", "tableName"), null,
                null, IcebergFileFormat.ORC, new HashMap<>(), 0L),
                () -> new IcebergPageSink(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(
                        Arrays.asList(0))), PartitionSpec.unpartitioned(), null,
                        new IcebergFileWriterFactory(new HdfsEnvironment(null, new HiveConfig(), null), null,
                                new NodeVersion("version"), new FileFormatDataSourceStats(), new OrcWriterConfig()),
                        null, new HdfsEnvironment(null, new HiveConfig(), null),
                        new HdfsEnvironment.HdfsContext(null, "schemaName", "tableName"), Arrays.asList(
                                new IcebergColumnHandle(
                                        new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()),
                                null, Arrays.asList(0), null, Optional.of("value"))), null,
                        null, IcebergFileFormat.ORC, new HashMap<>(), 0),
                Arrays.asList(new IcebergColumnHandle(
                        new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), INTEGER,
                        Arrays.asList(0), INTEGER, Optional.of("value"))));
    }

    @Test
    public void testGetCompletedBytes()
    {
        // Setup
        when(mockDelegate.getCompletedBytes()).thenReturn(0L);

        // Run the test
        final long result = icebergPageSourceUnderTest.getCompletedBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetCompletedPositions()
    {
        // Setup
        final OptionalLong expectedResult = OptionalLong.of(0);
        when(mockDelegate.getCompletedPositions()).thenReturn(OptionalLong.of(0));

        // Run the test
        final OptionalLong result = icebergPageSourceUnderTest.getCompletedPositions();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetCompletedPositions_ConnectorPageSourceReturnsAbsent()
    {
        // Setup
        when(mockDelegate.getCompletedPositions()).thenReturn(OptionalLong.empty());

        // Run the test
        final OptionalLong result = icebergPageSourceUnderTest.getCompletedPositions();

        // Verify the results
        assertEquals(OptionalLong.empty(), result);
    }

    @Test
    public void testGetReadTimeNanos()
    {
        // Setup
        when(mockDelegate.getReadTimeNanos()).thenReturn(0L);

        // Run the test
        final long result = icebergPageSourceUnderTest.getReadTimeNanos();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testIsFinished()
    {
        // Setup
        when(mockDelegate.isFinished()).thenReturn(false);

        // Run the test
        final boolean result = icebergPageSourceUnderTest.isFinished();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testGetNextPage()
    {
        // Setup
        when(mockDelegate.getNextPage()).thenReturn(new Page(0, new Properties(), null));

        // Run the test
        final Page result = icebergPageSourceUnderTest.getNextPage();

        // Verify the results
    }

    @Test
    public void testGetNextPage_ConnectorPageSourceReturnsNull()
    {
        // Setup
        when(mockDelegate.getNextPage()).thenReturn(null);

        // Run the test
        final Page result = icebergPageSourceUnderTest.getNextPage();

        // Verify the results
        assertNull(result);
    }

    @Test
    public void testDeleteRows()
    {
        // Setup
        final Block rowIds = null;

        // Run the test
        icebergPageSourceUnderTest.deleteRows(rowIds);

        // Verify the results
    }

    @Test
    public void testUpdateRows1()
    {
        long[] longs = new long[1];
        longs[0] = 12;
        Block columnT = new LongArrayBlock(1, Optional.empty(), longs);
        final Page page = new Page(0, new Properties(), columnT);
        // Configure Schema.columns(...).
        final List<Types.NestedField> nestedFields = Arrays.asList(Types.NestedField.optional(0, "name", null));
        when(mockSchema.columns()).thenReturn(nestedFields);

        // Run the test
        icebergPageSourceUnderTest.updateRows(page, Arrays.asList(0), Arrays.asList("value"));

        // Verify the results
    }

    @Test
    public void testUpdateRows1_SchemaReturnsNoItems()
    {
        // Setup
        final Page page = new Page(0, new Properties(), null);
        when(mockSchema.columns()).thenReturn(Collections.emptyList());

        // Run the test
        icebergPageSourceUnderTest.updateRows(page, Arrays.asList(0), Arrays.asList("value"));

        // Verify the results
    }

    @Test
    public void testUpdateRows2()
    {
        // Setup
        final Page page = new Page(0, new Properties(), null);

        // Configure Schema.columns(...).
        final List<Types.NestedField> nestedFields = Arrays.asList(Types.NestedField.optional(0, "name", null));
        when(mockSchema.columns()).thenReturn(nestedFields);

        // Run the test
        icebergPageSourceUnderTest.updateRows(page, Arrays.asList(0));

        // Verify the results
    }

    @Test
    public void testUpdateRows2_SchemaReturnsNoItems()
    {
        // Setup
        final Page page = new Page(0, new Properties(), null);
        when(mockSchema.columns()).thenReturn(Collections.emptyList());

        // Run the test
        icebergPageSourceUnderTest.updateRows(page, Arrays.asList(0));

        // Verify the results
    }

    @Test
    public void testFinish()
    {
        // Setup
        // Run the test
        final CompletableFuture<Collection<Slice>> result = icebergPageSourceUnderTest.finish();

        // Verify the results
    }

    @Test
    public void testAbort()
    {
        // Setup
        // Run the test
        icebergPageSourceUnderTest.abort();

        // Verify the results
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        icebergPageSourceUnderTest.close();

        // Verify the results
        verify(mockDelegate).close();
    }

    @Test
    public void testClose_ConnectorPageSourceThrowsIOException() throws Exception
    {
        // Setup
        doThrow(IOException.class).when(mockDelegate).close();

        // Run the test
        icebergPageSourceUnderTest.close();

        // Verify the results
    }

    @Test
    public void testToString() throws Exception
    {
        // Setup
        when(mockDelegate.toString()).thenReturn("result");

        // Run the test
        final String result = icebergPageSourceUnderTest.toString();

        // Verify the results
        assertEquals("delegate", result);
    }

    @Test
    public void testGetMemoryUsage()
    {
        // Setup
        when(mockDelegate.getMemoryUsage()).thenReturn(0L);

        // Run the test
        final long result = icebergPageSourceUnderTest.getMemoryUsage();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetSystemMemoryUsage()
    {
        // Setup
        when(mockDelegate.getMemoryUsage()).thenReturn(0L);

        // Run the test
        final long result = icebergPageSourceUnderTest.getSystemMemoryUsage();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetMetrics()
    {
        // Setup
        when(mockDelegate.getMetrics()).thenReturn(new Metrics(new HashMap<>()));

        // Run the test
        final Metrics result = icebergPageSourceUnderTest.getMetrics();

        // Verify the results
    }

    @Test
    public void testCloseWithSuppression()
    {
        // Setup
        // Run the test
        icebergPageSourceUnderTest.closeWithSuppression(new Exception("message"));

        // Verify the results
    }
}
