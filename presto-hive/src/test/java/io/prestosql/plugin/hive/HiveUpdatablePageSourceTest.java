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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.prestosql.plugin.hive.acid.AcidOperation;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class HiveUpdatablePageSourceTest
{
    @Mock
    private HiveTableHandle mockHiveTableHandle;
    @Mock
    private ConnectorPageSource mockHivePageSource;
    @Mock
    private TypeManager mockTypeManager;
    @Mock
    private Path mockBucketPath;
    @Mock
    private OrcFileWriterFactory mockOrcFileWriterFactory;
    @Mock
    private Configuration mockConfiguration;
    @Mock
    private ConnectorSession mockSession;

    private HiveUpdatablePageSource hiveUpdatablePageSourceUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        HiveTableHandle expected = new HiveTableHandle("schema", "table", ImmutableMap.of(), ImmutableList.of(), Optional.empty());
        HiveColumnHandle hiveColumnHandle = new HiveColumnHandle("name", HiveType.HIVE_BOOLEAN,
                new TypeSignature("base", TypeSignatureParameter.of(0L)),
                0,
                HiveColumnHandle.ColumnType.PARTITION_KEY,
                Optional.of("value"),
                false);

        hiveUpdatablePageSourceUnderTest = new HiveUpdatablePageSource(
                expected,
                "partitionName",
                0,
                mockHivePageSource,
                mockTypeManager,
                OptionalInt.of(0),
                mockBucketPath,
                false,
                mockOrcFileWriterFactory,
                mockConfiguration,
                mockSession,
                HiveType.HIVE_BOOLEAN,
                Arrays.asList(hiveColumnHandle),
                AcidOperation.NONE,
                0L,
                0L);
    }

    @Test
    public void testDeleteRows() throws Exception
    {
        // Setup
        final Block rowIds = null;

        // Run the test
        hiveUpdatablePageSourceUnderTest.deleteRows(rowIds);

        // Verify the results
    }

    @Test
    public void testUpdateRows() throws Exception
    {
        // Setup
        final Page page = new Page(0, new Properties(), null);

        // Run the test
        hiveUpdatablePageSourceUnderTest.updateRows(page, Arrays.asList(0));

        // Verify the results
    }

    @Test
    public void testCreateRowIdBlock()
    {
        // Setup
        // Run the test
        final Block result = hiveUpdatablePageSourceUnderTest.createRowIdBlock(0);

        // Verify the results
    }

    @Test
    public void testFinish()
    {
        // Setup
        // Run the test
        final CompletableFuture<Collection<Slice>> result = hiveUpdatablePageSourceUnderTest.finish();

        // Verify the results
    }

    @Test
    public void testGetCompletedBytes() throws Exception
    {
        // Setup
        when(mockHivePageSource.getCompletedBytes()).thenReturn(0L);

        // Run the test
        final long result = hiveUpdatablePageSourceUnderTest.getCompletedBytes();
    }

    @Test
    public void testGetReadTimeNanos()
    {
        // Setup
        when(mockHivePageSource.getReadTimeNanos()).thenReturn(0L);

        // Run the test
        final long result = hiveUpdatablePageSourceUnderTest.getReadTimeNanos();
    }

    @Test
    public void testIsFinished()
    {
        hiveUpdatablePageSourceUnderTest.isFinished();
    }

    @Test
    public void testGetNextPage() throws Exception
    {
        // Setup
        when(mockHivePageSource.getNextPage()).thenReturn(new Page(0, new Properties(), null));

        // Run the test
        final Page result = hiveUpdatablePageSourceUnderTest.getNextPage();

        // Verify the results
        verify(mockHivePageSource).close();
    }

    @Test
    public void testGetNextPage_ConnectorPageSourceGetNextPageReturnsNull() throws Exception
    {
        // Setup
        when(mockHivePageSource.getNextPage()).thenReturn(null);

        // Run the test
        final Page result = hiveUpdatablePageSourceUnderTest.getNextPage();
    }

    @Test
    public void testGetNextPage_ConnectorPageSourceCloseThrowsIOException() throws Exception
    {
        // Setup
        when(mockHivePageSource.getNextPage()).thenReturn(new Page(0, new Properties(), null));
        doThrow(IOException.class).when(mockHivePageSource).close();

        // Run the test
        final Page result = hiveUpdatablePageSourceUnderTest.getNextPage();

        // Verify the results
    }

    @Test
    public void testGetSystemMemoryUsage() throws Exception
    {
        // Setup
        when(mockHivePageSource.getMemoryUsage()).thenReturn(0L);

        // Run the test
        final long result = hiveUpdatablePageSourceUnderTest.getSystemMemoryUsage();
    }

    @Test
    public void testGetMemoryUsage()
    {
        // Setup
        when(mockHivePageSource.getMemoryUsage()).thenReturn(0L);

        // Run the test
        final long result = hiveUpdatablePageSourceUnderTest.getMemoryUsage();
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        hiveUpdatablePageSourceUnderTest.close();
    }
}
