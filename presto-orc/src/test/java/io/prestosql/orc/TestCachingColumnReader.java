/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.orc;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.metadata.OrcColumnId;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.orc.reader.CachingColumnReader;
import io.prestosql.orc.reader.ColumnReader;
import io.prestosql.orc.stream.InputStreamSources;
import io.prestosql.orc.stream.StreamSourceMeta;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.block.Block;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.ZoneId;
import java.util.concurrent.Callable;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class TestCachingColumnReader
{
    private final OrcColumn column = new OrcColumn(
            "hdfs://hacluster/user/hive/warehouse/tpcds_orc_hive_1000.db/catalog_sales/cs_sold_date_sk=2452268/000896_0",
            new OrcColumnId(3),
            "cs_order_number",
            OrcType.OrcTypeKind.INT,
            new OrcDataSourceId("hdfs://hacluster/user/hive/warehouse/tpcds_orc_hive_1000.db/catalog_sales/cs_sold_date_sk=2452268/000896_0"),
            ImmutableList.of());

    @Test
    public void testBlockCachedOnStartRowGroup() throws Exception
    {
        ColumnReader columnReader = mock(ColumnReader.class);
        Cache<OrcRowDataCacheKey, Block> cache = spy(CacheBuilder.newBuilder().build());
        CachingColumnReader cachingColumnReader = new CachingColumnReader(columnReader, column, cache);

        InputStreamSources inputStreamSources = mock(InputStreamSources.class);
        StreamSourceMeta streamSourceMeta = new StreamSourceMeta();
        OrcDataSourceId orcDataSourceId = new OrcDataSourceId("2");
        streamSourceMeta.setDataSourceId(orcDataSourceId);
        Block block = mock(Block.class);
        when(inputStreamSources.getStreamSourceMeta()).thenReturn(streamSourceMeta);
        when(columnReader.readBlock()).thenReturn(block);

        cachingColumnReader.startRowGroup(inputStreamSources);

        verify(columnReader, atLeastOnce()).startRowGroup(eq(inputStreamSources));
        verify(columnReader, times(1)).readBlock();
        verify(cache, times(1)).get(any(OrcRowDataCacheKey.class), any(Callable.class));
        assertEquals(cache.size(), 1);
    }

    @Test(expectedExceptions = IOException.class)
    public void testDelegateThrowsException() throws IOException
    {
        ColumnReader columnReader = mock(ColumnReader.class);
        Cache<OrcRowDataCacheKey, Block> cache = spy(CacheBuilder.newBuilder().build());
        CachingColumnReader cachingColumnReader = new CachingColumnReader(columnReader, column, cache);

        InputStreamSources inputStreamSources = mock(InputStreamSources.class);
        StreamSourceMeta streamSourceMeta = new StreamSourceMeta();
        OrcDataSourceId orcDataSourceId = new OrcDataSourceId("2");
        streamSourceMeta.setDataSourceId(orcDataSourceId);
        when(inputStreamSources.getStreamSourceMeta()).thenReturn(streamSourceMeta);
        when(columnReader.readBlock())
                .thenThrow(new OrcCorruptionException(orcDataSourceId, "Value is null but stream is missing"))
                .thenThrow(new OrcCorruptionException(orcDataSourceId, "Value is null but stream is missing"));

        try {
            cachingColumnReader.startRowGroup(inputStreamSources);
        }
        catch (IOException ioEx) {
            verify(columnReader, atLeastOnce()).startRowGroup(eq(inputStreamSources));
            verify(columnReader, times(2)).readBlock();
            assertEquals(cache.size(), 0);
            throw ioEx;
        }
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*Read interrupted.*")
    public void testCacheLoaderThrowsInterruptedException()
            throws IOException
    {
        ColumnReader columnReader = mock(ColumnReader.class);
        Cache<OrcRowDataCacheKey, Block> cache = spy(CacheBuilder.newBuilder().build());
        CachingColumnReader cachingColumnReader = new CachingColumnReader(columnReader, column, cache);

        InputStreamSources inputStreamSources = mock(InputStreamSources.class);
        StreamSourceMeta streamSourceMeta = new StreamSourceMeta();
        OrcDataSourceId orcDataSourceId = new OrcDataSourceId("2");
        streamSourceMeta.setDataSourceId(orcDataSourceId);
        when(inputStreamSources.getStreamSourceMeta()).thenReturn(streamSourceMeta);
        when(columnReader.readBlock()).then((Answer<Block>) invocationOnMock -> {
            Thread.currentThread().interrupt();
            throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Read interrupted");
        });

        try {
            cachingColumnReader.startRowGroup(inputStreamSources);
        }
        catch (IOException ioEx) {
            verify(columnReader, atLeastOnce()).startRowGroup(eq(inputStreamSources));
            verify(columnReader, times(1)).readBlock();
            assertEquals(cache.size(), 0);
            throw ioEx;
        }
        finally {
            //clear interrupted flag status
            Thread.interrupted();
        }
    }

    @Test
    public void testReadBlockRetrievesFromCache() throws Exception
    {
        ColumnReader columnReader = mock(ColumnReader.class);
        Cache<OrcRowDataCacheKey, Block> cache = spy(CacheBuilder.newBuilder().build());
        CachingColumnReader cachingColumnReader = new CachingColumnReader(columnReader, column, cache);

        InputStreamSources inputStreamSources = mock(InputStreamSources.class);
        StreamSourceMeta streamSourceMeta = new StreamSourceMeta();
        OrcDataSourceId orcDataSourceId = new OrcDataSourceId("2");
        streamSourceMeta.setDataSourceId(orcDataSourceId);
        when(inputStreamSources.getStreamSourceMeta()).thenReturn(streamSourceMeta);
        Block block = mock(Block.class);
        when(columnReader.readBlock()).thenReturn(block);

        cachingColumnReader.startRowGroup(inputStreamSources);
        cachingColumnReader.prepareNextRead(10);
        cachingColumnReader.readBlock();
        cachingColumnReader.prepareNextRead(20);
        cachingColumnReader.readBlock();

        verify(columnReader, atLeastOnce()).startRowGroup(eq(inputStreamSources));
        verify(columnReader, times(1)).readBlock();
        InOrder inOrder = inOrder(block);
        inOrder.verify(block, times(1)).getRegion(0, 10);
        inOrder.verify(block, times(1)).getRegion(10, 20);
        verify(cache, times(1)).get(any(OrcRowDataCacheKey.class), any(Callable.class));
        assertEquals(cache.size(), 1);
    }

    @Test
    public void testBlockCachedOnStartStripe() throws IOException
    {
        ColumnReader streamReader = mock(ColumnReader.class);
        Cache<OrcRowDataCacheKey, Block> cache = spy(CacheBuilder.newBuilder().build());
        CachingColumnReader cachingColumnReader = new CachingColumnReader(streamReader, column, cache);

        InputStreamSources inputStreamSources = mock(InputStreamSources.class);
        Stripe stripe = mock(Stripe.class);
        ZoneId fileTimeZone = stripe.getFileTimeZone();
        ColumnMetadata<ColumnEncoding> columnEncodings = stripe.getColumnEncodings();

        cachingColumnReader.startStripe(fileTimeZone, inputStreamSources, columnEncodings);
        verify(streamReader, atLeastOnce()).startStripe(eq(fileTimeZone), eq(inputStreamSources),
                eq(columnEncodings));
    }

    @Test
    public void testCachingStringReaderClosed()
    {
        ColumnReader streamReader = mock(ColumnReader.class);
        Cache<OrcRowDataCacheKey, Block> cache = spy(CacheBuilder.newBuilder().build());
        CachingColumnReader cachingColumnReader = new CachingColumnReader(streamReader, column, cache);
        cachingColumnReader.close();
        verify(streamReader, times(1)).close();
    }

    @Test
    public void testGetRetainedSizeInBytes()
    {
        ColumnReader streamReader = mock(ColumnReader.class);
        Cache<OrcRowDataCacheKey, Block> cache = spy(CacheBuilder.newBuilder().build());
        CachingColumnReader cachingColumnReader = new CachingColumnReader(streamReader, column, cache);

        when(streamReader.getRetainedSizeInBytes()).thenReturn((long) 1);
        assertEquals(cachingColumnReader.getRetainedSizeInBytes(), 1);
    }
}
