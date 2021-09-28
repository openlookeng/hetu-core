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
package io.prestosql.orc.reader;

import com.google.common.cache.Cache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcDataSourceIdWithTimeStamp;
import io.prestosql.orc.OrcRowDataCacheKey;
import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.metadata.OrcColumnId;
import io.prestosql.orc.stream.InputStreamSources;
import io.prestosql.orc.stream.StreamSourceMeta;
import io.prestosql.spi.block.Block;

import java.io.IOException;
import java.time.ZoneId;
import java.util.concurrent.ExecutionException;

import static io.prestosql.orc.OrcReader.handleCacheLoadException;

public class CachingColumnReader<T>
        implements ColumnReader<T>
{
    private static final Logger log = Logger.get(CachingColumnReader.class);

    private final Cache<OrcRowDataCacheKey, Block> cache;
    private final ColumnReader delegate;
    private final OrcColumn column;
    private final OrcColumnId columnId;

    private OrcDataSourceId orcDataSourceId;
    private long lastModifiedTime;
    private long stripeOffset;
    private long rowGroupOffset;
    private int offset;
    private int nextBatchSize;
    private Block cachedBlock;

    public CachingColumnReader(ColumnReader delegate, OrcColumn column,
                               Cache<OrcRowDataCacheKey, Block> cache)
    {
        this.delegate = delegate;
        this.column = column;
        this.columnId = column.getColumnId();
        this.cache = cache;
    }

    @Override
    public Block readBlock() throws IOException
    {
        log.debug("Reading from cached block. DatasourceId = %s, columnId = %s, stripeOffset = %d, rowGroupOffset = %d, Block offset = %d, Block size = %d, Column = %s", orcDataSourceId, columnId, stripeOffset, rowGroupOffset, offset, nextBatchSize, column);
        return cachedBlock.getRegion(offset, nextBatchSize);
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        this.offset += this.nextBatchSize;
        this.nextBatchSize = batchSize;
        delegate.prepareNextRead(batchSize);
    }

    @Override
    public void startStripe(ZoneId fileTimeZone, InputStreamSources dictionaryStreamSources,
            ColumnMetadata<ColumnEncoding> encoding) throws IOException
    {
        this.offset = 0;
        this.nextBatchSize = 0;
        delegate.startStripe(fileTimeZone, dictionaryStreamSources, encoding);
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources) throws IOException
    {
        this.offset = 0;
        this.nextBatchSize = 0;

        StreamSourceMeta streamSourceMeta = dataStreamSources.getStreamSourceMeta();
        orcDataSourceId = streamSourceMeta.getDataSourceId();
        lastModifiedTime = streamSourceMeta.getLastModifiedTime();
        stripeOffset = streamSourceMeta.getStripeOffset();
        rowGroupOffset = streamSourceMeta.getRowGroupOffset();
        cachedBlock = getCachedBlock(streamSourceMeta.getRowCount(), dataStreamSources);
        //reset the stream - may not be required at all
        delegate.startRowGroup(dataStreamSources);
    }

    private Block getCachedBlock(long rowCount, InputStreamSources dataStreamSources) throws IOException
    {
        OrcRowDataCacheKey cacheKey = new OrcRowDataCacheKey();
        cacheKey.setOrcDataSourceId(new OrcDataSourceIdWithTimeStamp(orcDataSourceId, lastModifiedTime));
        cacheKey.setStripeOffset(stripeOffset);
        cacheKey.setRowGroupOffset(rowGroupOffset);
        cacheKey.setColumnId(columnId);
        try {
            return cache.get(cacheKey, () -> {
                delegate.startRowGroup(dataStreamSources);
                delegate.prepareNextRead((int) rowCount);
                log.debug("Caching row group data. DatasourceId = %s, columnId = %s, stripeOffset = %d, rowGroupOffset = %d, Column = %s", orcDataSourceId, columnId, stripeOffset, rowGroupOffset, column);
                return delegate.readBlock();
            });
        }
        catch (UncheckedExecutionException | ExecutionException executionException) {
            handleCacheLoadException(executionException);
            log.debug(executionException.getCause(), "Error while caching row group data. Falling back to default flow...");
            delegate.startRowGroup(dataStreamSources);
            delegate.prepareNextRead((int) rowCount);
            cachedBlock = delegate.readBlock();
            return cachedBlock;
        }
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return delegate.getRetainedSizeInBytes();
    }
}
