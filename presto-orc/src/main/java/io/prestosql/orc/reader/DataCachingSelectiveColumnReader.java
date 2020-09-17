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
package io.prestosql.orc.reader;

import com.google.common.cache.Cache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcRowDataCacheKey;
import io.prestosql.orc.TupleDomainFilter;
import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.metadata.OrcColumnId;
import io.prestosql.orc.stream.InputStreamSources;
import io.prestosql.orc.stream.StreamSourceMeta;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.DictionaryBlock;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.prestosql.orc.OrcReader.handleCacheLoadException;

public class DataCachingSelectiveColumnReader<T>
        implements SelectiveColumnReader<T>
{
    private static final Logger log = Logger.get(DataCachingSelectiveColumnReader.class);

    private final Cache<OrcRowDataCacheKey, Block> cache;
    private final ColumnReader delegate;
    private final OrcColumn column;
    private final OrcColumnId columnId;

    private OrcDataSourceId orcDataSourceId;
    private long stripeOffset;
    private long rowGroupOffset;
    private int[] positions;
    private int positionCount;

    private Block cachedBlock;
    private Block resultBlock;
    private boolean isDictionary;

    public DataCachingSelectiveColumnReader(ColumnReader delegate, OrcColumn column,
                               Cache<OrcRowDataCacheKey, Block> cache)
    {
        this.delegate = delegate;
        this.column = column;
        this.columnId = column.getColumnId();
        this.cache = cache;
    }

    @Override
    public int read(int offset, int[] positions, int positionCount, TupleDomainFilter filter) throws IOException
    {
        int maxPosition = Integer.min(cachedBlock.getPositionCount(), positions[positionCount - 1]) + 1;
        Block block = cachedBlock.getRegion(offset, maxPosition);
        this.isDictionary = false;
        this.resultBlock = block;

        if (filter != null) {
            this.positions = new int[positionCount];

            /* Traverse block and find matches:
             *      Filter condition is pushdown to the cached row data block such that only matched positions are
             *      given to the upper layer
             */
            this.positionCount = block.filter(positions, positionCount, this.positions, (value) -> delegate.filterTest(filter, value));
            if (this.positionCount != positionCount) {
                this.positions = Arrays.copyOf(this.positions, this.positionCount); /* since dictionary block uses arr.length */
                this.isDictionary = true;
            }
        }
        else {
            this.positions = positions.clone();
            this.positionCount = positionCount;
        }

        return this.positionCount;
    }

    @Override
    public int readOr(int offset, int[] positions, int positionCount, List<TupleDomainFilter> filter, BitSet accumulator)
    {
        int maxPosition = Integer.min(cachedBlock.getPositionCount(), positions[positionCount - 1]) + 1;
        Block block = cachedBlock.getRegion(offset, maxPosition);
        this.positionCount = positionCount;
        this.positions = positions.clone();
        this.isDictionary = false;

        /* traverse block and find matches */
        for (int i = 0; i < positionCount; i++) {
            if (accumulator.get(positions[i])
                    || delegate.filterTest(filter.get(0), block.get(positions[i]))) {
                accumulator.set(positions[i]);
            }
        }

        this.resultBlock = block;
        return this.positionCount;
    }

    @Override
    public int[] getReadPositions()
    {
        return positions;
    }

    @Override
    public Block getBlock(int[] positions, int positionCount)
    {
        if (this.resultBlock.getPositionCount() != positionCount || this.isDictionary) {
            return new DictionaryBlock<T>(this.resultBlock, positions);
        }
        return this.resultBlock;
    }

    @Override
    public void startStripe(ZoneId fileTimeZone, ZoneId storageTimeZone, InputStreamSources dictionaryStreamSources, ColumnMetadata<ColumnEncoding> encoding) throws IOException
    {
        delegate.startStripe(fileTimeZone, storageTimeZone, dictionaryStreamSources, encoding);
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources) throws IOException
    {
        StreamSourceMeta streamSourceMeta = dataStreamSources.getStreamSourceMeta();
        orcDataSourceId = streamSourceMeta.getDataSourceId();
        stripeOffset = streamSourceMeta.getStripeOffset();
        rowGroupOffset = streamSourceMeta.getRowGroupOffset();
        cachedBlock = getCachedBlock(streamSourceMeta.getRowCount(), dataStreamSources);
        //reset the stream - may not be required at all
        delegate.startRowGroup(dataStreamSources);
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

    private Block getCachedBlock(long rowCount, InputStreamSources dataStreamSources) throws IOException
    {
        OrcRowDataCacheKey cacheKey = new OrcRowDataCacheKey();
        cacheKey.setOrcDataSourceId(orcDataSourceId);
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
}
