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
import io.airlift.log.Logger;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcPredicate;
import io.prestosql.orc.OrcRowDataCacheKey;
import io.prestosql.orc.OrcSelectiveRowDataCacheKey;
import io.prestosql.orc.TupleDomainFilter;
import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.stream.InputStreamSources;
import io.prestosql.orc.stream.StreamSourceMeta;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockListBlock;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.function.BiConsumer;

public class ResultCachingSelectiveColumnReader<T>
        implements SelectiveColumnReader<T>
{
    private static final Logger log = Logger.get(SelectiveColumnReader.class);
    private final Cache<OrcRowDataCacheKey, Block> cache;
    private final SelectiveColumnReader delegate;
    private final OrcColumn column;
    private final OrcPredicate predicate;

    private OrcDataSourceId orcDataSourceId;
    private long stripeOffset;
    private long rowGroupOffset;
    private int offset;
    private int readSize;
    private int totalPositionCount;
    private Block cachedBlock;
    private int[] positions;

    private List<Block<T>> accumulatorBlocks;
    private OrcSelectiveRowDataCacheKey cacheKey;

    public ResultCachingSelectiveColumnReader(Cache<OrcRowDataCacheKey, Block> cache,
                                              SelectiveColumnReader delegate, OrcColumn column,
                                              OrcPredicate predicate)
    {
        this.cache = cache;
        this.delegate = delegate;
        this.column = column;
        this.predicate = predicate;
        this.accumulatorBlocks = new ArrayList<>(5);
    }

    @Override
    public int read(int offset, int[] positions, int positionCount, TupleDomainFilter filter) throws IOException
    {
        if (cachedBlock != null) {
            this.readSize = Integer.min(positionCount, (cachedBlock.getPositionCount() - this.offset));
            this.offset += this.readSize;
            this.positions = positions;

            return this.readSize;
        }

        return delegate.read(offset, positions, positionCount, null);
    }

    @Override
    public int readOr(int offset, int[] positions, int positionCount,
                      List<TupleDomainFilter> filter, BitSet accumulator) throws IOException
    {
        if (cachedBlock != null) {
            this.readSize = Integer.min(positionCount, (cachedBlock.getPositionCount() - this.offset));
            this.offset += this.readSize;
            this.positions = positions;

            if (this.readSize > 0) {
                accumulator.set(offset, offset + this.readSize);
            }
            return this.readSize;
        }

        return delegate.readOr(offset, positions, positionCount, filter, accumulator);
    }

    @Override
    public int[] getReadPositions()
    {
        if (cachedBlock != null) {
            return this.positions;
        }

        return delegate.getReadPositions();
    }

    @Override
    public Block getBlock(int[] positions, int positionCount)
    {
        if (cachedBlock != null) {
            return cachedBlock.getRegion(offset - readSize, readSize);
        }

        Block result = delegate.getBlock(positions, positionCount);
        accumulatorBlocks.add(result);
        totalPositionCount += result.getPositionCount();

        return result;
    }

    @Override
    public void startStripe(ZoneId fileTimeZone, ZoneId storageTimeZone,
                            InputStreamSources dictionaryStreamSources,
                            ColumnMetadata<ColumnEncoding> encoding) throws IOException
    {
        this.offset = 0;
        this.readSize = 0;
        this.cachedBlock = null;

        /* Todo(Nitin) Check if stripe level caching would make sense or reckon be too big a block?
        cacheKey.setOrcDataSourceId(orcDataSourceId);
        cacheKey.setStripeOffset(stripeOffset);
        cacheKey.setRowGroupOffset(-1);
        cacheKey.setColumnId(column.getColumnId());
        cacheKey.setPredicate(predicate);

        cachedBlock = cacheAccumulated(stripeInformation);*/

        delegate.startStripe(fileTimeZone, storageTimeZone, dictionaryStreamSources, encoding);
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources) throws IOException
    {
        this.offset = 0;
        this.readSize = 0;

        StreamSourceMeta streamSourceMeta = dataStreamSources.getStreamSourceMeta();
        orcDataSourceId = streamSourceMeta.getDataSourceId();
        stripeOffset = streamSourceMeta.getStripeOffset();
        rowGroupOffset = streamSourceMeta.getRowGroupOffset();

        OrcSelectiveRowDataCacheKey newCachKey = new OrcSelectiveRowDataCacheKey();
        newCachKey.setOrcDataSourceId(orcDataSourceId);
        newCachKey.setStripeOffset(streamSourceMeta.getStripeOffset());
        newCachKey.setRowGroupOffset(streamSourceMeta.getRowGroupOffset());
        newCachKey.setColumnId(column.getColumnId());
        newCachKey.setPredicate(predicate);

        cachedBlock = cacheAccumulated(newCachKey);

        //reset the stream - may not be required at all
        delegate.startRowGroup(dataStreamSources);
    }

    private Block cacheAccumulated(OrcSelectiveRowDataCacheKey newCachKey) throws IOException
    {
        if (cacheKey != null && cachedBlock == null) {
            /* Fixme(Nitin): check if need to use separate cache for selective? */
            cache.put(cacheKey, mergeAccumulatedBlocks(accumulatorBlocks));
            accumulatorBlocks.clear();
            totalPositionCount = 0;
        }

        this.cacheKey = newCachKey;
        return cache.getIfPresent(newCachKey);
    }

    @Override
    public void close()
    {
        if (cacheKey != null && cachedBlock == null) {
            cache.put(this.cacheKey, mergeAccumulatedBlocks(accumulatorBlocks));
            accumulatorBlocks.clear();
            totalPositionCount = 0;
        }

        delegate.close();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return delegate.getRetainedSizeInBytes();
    }

    private Block mergeAccumulatedBlocks(List<Block<T>> accumulatedBlocks)
    {
        /* Fixme(Nitin): merge all accumulated blocks in single array */
        if (accumulatedBlocks.size() > 0) {
            return new BlockListBlock(accumulatedBlocks.toArray(new Block[accumulatedBlocks.size()]),
                    accumulatedBlocks.size(),
                    totalPositionCount);
        }

        return new Block<T>() {
            @Override
            public void writePositionTo(int position, BlockBuilder blockBuilder)
            {
            }

            @Override
            public Block getSingleValueBlock(int position)
            {
                return null;
            }

            @Override
            public int getPositionCount()
            {
                return 0;
            }

            @Override
            public long getSizeInBytes()
            {
                return 0;
            }

            @Override
            public long getRegionSizeInBytes(int position, int length)
            {
                return 0;
            }

            @Override
            public long getPositionsSizeInBytes(boolean[] positions)
            {
                return 0;
            }

            @Override
            public long getRetainedSizeInBytes()
            {
                return 0;
            }

            @Override
            public long getEstimatedDataSizeForStats(int position)
            {
                return 0;
            }

            @Override
            public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
            {
            }

            @Override
            public String getEncodingName()
            {
                return null;
            }

            @Override
            public Block copyPositions(int[] positions, int offset, int length)
            {
                return null;
            }

            @Override
            public Block getRegion(int positionOffset, int length)
            {
                return null;
            }

            @Override
            public Block copyRegion(int position, int length)
            {
                return null;
            }

            @Override
            public boolean isNull(int position)
            {
                return false;
            }
        };
    }
}
