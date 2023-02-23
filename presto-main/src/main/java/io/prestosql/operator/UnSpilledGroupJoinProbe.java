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

package io.prestosql.operator;

import io.prestosql.operator.aggregation.builder.AggregationBuilder;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

/**
 * UnSpilledGroupJoinProbe
 *
 * @since 21-Feb-2023
 */
public class UnSpilledGroupJoinProbe
        extends GroupJoinProbe
{
    private final int[] probeOutputChannels;
    private final Page page;
    private final long[] joinPositionCache;
    private int position = -1;

    public UnSpilledGroupJoinProbe(int[] probeOutputChannels, Page page, List<Integer> probeJoinChannels, OptionalInt probeHashChannel, Page probePage, LookupSource lookupSource, @Nullable Block probeHashBlock, OptionalInt probeCountChannel, AggregationBuilder probeAggregationBuilder, AggregationBuilder buildAggregationBuilder)
    {
        super(probeOutputChannels, page, probeJoinChannels, probeHashChannel, probeCountChannel, probeAggregationBuilder, buildAggregationBuilder);

        this.probeOutputChannels = requireNonNull(probeOutputChannels, "probeOutputChannels is null");
        this.page = requireNonNull(page, "page is null");

        joinPositionCache = fillCache(lookupSource, page, probeHashBlock, probePage);
    }

    public int[] getOutputChannels()
    {
        return probeOutputChannels;
    }

    public boolean advanceNextPosition()
    {
        verify(++position <= page.getPositionCount(), "already finished");
        return !isFinished();
    }

    public boolean isFinished()
    {
        return position == page.getPositionCount();
    }

    @Override
    public long getCurrentJoinPosition(LookupSource lookupSource)
    {
        return joinPositionCache[position];
    }

    public int getPosition()
    {
        return position;
    }

    public Page getPage()
    {
        return page;
    }

    private static long[] fillCache(
            LookupSource lookupSource,
            Page page,
            Block probeHashBlock,
            Page probePage)
    {
        int positionCount = page.getPositionCount();
        List<Block> nullableBlocks = IntStream.range(0, probePage.getChannelCount())
                .mapToObj(i -> probePage.getBlock(i))
                .filter(Block::mayHaveNull)
                .collect(toImmutableList());

        long[] joinPositionsCache = new long[positionCount];
        if (!nullableBlocks.isEmpty()) {
            Arrays.fill(joinPositionsCache, -1);
            boolean[] isNull = new boolean[positionCount];
            int nonNullCount = getIsNull(nullableBlocks, positionCount, isNull);
            if (nonNullCount < positionCount) {
                // We only store positions that are not null
                int[] positions = new int[nonNullCount];
                nonNullCount = 0;
                for (int i = 0; i < positionCount; i++) {
                    if (!isNull[i]) {
                        positions[nonNullCount] = i;
                    }
                    // This way less code is in the if branch and CPU should be able to optimize branch prediction better
                    nonNullCount += isNull[i] ? 0 : 1;
                }
                if (probeHashBlock != null) {
                    long[] hashes = new long[positionCount];
                    for (int i = 0; i < positionCount; i++) {
                        hashes[i] = BIGINT.getLong(probeHashBlock, i);
                    }
                    lookupSource.getJoinPosition(positions, probePage, page, hashes, joinPositionsCache);
                }
                else {
                    lookupSource.getJoinPosition(positions, probePage, page, joinPositionsCache);
                }
                return joinPositionsCache;
            } // else fall back to non-null path
        }
        int[] positions = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            positions[i] = i;
        }
        if (probeHashBlock != null) {
            long[] hashes = new long[positionCount];
            for (int i = 0; i < positionCount; i++) {
                hashes[i] = BIGINT.getLong(probeHashBlock, i);
            }
            lookupSource.getJoinPosition(positions, probePage, page, hashes, joinPositionsCache);
        }
        else {
            lookupSource.getJoinPosition(positions, probePage, page, joinPositionsCache);
        }

        return joinPositionsCache;
    }

    private static int getIsNull(List<Block> nullableBlocks, int positionCount, boolean[] isNull)
    {
        for (int i = 0; i < nullableBlocks.size() - 1; i++) {
            Block block = nullableBlocks.get(i);
            for (int jPosition = 0; jPosition < positionCount; jPosition++) {
                isNull[jPosition] |= block.isNull(jPosition);
            }
        }
        // Last block will also calculate `nonNullCount`
        int nonNullCount = 0;
        Block lastBlock = nullableBlocks.get(nullableBlocks.size() - 1);
        for (int jPosition = 0; jPosition < positionCount; jPosition++) {
            isNull[jPosition] |= lastBlock.isNull(jPosition);
            nonNullCount += isNull[jPosition] ? 0 : 1;
        }

        return nonNullCount;
    }
}
