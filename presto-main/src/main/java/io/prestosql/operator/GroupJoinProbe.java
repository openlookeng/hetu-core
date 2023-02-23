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

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class GroupJoinProbe
{
    public static class GroupJoinProbeFactory
    {
        private final int[] probeOutputChannels;
        private final List<Integer> probeJoinChannels;
        private final OptionalInt probeHashChannel;
        private final OptionalInt probeCountChannel;

        public GroupJoinProbeFactory(int[] probeOutputChannels, List<Integer> probeJoinChannels, OptionalInt probeHashChannel, OptionalInt probeCountChannel)
        {
            this.probeOutputChannels = probeOutputChannels;
            this.probeJoinChannels = probeJoinChannels;
            this.probeHashChannel = probeHashChannel;
            this.probeCountChannel = probeCountChannel;
        }

        public GroupJoinProbe createGroupJoinProbe(Page page, boolean isSpilled, LookupSourceProvider lookupSourceProvider,
                AggregationBuilder probeAggregationBuilder, AggregationBuilder buildAggregationBuilder)
        {
            LookupSource lookupSource = lookupSourceProvider.withLease((lookupSourceLease -> lookupSourceLease.getLookupSource()));
            if (isSpilled || !(lookupSource instanceof JoinHash || lookupSource instanceof OuterLookupSource)) {
                return new GroupJoinProbe(probeOutputChannels, page, probeJoinChannels, probeHashChannel, probeCountChannel, probeAggregationBuilder, buildAggregationBuilder);
            }
            else {
                Page loadedProbePage = page.getLoadedPage(probeJoinChannels.stream().mapToInt(i -> i).toArray());
                return new UnSpilledGroupJoinProbe(probeOutputChannels, page, probeJoinChannels, probeHashChannel, loadedProbePage, lookupSource, (probeHashChannel.isPresent() ? (probeHashChannel.getAsInt() >= 0 ? page.getBlock(probeHashChannel.getAsInt()).getLoadedBlock() : null) : null), probeCountChannel, probeAggregationBuilder, buildAggregationBuilder);
            }
        }
    }

    private final int[] probeOutputChannels;
    private final int positionCount;
    private final Block[] probeBlocks;
    private final AggregationBuilder probeAggregationBuilder;
    private final AggregationBuilder buildAggregationBuilder;
    private final Page page;
    private final Page probePage;
    private final Optional<Block> probeHashBlock;
    private final Optional<Block> probeCountBlock;

    private int position = -1;

    protected GroupJoinProbe(int[] probeOutputChannels, Page page, List<Integer> probeJoinChannels, OptionalInt probeHashChannel,
            OptionalInt probeCountChannel, AggregationBuilder probeAggregationBuilder, AggregationBuilder buildAggregationBuilder)
    {
        this.probeOutputChannels = probeOutputChannels;
        this.positionCount = page.getPositionCount();
        this.probeBlocks = new Block[probeJoinChannels.size()];
        this.probeAggregationBuilder = requireNonNull(probeAggregationBuilder, "probeAggregationBuilder is null");
        this.buildAggregationBuilder = requireNonNull(buildAggregationBuilder, "buildAggregationBuilder is null");

        for (int i = 0; i < probeJoinChannels.size(); i++) {
            probeBlocks[i] = page.getBlock(probeJoinChannels.get(i));
        }
        this.page = page;
        this.probePage = new Page(page.getPositionCount(), probeBlocks);
        this.probeHashBlock = probeHashChannel.isPresent() ? Optional.of(page.getBlock(probeHashChannel.getAsInt())) : Optional.empty();
        this.probeCountBlock = probeCountChannel.isPresent() ? Optional.of(page.getBlock(probeCountChannel.getAsInt())) : Optional.empty();
    }

    public AggregationBuilder getProbeAggregationBuilder()
    {
        return probeAggregationBuilder;
    }

    public AggregationBuilder getBuildAggregationBuilder()
    {
        return buildAggregationBuilder;
    }

    public int[] getOutputChannels()
    {
        return probeOutputChannels;
    }

    public boolean advanceNextPosition()
    {
        position++;
        return position < positionCount;
    }

    public long getCurrentJoinPosition(LookupSource lookupSource)
    {
        if (currentRowContainsNull()) {
            return -1;
        }
        if (probeHashBlock.isPresent()) {
            long rawHash = BIGINT.getLong(probeHashBlock.get(), position);
            return lookupSource.getJoinPosition(position, probePage, page, rawHash);
        }
        return lookupSource.getJoinPosition(position, probePage, page);
    }

    public int getPosition()
    {
        return position;
    }

    public Page getPage()
    {
        return page;
    }

    private boolean currentRowContainsNull()
    {
        for (Block probeBlock : probeBlocks) {
            if (probeBlock.isNull(position)) {
                return true;
            }
        }
        return false;
    }

    public long getCountProbeRecord()
    {
        return BIGINT.getLong(probeCountBlock.get(), position);
    }
}
