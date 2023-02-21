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

import com.google.common.collect.ImmutableList;
import io.prestosql.operator.aggregation.builder.AggregationBuilder;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;
import io.prestosql.spi.type.Type;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.util.Objects.requireNonNull;

/**
 * This page builder creates pages for group join after applying aggregator on probe and build side based on count.
 */
public class LookupGroupJoinPageBuilder
        implements Restorable
{
    private final PageBuilder finalPageBuilder;
    private final PageBuilder buildPageBuilderTmp;
    private final List<Integer> outputBuildChannels;
    private final List<Integer> outputProbeChannels;
    private int estimatedProbeBlockBytes;

    public LookupGroupJoinPageBuilder(List<Type> outputTypes, List<Type> buildTypes,
            List<Integer> outputBuildChannels, List<Integer> outputProbeChannels)
    {
        this.outputBuildChannels = requireNonNull(outputBuildChannels, "outputBuildChannels is null");
        this.outputProbeChannels = requireNonNull(outputProbeChannels, "outputProbeChannels is null");
        this.finalPageBuilder = new PageBuilder(ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null")));
        this.buildPageBuilderTmp = new PageBuilder(requireNonNull(buildTypes, "buildTypes is null"));
    }

    public boolean isFull()
    {
        return estimatedProbeBlockBytes + finalPageBuilder.getSizeInBytes() >= DEFAULT_MAX_PAGE_SIZE_IN_BYTES || finalPageBuilder.isFull();
    }

    public boolean isEmpty()
    {
        return finalPageBuilder.isEmpty();
    }

    public void reset()
    {
        finalPageBuilder.reset();
        buildPageBuilderTmp.reset();
        estimatedProbeBlockBytes = 0;
    }

    /**
     * append the index for the probe and copy the row for the build
     */
    public void appendRow(GroupJoinProbe probe, LookupSource lookupSource, long joinPosition)
    {
        // count is stored in last channel.
        long buildCount = lookupSource.getCountForJoinPosition(joinPosition, lookupSource.getChannelCount() - 1);
        long probeCount = probe.getCountProbeRecord();
        Page probePage = probe.getPage().getRegion(probe.getPosition(), 1);
        buildPageBuilderTmp.declarePosition();
        lookupSource.appendTo(joinPosition, buildPageBuilderTmp, 0);
        Page buildPage = buildPageBuilderTmp.build();

        // probe side
        Page probeFinalPage = null;
        if (outputProbeChannels.size() != 0) {
            probeFinalPage = processAggregationOnPage(probe.getProbeAggregationBuilder().getAggregationCount() == 0 ? 1 : buildCount,
                    probePage,
                    probe.getProbeAggregationBuilder());
        }

        // build side
        Page buildFinalPage = null;
        if (outputBuildChannels.size() != 0) {
            buildFinalPage = processAggregationOnPage(probe.getBuildAggregationBuilder().getAggregationCount() == 0 ? 1 : probeCount,
                    buildPage,
                    probe.getBuildAggregationBuilder());
        }

        int probeChannelLength = outputProbeChannels.size();
        for (int i = 0; i < probeChannelLength; i++) {
            if (probeFinalPage.getBlock(outputProbeChannels.get(i)).isNull(0)) {
                finalPageBuilder.getBlockBuilder(i).appendNull();
                continue;
            }
            probeFinalPage.getBlock(outputProbeChannels.get(i)).writePositionTo(0, finalPageBuilder.getBlockBuilder(i));
        }

        for (int i = 0; i < outputBuildChannels.size(); i++) {
            if (buildFinalPage.getBlock(outputBuildChannels.get(i)).isNull(0)) {
                finalPageBuilder.getBlockBuilder(i + probeChannelLength).appendNull();
                continue;
            }
            buildFinalPage.getBlock(outputBuildChannels.get(i)).writePositionTo(0, finalPageBuilder.getBlockBuilder(i + probeChannelLength));
        }
        buildPageBuilderTmp.reset();
    }

    private static Page processAggregationOnPage(long count, Page sourcePage, AggregationBuilder aggregationBuilder)
    {
        // TODO Vineet check on how to convert into future object and relate in normal code flow.
        Page finalPage;
        for (int i = 0; i < count; i++) {
            Work<?> work = aggregationBuilder.processPage(sourcePage);
            // Knowingly kept empty while loop
            while (!work.process()) {
                i = i;
            }
        }
        WorkProcessor<Page> pageWorkProcessor = aggregationBuilder.buildResult();
        while (!pageWorkProcessor.process()) {
            finalPage = null;
        }
        aggregationBuilder.updateMemory();
        finalPage = pageWorkProcessor.getResult();
        return finalPage;
    }

    public Page build(GroupJoinProbe probe)
    {
        return finalPageBuilder.build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("estimatedSize", estimatedProbeBlockBytes + finalPageBuilder.getSizeInBytes())
                .add("positionCount", finalPageBuilder.getPositionCount())
                .toString();
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        throw new UnsupportedOperationException("Not supported");
    }
}
