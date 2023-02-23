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
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.aggregation.builder.AggregationBuilder;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.type.TypeUtils;
import org.openjdk.jol.info.ClassLayout;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.util.Failures.internalError;
import static java.util.Objects.requireNonNull;

public class SimplePagesHashStrategy
        implements PagesHashStrategy
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SimplePagesHashStrategy.class).instanceSize();
    private final List<Type> types;
    private final List<Integer> outputChannels;
    private final List<List<Block>> channels;
    private final List<Integer> hashChannels;
    private final List<Block> precomputedHashChannel;
    private final Optional<Integer> sortChannel;
    private final List<MethodHandle> distinctFromMethodHandles;
    private final AggregationBuilder aggregationBuilder;

    public SimplePagesHashStrategy(
            List<Type> types,
            List<Integer> outputChannels,
            List<List<Block>> channels,
            List<Integer> hashChannels,
            OptionalInt precomputedHashChannel,
            Optional<Integer> sortChannel,
            Optional<AggregationBuilder> aggregationBuilder,
            Metadata metadata)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
        this.channels = ImmutableList.copyOf(requireNonNull(channels, "channels is null"));

        checkArgument(types.size() == channels.size(), "Expected types and channels to be the same length");
        this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
        if (precomputedHashChannel.isPresent()) {
            this.precomputedHashChannel = channels.get(precomputedHashChannel.getAsInt());
        }
        else {
            this.precomputedHashChannel = null;
        }
        this.sortChannel = requireNonNull(sortChannel, "sortChannel is null");
        requireNonNull(aggregationBuilder, "aggregationBuilder is null");
        this.aggregationBuilder = aggregationBuilder.orElse(null);
        requireNonNull(metadata, "metadata is null");
        FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
        requireNonNull(metadata, "functionManager is null");
        ImmutableList.Builder<MethodHandle> distinctFromMethodHandlesBuilder = ImmutableList.builder();
        for (Type type : types) {
            distinctFromMethodHandlesBuilder.add(
                    functionAndTypeManager.getBuiltInScalarFunctionImplementation(functionAndTypeManager.resolveOperatorFunctionHandle(IS_DISTINCT_FROM, TypeSignatureProvider.fromTypes(type, type))).getMethodHandle());
        }
        distinctFromMethodHandles = distinctFromMethodHandlesBuilder.build();
    }

    @Override
    public int getChannelCount()
    {
        return outputChannels.size();
    }

    @Override
    public long getSizeInBytes()
    {
        return INSTANCE_SIZE + channels.stream()
                .flatMap(List::stream)
                .mapToLong(Block::getRetainedSizeInBytes)
                .sum();
    }

    @Override
    public long getCountForJoinPosition(int blockIndex, int blockPosition, int channel)
    {
        return BIGINT.getLong(channels.get(channel).get(blockIndex), blockPosition);
    }

    @Override
    public AggregationBuilder getAggregationBuilder()
    {
        return aggregationBuilder;
    }

    @Override
    public void appendTo(int blockIndex, int position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        int channelOffset = outputChannelOffset;
        for (int outputIndex : outputChannels) {
            Type type = types.get(outputIndex);
            List<Block> channel = channels.get(outputIndex);
            Block block = channel.get(blockIndex);
            type.appendTo(block, position, pageBuilder.getBlockBuilder(channelOffset));
            channelOffset++;
        }
    }

    @Override
    public long hashPosition(int blockIndex, int position)
    {
        if (precomputedHashChannel != null) {
            return BIGINT.getLong(precomputedHashChannel.get(blockIndex), position);
        }
        long result = 0;
        for (int hashChannel : hashChannels) {
            Type type = types.get(hashChannel);
            Block block = channels.get(hashChannel).get(blockIndex);
            result = result * 31 + TypeUtils.hashPosition(type, block, position);
        }
        return result;
    }

    @Override
    public long hashRow(int position, Page page)
    {
        long result = 0;
        for (int i = 0; i < hashChannels.size(); i++) {
            int hashChannel = hashChannels.get(i);
            Type type = types.get(hashChannel);
            Block block = page.getBlock(i);
            result = result * 31 + TypeUtils.hashPosition(type, block, position);
        }
        return result;
    }

    @Override
    public boolean rowEqualsRow(int leftPosition, Page leftPage, int rightPosition, Page rightPage)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            int hashChannel = hashChannels.get(i);
            Type type = types.get(hashChannel);
            Block leftBlock = leftPage.getBlock(i);
            Block rightBlock = rightPage.getBlock(i);
            if (!TypeUtils.positionEqualsPosition(type, leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsRow(int leftBlockIndex, int leftPosition, int rightPosition, Page rightPage)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            int hashChannel = hashChannels.get(i);
            Type type = types.get(hashChannel);
            Block leftBlock = channels.get(hashChannel).get(leftBlockIndex);
            Block rightBlock = rightPage.getBlock(i);
            if (!TypeUtils.positionEqualsPosition(type, leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsRowIgnoreNulls(int leftBlockIndex, int leftPosition, int rightPosition, Page rightPage)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            int hashChannel = hashChannels.get(i);
            Type type = types.get(hashChannel);
            Block leftBlock = channels.get(hashChannel).get(leftBlockIndex);
            Block rightBlock = rightPage.getBlock(i);
            if (!type.equalTo(leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsRow(int leftBlockIndex, int leftPosition, int rightPosition, Page page, int[] rightHashChannels)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            int hashChannel = hashChannels.get(i);
            Type type = types.get(hashChannel);
            Block leftBlock = channels.get(hashChannel).get(leftBlockIndex);
            Block rightBlock = page.getBlock(rightHashChannels[i]);
            if (!TypeUtils.positionEqualsPosition(type, leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionNotDistinctFromRow(int leftBlockIndex, int leftPosition, int rightPosition, Page page, int[] rightChannels)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            int hashChannel = hashChannels.get(i);
            Block leftBlock = channels.get(hashChannel).get(leftBlockIndex);
            Block rightBlock = page.getBlock(rightChannels[i]);
            MethodHandle methodHandle = distinctFromMethodHandles.get(hashChannel);
            try {
                if (!(boolean) methodHandle.invokeExact(leftBlock, leftPosition, rightBlock, rightPosition)) {
                    return false;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsPosition(int leftBlockIndex, int leftPosition, int rightBlockIndex, int rightPosition)
    {
        for (int hashChannel : hashChannels) {
            Type type = types.get(hashChannel);
            List<Block> channel = channels.get(hashChannel);
            Block leftBlock = channel.get(leftBlockIndex);
            Block rightBlock = channel.get(rightBlockIndex);
            if (!TypeUtils.positionEqualsPosition(type, leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsPositionIgnoreNulls(int leftBlockIndex, int leftPosition, int rightBlockIndex, int rightPosition)
    {
        for (int hashChannel : hashChannels) {
            Type type = types.get(hashChannel);
            List<Block> channel = channels.get(hashChannel);
            Block leftBlock = channel.get(leftBlockIndex);
            Block rightBlock = channel.get(rightBlockIndex);
            if (!type.equalTo(leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isPositionNull(int blockIndex, int blockPosition)
    {
        for (int hashChannel : hashChannels) {
            if (isChannelPositionNull(hashChannel, blockIndex, blockPosition)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int compareSortChannelPositions(int leftBlockIndex, int leftBlockPosition, int rightBlockIndex, int rightBlockPosition)
    {
        int channel = getSortChannel();

        Block leftBlock = channels.get(channel).get(leftBlockIndex);
        Block rightBlock = channels.get(channel).get(rightBlockIndex);

        return types.get(channel).compareTo(leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
    }

    @Override
    public boolean isSortChannelPositionNull(int blockIndex, int blockPosition)
    {
        return isChannelPositionNull(getSortChannel(), blockIndex, blockPosition);
    }

    private boolean isChannelPositionNull(int channelIndex, int blockIndex, int blockPosition)
    {
        List<Block> channel = channels.get(channelIndex);
        Block block = channel.get(blockIndex);
        return block.isNull(blockPosition);
    }

    private int getSortChannel()
    {
        return sortChannel.get();
    }
}
