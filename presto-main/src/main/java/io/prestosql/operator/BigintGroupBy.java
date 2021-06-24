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
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public abstract class BigintGroupBy
{
    protected static final float FILL_RATIO = 0.75f;
    protected static final List<Type> TYPES = ImmutableList.of(BIGINT);
    protected static final List<Type> TYPES_WITH_RAW_HASH = ImmutableList.of(BIGINT, BIGINT);

    protected final int hashChannel;
    protected final boolean outputRawHash;

    protected int nextGroupId;
    protected long currentPageSizeInBytes;
    protected int maxFill;

    // groupId for the null value
    protected int nullGroupId = -1;

    public BigintGroupBy(int hashChannel, boolean outputRawHash)
    {
        this.hashChannel = hashChannel;
        this.outputRawHash = outputRawHash;
        this.nextGroupId = 0;
    }

    public boolean needMoreCapacity()
    {
        return nextGroupId >= maxFill;
    }

    public static int calculateMaxFill(int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must be greater than 0");
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
        return maxFill;
    }

    protected class AddPageWork
            implements Work<Void>
    {
        private final Block block;

        private int lastPosition;
        private GroupBy groupBy;

        public AddPageWork(Block block, GroupBy groupBy)
        {
            this.block = requireNonNull(block, "block is null");
            this.groupBy = groupBy;
        }

        @Override
        public boolean process()
        {
            int positionCount = block.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (groupBy.needMoreCapacity() && !groupBy.tryToIncreaseCapacity()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !groupBy.needMoreCapacity()) {
                // get the group for the current row
                groupBy.putIfAbsent(lastPosition, block);
                lastPosition++;
            }
            return lastPosition == positionCount;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    protected class GetGroupIdsWork
            implements Work<GroupByIdBlock>
    {
        private final BlockBuilder blockBuilder;
        private final Block block;

        private boolean finished;
        private int lastPosition;
        private GroupBy groupBy;

        public GetGroupIdsWork(Block block, GroupBy groupBy)
        {
            this.block = requireNonNull(block, "block is null");
            // we know the exact size required for the block
            this.blockBuilder = BIGINT.createFixedSizeBlockBuilder(block.getPositionCount());
            this.groupBy = groupBy;
        }

        @Override
        public boolean process()
        {
            int positionCount = block.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");
            checkState(!finished);

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (groupBy.needMoreCapacity() && !groupBy.tryToIncreaseCapacity()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !groupBy.needMoreCapacity()) {
                // output the group id for this row
                BIGINT.writeLong(blockBuilder, groupBy.putIfAbsent(lastPosition, block));
                lastPosition++;
            }
            return lastPosition == positionCount;
        }

        @Override
        public GroupByIdBlock getResult()
        {
            checkState(lastPosition == block.getPositionCount(), "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return new GroupByIdBlock(nextGroupId, blockBuilder.build());
        }
    }
}
