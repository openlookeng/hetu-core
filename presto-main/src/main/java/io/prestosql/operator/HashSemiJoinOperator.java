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
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.operator.SetBuilderOperator.SetSupplier;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"channelSetFuture", "probeHashChannel", "channelSet", "outputPage", "snapshotState"})
public class HashSemiJoinOperator
        implements Operator
{
    private final OperatorContext operatorContext;

    public static class HashSemiJoinOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final SetSupplier setSupplier;
        private final List<Type> probeTypes;
        private final int probeJoinChannel;
        private final Optional<Integer> probeJoinHashChannel;
        private boolean closed;

        public HashSemiJoinOperatorFactory(int operatorId, PlanNodeId planNodeId, SetSupplier setSupplier, List<? extends Type> probeTypes, int probeJoinChannel, Optional<Integer> probeJoinHashChannel)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.setSupplier = setSupplier;
            this.probeTypes = ImmutableList.copyOf(probeTypes);
            checkArgument(probeJoinChannel >= 0, "probeJoinChannel is negative");
            this.probeJoinChannel = probeJoinChannel;
            this.probeJoinHashChannel = probeJoinHashChannel;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext addOperatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashSemiJoinOperator.class.getSimpleName());
            return new HashSemiJoinOperator(addOperatorContext, setSupplier, probeJoinChannel, probeJoinHashChannel);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new HashSemiJoinOperatorFactory(operatorId, planNodeId, setSupplier, probeTypes, probeJoinChannel, probeJoinHashChannel);
        }
    }

    private final int probeJoinChannel;
    private final ListenableFuture<ChannelSet> channelSetFuture;
    private final Optional<Integer> probeHashChannel;

    private ChannelSet channelSet;
    private Page outputPage;
    private boolean finishing;

    private final SingleInputSnapshotState snapshotState;

    public HashSemiJoinOperator(OperatorContext operatorContext, SetSupplier channelSetFuture, int probeJoinChannel, Optional<Integer> probeHashChannel)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");

        // todo pass in desired projection
        requireNonNull(channelSetFuture, "hashProvider is null");
        checkArgument(probeJoinChannel >= 0, "probeJoinChannel is negative");

        this.channelSetFuture = channelSetFuture.getChannelSet();
        this.probeJoinChannel = probeJoinChannel;
        this.probeHashChannel = probeHashChannel;
        this.snapshotState = operatorContext.isSnapshotEnabled() ? SingleInputSnapshotState.forOperator(this, operatorContext) : null;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        if (snapshotState != null && snapshotState.hasMarker()) {
            // Snapshot: there are pending markers. Need to send them out before finishing this operator.
            return false;
        }

        return finishing && outputPage == null;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (snapshotState != null && allowMarker()) {
            // TODO-cp-I3AJIP: this may unblock too often
            return NOT_BLOCKED;
        }

        return channelSetFuture;
    }

    @Override
    public boolean needsInput()
    {
        if (!allowMarker()) {
            return false;
        }

        if (channelSet == null) {
            channelSet = tryGetFutureValue(channelSetFuture).orElse(null);
        }
        return channelSet != null;
    }

    @Override
    public boolean allowMarker()
    {
        return !finishing && outputPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");

        if (snapshotState != null) {
            if (snapshotState.processPage(page)) {
                return;
            }
        }

        checkState(channelSet != null, "Set has not been built yet");
        checkState(outputPage == null, "Operator still has pending output");

        // create the block builder for the new boolean column
        // we know the exact size required for the block
        BlockBuilder blockBuilder = BOOLEAN.createFixedSizeBlockBuilder(page.getPositionCount());

        Page probeJoinPage = new Page(page.getBlock(probeJoinChannel));
        Optional<Block> hashBlock = probeHashChannel.map(page::getBlock);

        // update hashing strategy to use probe cursor
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (probeJoinPage.getBlock(0).isNull(position)) {
                if (channelSet.isEmpty()) {
                    BOOLEAN.writeBoolean(blockBuilder, false);
                }
                else {
                    blockBuilder.appendNull();
                }
            }
            else {
                boolean contains;
                if (hashBlock.isPresent()) {
                    long rawHash = BIGINT.getLong(hashBlock.get(), position);
                    contains = channelSet.contains(position, probeJoinPage, rawHash);
                }
                else {
                    contains = channelSet.contains(position, probeJoinPage);
                }
                if (!contains && channelSet.containsNull()) {
                    blockBuilder.appendNull();
                }
                else {
                    BOOLEAN.writeBoolean(blockBuilder, contains);
                }
            }
        }

        // add the new boolean column to the page
        outputPage = page.appendColumn(blockBuilder.build());
    }

    @Override
    public Page getOutput()
    {
        if (snapshotState != null) {
            Page marker = snapshotState.nextMarker();
            if (marker != null) {
                return marker;
            }
        }

        Page result = outputPage;
        outputPage = null;
        return result;
    }

    @Override
    public Page pollMarker()
    {
        return snapshotState.nextMarker();
    }

    @Override
    public void close()
    {
        if (snapshotState != null) {
            snapshotState.close();
        }
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        HashSemiJoinOperatorState myState = new HashSemiJoinOperatorState();
        myState.operatorContext = operatorContext.capture(serdeProvider);
        myState.finishing = finishing;
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        HashSemiJoinOperatorState myState = (HashSemiJoinOperatorState) state;
        this.operatorContext.restore(myState.operatorContext, serdeProvider);
        this.finishing = myState.finishing;
    }

    private static class HashSemiJoinOperatorState
            implements Serializable
    {
        private Object operatorContext;
        private boolean finishing;
    }
}
