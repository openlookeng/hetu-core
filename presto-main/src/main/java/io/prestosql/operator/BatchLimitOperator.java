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

import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"nextPage", "snapshotState"})
public class BatchLimitOperator
        implements Operator
{
    public static class BatchLimitOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final AtomicLong limit;
        private boolean closed;

        public BatchLimitOperatorFactory(int operatorId, PlanNodeId planNodeId, AtomicLong limit)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.limit = limit;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext addOperatorContext = driverContext.addOperatorContext(operatorId, planNodeId, BatchLimitOperator.class.getSimpleName());
            return new BatchLimitOperator(addOperatorContext, limit);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new BatchLimitOperatorFactory(operatorId, planNodeId, limit);
        }
    }

    private final OperatorContext operatorContext;
    private Page nextPage;
    private AtomicLong remainingLimit;

    private final SingleInputSnapshotState snapshotState;

    public BatchLimitOperator(OperatorContext operatorContext, AtomicLong limit)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.remainingLimit = limit;
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
        remainingLimit.set(0);
    }

    @Override
    public boolean isFinished()
    {
        if (snapshotState != null && snapshotState.hasMarker()) {
            // Snapshot: there are pending markers. Need to send them out before finishing this operator.
            return false;
        }

        return remainingLimit.get() <= 0 && nextPage == null;
    }

    @Override
    public boolean needsInput()
    {
        return remainingLimit.get() > 0 && nextPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        if (!needsInput()) {
            return;
        }

        if (snapshotState != null) {
            if (snapshotState.processPage(page)) {
                return;
            }
        }
        long remainLimit = remainingLimit.get();
        if (page.getPositionCount() <= remainLimit) {
            remainingLimit.addAndGet(-page.getPositionCount());
            nextPage = page;
        }
        else {
            Block[] blocks = new Block[page.getChannelCount()];
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                blocks[channel] = block.getRegion(0, (int) remainLimit);
            }
            nextPage = new Page((int) remainLimit, blocks);
            remainingLimit.set(0);
        }
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

        Page page = nextPage;
        nextPage = null;
        return page;
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
        BatchLimitOperatorState myState = new BatchLimitOperatorState();
        myState.operatorContext = operatorContext.capture(serdeProvider);
        myState.remainingLimit = remainingLimit.get();
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        BatchLimitOperatorState myState = (BatchLimitOperatorState) state;
        this.operatorContext.restore(myState.operatorContext, serdeProvider);
        this.remainingLimit = new AtomicLong(myState.remainingLimit);
    }

    private static class BatchLimitOperatorState
            implements Serializable
    {
        private Object operatorContext;
        private long remainingLimit;
    }
}
