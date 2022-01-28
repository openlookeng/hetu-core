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

import io.prestosql.execution.TaskId;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

// When a marker is received (needsInput returns true), inputPage must be null
@RestorableConfig(uncapturedFields = {"snapshotState", "inputPage"})
public class AssignUniqueIdOperator
        implements Operator
{
    private static final long ROW_IDS_PER_REQUEST = 1L << 20L;
    private static final long MAX_ROW_ID = 1L << 40L;

    public static class AssignUniqueIdOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private boolean closed;
        private final AtomicLong valuePool = new AtomicLong();

        public AssignUniqueIdOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext addOperatorContext = driverContext.addOperatorContext(
                    operatorId,
                    planNodeId,
                    AssignUniqueIdOperator.class.getSimpleName());
            return new AssignUniqueIdOperator(addOperatorContext, valuePool);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new AssignUniqueIdOperatorFactory(operatorId, planNodeId);
        }
    }

    private final OperatorContext operatorContext;
    private final SingleInputSnapshotState snapshotState;
    private boolean finishing;
    private final AtomicLong rowIdPool;
    private final long uniqueValueMask;

    private Page inputPage;
    private long rowIdCounter;
    private long maxRowIdCounterValue;

    public AssignUniqueIdOperator(OperatorContext operatorContext, AtomicLong rowIdPool)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.snapshotState = operatorContext.isSnapshotEnabled() ? SingleInputSnapshotState.forOperator(this, operatorContext) : null;
        this.rowIdPool = requireNonNull(rowIdPool, "rowIdPool is null");

        TaskId fullTaskId = operatorContext.getDriverContext().getTaskId();
        uniqueValueMask = (((long) fullTaskId.getStageId().getId()) << 54) | (((long) fullTaskId.getId()) << 40);

        requestValues();
    }

    private void requestValues()
    {
        rowIdCounter = rowIdPool.getAndAdd(ROW_IDS_PER_REQUEST);
        maxRowIdCounterValue = Math.min(rowIdCounter + ROW_IDS_PER_REQUEST, MAX_ROW_ID);
        checkState(rowIdCounter < MAX_ROW_ID, "Unique row id exceeds a limit: %s", MAX_ROW_ID);
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

        return finishing && inputPage == null;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && inputPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        if (snapshotState != null) {
            if (snapshotState.processPage(page)) {
                return;
            }
        }

        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        checkState(inputPage == null);
        inputPage = page;
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

        if (inputPage == null) {
            return null;
        }

        Page outputPage = processPage();
        inputPage = null;
        return outputPage;
    }

    @Override
    public Page pollMarker()
    {
        return snapshotState.nextMarker();
    }

    private Page processPage()
    {
        return inputPage.appendColumn(generateIdColumn());
    }

    private Block generateIdColumn()
    {
        BlockBuilder block = BIGINT.createFixedSizeBlockBuilder(inputPage.getPositionCount());
        for (int currentPosition = 0; currentPosition < inputPage.getPositionCount(); currentPosition++) {
            if (rowIdCounter >= maxRowIdCounterValue) {
                requestValues();
            }
            long rowId = rowIdCounter++;
            verify((rowId & uniqueValueMask) == 0, "RowId and uniqueValue mask overlaps");
            BIGINT.writeLong(block, uniqueValueMask | rowId);
        }
        return block.build();
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
        AssignUniqueIdOperatorState myState = new AssignUniqueIdOperatorState();
        myState.operatorContext = operatorContext.capture(serdeProvider);
        myState.finishing = finishing;
        myState.rowIdPool = rowIdPool.get();
        myState.rowIdCounter = rowIdCounter;
        myState.maxRowIdCounterValue = maxRowIdCounterValue;
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        AssignUniqueIdOperatorState myState = (AssignUniqueIdOperatorState) state;
        operatorContext.restore(myState.operatorContext, serdeProvider);
        finishing = myState.finishing;
        rowIdPool.set(myState.rowIdPool);
        rowIdCounter = myState.rowIdCounter;
        maxRowIdCounterValue = myState.maxRowIdCounterValue;
    }

    private static class AssignUniqueIdOperatorState
            implements Serializable
    {
        private Object operatorContext;
        private boolean finishing;
        private long rowIdPool;
        private long rowIdCounter;
        private long maxRowIdCounterValue;
    }
}
