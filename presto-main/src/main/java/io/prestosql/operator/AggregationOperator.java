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
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.plan.AggregationNode.Step;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;

import java.io.Serializable;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Group input data and produce a single block for each sequence of identical values.
 */
@RestorableConfig(uncapturedFields = {"snapshotState"})
public class AggregationOperator
        implements Operator
{
    public static class AggregationOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Step step;
        private final List<AccumulatorFactory> accumulatorFactories;
        private final boolean useSystemMemory;
        private boolean closed;

        public AggregationOperatorFactory(int operatorId, PlanNodeId planNodeId, Step step, List<AccumulatorFactory> accumulatorFactories, boolean useSystemMemory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.step = step;
            this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
            this.useSystemMemory = useSystemMemory;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext addOperatorContext = driverContext.addOperatorContext(operatorId, planNodeId, AggregationOperator.class.getSimpleName());
            return new AggregationOperator(addOperatorContext, step, accumulatorFactories, useSystemMemory);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new AggregationOperatorFactory(operatorId, planNodeId, step, accumulatorFactories, useSystemMemory);
        }
    }

    private enum State
    {
        NEEDS_INPUT,
        HAS_OUTPUT,
        FINISHED
    }

    private final OperatorContext operatorContext;
    private final LocalMemoryContext systemMemoryContext;
    private final LocalMemoryContext userMemoryContext;
    private final List<Aggregator> aggregates;
    private final boolean useSystemMemory;

    private State state = State.NEEDS_INPUT;

    private final SingleInputSnapshotState snapshotState;

    public AggregationOperator(OperatorContext operatorContext, Step step, List<AccumulatorFactory> accumulatorFactories, boolean useSystemMemory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.systemMemoryContext = operatorContext.newLocalSystemMemoryContext(AggregationOperator.class.getSimpleName());
        this.userMemoryContext = operatorContext.localUserMemoryContext();
        this.useSystemMemory = useSystemMemory;

        requireNonNull(step, "step is null");

        // wrapper each function with an aggregator
        requireNonNull(accumulatorFactories, "accumulatorFactories is null");
        ImmutableList.Builder<Aggregator> builder = ImmutableList.builder();
        for (AccumulatorFactory accumulatorFactory : accumulatorFactories) {
            builder.add(new Aggregator(accumulatorFactory, step));
        }
        aggregates = builder.build();
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
        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;
        }
    }

    @Override
    public void close()
    {
        userMemoryContext.setBytes(0);
        systemMemoryContext.close();
        if (snapshotState != null) {
            snapshotState.close();
        }
    }

    @Override
    public boolean isFinished()
    {
        if (snapshotState != null && snapshotState.hasMarker()) {
            // Snapshot: there are pending markers. Need to send them out before finishing this operator.
            return false;
        }

        return state == State.FINISHED;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(needsInput(), "Operator is already finishing");
        requireNonNull(page, "page is null");

        if (snapshotState != null) {
            if (snapshotState.processPage(page)) {
                return;
            }
        }

        long memorySize = 0;
        for (Aggregator aggregate : aggregates) {
            aggregate.processPage(page);
            memorySize += aggregate.getEstimatedSize();
        }
        if (useSystemMemory) {
            systemMemoryContext.setBytes(memorySize);
        }
        else {
            userMemoryContext.setBytes(memorySize);
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

        if (state != State.HAS_OUTPUT) {
            return null;
        }

        // project results into output blocks
        List<Type> types = aggregates.stream().map(Aggregator::getType).collect(toImmutableList());

        // output page will only be constructed once,
        // so a new PageBuilder is constructed (instead of using PageBuilder.reset)
        PageBuilder pageBuilder = new PageBuilder(1, types);

        pageBuilder.declarePosition();
        for (int i = 0; i < aggregates.size(); i++) {
            Aggregator aggregator = aggregates.get(i);
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
            aggregator.evaluate(blockBuilder);
        }

        state = State.FINISHED;
        return pageBuilder.build();
    }

    @Override
    public Page pollMarker()
    {
        return snapshotState.nextMarker();
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        AggregationOperatorState myState = new AggregationOperatorState();
        myState.operatorContext = operatorContext.capture(serdeProvider);
        myState.systemMemoryContext = systemMemoryContext.getBytes();
        myState.userMemoryContext = userMemoryContext.getBytes();
        myState.aggregates = new Object[aggregates.size()];
        for (int i = 0; i < aggregates.size(); i++) {
            myState.aggregates[i] = aggregates.get(i).capture(serdeProvider);
        }
        myState.state = state.toString();
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        AggregationOperatorState myState = (AggregationOperatorState) state;
        this.operatorContext.restore(myState.operatorContext, serdeProvider);
        this.systemMemoryContext.setBytes(myState.systemMemoryContext);
        this.userMemoryContext.setBytes(myState.userMemoryContext);
        for (int i = 0; i < aggregates.size(); i++) {
            aggregates.get(i).restore(myState.aggregates[i], serdeProvider);
        }
        this.state = State.valueOf(myState.state);
    }

    private static class AggregationOperatorState
            implements Serializable
    {
        private Object operatorContext;
        private long systemMemoryContext;
        private long userMemoryContext;
        //each element is another Object[] that is the state of all fields of state_0 that's worth capturing
        private Object[] aggregates;
        private String state;
    }
}
