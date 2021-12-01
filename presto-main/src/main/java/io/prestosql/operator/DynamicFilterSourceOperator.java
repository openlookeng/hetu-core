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

import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.snapshot.SnapshotUtils;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

/**
 * This operator acts as a simple "pass-through" pipe, while saving its input pages.
 * The collected pages' value are used for creating a run-time filtering constraint (for probe-side table scan in an inner join).
 * We support only small build-side pages (which should be the case when using "broadcast" join).
 */
@RestorableConfig(uncapturedFields = {"dynamicPredicateConsumer", "channels", "finished", "current", "snapshotState"})
public class DynamicFilterSourceOperator
        implements Operator
{
    public static final Logger log = Logger.get(DynamicFilterSourceOperator.class);
    private final OperatorContext context;
    private final Consumer<Map<Channel, Set>> dynamicPredicateConsumer;
    private final int maxFilterPositionsCount;
    private final long maxFilterSizeInBytes;
    private final List<Channel> channels;
    private boolean finished;
    private Page current;

    private Map<Channel, Set> values;

    private final SingleInputSnapshotState snapshotState;

    /**
     * Constructor for the Dynamic Filter Source Operator
     * TODO: no need to collect dynamic filter if it's cached
     */
    public DynamicFilterSourceOperator(OperatorContext context,
            Consumer<Map<Channel, Set>> dynamicPredicateConsumer,
            List<Channel> channels,
            PlanNodeId planNodeId,
            int maxFilterPositionsCount,
            DataSize maxFilterSize)
    {
        this.context = requireNonNull(context, "context is null");
        this.maxFilterPositionsCount = maxFilterPositionsCount;
        this.maxFilterSizeInBytes = maxFilterSize.toBytes();

        this.dynamicPredicateConsumer = requireNonNull(dynamicPredicateConsumer, "dynamicPredicateConsumer is null");
        this.channels = requireNonNull(channels, "channels is null");

        this.values = new HashMap<>();
        for (Channel channel : channels) {
            values.put(channel, new HashSet<>());
        }
        this.snapshotState = context.isSnapshotEnabled() ? SingleInputSnapshotState.forOperator(this, context) : null;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return context;
    }

    @Override
    public boolean needsInput()
    {
        return current == null && !finished;
    }

    @Override
    public void addInput(Page page)
    {
        verify(!finished, "DynamicFilterSourceOperator: addInput() may not be called after finish()");

        if (snapshotState != null) {
            if (snapshotState.processPage(page)) {
                return;
            }
        }

        current = page;
        if (values == null) {
            return;  // the predicate became too large.
        }

        // TODO: we should account for the memory used for collecting build-side values using MemoryContext
        long filterSizeInBytes = 0;
        int filterPositionsCount = 0;
        // Collect only the columns which are relevant for the JOIN.
        for (Channel channel : channels) {
            Block block = page.getBlock(channel.index);

            //saving the cloned block, to be processed in the "finish()" to avoid blocking down stream operators
            for (int i = 0; i < block.getPositionCount(); i++) {
                Object value = TypeUtils.readNativeValue(channel.type, block, i);
                if (value != null) { //ignoring null values
                    values.get(channel).add(value);
                }
            }

            filterSizeInBytes += block.getRetainedSizeInBytes();
            filterPositionsCount += values.get(channel).size();
        }
        if (filterPositionsCount > maxFilterPositionsCount || filterSizeInBytes > maxFilterSizeInBytes) {
            // The whole filter (summed over all columns) contains too much values or exceeds maxFilterSizeInBytes.
            log.debug("Partial DynamicFilter is too large, value count: " + filterPositionsCount + ", size: " + filterSizeInBytes / (1024 * 1024));
            handleTooLargePredicate();
        }
    }

    private void handleTooLargePredicate()
    {
        values = null;
        dynamicPredicateConsumer.accept(null);
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

        Page result = current;
        current = null;
        return result;
    }

    @Override
    public Page pollMarker()
    {
        return snapshotState.nextMarker();
    }

    @Override
    public void finish()
    {
        if (finished) {
            // NOTE: finish() may be called multiple times (see comment at Driver::processInternal).
            return;
        }
        finished = true;

        // Dynamic Filter became too large
        if (values == null) {
            return;
        }

        dynamicPredicateConsumer.accept(values);
    }

    @Override
    public boolean isFinished()
    {
        if (snapshotState != null && snapshotState.hasMarker()) {
            // Snapshot: there are pending markers. Need to send them out before finishing this operator.
            return false;
        }

        return current == null && finished;
    }

    public static class Channel
    {
        private final String filterId;
        private final Type type;
        private final int index;
        String queryId;

        public Channel(String filterId, Type type, int index, String queryId)
        {
            this.filterId = filterId;
            this.type = type;
            this.index = index;
            this.queryId = queryId;
        }

        public String getFilterId()
        {
            return filterId;
        }

        public Type getType()
        {
            return type;
        }

        public String getQueryId()
        {
            return queryId;
        }

        public int getIndex()
        {
            return index;
        }
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
        DynamicFilterSourceOperatorState myState = new DynamicFilterSourceOperatorState();
        myState.context = context.capture(serdeProvider);
        if (values != null) {
            myState.values = channels
                    .stream()
                    .map(channel -> values.get(channel)
                            .stream()
                            .map(value -> SnapshotUtils.captureHelper(value, serdeProvider))
                            .toArray())
                    .toArray(Object[][]::new);
        }
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        DynamicFilterSourceOperatorState myState = (DynamicFilterSourceOperatorState) state;
        this.context.restore(myState.context, serdeProvider);
        if (myState.values == null) {
            this.values = null;
        }
        else {
            if (this.values == null) {
                this.values = new HashMap<>();
                for (Channel channel : channels) {
                    this.values.put(channel, new HashSet<>());
                }
            }
            checkState(myState.values.length == channels.size());
            for (int i = 0; i < channels.size(); i++) {
                Type valueType = channels.get(i).type;
                Set<?> set = Arrays
                        .stream(myState.values[i])
                        .map(value -> SnapshotUtils.restoreHelper(value, valueType.getJavaType(), serdeProvider))
                        .collect(Collectors.toSet());
                values.get(channels.get(i)).clear();
                values.get(channels.get(i)).addAll(set);
            }
        }
    }

    private static class DynamicFilterSourceOperatorState
            implements Serializable
    {
        private Object context;
        private Object[][] values;
    }

    public static class DynamicFilterSourceOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Consumer<Map<Channel, Set>> dynamicPredicateConsumer;
        private final List<Channel> channels;
        private final int maxFilterPositionsCount;
        private final DataSize maxFilterSize;
        private boolean closed;

        /**
         * Constructor for the Dynamic Filter Source Operator Factory
         */
        public DynamicFilterSourceOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                Consumer<Map<Channel, Set>> dynamicPredicateConsumer,
                List<Channel> channels,
                int maxFilterPositionsCount,
                DataSize maxFilterSize)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.dynamicPredicateConsumer = requireNonNull(dynamicPredicateConsumer, "dynamicPredicateConsumer is null");
            this.channels = requireNonNull(channels, "channels is null");
            verify(channels.stream().map(channel -> channel.filterId).collect(toSet()).size() == channels.size(),
                    "duplicate dynamic filters are not allowed");
            verify(channels.stream().map(channel -> channel.index).collect(toSet()).size() == channels.size(),
                    "duplicate channel indices are not allowed");
            this.maxFilterPositionsCount = maxFilterPositionsCount;
            this.maxFilterSize = maxFilterSize;
        }

        /**
         * creating the Dynamic Filter Source Operator using the driver context
         *
         * @param driverContext
         * @return Operator
         */
        @Override
        public DynamicFilterSourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            return new DynamicFilterSourceOperator(
                    driverContext.addOperatorContext(operatorId, planNodeId, DynamicFilterSourceOperator.class.getSimpleName()),
                    dynamicPredicateConsumer,
                    channels,
                    planNodeId,
                    maxFilterPositionsCount,
                    maxFilterSize);
        }

        /**
         * No More Operators, function override
         */
        @Override
        public void noMoreOperators()
        {
            checkState(!closed, "Factory is already closed");
            closed = true;
        }

        /**
         * Duplicating the Dynamic Filter Source Operator
         */
        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("duplicate() is not supported for DynamicFilterSourceOperatorFactory");
        }
    }
}
