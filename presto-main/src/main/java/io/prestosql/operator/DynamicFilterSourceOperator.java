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
import io.airlift.node.NodeInfo;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.dynamicfilter.BloomFilterDynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilter.DataType;
import io.prestosql.spi.statestore.StateSet;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeUtils;
import io.prestosql.spi.util.BloomFilter;
import io.prestosql.sql.analyzer.FeaturesConfig.DynamicFilterDataType;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.statestore.StateStoreProvider;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringBloomFilterFpp;
import static io.prestosql.spi.dynamicfilter.BloomFilterDynamicFilter.convertBloomFilterToByteArray;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.DataType.BLOOM_FILTER;
import static io.prestosql.spi.statestore.StateCollection.Type.SET;
import static io.prestosql.utils.DynamicFilterUtils.FINISHPREFIX;
import static io.prestosql.utils.DynamicFilterUtils.PARTIALPREFIX;
import static io.prestosql.utils.DynamicFilterUtils.REGISTERPREFIX;
import static io.prestosql.utils.DynamicFilterUtils.WORKERSPREFIX;
import static io.prestosql.utils.DynamicFilterUtils.createKey;
import static io.prestosql.utils.DynamicFilterUtils.getDynamicFilterDataType;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

/**
 * This operator acts as a simple "pass-through" pipe, while saving its input pages.
 * The collected pages' value are used for creating a run-time filtering constraint (for probe-side table scan in an inner join).
 * We support only small build-side pages (which should be the case when using "broadcast" join).
 */
public class DynamicFilterSourceOperator
        implements Operator
{
    public static final Logger log = Logger.get(DynamicFilterSourceOperator.class);
    private final OperatorContext context;
    private final double bloomFilterFpp;
    private final Consumer<Map<Channel, Set>> dynamicPredicateConsumer;
    private final int maxFilterPositionsCount;
    private final long maxFilterSizeInBytes;
    private final DynamicFilterDataType dynamicFilterDataType;
    private final List<Channel> channels;
    private final DynamicFilter.Type filterType;
    private final NodeInfo nodeInfo;
    private final StateStoreProvider stateStoreProvider;
    private boolean finished;
    private Page current;

    private Map<Channel, Set> values;

    private long driverId;
    private boolean haveRegistered;

    /**
     * Constructor for the Dynamic Filter Source Operator
     * TODO: no need to collect dynamic filter if it's cached
     */
    public DynamicFilterSourceOperator(OperatorContext context,
            Consumer<Map<Channel, Set>> dynamicPredicateConsumer,
            List<Channel> channels,
            PlanNodeId planNodeId,
            int maxFilterPositionsCount,
            DataSize maxFilterSize,
            DynamicFilterDataType dynamicFilteringDataType,
            DynamicFilter.Type filterType,
            NodeInfo nodeInfo,
            StateStoreProvider stateStoreProvider)
    {
        this.context = requireNonNull(context, "context is null");
        this.bloomFilterFpp = getDynamicFilteringBloomFilterFpp(this.context.getSession());
        this.maxFilterPositionsCount = maxFilterPositionsCount;
        this.maxFilterSizeInBytes = maxFilterSize.toBytes();
        this.dynamicFilterDataType = dynamicFilteringDataType;

        this.dynamicPredicateConsumer = requireNonNull(dynamicPredicateConsumer, "dynamicPredicateConsumer is null");
        this.channels = requireNonNull(channels, "channels is null");
        this.nodeInfo = nodeInfo;

        this.values = new HashMap<>();
        for (Channel channel : channels) {
            values.put(channel, new HashSet<>());
        }

        this.filterType = filterType;
        this.stateStoreProvider = requireNonNull(stateStoreProvider, "stateStoreProvider is null");
        if (stateStoreProvider.getStateStore() != null) {
            driverId = stateStoreProvider.getStateStore().generateId();
            registerDynamicFilterTask(context.getSession().getQueryId().toString());
        }
    }

    private static BloomFilter createBloomFilterFromSet(Channel channel, Set values, double bloomFilterFpp)
    {
        BloomFilter bloomFilter = new BloomFilter(BloomFilterDynamicFilter.DEFAULT_DYNAMIC_FILTER_SIZE, bloomFilterFpp);
        if (channel.type.getJavaType() == long.class) {
            for (Object value : values) {
                long lv = (Long) value;
                bloomFilter.add(lv);
            }
        }
        else if (channel.type.getJavaType() == double.class) {
            for (Object value : values) {
                double lv = (Double) value;
                bloomFilter.add(lv);
            }
        }
        else if (channel.type.getJavaType() == Slice.class) {
            for (Object value : values) {
                bloomFilter.add((Slice) value);
            }
        }
        else {
            for (Object value : values) {
                bloomFilter.add(String.valueOf(value).getBytes());
            }
        }
        return bloomFilter;
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
        Page result = current;
        current = null;
        return result;
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

        finishDynamicFilterTask();
        if (filterType == DynamicFilter.Type.LOCAL) {
            dynamicPredicateConsumer.accept(values);
        }
    }

    @Override
    public boolean isFinished()
    {
        return current == null && finished;
    }

    private void registerDynamicFilterTask(String queryId)
    {
        for (Channel channel : channels) {
            try {
                final StateStore stateStore = stateStoreProvider.getStateStore();
                StateSet<Long> registeredTasks = (StateSet<Long>) stateStore.createStateCollection(createKey(REGISTERPREFIX, channel.filterId, queryId), SET);
                stateStore.createStateCollection(createKey(WORKERSPREFIX, channel.filterId, queryId), SET);
                stateStore.createStateCollection(createKey(PARTIALPREFIX, channel.filterId, queryId), SET);
                stateStore.createStateCollection(createKey(FINISHPREFIX, channel.filterId, queryId), SET);
                registeredTasks.add(driverId);
                this.haveRegistered = true;
            }
            catch (Exception e) {
                log.error("could not register dynamic filter, Exception happened:" + e.getMessage());
            }
        }
    }

    private void finishDynamicFilterTask()
    {
        if (!haveRegistered) {
            return;
        }

        StateStore stateStore = stateStoreProvider.getStateStore();
        DataType dataType = getDynamicFilterDataType(filterType, dynamicFilterDataType);
        for (Map.Entry<Channel, Set> value : values.entrySet()) {
            Channel channel = value.getKey();
            Set values = value.getValue();
            String id = channel.filterId;
            String key = createKey(PARTIALPREFIX, id, channel.queryId);

            if (dataType == BLOOM_FILTER) {
                byte[] finalOutput = convertBloomFilterToByteArray(createBloomFilterFromSet(channel, values, bloomFilterFpp));
                if (finalOutput != null) {
                    ((StateSet) stateStore.getStateCollection(key)).add(finalOutput);
                }
            }
            else {
                ((StateSet) stateStore.getStateCollection(key)).add(values);
            }
            log.debug("creating new " + dataType + " dynamic filter for size of: " + values.size() + ", key: " + key + ", driver: " + driverId);
            ((StateSet) stateStore.getStateCollection(createKey(FINISHPREFIX, channel.filterId, channel.queryId))).add(driverId);
            ((StateSet) stateStore.getStateCollection(createKey(WORKERSPREFIX, channel.filterId, channel.queryId))).add(nodeInfo.getNodeId());
        }
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
        private final DynamicFilterDataType dynamicFilterDataType;
        private final DynamicFilter.Type filterType;
        private final StateStoreProvider stateStoreProvider;
        private final NodeInfo nodeInfo;
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
                DataSize maxFilterSize,
                DynamicFilterDataType dynamicFilteringDataType,
                DynamicFilter.Type filterType,
                NodeInfo nodeInfo,
                StateStoreProvider stateStoreProvider)
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
            this.dynamicFilterDataType = dynamicFilteringDataType;
            this.filterType = filterType;
            this.nodeInfo = nodeInfo;
            this.stateStoreProvider = requireNonNull(stateStoreProvider, "stateStoreProvider is null");
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
                    maxFilterSize,
                    dynamicFilterDataType,
                    filterType,
                    nodeInfo,
                    stateStoreProvider);
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
