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
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"types", "groupingSetInputs", "currentPage", "snapshotState"})
public class GroupIdOperator
        implements Operator
{
    public static class GroupIdOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> outputTypes;
        private final List<Map<Integer, Integer>> groupingSetMappings;

        private boolean closed;

        public GroupIdOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> outputTypes,
                List<Map<Integer, Integer>> groupingSetMappings)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes));
            this.groupingSetMappings = ImmutableList.copyOf(requireNonNull(groupingSetMappings));
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext addOperatorContext = driverContext.addOperatorContext(operatorId, planNodeId, GroupIdOperator.class.getSimpleName());

            // create an int array for fast lookup of input columns for every grouping set
            int[][] groupingSets = new int[groupingSetMappings.size()][outputTypes.size() - 1];
            for (int i = 0; i < groupingSetMappings.size(); i++) {
                // -1 means the output column is null
                Arrays.fill(groupingSets[i], -1);

                // anything else is an input column to copy
                for (int outputChannel : groupingSetMappings.get(i).keySet()) {
                    groupingSets[i][outputChannel] = groupingSetMappings.get(i).get(outputChannel);
                }
            }

            // it's easier to create null blocks for every output column even though we only null out some grouping column outputs
            Block[] outputNullBlocks = new Block[outputTypes.size()];
            for (int i = 0; i < outputTypes.size(); i++) {
                outputNullBlocks[i] = outputTypes.get(i).createBlockBuilder(null, 1)
                        .appendNull()
                        .build();
            }

            // create groupid blocks for every group
            Block[] groupSetBlocks = new Block[groupingSetMappings.size()];
            for (int i = 0; i < groupingSetMappings.size(); i++) {
                BlockBuilder builder = BIGINT.createBlockBuilder(null, 1);
                BIGINT.writeLong(builder, i);
                groupSetBlocks[i] = builder.build();
            }

            return new GroupIdOperator(addOperatorContext, outputTypes, groupingSets, outputNullBlocks, groupSetBlocks);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new GroupIdOperatorFactory(operatorId, planNodeId, outputTypes, groupingSetMappings);
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final int[][] groupingSetInputs;
    private final Block[] nullBlocks;
    private final Block[] groupIdBlocks;

    private Page currentPage;
    private int currentGroupingSet;
    private boolean finishing;

    private final SingleInputSnapshotState snapshotState;

    public GroupIdOperator(
            OperatorContext operatorContext,
            List<Type> types,
            int[][] groupingSetInputs,
            Block[] nullBlocks,
            Block[] groupIdBlocks)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.groupingSetInputs = requireNonNull(groupingSetInputs, "groupingSetInputs is null");
        this.nullBlocks = requireNonNull(nullBlocks, "nullBlocks is null");
        this.groupIdBlocks = requireNonNull(groupIdBlocks, "groupIdBlocks is null");
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

        return finishing && currentPage == null;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && currentPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        checkState(currentPage == null, "currentPage must be null to add a new page");

        if (snapshotState != null) {
            if (snapshotState.processPage(page)) {
                return;
            }
        }

        currentPage = requireNonNull(page, "page is null");
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

        if (currentPage == null) {
            return null;
        }

        return generateNextPage();
    }

    @Override
    public Page pollMarker()
    {
        return snapshotState.nextMarker();
    }

    private Page generateNextPage()
    {
        // generate 'n' pages for every input page, where n is the number of grouping sets
        Block[] outputBlocks = new Block[types.size()];

        for (int i = 0; i < groupingSetInputs[currentGroupingSet].length; i++) {
            if (groupingSetInputs[currentGroupingSet][i] == -1) {
                outputBlocks[i] = new RunLengthEncodedBlock(nullBlocks[i], currentPage.getPositionCount());
            }
            else {
                outputBlocks[i] = currentPage.getBlock(groupingSetInputs[currentGroupingSet][i]);
            }
        }

        outputBlocks[outputBlocks.length - 1] = new RunLengthEncodedBlock(groupIdBlocks[currentGroupingSet], currentPage.getPositionCount());
        currentGroupingSet = (currentGroupingSet + 1) % groupingSetInputs.length;
        Page outputPage = new Page(currentPage.getPositionCount(), outputBlocks);

        if (currentGroupingSet == 0) {
            currentPage = null;
        }

        return outputPage;
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
        GroupIdOperatorState myState = new GroupIdOperatorState();
        myState.operatorContext = operatorContext.capture(serdeProvider);
        BlockEncodingSerde blockSerde = serdeProvider.getBlockEncodingSerde();
        myState.nullBlocks = new byte[nullBlocks.length][];
        for (int i = 0; i < nullBlocks.length; i++) {
            myState.nullBlocks[i] = serializeBlock(nullBlocks[i], blockSerde);
        }
        myState.groupIdBlocks = new byte[groupIdBlocks.length][];
        for (int i = 0; i < groupIdBlocks.length; i++) {
            myState.groupIdBlocks[i] = serializeBlock(groupIdBlocks[i], blockSerde);
        }
        myState.currentGroupingSet = currentGroupingSet;
        myState.finishing = finishing;
        return myState;
    }

    private byte[] serializeBlock(Block block, BlockEncodingSerde serde)
    {
        SliceOutput output = new DynamicSliceOutput(0);
        serde.writeBlock(output, block);
        return output.getUnderlyingSlice().getBytes();
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        GroupIdOperatorState myState = (GroupIdOperatorState) state;
        this.operatorContext.restore(myState.operatorContext, serdeProvider);
        BlockEncodingSerde blockSerde = serdeProvider.getBlockEncodingSerde();
        for (int i = 0; i < nullBlocks.length; i++) {
            this.nullBlocks[i] = deserializeBlock(myState.nullBlocks[i], blockSerde);
        }
        for (int i = 0; i < groupIdBlocks.length; i++) {
            this.groupIdBlocks[i] = deserializeBlock(myState.groupIdBlocks[i], blockSerde);
        }
        this.currentGroupingSet = myState.currentGroupingSet;
        this.finishing = myState.finishing;
    }

    private Block deserializeBlock(byte[] array, BlockEncodingSerde serde)
    {
        Slice input = Slices.wrappedBuffer(array);
        return serde.readBlock(input.getInput());
    }

    private static class GroupIdOperatorState
            implements Serializable
    {
        private Object operatorContext;

        private byte[][] nullBlocks;
        private byte[][] groupIdBlocks;

        private int currentGroupingSet;
        private boolean finishing;
    }
}
