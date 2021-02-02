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

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.execution.Lifespan;
import io.prestosql.execution.TaskId;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.PlanNodeId;
import nova.hetu.omnicache.runtime.OmniRuntime;
import nova.hetu.omnicache.vector.LongVec;
import nova.hetu.omnicache.vector.Vec;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class HashAggregationOmniOperator
        implements Operator
{
    OperatorContext operatorContext;
    //omni
    String compileID;
    OmniRuntime omniRuntime;

    private boolean finishing;
    private boolean finished;
    // for yield when memory is not available
//    private Work<?> unfinishedWork;

    private OmniPageContainer omniPageContainer;
    HashAggregationOmniWork<Object> omniWork;
    private String omniKey;

    public HashAggregationOmniOperator(OperatorContext operatorContext, OmniRuntime omniRuntime, String compileID)
    {
        this.operatorContext = operatorContext;
        int driverId = this.operatorContext.getDriverContext().getDriverId();
        String taskId = this.operatorContext.getDriverContext().getTaskId().getFullId();
        this.omniKey = taskId + driverId;
        this.omniRuntime = omniRuntime;
        this.compileID = compileID;
    }

    private static class OmniPageContainer
    {
        private final int capacity = 128 * 1024 * 1024;
        private Vec[] buffers;
        private int totalRowCount = 0;

        public OmniPageContainer(int length)
        {
            buffers = new Vec[length];
            for (int idx = 0; idx < length; idx++) {
                buffers[idx] = new LongVec(capacity);
            }
        }

        public void appendPage(Page page)
        {
            for (int ridx = 0; ridx < page.getPositionCount(); ridx++) {
                for (int cidx = 0; cidx < page.getChannelCount(); cidx++) {
                    LongVec vec = (LongVec) page.getBlock(cidx).getValuesVec();
                    long value = vec.get(ridx);
                    buffers[cidx].set(totalRowCount, value);
                }
                totalRowCount++;
            }
        }

        public Vec<?>[] getBuffers()
        {
            return buffers;
        }
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return this.operatorContext;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public void close()
            throws Exception
    {

    }

    @Override
    public boolean needsInput()
    {
        if (finishing) {
            return false;
        }
        if (omniWork != null && !omniWork.isFinished()) {
            return false;
        }
        return true;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");

        if (omniWork == null) {
            omniWork = new HashAggregationOmniWork(page, omniRuntime, compileID, omniKey);
        }
        else {
            omniWork.updatePages(page);
        }
        omniWork.process();
    }

    public Page toResult(Vec<?>[] omniExecutionResult)
    {
        List<Type> builderType = new ArrayList<>();
        builderType.add(BigintType.BIGINT);
        builderType.add(BigintType.BIGINT);

        long start1 = System.currentTimeMillis();

        Vec<?>[] vecs = omniExecutionResult;
        int size = vecs[0].size();

        boolean[] valueIsNull = new boolean[size];
        for (int i = 0; i < size; i++) {
            valueIsNull[i] = false;
        }
        LongArrayBlock[] longArrayBlocks = new LongArrayBlock[2];
        longArrayBlocks[0] = new LongArrayBlock(size, Optional.of(valueIsNull), (LongVec) vecs[0]);
        longArrayBlocks[1] = new LongArrayBlock(size, Optional.of(valueIsNull), (LongVec) vecs[1]);
//            longArrayBlocks[2] = new LongArrayBlock(size, Optional.of(valueIsNull), (LongVec) vecs[1]);
        Page page = new Page(longArrayBlocks);

        long end1 = System.currentTimeMillis();
        System.out.println("get omni result: " + (end1 - start1));
        return page;
    }

    @Override
    public Page getOutput()
    {
        if (finished) {
            return null;
        }
        if (finishing && omniWork.isFinished()) {
            finished = true;
            Vec<?>[] result = omniWork.getResult();
            return toResult(result);

        }

        return null;
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        return null;
    }

    @Override
    public void finishMemoryRevoke()
    {

    }

    public static class HashAggregationOmniOperatorFactory
            implements OperatorFactory
    {
        OmniRuntime omniRuntime;
        String compileID;

        public HashAggregationOmniOperatorFactory(OmniRuntime omniRuntime, String compileID)
        {
            this.omniRuntime = omniRuntime;
            this.compileID = compileID;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
//            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, "BenchmarkSource");
            OperatorContext operatorContext = driverContext.addOperatorContext(1, new PlanNodeId("1"), "BenchmarkSource");
            HashAggregationOmniOperator hashAggregationOperator = new HashAggregationOmniOperator(operatorContext, omniRuntime, compileID);
            return hashAggregationOperator;
        }

        @Override
        public void noMoreOperators()
        {

        }

        @Override
        public void noMoreOperators(Lifespan lifespan)
        {

        }

        @Override
        public OperatorFactory duplicate()
        {
            return null;
        }
    }
}
