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
import io.prestosql.spi.Page;
import io.prestosql.sql.planner.plan.PlanNodeId;
import nova.hetu.omnicache.runtime.OmniRuntime;
import nova.hetu.omnicache.vector.LongVec;
import nova.hetu.omnicache.vector.Vec;

import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class HashAggregationOmniOperator
        implements Operator
{
    OperatorContext operatorContext;
    //omni
    List<String> compileID;
    OmniRuntime omniRuntime;

    private boolean finishing;
    private boolean finished;
    // for yield when memory is not available
//    private Work<?> unfinishedWork;

    HashAggregationOmniWork<Object> omniWork;
    private String omniKey;

    public HashAggregationOmniOperator(OperatorContext operatorContext, OmniRuntime omniRuntime, List<String> compileID)
    {
        this.operatorContext = operatorContext;
        this.omniKey= UUID.randomUUID().toString()+"-"+operatorContext.getDriverContext().getPipelineContext().getTaskId();
        this.omniRuntime = omniRuntime;
        this.compileID = compileID;
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

    @Override
    public Page getOutput()
    {
        if (finished) {
            return null;
        }
        if (finishing) {
            if (omniWork == null) {
                finished = true;
                return null;
            }
            if (omniWork != null && omniWork.isFinished()) {
                finished = true;
                return omniWork.getResult();
            }

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
        List<String> compileID;
        int operatorId;
        PlanNodeId planNodeId;

        public HashAggregationOmniOperatorFactory(int operatorId, PlanNodeId planNodeId, OmniRuntime omniRuntime, List<String> compileID)
        {
            this.operatorId = operatorId;
            this.planNodeId = planNodeId;
            this.omniRuntime = omniRuntime;
            this.compileID = compileID;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashAggregationOmniOperator.class.getSimpleName());
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
