/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class CommonTableExpressionOperator
        implements Operator, Closeable
{
    private static final Logger LOG = Logger.get(CommonTableExpressionOperator.class);

    private final PlanNodeId self;
    private final OperatorContext operatorContext;
    private final PlanNodeId consumer;
    private final CommonTableExecutionContext cteContext;
    private final int operatorInstaceId;
    private boolean finish;
    private boolean isProducer;

    public CommonTableExpressionOperator(
            PlanNodeId self,
            PlanNodeId consumer,
            OperatorContext operatorContext,
            CommonTableExecutionContext cteContext,
            int operatorInstaceId)
    {
        this.self = requireNonNull(self, "PlanNode Id is null");
        this.consumer = requireNonNull(consumer, "consumer cannot be null");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.cteContext = requireNonNull(cteContext, "CTE context is null");
        this.operatorInstaceId = operatorInstaceId;

        synchronized (cteContext) {
            if (cteContext.isProducer(consumer)) {
                this.isProducer = true;
                cteContext.setProducerState(consumer, operatorInstaceId, true);
            }
        }

        LOG.debug("CTE(" + cteContext.getName() + ")[" + consumer + "-" + operatorInstaceId + "] Operator Initialized (Producer: " + this.isProducer + ")");
    }

    public static class CommonTableExpressionOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> types;
        private final DataSize minOutputPageSize;
        private final int minOutputPageRowCount;
        private boolean closed;
        private Set<PlanNodeId> parents = new HashSet<>();
        private CommonTableExecutionContext cteCtx;
        private final AtomicInteger operatorCounter = new AtomicInteger(0);

        public CommonTableExpressionOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                CommonTableExecutionContext cteCtx,
                List<Type> types,
                DataSize minOutputPageSize,
                int minOutputPageRowCount)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.minOutputPageSize = requireNonNull(minOutputPageSize, "minOutputPageSize is null");
            this.minOutputPageRowCount = minOutputPageRowCount;
            this.cteCtx = cteCtx;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            checkArgument(parents.size() > 0, "No parent assigned for CTE");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, CommonTableExpressionOperator.class.getSimpleName());
            return new CommonTableExpressionOperator(
                    planNodeId,
                    parents.stream().findAny().get(),
                    operatorContext,
                    cteCtx,
                    operatorCounter.incrementAndGet());
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new CommonTableExpressionOperatorFactory(operatorId, planNodeId, cteCtx, types, minOutputPageSize, minOutputPageRowCount);
        }

        public void addConsumer(PlanNodeId id)
        {
            parents.add(id);
        }
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    /**
     * Returns a future that will be completed when the operator becomes
     * unblocked.  If the operator is not blocked, this method should return
     * {@code NOT_BLOCKED}.
     */
    @Override
    public ListenableFuture<?> isBlocked()
    {
        return cteContext.isBlocked(consumer);
    }

    /**
     * Returns true if and only if this operator can accept an input page.
     */
    @Override
    public boolean needsInput()
    {
        return isProducer && !finish;
    }

    /**
     * Adds an input page to the operator.  This method will only be called if
     * {@code needsInput()} returns true.
     *
     * @param page
     */
    @Override
    public void addInput(Page page)
    {
        /* Got a new page... Place it in the Queue! */
        cteContext.addPage(page);
        LOG.debug("CTE(" + cteContext.getName() + ")" + "[" + consumer + "-" + operatorInstaceId + "] Page added with " + page.getPositionCount() + " rows");
    }

    /**
     * Gets an output page from the operator.  If no output data is currently
     * available, return null.
     */
    @Override
    public Page getOutput()
    {
        try {
            Page page = cteContext.getPage(consumer);
            if (page != null) {
                LOG.debug("CTE(" + cteContext.getName() + ")" + "[" + consumer + "-" + operatorInstaceId + "] got a page with " + page.getPositionCount() + " rows");
            }

            return page;
        }
        catch (CommonTableExecutionContext.CTEDoneException e) {
            if (!finish) {
                finish = true;
                LOG.debug("CTE(" + cteContext.getName() + ")" + "[" + consumer + "-" + operatorInstaceId + "] Done(empty) directed");
            }
        }

        return null;
    }

    /**
     * After calling this method operator should revoke all reserved revocable memory.
     * As soon as memory is revoked returned future should be marked as done.
     * <p>
     * Spawned threads can not modify OperatorContext because it's not thread safe.
     * For this purpose implement {@link #finishMemoryRevoke()}
     * <p>
     * Since memory revoking signal is delivered asynchronously to the Operator, implementation
     * must gracefully handle the case when there no longer is any revocable memory allocated.
     * <p>
     * After this method is called on Operator the Driver is disallowed to call any
     * processing methods on it (isBlocked/needsInput/addInput/getOutput) until
     * {@link #finishMemoryRevoke()} is called.
     */
    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        return NOT_BLOCKED;
    }

    /**
     * Clean up and release resources after completed memory revoking. Called by driver
     * once future returned by startMemoryRevoke is completed.
     */
    @Override
    public void finishMemoryRevoke()
    {
    }

    /**
     * Notifies the operator that no more pages will be added and the
     * operator should finish processing and flush results. This method
     * will not be called if the Task is already failed or canceled.
     */
    @Override
    public void finish()
    {
        if (isProducer) {
            cteContext.setProducerState(consumer, operatorInstaceId, false);
        }
        LOG.debug("CTE(" + cteContext.getName() + ")[" + consumer + "-" + operatorInstaceId + "] Operator Finished (deferred)");
    }

    /**
     * Is this operator completely finished processing and no more
     * output pages will be produced.
     */
    @Override
    public boolean isFinished()
    {
        return finish;
    }

    /**
     * This method will always be called before releasing the Operator reference.
     */
    @Override
    public void close() throws IOException
    {
        LOG.debug("CTE(" + cteContext.getName() + ")[" + consumer + "-" + operatorInstaceId + "] Operator Closed");
    }
}
