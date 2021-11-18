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
import io.prestosql.Session;
import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;
import io.prestosql.spi.snapshot.RestorableConfig;

import java.io.Serializable;

import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"pages", "snapshotState"})
public class WorkProcessorOperatorAdapter
        implements Operator
{
    private final OperatorContext operatorContext;
    private final AdapterWorkProcessorOperator workProcessorOperator;
    private final WorkProcessor<Page> pages;
    private final SingleInputSnapshotState snapshotState;

    public interface AdapterWorkProcessorOperator
            extends WorkProcessorOperator, Restorable
    {
        boolean needsInput();

        void addInput(Page page);

        void finish();
    }

    public interface AdapterWorkProcessorOperatorFactory
            extends WorkProcessorOperatorFactory
    {
        AdapterWorkProcessorOperator create(
                Session session,
                MemoryTrackingContext memoryTrackingContext,
                DriverYieldSignal yieldSignal);
    }

    public WorkProcessorOperatorAdapter(OperatorContext operatorContext, AdapterWorkProcessorOperatorFactory workProcessorOperatorFactory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.snapshotState = operatorContext.isSnapshotEnabled() ? SingleInputSnapshotState.forOperator(this, operatorContext) : null;
        this.workProcessorOperator = requireNonNull(workProcessorOperatorFactory, "workProcessorOperatorFactory is null")
                .create(
                        operatorContext.getSession(),
                        new MemoryTrackingContext(
                                operatorContext.aggregateUserMemoryContext(),
                                operatorContext.aggregateRevocableMemoryContext(),
                                operatorContext.aggregateSystemMemoryContext()),
                        operatorContext.getDriverContext().getYieldSignal());
        this.pages = workProcessorOperator.getOutputPages();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (!pages.isBlocked()) {
            return NOT_BLOCKED;
        }

        return pages.getBlockedFuture();
    }

    @Override
    public boolean needsInput()
    {
        return !isFinished() && workProcessorOperator.needsInput();
    }

    @Override
    public void addInput(Page page)
    {
        if (snapshotState != null) {
            if (snapshotState.processPage(page)) {
                return;
            }
        }

        workProcessorOperator.addInput(page);
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

        if (!pages.process()) {
            return null;
        }

        if (pages.isFinished()) {
            return null;
        }

        return pages.getResult();
    }

    @Override
    public Page pollMarker()
    {
        return snapshotState.nextMarker();
    }

    @Override
    public void finish()
    {
        workProcessorOperator.finish();
    }

    @Override
    public boolean isFinished()
    {
        if (snapshotState != null && snapshotState.hasMarker()) {
            // Snapshot: there are pending markers. Need to send them out before finishing this operator.
            return false;
        }

        return pages.isFinished();
    }

    @Override
    public void close()
            throws Exception
    {
        if (snapshotState != null) {
            snapshotState.close();
        }
        workProcessorOperator.close();
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        WorkProcessorOperatorAdapterState myState = new WorkProcessorOperatorAdapterState();
        myState.operatorContext = operatorContext.capture(serdeProvider);
        myState.workProcessorOperator = workProcessorOperator.capture(serdeProvider);
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        WorkProcessorOperatorAdapterState myState = (WorkProcessorOperatorAdapterState) state;
        this.operatorContext.restore(myState.operatorContext, serdeProvider);
        this.workProcessorOperator.restore(myState.workProcessorOperator, serdeProvider);
    }

    private static class WorkProcessorOperatorAdapterState
            implements Serializable
    {
        private Object operatorContext;
        private Object workProcessorOperator;
    }
}
