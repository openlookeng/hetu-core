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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.snapshot.RestorableConfig;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"pages", "position", "snapshotState"})
public class ValuesOperator
        implements Operator
{
    public static class ValuesOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Page> pages;
        // Snapshot: after resume, indicate which pages to send
        private final int fromPosition;
        private boolean closed;

        @VisibleForTesting
        public ValuesOperatorFactory(int operatorId, PlanNodeId planNodeId, List<Page> pages)
        {
            this(operatorId, planNodeId, pages, 0);
        }

        @VisibleForTesting
        public ValuesOperatorFactory(int operatorId, PlanNodeId planNodeId, List<Page> pages, int fromPosition)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.pages = ImmutableList.copyOf(requireNonNull(pages, "pages is null"));
            this.fromPosition = fromPosition;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext addOperatorContext = driverContext.addOperatorContext(operatorId, planNodeId, ValuesOperator.class.getSimpleName());
            return new ValuesOperator(addOperatorContext, pages, fromPosition);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new ValuesOperatorFactory(operatorId, planNodeId, pages, fromPosition);
        }
    }

    private final OperatorContext operatorContext;
    private final List<Page> pages;
    private int position;
    private final SingleInputSnapshotState snapshotState;

    public ValuesOperator(OperatorContext operatorContext, List<Page> pages, int fromPosition)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");

        requireNonNull(pages, "pages is null");

        this.pages = ImmutableList.copyOf(pages);
        this.position = fromPosition;
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
        position = pages.size();
    }

    @Override
    public boolean isFinished()
    {
        return position == pages.size();
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        Page page = null;
        if (position < pages.size()) {
            page = pages.get(position);
            position++;
            if (page != null) {
                operatorContext.recordProcessedInput(page.getSizeInBytes(), page.getPositionCount());
                if (snapshotState != null) {
                    snapshotState.processPage(page);
                }
            }
        }
        return page;
    }

    @Override
    public Page pollMarker()
    {
        Page marker = null;
        if (position < pages.size()) {
            Page page = pages.get(position);
            if (page instanceof MarkerPage) {
                position++;
                marker = page;
                snapshotState.processPage(page);
            }
        }
        return marker;
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
        return operatorContext.capture(serdeProvider);
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        operatorContext.restore(state, serdeProvider);
    }
}
