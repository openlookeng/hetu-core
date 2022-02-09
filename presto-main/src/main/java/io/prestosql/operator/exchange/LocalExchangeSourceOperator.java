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
package io.prestosql.operator.exchange;

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.exchange.LocalExchange.LocalExchangeFactory;
import io.prestosql.snapshot.MultiInputRestorable;
import io.prestosql.snapshot.MultiInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;

import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"snapshotState", "source"})
public class LocalExchangeSourceOperator
        implements Operator, MultiInputRestorable
{
    public static class LocalExchangeSourceOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final LocalExchangeFactory localExchangeFactory;
        private final int totalInputChannels;
        private boolean closed;

        public LocalExchangeSourceOperatorFactory(int operatorId, PlanNodeId planNodeId, LocalExchangeFactory localExchangeFactory, int totalInputChannels)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.localExchangeFactory = requireNonNull(localExchangeFactory, "localExchangeFactory is null");
            this.totalInputChannels = totalInputChannels;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            LocalExchange inMemoryExchange = localExchangeFactory.getLocalExchange(driverContext.getLifespan(), driverContext.getPipelineContext().getTaskContext());

            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, LocalExchangeSourceOperator.class.getSimpleName());
            LocalExchangeSource localExchangeSource = inMemoryExchange.getNextSource();
            // Snapshot: make driver ID and source/partition index the same, to ensure consistency before and after resuming.
            // HashBuilderOperator also uses the same mechanism to ensure consistency.
            if (context.isSnapshotEnabled()) {
                localExchangeSource = inMemoryExchange.getSource(driverContext.getDriverId());
            }
            return new LocalExchangeSourceOperator(context, localExchangeSource, totalInputChannels);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Source operator factories can not be duplicated");
        }

        public LocalExchangeFactory getLocalExchangeFactory()
        {
            return localExchangeFactory;
        }
    }

    private final OperatorContext operatorContext;
    private final MultiInputSnapshotState snapshotState;
    private final LocalExchangeSource source;
    // Snapshot: total number of local-sinks that send data to this operator
    private final int totalInputChannels;

    public LocalExchangeSourceOperator(OperatorContext operatorContext, LocalExchangeSource source, int totalInputChannels)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.snapshotState = operatorContext.isSnapshotEnabled()
                ? MultiInputSnapshotState.forOperator(this, operatorContext)
                : null;
        this.source = requireNonNull(source, "source is null");
        this.totalInputChannels = totalInputChannels;
        operatorContext.setInfoSupplier(source::getBufferInfo);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        source.finish();
    }

    @Override
    public boolean isFinished()
    {
        if (snapshotState != null && snapshotState.hasPendingDataPages()) {
            // Snapshot: there are pending restored pages. Need to send them out before finishing this operator.
            return false;
        }

        // Snapshot: must also use up all resumed pages
        return source.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return source.waitForReading();
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
        Page page;
        if (snapshotState != null) {
            page = snapshotState.processPage(() -> source.removePage()).orElse(null);
        }
        else {
            // origin not needed in this case
            page = source.removePage().getLeft();
        }
        if (page != null) {
            operatorContext.recordProcessedInput(page.getSizeInBytes(), page.getPositionCount());
        }
        return page;
    }

    @Override
    public Page pollMarker()
    {
        return snapshotState.nextMarker(() -> source.removePage()).orElse(null);
    }

    @Override
    public Optional<Set<String>> getInputChannels()
    {
        Set<String> channels = source.getAllInputChannels();
        return totalInputChannels == channels.size() ? Optional.of(channels) : Optional.empty();
    }

    @Override
    public void close()
    {
        source.close();
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
