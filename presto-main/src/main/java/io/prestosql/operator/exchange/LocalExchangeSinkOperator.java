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
import io.prestosql.execution.Lifespan;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.LocalPlannerAware;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.SinkOperator;
import io.prestosql.operator.exchange.LocalExchange.LocalExchangeFactory;
import io.prestosql.operator.exchange.LocalExchange.LocalExchangeSinkFactory;
import io.prestosql.operator.exchange.LocalExchange.LocalExchangeSinkFactoryId;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.snapshot.RestorableConfig;

import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"sink", "pagePreprocessor", "snapshotState"})
public class LocalExchangeSinkOperator
        implements SinkOperator
{
    public static class LocalExchangeSinkOperatorFactory
            implements OperatorFactory, LocalPlannerAware
    {
        private final LocalExchangeFactory localExchangeFactory;

        private final int operatorId;
        // There will be a LocalExchangeSinkFactory per LocalExchangeSinkOperatorFactory per Driver Group.
        // A LocalExchangeSinkOperatorFactory needs to have access to LocalExchangeSinkFactories for each Driver Group.
        private final LocalExchangeSinkFactoryId sinkFactoryId;
        private final PlanNodeId planNodeId;
        private final Function<Page, Page> pagePreprocessor;
        private boolean closed;

        public LocalExchangeSinkOperatorFactory(LocalExchangeFactory localExchangeFactory, int operatorId, PlanNodeId planNodeId, LocalExchangeSinkFactoryId sinkFactoryId, Function<Page, Page> pagePreprocessor)
        {
            this.localExchangeFactory = requireNonNull(localExchangeFactory, "localExchangeFactory is null");

            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sinkFactoryId = requireNonNull(sinkFactoryId, "sinkFactoryId is null");
            this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, LocalExchangeSinkOperator.class.getSimpleName());

            LocalExchangeSinkFactory localExchangeSinkFactory = localExchangeFactory.getLocalExchange(driverContext.getLifespan(),
                    driverContext.getPipelineContext().getTaskContext(),
                    planNodeId.toString(),
                    context.isSnapshotEnabled()).getSinkFactory(sinkFactoryId);

            String sinkId = context.getUniqueId();
            return new LocalExchangeSinkOperator(sinkId, context, localExchangeSinkFactory.createSink(sinkId), pagePreprocessor);
        }

        @Override
        public void noMoreOperators()
        {
            if (!closed) {
                closed = true;
                localExchangeFactory.closeSinks(sinkFactoryId);
            }
        }

        @Override
        public void noMoreOperators(Lifespan lifespan)
        {
            localExchangeFactory.getLocalExchange(lifespan).getSinkFactory(sinkFactoryId).close();
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new LocalExchangeSinkOperatorFactory(localExchangeFactory, operatorId, planNodeId, localExchangeFactory.newSinkFactoryId(), pagePreprocessor);
        }

        @Override
        public void localPlannerComplete()
        {
            localExchangeFactory.noMoreSinkFactories();
        }

        public void broadcastMarker(Lifespan lifespan, MarkerPage markerPage, String origin)
        {
            localExchangeFactory.getLocalExchange(lifespan).broadcastMarker(markerPage, origin);
        }
    }

    private final String id;
    private final OperatorContext operatorContext;
    private final LocalExchangeSink sink;
    private final Function<Page, Page> pagePreprocessor;
    private final SingleInputSnapshotState snapshotState;

    LocalExchangeSinkOperator(String id, OperatorContext operatorContext, LocalExchangeSink sink, Function<Page, Page> pagePreprocessor)
    {
        this.id = id;
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sink = requireNonNull(sink, "sink is null");
        this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
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
        sink.finish();
    }

    @Override
    public boolean isFinished()
    {
        return sink.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return sink.waitForWriting();
    }

    @Override
    public boolean needsInput()
    {
        return !isFinished() && isBlocked().isDone();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");

        Page inputPage = page;
        if (snapshotState != null) {
            if (snapshotState.processPage(inputPage)) {
                inputPage = snapshotState.nextMarker();
            }
        }

        if (!(inputPage instanceof MarkerPage)) {
            inputPage = pagePreprocessor.apply(inputPage);
        }
        sink.addPage(inputPage, id);
        operatorContext.recordOutput(inputPage.getSizeInBytes(), inputPage.getPositionCount());
    }

    @Override
    public void close()
    {
        if (snapshotState != null) {
            snapshotState.close();
        }
        finish();
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
