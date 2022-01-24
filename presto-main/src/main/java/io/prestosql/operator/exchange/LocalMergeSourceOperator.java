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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.PageWithPositionComparator;
import io.prestosql.operator.WorkProcessor;
import io.prestosql.operator.exchange.LocalExchange.LocalExchangeFactory;
import io.prestosql.snapshot.MultiInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.OrderingCompiler;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.util.MergeSortedPages.mergeSortedPages;
import static java.util.Objects.requireNonNull;

// This operator's state will not be captured, because markers are received before any data pages
@RestorableConfig(unsupported = true)
public class LocalMergeSourceOperator
        implements Operator
{
    public static class LocalMergeSourceOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final LocalExchangeFactory localExchangeFactory;
        private final List<Type> types;
        private final OrderingCompiler orderingCompiler;
        private final List<Integer> sortChannels;
        private final List<SortOrder> orderings;
        private boolean closed;

        public LocalMergeSourceOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                LocalExchangeFactory localExchangeFactory,
                List<Type> types,
                OrderingCompiler orderingCompiler,
                List<Integer> sortChannels,
                List<SortOrder> orderings)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.localExchangeFactory = requireNonNull(localExchangeFactory, "exchange is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.orderings = ImmutableList.copyOf(requireNonNull(orderings, "orderings is null"));
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, LocalMergeSourceOperator.class.getSimpleName());

            LocalExchange localExchange = localExchangeFactory.getLocalExchange(driverContext.getLifespan(), driverContext.getPipelineContext().getTaskContext(), planNodeId.toString(), context.isSnapshotEnabled());

            PageWithPositionComparator comparator = orderingCompiler.compilePageWithPositionComparator(types, sortChannels, orderings);
            List<LocalExchangeSource> localExchangeSources = IntStream.range(0, localExchange.getBufferCount())
                    .boxed()
                    .map(index -> localExchange.getNextSource())
                    .collect(toImmutableList());
            return new LocalMergeSourceOperator(context, localExchangeSources, types, comparator, localExchange.getSnapshotState(), planNodeId.toString());
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
    }

    private final OperatorContext operatorContext;
    private final List<LocalExchangeSource> sources;
    private final WorkProcessor<Page> mergedPages;

    // Snapshot: this is created for the local-exchange, but made available here,
    // so this operator can return markers to downstream operators
    private final MultiInputSnapshotState snapshotState;
    // For taskSnapshotManager.updateFinishedComponents(), LocalMergeSourceOperator needs to report the same id as LocalExchange
    // because SnapshotState is affiliated with LocalExchange rather than LocalMergeSourceOperator. LocalExchange is identified by
    // planNodeId, so LocalMerge will also store a copy to identify itself so it won't get counted as a new component.
    private final String planNodeId;

    public LocalMergeSourceOperator(OperatorContext operatorContext, List<LocalExchangeSource> sources, List<Type> types, PageWithPositionComparator comparator, MultiInputSnapshotState snapshotState, String planNodeId)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sources = requireNonNull(sources, "sources is null");
        this.snapshotState = snapshotState;
        List<WorkProcessor<Page>> pageProducers = sources.stream()
                .map(LocalExchangeSource::pages)
                .collect(toImmutableList());
        mergedPages = mergeSortedPages(
                pageProducers,
                requireNonNull(comparator, "comparator is null"),
                types,
                operatorContext.aggregateUserMemoryContext(),
                operatorContext.getDriverContext().getYieldSignal());
        this.planNodeId = planNodeId;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    public String getPlanNodeId()
    {
        return planNodeId;
    }

    @Override
    public void finish()
    {
        sources.forEach(LocalExchangeSource::finish);
    }

    @Override
    public boolean isFinished()
    {
        if (snapshotState != null && snapshotState.hasPendingPages()) {
            // Snapshot: there are pending markers. Need to send them out before finishing this operator.
            return false;
        }

        return mergedPages.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (mergedPages.isBlocked()) {
            return mergedPages.getBlockedFuture();
        }

        return NOT_BLOCKED;
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
        if (snapshotState != null) {
            Page marker = snapshotState.nextMarker();
            if (marker != null) {
                return marker;
            }
        }

        if (!mergedPages.process() || mergedPages.isFinished()) {
            return null;
        }

        Page page = mergedPages.getResult();
        operatorContext.recordProcessedInput(page.getSizeInBytes(), page.getPositionCount());
        return page;
    }

    @Override
    public Page pollMarker()
    {
        return snapshotState.nextMarker();
    }

    @Override
    public void close()
            throws IOException
    {
        sources.forEach(LocalExchangeSource::close);
    }
}
