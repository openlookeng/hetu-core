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
package io.prestosql.util;

import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.DriverYieldSignal;
import io.prestosql.operator.PageWithPositionComparator;
import io.prestosql.operator.WorkProcessor;
import io.prestosql.operator.WorkProcessor.ProcessState;
import io.prestosql.operator.WorkProcessor.TransformationState;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.operator.WorkProcessor.mergeSorted;
import static java.util.Objects.requireNonNull;

public final class MergeSortedPages
{
    private MergeSortedPages() {}

    public static WorkProcessor<Page> mergeSortedPages(
            List<WorkProcessor<Page>> pageProducers,
            PageWithPositionComparator comparator,
            List<Type> outputTypes,
            AggregatedMemoryContext aggregatedMemoryContext,
            DriverYieldSignal yieldSignal)
    {
        return mergeSortedPages(
                pageProducers,
                comparator,
                IntStream.range(0, outputTypes.size()).boxed().collect(toImmutableList()),
                outputTypes,
                (pageBuilder, pageWithPosition) -> pageBuilder.isFull(),
                false,
                aggregatedMemoryContext,
                yieldSignal);
    }

    public static WorkProcessor<Page> mergeSortedPages(
            List<WorkProcessor<Page>> pageProducers,
            PageWithPositionComparator comparator,
            List<Integer> outputChannels,
            List<Type> outputTypes,
            BiPredicate<PageBuilder, PageWithPosition> pageBreakPredicate,
            boolean updateMemoryAfterEveryPosition,
            AggregatedMemoryContext aggregatedMemoryContext,
            DriverYieldSignal yieldSignal)
    {
        requireNonNull(pageProducers, "pageProducers is null");
        requireNonNull(comparator, "comparator is null");
        requireNonNull(outputChannels, "outputChannels is null");
        requireNonNull(outputTypes, "outputTypes is null");
        requireNonNull(pageBreakPredicate, "pageBreakPredicate is null");
        requireNonNull(aggregatedMemoryContext, "aggregatedMemoryContext is null");
        requireNonNull(yieldSignal, "yieldSignal is null");

        List<WorkProcessor<PageWithPosition>> pageWithPositionProducers = pageProducers.stream()
                .map(pageProducer -> pageWithPositions(pageProducer, aggregatedMemoryContext))
                .collect(toImmutableList());

        Comparator<PageWithPosition> pageWithPositionComparator = (firstPageWithPosition, secondPageWithPosition) -> comparator.compareTo(
                firstPageWithPosition.getPage(), firstPageWithPosition.getPosition(),
                secondPageWithPosition.getPage(), secondPageWithPosition.getPosition());

        return buildPage(
                mergeSorted(pageWithPositionProducers, pageWithPositionComparator),
                outputChannels,
                outputTypes,
                pageBreakPredicate,
                updateMemoryAfterEveryPosition,
                aggregatedMemoryContext,
                yieldSignal);
    }

    private static WorkProcessor<Page> buildPage(
            WorkProcessor<PageWithPosition> pageWithPositions,
            List<Integer> outputChannels,
            List<Type> outputTypes,
            BiPredicate<PageBuilder, PageWithPosition> pageBreakPredicate,
            boolean updateMemoryAfterEveryPosition,
            AggregatedMemoryContext aggregatedMemoryContext,
            DriverYieldSignal yieldSignal)
    {
        LocalMemoryContext memoryContext = aggregatedMemoryContext.newLocalMemoryContext(MergeSortedPages.class.getSimpleName());
        PageBuilder pageBuilder = new PageBuilder(outputTypes);
        return pageWithPositions
                .yielding(yieldSignal::isSet)
                .transform(
                        new WorkProcessor.Transformation<PageWithPosition, Page>()
                        {
                            @RestorableConfig(stateClassName = "BuildPageState",
                                    uncapturedFields = {"val$outputTypes", "val$pageBreakPredicate", "val$outputChannels"})
                            private final RestorableConfig restorableConfig = null;

                            @Override
                            public TransformationState<Page> process(@Nullable PageWithPosition pageWithPosition)
                            {
                                boolean finished = pageWithPosition == null;
                                if (finished && pageBuilder.isEmpty()) {
                                    memoryContext.close();
                                    return TransformationState.finished();
                                }

                                if (finished || pageBreakPredicate.test(pageBuilder, pageWithPosition)) {
                                    if (!updateMemoryAfterEveryPosition) {
                                        // update memory usage just before producing page to cap from top
                                        memoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
                                    }

                                    Page page = pageBuilder.build();
                                    pageBuilder.reset();
                                    if (!finished) {
                                        pageWithPosition.appendTo(pageBuilder, outputChannels, outputTypes);
                                    }

                                    if (updateMemoryAfterEveryPosition) {
                                        memoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
                                    }

                                    return TransformationState.ofResult(page, !finished);
                                }

                                pageWithPosition.appendTo(pageBuilder, outputChannels, outputTypes);

                                if (updateMemoryAfterEveryPosition) {
                                    memoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
                                }

                                return TransformationState.needsMoreData();
                            }

                            @Override
                            public Object captureResult(Page result, BlockEncodingSerdeProvider serdeProvider)
                            {
                                if (result != null) {
                                    SerializedPage serializedPage = ((PagesSerde) serdeProvider).serialize(result);
                                    return serializedPage.capture(serdeProvider);
                                }
                                return null;
                            }

                            @Override
                            public Page restoreResult(Object resultState, BlockEncodingSerdeProvider serdeProvider)
                            {
                                if (resultState != null) {
                                    return ((PagesSerde) serdeProvider).deserialize(SerializedPage.restoreSerializedPage(resultState));
                                }
                                return null;
                            }

                            @Override
                            public Object captureInput(PageWithPosition input, BlockEncodingSerdeProvider serdeProvider)
                            {
                                if (input != null) {
                                    return input.capture(serdeProvider);
                                }
                                return null;
                            }

                            @Override
                            public PageWithPosition restoreInput(Object inputState, PageWithPosition input, BlockEncodingSerdeProvider serdeProvider)
                            {
                                if (inputState != null) {
                                    return PageWithPosition.restorePageWithPosition(inputState, serdeProvider);
                                }
                                return null;
                            }

                            @Override
                            public Object capture(BlockEncodingSerdeProvider serdeProvider)
                            {
                                BuildPageState myState = new BuildPageState();
                                myState.pageBuilder = pageBuilder.capture(serdeProvider);
                                myState.memoryContext = memoryContext.getBytes();
                                return myState;
                            }

                            @Override
                            public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
                            {
                                BuildPageState myState = (BuildPageState) state;
                                pageBuilder.restore(myState.pageBuilder, serdeProvider);
                                memoryContext.setBytes(myState.memoryContext);
                            }
                        });
    }

    private static class BuildPageState
            implements Serializable
    {
        private Object pageBuilder;
        private long memoryContext;
    }

    private static WorkProcessor<PageWithPosition> pageWithPositions(WorkProcessor<Page> pages, AggregatedMemoryContext aggregatedMemoryContext)
    {
        return pages.flatMap(
                new WorkProcessor.RestorableFunction<Page, WorkProcessor<PageWithPosition>>()
                {
                    @Override
                    public WorkProcessor<PageWithPosition> apply(Page page)
                    {
                        LocalMemoryContext memoryContext = aggregatedMemoryContext.newLocalMemoryContext(MergeSortedPages.class.getSimpleName());
                        memoryContext.setBytes(page.getRetainedSizeInBytes());

                        return WorkProcessor.create(new WorkProcessor.Process<PageWithPosition>()
                        {
                            @RestorableConfig(stateClassName = "PageWithPositionProcessorState", uncapturedFields = {"page", "this$0"})
                            private final RestorableConfig restorableConfig = null;

                            int position;

                            @Override
                            public ProcessState<PageWithPosition> process()
                            {
                                if (position >= page.getPositionCount()) {
                                    memoryContext.close();
                                    return ProcessState.finished();
                                }

                                return ProcessState.ofResult(new PageWithPosition(page, position++));
                            }

                            @Override
                            public Object capture(BlockEncodingSerdeProvider serdeProvider)
                            {
                                PageWithPositionProcessorState myState = new PageWithPositionProcessorState();
                                myState.position = position;
                                myState.memoryContext = memoryContext.getBytes();
                                return myState;
                            }

                            @Override
                            public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
                            {
                                PageWithPositionProcessorState myState = (PageWithPositionProcessorState) state;
                                position = myState.position;
                                memoryContext.setBytes(myState.memoryContext);
                            }

                            @Override
                            public Object captureResult(PageWithPosition result, BlockEncodingSerdeProvider serdeProvider)
                            {
                                return result.capture(serdeProvider);
                            }

                            @Override
                            public PageWithPosition restoreResult(Object resultState, BlockEncodingSerdeProvider serdeProvider)
                            {
                                return PageWithPosition.restorePageWithPosition(resultState, serdeProvider);
                            }
                        });
                    }

                    @Override
                    public Object captureResult(WorkProcessor<PageWithPosition> result, BlockEncodingSerdeProvider serdeProvider)
                    {
                        if (result != null) {
                            return result.capture(serdeProvider);
                        }
                        return null;
                    }

                    @Override
                    public Page restoreInput(Object inputState, BlockEncodingSerdeProvider serdeProvider)
                    {
                        if (inputState != null) {
                            SerializedPage serializedPage = SerializedPage.restoreSerializedPage(inputState);
                            return ((PagesSerde) serdeProvider).deserialize(serializedPage);
                        }
                        return null;
                    }

                    @Override
                    public Object captureInput(Page input, BlockEncodingSerdeProvider serdeProvider)
                    {
                        if (input != null) {
                            SerializedPage serializedPage = ((PagesSerde) serdeProvider).serialize(input);
                            return serializedPage.capture(serdeProvider);
                        }
                        return null;
                    }

                    @Override
                    public Object capture(BlockEncodingSerdeProvider serdeProvider)
                    {
                        return 0;
                    }

                    @Override
                    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
                    {
                    }
                });
    }

    private static class PageWithPositionProcessorState
            implements Serializable
    {
        private int position;
        private long memoryContext;
    }

    public static class PageWithPosition
            implements Restorable
    {
        private final Page page;
        private final int position;

        private PageWithPosition(Page page, int position)
        {
            this.page = requireNonNull(page, "page is null");
            this.position = position;
        }

        public Page getPage()
        {
            return page;
        }

        public int getPosition()
        {
            return position;
        }

        public void appendTo(PageBuilder pageBuilder, List<Integer> outputChannels, List<Type> outputTypes)
        {
            pageBuilder.declarePosition();
            for (int i = 0; i < outputChannels.size(); i++) {
                Type type = outputTypes.get(i);
                Block block = page.getBlock(outputChannels.get(i));
                type.appendTo(block, position, pageBuilder.getBlockBuilder(i));
            }
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            PageWithPositionState myState = new PageWithPositionState();
            SerializedPage serializedPage = ((PagesSerde) serdeProvider).serialize(page);
            myState.page = serializedPage.capture(serdeProvider);
            myState.position = position;
            return myState;
        }

        public static PageWithPosition restorePageWithPosition(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            PageWithPositionState myState = (PageWithPositionState) state;
            return new PageWithPosition(((PagesSerde) serdeProvider).deserialize(SerializedPage.restoreSerializedPage(myState.page)), myState.position);
        }

        private static class PageWithPositionState
                implements Serializable
        {
            private Object page;
            private int position;
        }
    }
}
