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

import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.operator.WorkProcessor.ProcessState;
import io.prestosql.operator.WorkProcessor.Transformation;
import io.prestosql.operator.WorkProcessor.TransformationState;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public final class WorkProcessorUtils
{
    private WorkProcessorUtils() {}

    static <T> Iterator<T> iteratorFrom(WorkProcessor<T> processor)
    {
        requireNonNull(processor, "processor is null");
        return new AbstractIterator<T>()
        {
            final Iterator<Optional<T>> yieldingIterator = yieldingIteratorFrom(processor);

            @Override
            protected T computeNext()
            {
                if (!yieldingIterator.hasNext()) {
                    return endOfData();
                }

                return yieldingIterator.next()
                        .orElseThrow(() -> new IllegalStateException("Cannot iterate over yielding WorkProcessor"));
            }
        };
    }

    static <T> Iterator<Optional<T>> yieldingIteratorFrom(WorkProcessor<T> processor)
    {
        return new YieldingIterator<>(processor);
    }

    private static class YieldingIterator<T>
            extends AbstractIterator<Optional<T>>
    {
        @Nullable
        WorkProcessor<T> processor;

        YieldingIterator(WorkProcessor<T> processor)
        {
            this.processor = requireNonNull(processor, "processorParameter is null");
        }

        @Override
        protected Optional<T> computeNext()
        {
            if (processor.process()) {
                if (processor.isFinished()) {
                    processor = null;
                    return endOfData();
                }

                return Optional.of(processor.getResult());
            }

            if (processor.isBlocked()) {
                throw new IllegalStateException("Cannot iterate over blocking WorkProcessor");
            }

            // yielded
            return Optional.empty();
        }
    }

    // Snapshot: currently the work process created by this utility only captures the position in the iterator.
    // It's assumed the caller is responsible for capturing any state inside elements in the iterator.
    static <T> WorkProcessor<T> fromIterator(Iterator<T> iterator)
    {
        requireNonNull(iterator, "iterator is null");
        return create(
                new WorkProcessor.Process<T>()
                {
                    @RestorableConfig(uncapturedFields = {"val$iterator"})
                    private final RestorableConfig restorableConfig = null;

                    int position;

                    @Override
                    public ProcessState<T> process()
                    {
                        if (!iterator.hasNext()) {
                            return ProcessState.finished();
                        }

                        position++;
                        return ProcessState.ofResult(iterator.next());
                    }

                    @Override
                    public Object capture(BlockEncodingSerdeProvider serdeProvider)
                    {
                        return position;
                    }

                    @Override
                    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
                    {
                        this.position = (int) state;
                        for (int i = 0; i < position; i++) {
                            iterator.next();
                        }
                    }

                    @Override
                    public Object captureResult(@NotNull T result, BlockEncodingSerdeProvider serdeProvider)
                    {
                        checkArgument(iterator instanceof WorkProcessor.RestorableIterator);
                        return ((WorkProcessor.RestorableIterator) iterator).captureResult(result, serdeProvider);
                    }

                    @Override
                    public T restoreResult(Object resultState, BlockEncodingSerdeProvider serdeProvider)
                    {
                        checkArgument(iterator instanceof WorkProcessor.RestorableIterator);
                        return (T) ((WorkProcessor.RestorableIterator) iterator).restoreResult(resultState, serdeProvider);
                    }
                });
    }

    static <T> WorkProcessor<T> mergeSorted(List<WorkProcessor<T>> processorList, Comparator<T> comparator)
    {
        requireNonNull(comparator, "comparator is null");
        checkArgument(processorList.size() > 0, "There must be at least one base processor");
        PriorityQueue<ElementAndProcessor<T>> queue = new PriorityQueue<>(2, comparing(ElementAndProcessor::getElement, comparator));

        return create(new WorkProcessor.Process<T>()
        {
            @RestorableConfig(stateClassName = "MergeSortedState", uncapturedFields = {"val$queue"})
            private final RestorableConfig restorableConfig = null;

            int nextProcessor;
            WorkProcessor<T> processor = requireNonNull(processorList.get(nextProcessor++));

            @Override
            public ProcessState<T> process()
            {
                while (true) {
                    if (processor.process()) {
                        if (!processor.isFinished()) {
                            queue.add(new ElementAndProcessor<>(processor.getResult(), processor));
                        }
                    }
                    else if (processor.isBlocked()) {
                        return ProcessState.blocked(processor.getBlockedFuture());
                    }
                    else {
                        return ProcessState.yield();
                    }

                    if (nextProcessor < processorList.size()) {
                        processor = requireNonNull(processorList.get(nextProcessor++));
                        continue;
                    }

                    if (queue.isEmpty()) {
                        processor = null;
                        return ProcessState.finished();
                    }

                    ElementAndProcessor<T> elementAndProcessor = queue.poll();
                    processor = elementAndProcessor.getProcessor();
                    return ProcessState.ofResult(elementAndProcessor.getElement());
                }
            }

            @Override
            public Object capture(BlockEncodingSerdeProvider serdeProvider)
            {
                MergeSortedState myState = new MergeSortedState();
                myState.processorList = new Object[processorList.size()];
                for (int i = 0; i < processorList.size(); i++) {
                    myState.processorList[i] = processorList.get(i).capture(serdeProvider);
                }
                myState.nextProcessor = nextProcessor;
                //Record which processors are in queue
                myState.queueProcessorIndex = new ArrayList<>();
                for (ElementAndProcessor enp : queue) {
                    myState.queueProcessorIndex.add(processorList.indexOf(enp.processor));
                }
                myState.processor = processorList.indexOf(processor);
                return myState;
            }

            @Override
            public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
            {
                MergeSortedState myState = (MergeSortedState) state;
                checkArgument(myState.processorList.length == processorList.size());
                for (int i = 0; i < myState.processorList.length; i++) {
                    processorList.get(i).restore(myState.processorList[i], serdeProvider);
                }
                nextProcessor = myState.nextProcessor;
                queue.clear();
                for (Integer queueProcessorIndex : myState.queueProcessorIndex) {
                    checkArgument(queueProcessorIndex < processorList.size(), "Processor index exceeded processor list.");
                    queue.add(new ElementAndProcessor<>(processorList.get(queueProcessorIndex).getResult(), processorList.get(queueProcessorIndex)));
                }
                this.processor = processorList.get(myState.processor);
            }

            @Override
            public Object captureResult(T result, BlockEncodingSerdeProvider serdeProvider)
            {
                for (int i = 0; i < processorList.size(); i++) {
                    if (((ProcessWorkProcessor) processorList.get(i)).state.getType() == ProcessState.Type.RESULT && processorList.get(i).getResult() == result) {
                        return i;
                    }
                }
                throw new IllegalArgumentException("Unable to capture result.");
            }

            @Override
            public T restoreResult(Object resultState, BlockEncodingSerdeProvider serdeProvider)
            {
                checkArgument(((int) resultState) < processorList.size());
                ProcessWorkProcessor<T> targetProcessor = (ProcessWorkProcessor) processorList.get((int) resultState);
                checkArgument(targetProcessor.state != null && targetProcessor.state.getType() == ProcessState.Type.RESULT);
                return targetProcessor.getResult();
            }
        });
    }

    private static class MergeSortedState
            implements Serializable
    {
        private Object[] processorList;
        private int nextProcessor;
        private List<Integer> queueProcessorIndex;
        private int processor;
    }

    static <T> WorkProcessor<T> yielding(WorkProcessor<T> processor, BooleanSupplier yieldSignal)
    {
        return WorkProcessor.create(new YieldingProcess<>(processor, yieldSignal));
    }

    @RestorableConfig(uncapturedFields = "yieldSignal")
    private static class YieldingProcess<T>
            implements WorkProcessor.Process<T>
    {
        final WorkProcessor<T> processor;
        final BooleanSupplier yieldSignal;
        boolean lastProcessYielded;

        YieldingProcess(WorkProcessor<T> processor, BooleanSupplier yieldSignal)
        {
            this.processor = requireNonNull(processor, "processor is null");
            this.yieldSignal = requireNonNull(yieldSignal, "yieldSignal is null");
        }

        @Override
        public ProcessState<T> process()
        {
            if (!lastProcessYielded && yieldSignal.getAsBoolean()) {
                lastProcessYielded = true;
                return ProcessState.yield();
            }
            lastProcessYielded = false;

            return getNextState(processor);
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            YieldingProcessState myState = new YieldingProcessState();
            myState.processor = processor.capture(serdeProvider);
            myState.lastProcessYielded = lastProcessYielded;
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            YieldingProcessState myState = (YieldingProcessState) state;
            this.processor.restore(myState.processor, serdeProvider);
            this.lastProcessYielded = myState.lastProcessYielded;
        }

        private static class YieldingProcessState
                implements Serializable
        {
            private Object processor;
            private boolean lastProcessYielded;
        }
    }

    static <T> WorkProcessor<T> processEntryMonitor(WorkProcessor<T> processor, Runnable monitor)
    {
        requireNonNull(processor, "processor is null");
        requireNonNull(monitor, "monitor is null");
        return WorkProcessor.create(() -> {
            monitor.run();
            return getNextState(processor);
        });
    }

    static <T> WorkProcessor<T> processStateMonitor(WorkProcessor<T> processor, Consumer<ProcessState<T>> monitor)
    {
        requireNonNull(processor, "processor is null");
        requireNonNull(monitor, "monitor is null");
        return WorkProcessor.create(() -> {
            ProcessState<T> state = getNextState(processor);
            monitor.accept(state);
            return state;
        });
    }

    static <T> WorkProcessor<T> finishWhen(WorkProcessor<T> processor, BooleanSupplier finishSignal)
    {
        requireNonNull(processor, "processor is null");
        requireNonNull(finishSignal, "finishSignal is null");
        return WorkProcessor.create(() -> {
            if (finishSignal.getAsBoolean()) {
                return ProcessState.finished();
            }

            return getNextState(processor);
        });
    }

    private static <T> ProcessState<T> getNextState(WorkProcessor<T> processor)
    {
        if (processor.process()) {
            if (processor.isFinished()) {
                return ProcessState.finished();
            }

            return ProcessState.ofResult(processor.getResult());
        }

        if (processor.isBlocked()) {
            return ProcessState.blocked(processor.getBlockedFuture());
        }

        return ProcessState.yield();
    }

    static <T, R> WorkProcessor<R> flatMap(WorkProcessor<T> processor, WorkProcessor.RestorableFunction<T, WorkProcessor<R>> mapper)
    {
        requireNonNull(processor, "processor is null");
        requireNonNull(mapper, "mapper is null");
        return processor.flatTransform(
                new Transformation<T, WorkProcessor<R>>()
                {
                    @RestorableConfig(uncapturedFields = "mapper")
                    private final RestorableConfig restorableConfig = null;

                    T savedElement;

                    @Override
                    public TransformationState<WorkProcessor<R>> process(@org.jetbrains.annotations.Nullable T element)
                    {
                        if (element == null) {
                            return TransformationState.finished();
                        }
                        savedElement = element;
                        return TransformationState.ofResult(mapper.apply(element));
                    }

                    @Override
                    public Object captureResult(WorkProcessor<R> result, BlockEncodingSerdeProvider serdeProvider)
                    {
                        return result.capture(serdeProvider);
                    }

                    @Override
                    public WorkProcessor<R> restoreResult(Object resultState, BlockEncodingSerdeProvider serdeProvider)
                    {
                        if (savedElement != null) {
                            WorkProcessor<R> restoredProcessor = mapper.apply(savedElement);
                            restoredProcessor.restore(resultState, serdeProvider);
                            return restoredProcessor;
                        }
                        return null;
                    }

                    @Override
                    public Object captureInput(T input, BlockEncodingSerdeProvider serdeProvider)
                    {
                        return mapper.captureInput(input, serdeProvider);
                    }

                    @Override
                    public T restoreInput(Object inputState, T input, BlockEncodingSerdeProvider serdeProvider)
                    {
                        return mapper.restoreInput(inputState, serdeProvider);
                    }

                    @Override
                    public Object capture(BlockEncodingSerdeProvider serdeProvider)
                    {
                        return mapper.captureInput(savedElement, serdeProvider);
                    }

                    @Override
                    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
                    {
                        this.savedElement = mapper.restoreInput(state, serdeProvider);
                    }
                });
    }

    static <T, R> WorkProcessor<R> map(WorkProcessor<T> processor, WorkProcessor.RestorableFunction<T, R> mapper)
    {
        requireNonNull(processor, "processor is null");
        requireNonNull(mapper, "mapper is null");
        return processor.transform(
                new Transformation<T, R>()
                {
                    @Override
                    public TransformationState<R> process(@org.jetbrains.annotations.Nullable T element)
                    {
                        if (element == null) {
                            return TransformationState.finished();
                        }

                        return TransformationState.ofResult(mapper.apply(element));
                    }

                    @Override
                    public Object captureResult(R result, BlockEncodingSerdeProvider serdeProvider)
                    {
                        return mapper.captureResult(result, serdeProvider);
                    }

                    @Override
                    public R restoreResult(Object resultState, BlockEncodingSerdeProvider serdeProvider)
                    {
                        return mapper.restoreResult(resultState, serdeProvider);
                    }

                    @Override
                    public Object captureInput(T input, BlockEncodingSerdeProvider serdeProvider)
                    {
                        return mapper.captureInput(input, serdeProvider);
                    }

                    @Override
                    public T restoreInput(Object inputState, T input, BlockEncodingSerdeProvider serdeProvider)
                    {
                        return mapper.restoreInput(inputState, serdeProvider);
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

    static <T, R> WorkProcessor<R> flatTransform(WorkProcessor<T> processor, Transformation<T, WorkProcessor<R>> transformation)
    {
        requireNonNull(processor, "processor is null");
        requireNonNull(transformation, "transformation is null");
        return processor.transform(transformation).transformProcessor(WorkProcessorUtils::flatten);
    }

    static <T> WorkProcessor<T> flatten(WorkProcessor<WorkProcessor<T>> processor)
    {
        requireNonNull(processor, "processor is null");
        return processor.transform(
                new Transformation<WorkProcessor<T>, T>()
                {
                    @Override
                    public TransformationState<T> process(@org.jetbrains.annotations.Nullable WorkProcessor<T> nestedProcessor)
                    {
                        if (nestedProcessor == null) {
                            return TransformationState.finished();
                        }

                        if (nestedProcessor.process()) {
                            if (nestedProcessor.isFinished()) {
                                return TransformationState.needsMoreData();
                            }

                            return TransformationState.ofResult(nestedProcessor.getResult(), false);
                        }

                        if (nestedProcessor.isBlocked()) {
                            return TransformationState.blocked(nestedProcessor.getBlockedFuture());
                        }

                        return TransformationState.yield();
                    }

                    @Override
                    public Object captureInput(WorkProcessor<T> input, BlockEncodingSerdeProvider serdeProvider)
                    {
                        if (input != null) {
                            return input.capture(serdeProvider);
                        }
                        return null;
                    }

                    @Override
                    public WorkProcessor<T> restoreInput(Object inputState, WorkProcessor<T> input, BlockEncodingSerdeProvider serdeProvider)
                    {
                        if (input != null) {
                            input.restore(inputState, serdeProvider);
                            return input;
                        }
                        else {
                            throw new UnsupportedOperationException();
                        }
                    }

                    @Override
                    public Object captureResult(T result, BlockEncodingSerdeProvider serdeProvider)
                    {
                        if (result != null) {
                            checkArgument(processor.getResult().getResult() == result);
                            return true;
                        }
                        return false;
                    }

                    @Override
                    public T restoreResult(Object resultState, BlockEncodingSerdeProvider serdeProvider)
                    {
                        if ((boolean) resultState) {
                            checkArgument(processor.getResult().getResult() != null);
                            return processor.getResult().getResult();
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

    static <T, R> WorkProcessor<R> transform(WorkProcessor<T> processor, Transformation<T, R> transformation)
    {
        requireNonNull(processor, "processor is null");
        requireNonNull(transformation, "transformation is null");
        return create(new WorkProcessor.Process<R>()
        {
            @RestorableConfig(stateClassName = "TransformState", uncapturedFields = {"element"})
            private final RestorableConfig restorableConfig = null;

            T element;

            @Override
            public ProcessState<R> process()
            {
                while (true) {
                    if (element == null && !processor.isFinished()) {
                        if (processor.process()) {
                            if (!processor.isFinished()) {
                                element = requireNonNull(processor.getResult(), "result is null");
                            }
                        }
                        else if (processor.isBlocked()) {
                            return ProcessState.blocked(processor.getBlockedFuture());
                        }
                        else {
                            return ProcessState.yield();
                        }
                    }

                    TransformationState<R> state = requireNonNull(transformation.process(element), "state is null");

                    if (state.isNeedsMoreData()) {
                        checkState(!processor.isFinished(), "Cannot request more data when base processor is finished");
                        // set element to empty() in order to fetch a new one
                        element = null;
                    }

                    // pass-through transformation state if it doesn't require new data
                    switch (state.getType()) {
                        case NEEDS_MORE_DATA:
                            break;
                        case BLOCKED:
                            return ProcessState.blocked(state.getBlocked());
                        case YIELD:
                            return ProcessState.yield();
                        case RESULT:
                            return ProcessState.ofResult(state.getResult());
                        case FINISHED:
                            return ProcessState.finished();
                    }
                }
            }

            @Override
            public Object capture(BlockEncodingSerdeProvider serdeProvider)
            {
                TransformState myState = new TransformState();
                myState.transformation = transformation.capture(serdeProvider);
                myState.validElement = element != null;
                myState.processor = processor.capture(serdeProvider);
                return myState;
            }

            @Override
            public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
            {
                TransformState myState = (TransformState) state;
                transformation.restore(myState.transformation, serdeProvider);
                processor.restore(myState.processor, serdeProvider);
                if (myState.validElement) {
                    checkArgument(processor.getResult() != null);
                    element = processor.getResult();
                }
            }

            @Override
            public Object captureResult(R result, BlockEncodingSerdeProvider serdeProvider)
            {
                return transformation.captureResult(result, serdeProvider);
            }

            @Override
            public R restoreResult(Object resultState, BlockEncodingSerdeProvider serdeProvider)
            {
                return transformation.restoreResult(resultState, serdeProvider);
            }
        });
    }

    private static class TransformState
            implements Serializable
    {
        private Object transformation;
        private Object processor;
        private boolean validElement;
    }

    static <T> WorkProcessor<T> create(WorkProcessor.Process<T> process)
    {
        return new ProcessWorkProcessor<>(process);
    }

    private static class ProcessWorkProcessor<T>
            implements WorkProcessor<T>
    {
        @Nullable
        WorkProcessor.Process<T> process;
        @Nullable
        ProcessState<T> state;

        ProcessWorkProcessor(WorkProcessor.Process<T> process)
        {
            this.process = requireNonNull(process, "process is null");
        }

        @Override
        public boolean process()
        {
            if (isBlocked()) {
                return false;
            }
            if (isFinished()) {
                return true;
            }
            state = requireNonNull(process.process());

            if (state.getType() == ProcessState.Type.FINISHED) {
                process = null;
                return true;
            }

            return state.getType() == ProcessState.Type.RESULT;
        }

        @Override
        public boolean isBlocked()
        {
            return state != null && state.getType() == ProcessState.Type.BLOCKED && !state.getBlocked().isDone();
        }

        @Override
        public ListenableFuture<?> getBlockedFuture()
        {
            checkState(state != null && state.getType() == ProcessState.Type.BLOCKED, "Must be blocked to get blocked future");
            return state.getBlocked();
        }

        @Override
        public boolean isFinished()
        {
            return state != null && state.getType() == ProcessState.Type.FINISHED;
        }

        @Override
        public T getResult()
        {
            checkState(state != null && state.getType() == ProcessState.Type.RESULT, "process() must return true and must not be finished");
            return state.getResult();
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            ProcessWorkProcessorState myState = new ProcessWorkProcessorState();
            if (state != null && state.getType() == ProcessState.Type.RESULT) {
                T result = state.getResult();
                myState.state = process.captureResult(result, serdeProvider);
            }
            else if (state != null && (state.getType() == ProcessState.Type.FINISHED || state.getType() == ProcessState.Type.YIELD)) {
                myState.state = state.getType().toString();
            }
            if (process != null) {
                myState.process = process.capture(serdeProvider);
            }
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            ProcessWorkProcessorState myState = (ProcessWorkProcessorState) state;
            if (myState.process != null) {
                this.process.restore(myState.process, serdeProvider);
            }
            else {
                checkState(myState.state instanceof String && myState.state.equals("FINISHED"));
                this.process = null;
            }
            if (myState.state != null) {
                if (myState.state instanceof String) {
                    if (myState.state.equals("FINISHED")) {
                        this.state = ProcessState.finished();
                    }
                    else if (myState.state.equals("YIELD")) {
                        this.state = ProcessState.yield();
                    }
                }
                else {
                    this.state = ProcessState.ofResult(process.restoreResult(myState.state, serdeProvider));
                }
            }
            else {
                this.state = null;
            }
        }

        private static class ProcessWorkProcessorState
                implements Serializable
        {
            private Object process;
            private Object state;
        }
    }

    private static class ElementAndProcessor<T>
    {
        @Nullable final T element;
        final WorkProcessor<T> processor;

        ElementAndProcessor(T element, WorkProcessor<T> processor)
        {
            this.element = element;
            this.processor = requireNonNull(processor, "processor is null");
        }

        T getElement()
        {
            return element;
        }

        WorkProcessor<T> getProcessor()
        {
            return processor;
        }
    }
}
