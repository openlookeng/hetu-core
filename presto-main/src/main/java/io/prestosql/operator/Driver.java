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
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.SystemSessionProperties;
import io.prestosql.execution.ScheduledSplit;
import io.prestosql.execution.TaskSource;
import io.prestosql.execution.TaskState;
import io.prestosql.metadata.Split;
import io.prestosql.snapshot.TaskSnapshotManager;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.MarkerPage;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.prestosql.operator.Operator.NOT_BLOCKED;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_PRODUCER;
import static java.lang.Boolean.TRUE;
import static java.util.Objects.requireNonNull;

//
// NOTE:  As a general strategy the methods should "stage" a change and only
// process the actual change before lock release (DriverLockResult.close()).
// The assures that only one thread will be working with the operators at a
// time and state changer threads are not blocked.
//
public class Driver
        implements Closeable
{
    private static final Logger LOG = Logger.get(Driver.class);

    private final DriverContext driverContext;
    private final boolean isSnapshotEnabled;
    // Snapshot: whether completion of this driver has been reported to the snapshot manager. Make sure it's done once.
    private boolean reportedFinish;
    private final List<Operator> activeOperators;
    // this is present only for debugging
    @SuppressWarnings("unused")
    private final List<Operator> allOperators;
    private final Optional<SourceOperator> sourceOperator;
    private final Optional<DeleteOperator> deleteOperator;
    private final Optional<UpdateOperator> updateOperator;

    // This variable acts as a staging area. When new splits (encapsulated in TaskSource) are
    // provided to a Driver, the Driver will not process them right away. Instead, the splits are
    // added to this staging area. This staging area will be drained asynchronously. That's when
    // the new splits get processed.
    private final AtomicReference<TaskSource> pendingTaskSourceUpdates = new AtomicReference<>();
    private final Map<Operator, ListenableFuture<?>> revokingOperators = new HashMap<>();

    private final AtomicReference<State> state = new AtomicReference<>(State.ALIVE);

    private final DriverLock exclusiveLock = new DriverLock();

    @GuardedBy("exclusiveLock")
    private TaskSource currentTaskSource;

    private final AtomicReference<SettableFuture<?>> driverBlockedFuture = new AtomicReference<>();

    private enum State
    {
        ALIVE, NEED_DESTRUCTION, CANCEL_TO_RESUME, DESTROYED
    }

    public static Driver createDriver(DriverContext driverContext, List<Operator> operators)
    {
        requireNonNull(driverContext, "driverContext is null");
        requireNonNull(operators, "operators is null");
        Driver driver = new Driver(driverContext, operators);
        driver.initialize();
        return driver;
    }

    @VisibleForTesting
    public static Driver createDriver(DriverContext driverContext, Operator firstOperator, Operator... otherOperators)
    {
        requireNonNull(driverContext, "driverContext is null");
        requireNonNull(firstOperator, "firstOperator is null");
        requireNonNull(otherOperators, "otherOperators is null");
        ImmutableList<Operator> operators = ImmutableList.<Operator>builder()
                .add(firstOperator)
                .add(otherOperators)
                .build();
        return createDriver(driverContext, operators);
    }

    private Driver(DriverContext driverContext, List<Operator> operators)
    {
        this.driverContext = requireNonNull(driverContext, "driverContext is null");
        this.isSnapshotEnabled = SystemSessionProperties.isSnapshotEnabled(driverContext.getSession());
        this.allOperators = ImmutableList.copyOf(requireNonNull(operators, "operators is null"));
        checkArgument(allOperators.size() > 1, "At least two operators are required");
        this.activeOperators = new ArrayList<>(operators);
        checkArgument(!operators.isEmpty(), "There must be at least one operator");

        Optional<SourceOperator> optionalSourceOperator = Optional.empty();
        Optional<DeleteOperator> optionalDeleteOperator = Optional.empty();
        Optional<UpdateOperator> optionalUpdateOperator = Optional.empty();
        for (Operator operator : operators) {
            if (operator instanceof SourceOperator) {
                checkArgument(!optionalSourceOperator.isPresent(), "There must be at most one SourceOperator");
                optionalSourceOperator = Optional.of((SourceOperator) operator);
            }
            else if (operator instanceof DeleteOperator) {
                checkArgument(!optionalDeleteOperator.isPresent(), "There must be at most one DeleteOperator");
                optionalDeleteOperator = Optional.of((DeleteOperator) operator);
            }
            else if (operator instanceof UpdateOperator) {
                checkArgument(!optionalUpdateOperator.isPresent(), "There must be at most one UpdateOperator");
                optionalUpdateOperator = Optional.of((UpdateOperator) operator);
            }
        }
        this.sourceOperator = optionalSourceOperator;
        this.deleteOperator = optionalDeleteOperator;
        this.updateOperator = optionalUpdateOperator;

        currentTaskSource = optionalSourceOperator.map(operator -> new TaskSource(operator.getSourceId(), ImmutableSet.of(), false)).orElse(null);
        // initially the driverBlockedFuture is not blocked (it is completed)
        SettableFuture<?> future = SettableFuture.create();
        future.set(null);
        driverBlockedFuture.set(future);
    }

    // the memory revocation request listeners are added here in a separate initialize() method
    // instead of the constructor to prevent leaking the "this" reference to
    // another thread, which will cause unsafe publication of this instance.
    private void initialize()
    {
        activeOperators.stream()
                .map(Operator::getOperatorContext)
                .forEach(operatorContext -> operatorContext.setMemoryRevocationRequestListener(() -> driverBlockedFuture.get().set(null)));
    }

    public DriverContext getDriverContext()
    {
        return driverContext;
    }

    public Optional<PlanNodeId> getSourceId()
    {
        return sourceOperator.map(SourceOperator::getSourceId);
    }

    public synchronized void reportFinishedDriver()
    {
        if (isSnapshotEnabled && !reportedFinish) {
            TaskSnapshotManager snapshotManager = driverContext.getPipelineContext().getTaskContext().getSnapshotManager();
            snapshotManager.updateFinishedComponents(allOperators);
            reportedFinish = true;
        }
    }

    @Override
    public void close()
    {
        // mark the service for destruction
        State targetState = driverContext.getPipelineContext().getTaskContext().getState() == TaskState.CANCELED_TO_RESUME
                ? State.CANCEL_TO_RESUME // Snapshot: if task state is cancel-to-resume, then make sure operators are aware
                : State.NEED_DESTRUCTION;
        if (!state.compareAndSet(State.ALIVE, targetState)) {
            return;
        }

        exclusiveLock.interruptCurrentOwner();

        // if we can get the lock, attempt a clean shutdown; otherwise someone else will shutdown
        tryWithLock(() -> TRUE);
    }

    public boolean isFinished()
    {
        checkLockNotHeld("Can not check finished status while holding the driver lock");

        // if we can get the lock, attempt a clean shutdown; otherwise someone else will shutdown
        Optional<Boolean> result = tryWithLock(this::isFinishedInternal);
        return result.orElseGet(() -> state.get() != State.ALIVE || driverContext.isDone());
    }

    @GuardedBy("exclusiveLock")
    private boolean isFinishedInternal()
    {
        checkLockHeld("Lock must be held to call isFinishedInternal");

        boolean finished = state.get() != State.ALIVE || driverContext.isDone() || activeOperators.isEmpty() || activeOperators.get(activeOperators.size() - 1).isFinished();
        if (finished) {
            State targetState = driverContext.getPipelineContext().getTaskContext().getState() == TaskState.CANCELED_TO_RESUME
                    ? State.CANCEL_TO_RESUME // Snapshot: if task state is cancel-to-resume, then make sure operators are aware
                    : State.NEED_DESTRUCTION;
            state.compareAndSet(State.ALIVE, targetState);
        }
        return finished;
    }

    public void updateSource(TaskSource sourceUpdate)
    {
        checkLockNotHeld("Can not update sources while holding the driver lock");
        checkArgument(
                sourceOperator.isPresent() && sourceOperator.get().getSourceId().equals(sourceUpdate.getPlanNodeId()),
                "sourceUpdate is for a plan node that is different from this Driver's source node");

        // stage the new updates
        pendingTaskSourceUpdates.updateAndGet(current -> current == null ? sourceUpdate : current.update(sourceUpdate));

        // attempt to get the lock and process the updates we staged above
        // updates will be processed in close if and only if we got the lock
        tryWithLock(() -> TRUE);
    }

    @GuardedBy("exclusiveLock")
    private void processNewSources()
    {
        checkLockHeld("Lock must be held to call processNewSources");

        // only update if the driver is still alive
        if (state.get() != State.ALIVE) {
            return;
        }

        TaskSource sourceUpdate = pendingTaskSourceUpdates.getAndSet(null);
        if (sourceUpdate == null) {
            return;
        }

        // merge the current source and the specified source update
        TaskSource newSource = currentTaskSource.update(sourceUpdate);

        // if the update contains no new data, just return
        if (newSource == currentTaskSource) {
            return;
        }

        // determine new splits to add
        Set<ScheduledSplit> newSplits = Sets.difference(newSource.getSplits(), currentTaskSource.getSplits());

        // add new splits
        SourceOperator sourceOperatorNew = this.sourceOperator.orElseThrow(VerifyException::new);
        for (ScheduledSplit newSplit : newSplits) {
            Split split = newSplit.getSplit();

            Supplier<Optional<UpdatablePageSource>> pageSource = sourceOperatorNew.addSplit(split);
            deleteOperator.ifPresent(deleteOperator -> deleteOperator.setPageSource(pageSource));
            updateOperator.ifPresent(updateOperator -> updateOperator.setPageSource(pageSource));
        }

        // set no more splits
        if (newSource.isNoMoreSplits()) {
            sourceOperatorNew.noMoreSplits();
        }

        currentTaskSource = newSource;
    }

    public ListenableFuture<?> processFor(Duration duration)
    {
        checkLockNotHeld("Can not process for a duration while holding the driver lock");

        requireNonNull(duration, "duration is null");

        // if the driver is blocked we don't need to continue
        SettableFuture<?> blockedFuture = driverBlockedFuture.get();
        if (!blockedFuture.isDone()) {
            return blockedFuture;
        }

        long maxRuntime = duration.roundTo(TimeUnit.NANOSECONDS);

        Optional<ListenableFuture<?>> result = tryWithLock(100, TimeUnit.MILLISECONDS, () -> {
            OperationTimer operationTimer = createTimer();
            driverContext.startProcessTimer();
            driverContext.getYieldSignal().setWithDelay(maxRuntime, driverContext.getYieldExecutor());
            try {
                long start = System.nanoTime();
                do {
                    ListenableFuture<?> future = processInternal(operationTimer);
                    if (!future.isDone()) {
                        return updateDriverBlockedFuture(future);
                    }
                }
                while (System.nanoTime() - start < maxRuntime && !isFinishedInternal());
            }
            finally {
                driverContext.getYieldSignal().reset();
                driverContext.recordProcessed(operationTimer);
            }
            return NOT_BLOCKED;
        });
        return result.orElse(NOT_BLOCKED);
    }

    public ListenableFuture<?> process()
    {
        checkLockNotHeld("Can not process while holding the driver lock");

        // if the driver is blocked we don't need to continue
        SettableFuture<?> blockedFuture = driverBlockedFuture.get();
        if (!blockedFuture.isDone()) {
            return blockedFuture;
        }

        Optional<ListenableFuture<?>> result = tryWithLock(100, TimeUnit.MILLISECONDS, () -> {
            ListenableFuture<?> future = processInternal(createTimer());
            return updateDriverBlockedFuture(future);
        });
        return result.orElse(NOT_BLOCKED);
    }

    private OperationTimer createTimer()
    {
        return new OperationTimer(
                driverContext.isCpuTimerEnabled(),
                driverContext.isCpuTimerEnabled() && driverContext.isPerOperatorCpuTimerEnabled());
    }

    private ListenableFuture<?> updateDriverBlockedFuture(ListenableFuture<?> sourceBlockedFuture)
    {
        // driverBlockedFuture will be completed as soon as the sourceBlockedFuture is completed
        // or any of the operators gets a memory revocation request
        SettableFuture<?> newDriverBlockedFuture = SettableFuture.create();
        driverBlockedFuture.set(newDriverBlockedFuture);
        sourceBlockedFuture.addListener(() -> newDriverBlockedFuture.set(null), directExecutor());

        // it's possible that memory revoking is requested for some operator
        // before we update driverBlockedFuture above and we don't want to miss that
        // notification, so we check to see whether that's the case before returning.
        boolean memoryRevokingRequested = activeOperators.stream()
                .filter(operator -> !revokingOperators.containsKey(operator))
                .map(Operator::getOperatorContext)
                .anyMatch(OperatorContext::isMemoryRevokingRequested);

        if (memoryRevokingRequested) {
            newDriverBlockedFuture.set(null);
        }

        return newDriverBlockedFuture;
    }

    // Snapshot: for debugging only, to print number of rows received by each operator for each snapshot
    private final Map<Operator, Long> receivedRows = LOG.isDebugEnabled() ? new HashMap<>() : null;

    @GuardedBy("exclusiveLock")
    private ListenableFuture<?> processInternal(OperationTimer operationTimer)
    {
        checkLockHeld("Lock must be held to call processInternal");

        handleMemoryRevoke();

        try {
            processNewSources();

            // If there is only one operator, finish it
            // Some operators (LookupJoinOperator and HashBuildOperator) are broken and requires finish to be called continuously
            // TODO remove the second part of the if statement, when these operators are fixed
            // Note: finish should not be called on the natural source of the pipeline as this could cause the task to finish early
            if (!activeOperators.isEmpty() && activeOperators.size() != allOperators.size()) {
                Operator rootOperator = activeOperators.get(0);
                rootOperator.finish();
                rootOperator.getOperatorContext().recordFinish(operationTimer);
            }

            boolean movedPage = false;
            for (int i = 0; i < activeOperators.size() - 1 && !driverContext.isDone(); i++) {
                Operator current = activeOperators.get(i);
                Operator next = activeOperators.get(i + 1);

                if (LOG.isDebugEnabled()) {
                    if (getBlockedFuture(current).isPresent() || getBlockedFuture(next).isPresent()) {
                        LOG.debug("Blocking info next=%s: getBlockedFuture(current)=%b; current.isFinished=%b; getBlockedFuture(next)=%b; next.needsInput=%b",
                                next.getOperatorContext().getUniqueId(), getBlockedFuture(current).isPresent(), current.isFinished(), getBlockedFuture(next).isPresent(), next.needsInput());
                    }
                }

                // skip blocked operator
                if (isOperatorBlocked(current, next)) {
                    continue;
                }

                // if the current operator is not finished and next operator isn't blocked and needs input...
                if (!current.isFinished() && !getBlockedFuture(next).isPresent()) {
                    // get an output page from current operator
                    Page page = null;
                    if (next.needsInput()) {
                        page = current.getOutput();
                        current.getOperatorContext().recordGetOutput(operationTimer, page);
                        if (current instanceof SourceOperator) {
                            movedPage = true;
                        }
                    }
                    else if (isSnapshotEnabled && next.allowMarker()) {
                        // Snapshot: even when operators don't need (data) input, they may still allow markers to pass through.
                        // In particular, when join operators wait for build side to finish, they don't need inputs,
                        // but they need to receive and process markers, for snapshots to complete.
                        page = current.pollMarker();
                        current.getOperatorContext().recordGetOutput(operationTimer, page);
                        if (current instanceof SourceOperator) {
                            movedPage = true;
                        }
                    }

                    // if we got an output page, add it to the next operator
                    if (page != null && page.getPositionCount() != 0) {
                        if (LOG.isDebugEnabled()) {
                            // Snapshot: print number of rows received by each operator for each snapshot
                            final Page p = page;
                            if (page instanceof MarkerPage) {
                                long count = receivedRows.compute(next, (o, v) -> v == null ? 0 : v);
                                LOG.debug("Operator %s received %d rows at marker %s", next.getOperatorContext().getUniqueId(), count, page);
                            }
                            else {
                                receivedRows.compute(next, (o, v) -> v == null ? p.getPositionCount() : v + p.getPositionCount());
                            }
                        }
                        next.addInput(page);
                        next.getOperatorContext().recordAddInput(operationTimer, page);
                        movedPage = true;
                    }
                }

                // if current operator is finished...
                if (current.isFinished()) {
                    // let next operator know there will be no more data
                    next.finish();
                    next.getOperatorContext().recordFinish(operationTimer);
                }
            }

            for (int index = activeOperators.size() - 1; index >= 0; index--) {
                Operator operator = activeOperators.get(index);
                if (operator.isFinished()) {
                    // close and remove this operator and all source operators
                    List<Operator> finishedOperators = this.activeOperators.subList(0, index + 1);
                    Throwable throwable = closeAndDestroyOperators(finishedOperators, false);
                    finishedOperators.clear();
                    if (throwable != null) {
                        throwIfUnchecked(throwable);
                        throw new RuntimeException(throwable);
                    }
                    // Finish the next operator, which is now the first operator.
                    if (!activeOperators.isEmpty()) {
                        Operator newRootOperator = activeOperators.get(0);
                        newRootOperator.finish();
                        newRootOperator.getOperatorContext().recordFinish(operationTimer);
                        if (isSnapshotEnabled) {
                            // These join "builder" operators will not finish until the probe side finishes.
                            // This prevents the driver from being marked as finished, so snapshot manager
                            // continues to expect snapshots from this driver.
                            // Report the driver as being complete to the snapshot manager.
                            Operator last = activeOperators.get(activeOperators.size() - 1);
                            if (last instanceof HashBuilderOperator || last instanceof SpatialIndexBuilderOperator || last instanceof NestedLoopBuildOperator) {
                                reportFinishedDriver();
                            }
                        }
                    }
                    break;
                }
            }

            // if we did not move any pages, check if we are blocked
            if (!movedPage) {
                List<Operator> blockedOperators = new ArrayList<>();
                List<ListenableFuture<?>> blockedFutures = new ArrayList<>();
                for (Operator operator : activeOperators) {
                    Optional<ListenableFuture<?>> blocked = getBlockedFuture(operator);
                    if (blocked.isPresent()) {
                        blockedOperators.add(operator);
                        blockedFutures.add(blocked.get());
                    }
                }

                if (!blockedFutures.isEmpty()) {
                    // unblock when the first future is complete
                    ListenableFuture<?> blocked = firstFinishedFuture(blockedFutures);
                    // driver records serial blocked time
                    driverContext.recordBlocked(blocked);
                    // each blocked operator is responsible for blocking the execution
                    // until one of the operators can continue
                    for (Operator operator : blockedOperators) {
                        operator.getOperatorContext().recordBlocked(blocked);
                    }
                    return blocked;
                }
            }

            return NOT_BLOCKED;
        }
        catch (Throwable t) {
            List<StackTraceElement> interrupterStack = exclusiveLock.getInterrupterStack();
            if (interrupterStack == null) {
                driverContext.failed(t);
                throw t;
            }

            // Driver thread was interrupted which should only happen if the task is already finished.
            // If this becomes the actual cause of a failed query there is a bug in the task state machine.
            Exception exception = new Exception("Interrupted By");
            exception.setStackTrace(interrupterStack.stream().toArray(StackTraceElement[]::new));
            PrestoException newException = new PrestoException(GENERIC_INTERNAL_ERROR, "Driver was interrupted", exception);
            newException.addSuppressed(t);
            driverContext.failed(newException);
            throw newException;
        }
    }

    private boolean isOperatorBlocked(Operator current, Operator next)
    {
        if (SystemSessionProperties.isNonBlockingSpillOrderby(driverContext.getSession())) {
            return !(current instanceof CommonTableExpressionOperator || next instanceof OrderByOperator) && getBlockedFuture(current).isPresent();
        }
        return !(current instanceof CommonTableExpressionOperator) && getBlockedFuture(current).isPresent();
    }

    @GuardedBy("exclusiveLock")
    private void handleMemoryRevoke()
    {
        for (int i = 0; i < activeOperators.size() && !driverContext.isDone(); i++) {
            Operator operator = activeOperators.get(i);

            if (revokingOperators.containsKey(operator)) {
                checkOperatorFinishedRevoking(operator);
            }
            else if (operator.getOperatorContext().isMemoryRevokingRequested()) {
                ListenableFuture<?> future = operator.startMemoryRevoke();
                revokingOperators.put(operator, future);
                checkOperatorFinishedRevoking(operator);
            }
        }
    }

    @GuardedBy("exclusiveLock")
    private void checkOperatorFinishedRevoking(Operator operator)
    {
        ListenableFuture<?> future = revokingOperators.get(operator);
        if (future.isDone()) {
            getFutureValue(future); // propagate exception if there was some
            revokingOperators.remove(operator);
            operator.finishMemoryRevoke();
            operator.getOperatorContext().resetMemoryRevokingRequested();
        }
    }

    @GuardedBy("exclusiveLock")
    private void destroyIfNecessary()
    {
        checkLockHeld("Lock must be held to call destroyIfNecessary");

        boolean toResume = state.compareAndSet(State.CANCEL_TO_RESUME, State.DESTROYED);
        if (!toResume) {
            if (!state.compareAndSet(State.NEED_DESTRUCTION, State.DESTROYED)) {
                return;
            }
        }

        // if we get an error while closing a driver, record it and we will throw it at the end
        Throwable inFlightException = null;
        try {
            inFlightException = closeAndDestroyOperators(activeOperators, toResume);
            if (driverContext.getMemoryUsage() > 0) {
                LOG.error("Driver still has memory reserved after freeing all operator memory.");
            }
            if (driverContext.getSystemMemoryUsage() > 0) {
                LOG.error("Driver still has system memory reserved after freeing all operator memory.");
            }
            if (driverContext.getRevocableMemoryUsage() > 0) {
                LOG.error("Driver still has revocable memory reserved after freeing all operator memory. Freeing it.");
            }
            driverContext.finished();
        }
        catch (Throwable t) {
            // this shouldn't happen but be safe
            inFlightException = addSuppressedException(
                    inFlightException,
                    t,
                    "Error destroying driver for task %s",
                    driverContext.getTaskId());
        }

        if (inFlightException != null) {
            // this will always be an Error or Runtime
            throwIfUnchecked(inFlightException);
            throw new RuntimeException(inFlightException);
        }
    }

    private Throwable closeAndDestroyOperators(List<Operator> operators, boolean toResume)
    {
        // record the current interrupted status (and clear the flag); we'll reset it later
        boolean wasInterrupted = Thread.interrupted();

        Throwable inFlightException = null;
        try {
            for (Operator operator : operators) {
                try {
                    if (toResume) {
                        if (LOG.isDebugEnabled()) {
                            long count = receivedRows.compute(operator, (o, v) -> v == null ? 0 : v);
                            LOG.debug("Operator %s received %d rows when the operator is cancelled to resume", operator.getOperatorContext().getUniqueId(), count);
                        }
                        // Snapshot: different ways to cancel operators. They may choose different strategies.
                        // e.g. for table-writer, normal cancel should remove any data that's been written,
                        // but cancel-to-resume should keep partial data, so it can be used after resume.
                        // TODO-cp-I2BZ0A: may revisit this
                        operator.cancelToResume();
                    }
                    else {
                        if (LOG.isDebugEnabled()) {
                            long count = receivedRows.compute(operator, (o, v) -> v == null ? 0 : v);
                            LOG.debug("Operator %s received %d rows when the operator finishes", operator.getOperatorContext().getUniqueId(), count);
                        }
                        operator.close();
                    }
                }
                catch (InterruptedException t) {
                    // don't record the stack
                    wasInterrupted = true;
                }
                catch (Throwable t) {
                    inFlightException = addSuppressedException(
                            inFlightException,
                            t,
                            "Error closing operator %s for task %s",
                            operator.getOperatorContext().getOperatorId(),
                            driverContext.getTaskId());
                }
                try {
                    if (operator instanceof WorkProcessorSourceOperatorAdapter) {
                        if (!((WorkProcessorSourceOperatorAdapter) operator).getStrategy().equals(REUSE_STRATEGY_PRODUCER)
                                || ((WorkProcessorSourceOperatorAdapter) operator).isNotSpilled()) {
                            operator.getOperatorContext().destroy();
                        }
                    }
                    else if (operator instanceof TableScanOperator) {
                        if (!((TableScanOperator) operator).getStrategy().equals(REUSE_STRATEGY_PRODUCER)
                                || ((TableScanOperator) operator).isNotSpilled()) {
                            operator.getOperatorContext().destroy();
                        }
                    }
                    else {
                        operator.getOperatorContext().destroy();
                    }
                }
                catch (Throwable t) {
                    inFlightException = addSuppressedException(
                            inFlightException,
                            t,
                            "Error freeing all allocated memory for operator %s for task %s",
                            operator.getOperatorContext().getOperatorId(),
                            driverContext.getTaskId());
                }
            }
        }
        finally {
            // reset the interrupted flag
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
        return inFlightException;
    }

    private Optional<ListenableFuture<?>> getBlockedFuture(Operator operator)
    {
        ListenableFuture<?> blocked = revokingOperators.get(operator);
        if (blocked != null) {
            // We mark operator as blocked regardless of blocked.isDone(), because finishMemoryRevoke has not been called yet.
            if (SystemSessionProperties.isNonBlockingSpillOrderby(driverContext.getSession()) && operator instanceof OrderByOperator) {
                return Optional.empty();
            }
            return Optional.of(blocked);
        }
        blocked = operator.isBlocked();
        if (!blocked.isDone()) {
            return Optional.of(blocked);
        }
        blocked = operator.getOperatorContext().isWaitingForMemory();
        if (!blocked.isDone()) {
            return Optional.of(blocked);
        }
        blocked = operator.getOperatorContext().isWaitingForRevocableMemory();
        if (!blocked.isDone()) {
            return Optional.of(blocked);
        }
        return Optional.empty();
    }

    private static Throwable addSuppressedException(Throwable inFlightException, Throwable newException, String message, Object... args)
    {
        Throwable inFlightExceptionNew = inFlightException;
        if (newException instanceof Error) {
            if (inFlightExceptionNew == null) {
                inFlightExceptionNew = newException;
            }
            else {
                // Self-suppression not permitted
                if (inFlightExceptionNew != newException) {
                    inFlightExceptionNew.addSuppressed(newException);
                }
            }
        }
        else {
            // log normal exceptions instead of rethrowing them
            LOG.error(newException, message, args);
        }
        return inFlightExceptionNew;
    }

    private synchronized void checkLockNotHeld(String message)
    {
        checkState(!exclusiveLock.isHeldByCurrentThread(), message);
    }

    @GuardedBy("exclusiveLock")
    private synchronized void checkLockHeld(String message)
    {
        checkState(exclusiveLock.isHeldByCurrentThread(), message);
    }

    private static ListenableFuture<?> firstFinishedFuture(List<ListenableFuture<?>> futures)
    {
        if (futures.size() == 1) {
            return futures.get(0);
        }

        SettableFuture<?> result = SettableFuture.create();

        for (ListenableFuture<?> future : futures) {
            future.addListener(() -> result.set(null), directExecutor());
        }

        return result;
    }

    // Note: task can not return null
    private <T> Optional<T> tryWithLock(Supplier<T> task)
    {
        return tryWithLock(0, TimeUnit.MILLISECONDS, task);
    }

    // Note: task can not return null
    private <T> Optional<T> tryWithLock(long timeout, TimeUnit unit, Supplier<T> task)
    {
        checkLockNotHeld("Lock can not be reacquired");

        boolean acquired = false;
        try {
            acquired = exclusiveLock.tryLock(timeout, unit);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (!acquired) {
            return Optional.empty();
        }

        Optional<T> result;
        try {
            result = Optional.of(task.get());
        }
        finally {
            try {
                try {
                    processNewSources();
                }
                finally {
                    destroyIfNecessary();
                }
            }
            finally {
                exclusiveLock.unlock();
            }
        }

        // If there are more source updates available, attempt to reacquire the lock and process them.
        // This can happen if new sources are added while we're holding the lock here doing work.
        // NOTE: this is separate duplicate code to make debugging lock reacquisition easier
        // The first condition is for processing the pending updates if this driver is still ALIVE
        // The second condition is to destroy the driver if the state is NEED_DESTRUCTION or CANCEL_TO_RESUME.
        while (((pendingTaskSourceUpdates.get() != null && state.get() == State.ALIVE)
                || state.get() == State.NEED_DESTRUCTION || state.get() == State.CANCEL_TO_RESUME)
                && exclusiveLock.tryLock()) {
            try {
                try {
                    processNewSources();
                }
                finally {
                    destroyIfNecessary();
                }
            }
            finally {
                exclusiveLock.unlock();
            }
        }

        return result;
    }

    private static class DriverLock
    {
        private final ReentrantLock lock = new ReentrantLock();

        @GuardedBy("this")
        private Thread currentOwner;

        @GuardedBy("this")
        private List<StackTraceElement> interrupterStack;

        public boolean isHeldByCurrentThread()
        {
            return lock.isHeldByCurrentThread();
        }

        public boolean tryLock()
        {
            checkState(!lock.isHeldByCurrentThread(), "Lock is not reentrant");
            boolean acquired = lock.tryLock();
            if (acquired) {
                setOwner();
            }
            return acquired;
        }

        public boolean tryLock(long timeout, TimeUnit unit)
                throws InterruptedException
        {
            checkState(!lock.isHeldByCurrentThread(), "Lock is not reentrant");
            boolean acquired = lock.tryLock(timeout, unit);
            if (acquired) {
                setOwner();
            }
            return acquired;
        }

        private synchronized void setOwner()
        {
            checkState(lock.isHeldByCurrentThread(), "Current thread does not hold lock");
            currentOwner = Thread.currentThread();
            // NOTE: We do not use interrupted stack information to know that another
            // thread has attempted to interrupt the driver, and interrupt this new lock
            // owner.  The interrupted stack information is for debugging purposes only.
            // In the case of interruption, the caller should (and does) have a separate
            // state to prevent further processing in the Driver.
        }

        public synchronized void unlock()
        {
            checkState(lock.isHeldByCurrentThread(), "Current thread does not hold lock");
            currentOwner = null;
            lock.unlock();
        }

        public synchronized List<StackTraceElement> getInterrupterStack()
        {
            return interrupterStack;
        }

        public synchronized void interruptCurrentOwner()
        {
            // there is a benign race condition here were the lock holder
            // can be change between attempting to get lock and grabbing
            // the synchronized lock here, but in either case we want to
            // interrupt the lock holder thread
            if (interrupterStack == null) {
                interrupterStack = ImmutableList.copyOf(Thread.currentThread().getStackTrace());
            }

            if (currentOwner != null) {
                currentOwner.interrupt();
            }
        }
    }
}
