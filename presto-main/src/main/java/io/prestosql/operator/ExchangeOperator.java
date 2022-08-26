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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.hetu.core.transport.execution.buffer.PagesSerdeUtil;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.exchange.ExchangeManagerRegistry;
import io.prestosql.execution.TaskFailureListener;
import io.prestosql.execution.TaskId;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.metadata.Split;
import io.prestosql.snapshot.MultiInputRestorable;
import io.prestosql.snapshot.MultiInputSnapshotState;
import io.prestosql.snapshot.QueryRecoveryManager;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.exchange.ExchangeId;
import io.prestosql.spi.exchange.ExchangeManager;
import io.prestosql.spi.exchange.ExchangeSource;
import io.prestosql.spi.exchange.ExchangeSourceHandle;
import io.prestosql.spi.exchange.RetryPolicy;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.split.RemoteSplit;
import io.prestosql.split.RemoteSplit.DirectExchangeInput;
import io.prestosql.split.RemoteSplit.SpoolingExchangeInput;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Closeable;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"sourceId", "exchangeDataSource", "snapshotState", "blockedOnSplits", "inputChannels"})
public class ExchangeOperator
        implements SourceOperator, MultiInputRestorable, Closeable
{
    public static final CatalogName REMOTE_CONNECTOR_ID = new CatalogName("$remote");
    private static final Logger log = Logger.get(ExchangeOperator.class);

    public static class ExchangeOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final ExchangeClientSupplier exchangeClientSupplier;
        private boolean closed;
        private final RetryPolicy retryPolicy;
        private final ExchangeManagerRegistry exchangeManagerRegistry;
        private ExchangeDataSource exchangeDataSource;

        public ExchangeOperatorFactory(
                int operatorId,
                PlanNodeId sourceId,
                ExchangeClientSupplier exchangeClientSupplier,
                RetryPolicy retryPolicy,
                ExchangeManagerRegistry exchangeManagerRegistry)
        {
            this.operatorId = operatorId;
            this.sourceId = sourceId;
            this.exchangeClientSupplier = exchangeClientSupplier;
            this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy is null");
            this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            TaskContext taskContext = driverContext.getPipelineContext().getTaskContext();
            LocalMemoryContext memoryContext = driverContext.getPipelineContext().localSystemMemoryContext();
            OperatorContext addOperatorContext = driverContext.addOperatorContext(operatorId, sourceId, ExchangeOperator.class.getSimpleName());
            String uniqueId = addOperatorContext.getUniqueId();
            if (exchangeDataSource == null) {
                // The decision of what exchange to use (streaming vs external) is currently made at the scheduling phase. It is more convenient to deliver it as part of a RemoteSplit.
                // Postponing this decision until scheduling allows to dynamically change the exchange type as part of an adaptive query re-planning.
                // LazyExchangeDataSource allows to choose an exchange source implementation based on the information received from a split.
                exchangeDataSource = new LazyExchangeDataSource(
                        taskContext.getTaskId(),
                        sourceId,
                        exchangeClientSupplier,
                        memoryContext,
                        taskContext::sourceTaskFailed,
                        addOperatorContext.getRetryPolicy(),
                        exchangeManagerRegistry,
                        uniqueId,
                        addOperatorContext.isRecoveryEnabled(),
                        driverContext.getPipelineContext().getTaskContext().getRecoveryManager());
                // if recovery is enabled exchange client is required at the time of exchange operator creation to add Targets.
                // So if recovery is enabled DirectExchangeDataSource is set as dataSource.
                if (addOperatorContext.isRecoveryEnabled()) {
                    ExchangeClient exchangeClient = exchangeClientSupplier.get(memoryContext, taskContext::sourceTaskFailed, addOperatorContext.getRetryPolicy(), null, taskContext.getTaskId().getQueryId());
                    exchangeClient.setRecoveryEnabled(driverContext.getPipelineContext().getTaskContext().getRecoveryManager());
                    exchangeClient.addTarget(uniqueId);
                    exchangeDataSource.setDataSource(new DirectExchangeDataSource(exchangeClient));
                }
            }
            else {
                exchangeDataSource.addTarget(uniqueId);
            }
            return new ExchangeOperator(
                    uniqueId,
                    addOperatorContext,
                    sourceId,
                    exchangeDataSource);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
            if (exchangeDataSource != null) {
                LazyExchangeDataSource lazyExchangeDataSource = (LazyExchangeDataSource) exchangeDataSource;
                lazyExchangeDataSource.noMoreTargets();
            }
        }
    }

    private final String id;
    private final OperatorContext operatorContext;
    private final PlanNodeId sourceId;
    private final ExchangeDataSource exchangeDataSource;

    private final MultiInputSnapshotState snapshotState;
    private Optional<Set<String>> inputChannels = Optional.empty();

    private final SettableFuture<Void> blockedOnSplits = SettableFuture.create();

    public ExchangeOperator(
            String id,
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            ExchangeDataSource exchangeDataSource)
    {
        this.id = requireNonNull(id, "id is null");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.exchangeDataSource = requireNonNull(exchangeDataSource, "exchangeDataSource is null");
        this.snapshotState = operatorContext.isSnapshotEnabled()
                ? MultiInputSnapshotState.forOperator(this, operatorContext)
                : null;
        operatorContext.setInfoSupplier(exchangeDataSource::getInfo);
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        requireNonNull(split, "split is null");
        checkArgument(split.getCatalogName().equals(REMOTE_CONNECTOR_ID), "split is not a remote split");

        if (snapshotState != null) {
            LazyExchangeDataSource lazyExchangeDataSource = (LazyExchangeDataSource) exchangeDataSource;
            lazyExchangeDataSource.setSnapshotState(snapshotState, inputChannels);
        }
        exchangeDataSource.addSplit((RemoteSplit) split.getConnectorSplit());

        return Optional::empty;
    }

    @Override
    public void noMoreSplits()
    {
        if (exchangeDataSource instanceof LazyExchangeDataSource) {
            LazyExchangeDataSource lazyExchangeDataSource = (LazyExchangeDataSource) exchangeDataSource;
            lazyExchangeDataSource.noMoreLocations();
        }
        blockedOnSplits.set(null);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        close();
    }

    @Override
    public boolean isFinished()
    {
        if (snapshotState != null && snapshotState.hasPendingDataPages()) {
            // Snapshot: there are pending restored pages. Need to send them out before finishing this operator.
            return false;
        }
        return exchangeDataSource.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (snapshotState != null) {
            if (!blockedOnSplits.isDone()) {
                // Snapshot: wait for all source tasks to be added, so we have the complete list of input channels when markers are received
                return blockedOnSplits;
            }
            if (snapshotState.hasPendingDataPages()) {
                // Snapshot: there are pending restored pages.
                return Futures.immediateFuture(true);
            }
        }

        ListenableFuture<?> blocked = exchangeDataSource.isBlocked();
        if (blocked.isDone()) {
            return NOT_BLOCKED;
        }
        return blocked;
    }

    @Override
    public Page getOutput()
    {
        SerializedPage page;
        if (snapshotState != null) {
            page = snapshotState.processSerializedPage(() -> exchangeDataSource.pollPage(id)).orElse(null);
        }
        else {
            // origin not needed in this case
            page = exchangeDataSource.pollPage(id).getLeft();
        }
        if (page == null) {
            return null;
        }

        operatorContext.recordNetworkInput(page.getSizeInBytes(), page.getPositionCount());

        Page deserializedPage = operatorContext.getDriverContext().getSerde().deserialize(page);
        operatorContext.recordProcessedInput(deserializedPage.getSizeInBytes(), page.getPositionCount());

        return deserializedPage;
    }

    @Override
    public Page pollMarker()
    {
        return snapshotState.nextSerializedMarker(() -> exchangeDataSource.pollPage(id)).map(serializedPage -> serializedPage.toMarker()).orElse(null);
    }

    @Override
    public void close()
    {
        exchangeDataSource.close();
    }

    @Override
    public Optional<Set<String>> getInputChannels()
    {
        if (inputChannels.isPresent()) {
            return inputChannels;
        }

        LazyExchangeDataSource lazyExchangeDataSource = (LazyExchangeDataSource) exchangeDataSource;
        // Exchange Operator is blocked until noMoreSplits is set, so we can safely get all clients from exchangeClient.
        Set<String> channels = lazyExchangeDataSource.getAllClients();
        if (channels != null) {
            inputChannels = Optional.of(channels);
        }
        else {
            inputChannels = Optional.empty();
        }
        return inputChannels;
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

    private interface ExchangeDataSource
            extends Closeable
    {
        Pair<SerializedPage, String> pollPage(String target);

        boolean isFinished();

        ListenableFuture<?> isBlocked();

        void addSplit(RemoteSplit split);

        void noMoreSplits();

        default void setDataSource(ExchangeDataSource dataSource)
        {
        }

        default void addTarget(String uniqueId)
        {
        }

        OperatorInfo getInfo();

        @Override
        void close();
    }

    private static class LazyExchangeDataSource
            implements ExchangeDataSource
    {
        private final TaskId taskId;
        private final PlanNodeId sourceId;
        private final ExchangeClientSupplier exchangeClientSupplier;
        private final LocalMemoryContext systemMemoryContext;
        private final TaskFailureListener taskFailureListener;
        private final RetryPolicy retryPolicy;
        private final ExchangeManagerRegistry exchangeManagerRegistry;
        private final boolean recoveryEnabled;
        private final QueryRecoveryManager queryRecoveryManager;

        private final SettableFuture<Void> initializationFuture = SettableFuture.create();
        private final AtomicReference<ExchangeDataSource> delegate = new AtomicReference<>();
        private final AtomicBoolean closed = new AtomicBoolean();
        private final String uniqueId;

        private LazyExchangeDataSource(
                TaskId taskId,
                PlanNodeId sourceId,
                ExchangeClientSupplier exchangeClientSupplier,
                LocalMemoryContext systemMemoryContext,
                TaskFailureListener taskFailureListener,
                RetryPolicy retryPolicy,
                ExchangeManagerRegistry exchangeManagerRegistry,
                String uniqueId,
                boolean recoveryEnabled,
                QueryRecoveryManager queryRecoveryManager)
        {
            this.taskId = requireNonNull(taskId, "taskId is null");
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "directExchangeClientSupplier is null");
            this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
            this.taskFailureListener = requireNonNull(taskFailureListener, "taskFailureListener is null");
            this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy is null");
            this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
            this.queryRecoveryManager = queryRecoveryManager;
            this.uniqueId = uniqueId;
            this.recoveryEnabled = recoveryEnabled;
        }

        @Override
        public Pair<SerializedPage, String> pollPage(String target)
        {
            ExchangeDataSource dataSource = delegate.get();
            if (dataSource == null) {
                return null;
            }
            return dataSource.pollPage(target);
        }

        @Override
        public boolean isFinished()
        {
            if (closed.get()) {
                return true;
            }
            ExchangeDataSource dataSource = delegate.get();
            if (dataSource == null) {
                return false;
            }
            return dataSource.isFinished();
        }

        @Override
        public ListenableFuture<?> isBlocked()
        {
            if (closed.get()) {
                return Futures.immediateFuture(null);
            }
            if (!initializationFuture.isDone()) {
                return initializationFuture;
            }
            ExchangeDataSource dataSource = delegate.get();
            checkState(dataSource != null, "dataSource is expected to be initialized");
            return dataSource.isBlocked();
        }

        @Override
        public void addSplit(RemoteSplit split)
        {
            boolean initialized = false;
            synchronized (this) {
                if (closed.get()) {
                    return;
                }
                ExchangeDataSource dataSource = delegate.get();
                if (dataSource == null) {
                    if (split.getExchangeInput() instanceof DirectExchangeInput) {
                        ExchangeClient exchangeClient = exchangeClientSupplier.get(systemMemoryContext, taskFailureListener, retryPolicy, new ExchangeId(format("direct-exchange-%s-%s", this.taskId.getStageId().getId(), sourceId)), this.taskId.getQueryId());
                        if (recoveryEnabled) {
                            exchangeClient.setRecoveryEnabled(queryRecoveryManager);
                        }
                        exchangeClient.addTarget(uniqueId);
                        dataSource = new DirectExchangeDataSource(exchangeClient);
                    }
                    else if (split.getExchangeInput() instanceof RemoteSplit.SpoolingExchangeInput) {
                        RemoteSplit.SpoolingExchangeInput input = (RemoteSplit.SpoolingExchangeInput) split.getExchangeInput();
                        ExchangeManager exchangeManager = exchangeManagerRegistry.getExchangeManager();
                        List<ExchangeSourceHandle> sourceHandles = input.getExchangeSourceHandles();
                        ExchangeSource exchangeSource = exchangeManager.createSource(sourceHandles);
                        dataSource = new SpoolingExchangeDataSource(exchangeSource, sourceHandles, systemMemoryContext);
                    }
                    else {
                        throw new IllegalArgumentException("Unexpected split: " + split);
                    }
                    delegate.set(dataSource);
                    initialized = true;
                }
                else if (recoveryEnabled && !initializationFuture.isDone()) {
                    initialized = true;
                }
                dataSource.addSplit(split);
            }

            if (initialized) {
                initializationFuture.set(null);
            }
        }

        @Override
        public void addTarget(String uniqueId)
        {
            ExchangeDataSource dataSource = delegate.get();
            if (dataSource != null) {
                dataSource.addTarget(uniqueId);
            }
        }

        @Override
        public void setDataSource(ExchangeDataSource dataSource)
        {
            delegate.set(dataSource);
        }

        @Override
        public synchronized void noMoreSplits()
        {
            if (closed.get()) {
                return;
            }
            ExchangeDataSource dataSource = delegate.get();
            if (dataSource != null) {
                dataSource.noMoreSplits();
            }
            else {
                // to unblock when no splits are provided (and delegate hasn't been created)
                close();
            }
        }

        @Override
        public OperatorInfo getInfo()
        {
            ExchangeDataSource dataSource = delegate.get();
            if (dataSource == null) {
                return null;
            }
            return dataSource.getInfo();
        }

        @Override
        public void close()
        {
            synchronized (this) {
                if (!closed.compareAndSet(false, true)) {
                    return;
                }
                ExchangeDataSource dataSource = delegate.get();
                if (dataSource != null) {
                    dataSource.close();
                }
            }
            initializationFuture.set(null);
        }

        public Set<String> getAllClients()
        {
            ExchangeDataSource dataSource = delegate.get();
            if (dataSource instanceof DirectExchangeDataSource) {
                DirectExchangeDataSource directExchangeDataSource = (DirectExchangeDataSource) dataSource;
                return directExchangeDataSource.getAllClients();
            }
            else {
                return Collections.emptySet();
            }
        }

        public void setSnapshotState(MultiInputSnapshotState snapshotState, Optional<Set<String>> inputChannels)
        {
            ExchangeDataSource dataSource = delegate.get();
            if (dataSource instanceof DirectExchangeDataSource) {
                DirectExchangeDataSource directExchangeDataSource = (DirectExchangeDataSource) dataSource;
                directExchangeDataSource.setSnapshotState(snapshotState, inputChannels);
            }
        }

        public void noMoreLocations()
        {
            ExchangeDataSource dataSource = delegate.get();
            if (dataSource instanceof DirectExchangeDataSource) {
                DirectExchangeDataSource directExchangeDataSource = (DirectExchangeDataSource) dataSource;
                directExchangeDataSource.noMoreLocations();
            }
        }

        public void noMoreTargets()
        {
            ExchangeDataSource dataSource = delegate.get();
            if (dataSource instanceof DirectExchangeDataSource) {
                DirectExchangeDataSource directExchangeDataSource = (DirectExchangeDataSource) dataSource;
                directExchangeDataSource.noMoreTargets();
            }
        }
    }

    private static class DirectExchangeDataSource
            implements ExchangeDataSource
    {
        private final ExchangeClient exchangeClient;
        private MultiInputSnapshotState snapshotState;
        private Optional<Set<String>> inputChannels = Optional.empty();

        private DirectExchangeDataSource(ExchangeClient exchangeClient)
        {
            this.exchangeClient = requireNonNull(exchangeClient, "directExchangeClient is null");
        }

        @Override
        public Pair<SerializedPage, String> pollPage(String target)
        {
            return exchangeClient.pollPage(target);
        }

        @Override
        public boolean isFinished()
        {
            return exchangeClient.isFinished();
        }

        @Override
        public ListenableFuture<?> isBlocked()
        {
            return exchangeClient.isBlocked();
        }

        @Override
        public void addTarget(String uniqueId)
        {
            exchangeClient.addTarget(uniqueId);
        }

        @Override
        public void addSplit(RemoteSplit split)
        {
            DirectExchangeInput exchangeInput = (DirectExchangeInput) split.getExchangeInput();
            URI location = split.getLocation();
            String instanceId = split.getInstanceId();
            TaskId remoteTaskId = exchangeInput.getTaskId();
            boolean added = exchangeClient.addLocation(remoteTaskId, new TaskLocation(location, instanceId));
            if (snapshotState != null) {
                // When inputChannels is not empty, then we should have received all locations
                checkState(!inputChannels.isPresent() || !added);
            }
        }

        @Override
        public void noMoreSplits()
        {
            exchangeClient.noMoreLocations();
        }

        @Override
        public OperatorInfo getInfo()
        {
            return exchangeClient.getStatus();
        }

        public Set<String> getAllClients()
        {
            return exchangeClient.getAllClients();
        }

        @Override
        public void close()
        {
            exchangeClient.close();
        }

        public void setSnapshotState(MultiInputSnapshotState snapshotState, Optional<Set<String>> inputChannels)
        {
            this.snapshotState = snapshotState;
            this.inputChannels = inputChannels;
        }

        public void noMoreLocations()
        {
            exchangeClient.noMoreLocations();
        }

        public void noMoreTargets()
        {
            exchangeClient.noMoreTargets();
        }
    }

    private static class SpoolingExchangeDataSource
            implements ExchangeDataSource
    {
        // This field is not final to allow releasing the memory retained by the ExchangeSource instance.
        // It is modified (assigned to null) when the ExchangeOperator is closed.
        // It doesn't have to be declared as volatile as the nullification of this variable doesn't have to be immediately visible to other threads.
        // However since close can be called at any moment this variable has to be accessed in a safe way (avoiding "check-then-use").
        private ExchangeSource exchangeSource;
        private final List<ExchangeSourceHandle> exchangeSourceHandles;
        private final LocalMemoryContext systemMemoryContext;
        private volatile boolean closed;

        private SpoolingExchangeDataSource(
                ExchangeSource exchangeSource,
                List<ExchangeSourceHandle> exchangeSourceHandles,
                LocalMemoryContext systemMemoryContext)
        {
            // this assignment is expected to be followed by an assignment of a final field to ensure safe publication
            this.exchangeSource = requireNonNull(exchangeSource, "exchangeSource is null");
            this.exchangeSourceHandles = ImmutableList.copyOf(requireNonNull(exchangeSourceHandles, "exchangeSourceHandles is null"));
            this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        }

        @Override
        public Pair<SerializedPage, String> pollPage(String target)
        {
            ExchangeSource spoolExchangeSource = this.exchangeSource;
            if (spoolExchangeSource == null) {
                return null;
            }

            Slice slice = spoolExchangeSource.read();
            SerializedPage page = slice != null ? PagesSerdeUtil.readSerializedPage(slice) : null;
            if (page != null && page.isExchangeMarkerPage()) {
                return pollPage(target);
            }
            systemMemoryContext.setBytes(spoolExchangeSource.getMemoryUsage());

            // If the data source has been closed in a meantime reset memory usage back to 0
            if (closed) {
                systemMemoryContext.setBytes(0);
            }
            return Pair.of(page, null);
        }

        @Override
        public boolean isFinished()
        {
            ExchangeSource spoolExchangeSource = this.exchangeSource;
            if (spoolExchangeSource == null) {
                return true;
            }
            return spoolExchangeSource.isFinished();
        }

        @Override
        public ListenableFuture<?> isBlocked()
        {
            ExchangeSource spoolExchangeSource = this.exchangeSource;
            if (spoolExchangeSource == null) {
                return Futures.immediateFuture(null);
            }
            return toListenableFuture(spoolExchangeSource.isBlocked());
        }

        @Override
        public void addSplit(RemoteSplit split)
        {
            SpoolingExchangeInput exchangeInput = (SpoolingExchangeInput) split.getExchangeInput();
            // Only a single split is expected when external exchange is used.
            // The engine adds the same split to every instance of the ExchangeOperator.
            // Since the ExchangeDataSource is shared between ExchangeOperator instances
            // the same split may be delivered multiple times.
            checkState(
                    exchangeInput.getExchangeSourceHandles().equals(exchangeSourceHandles),
                    "split is expected to contain an identical exchangeSourceHandles list: %s != %s",
                    exchangeInput.getExchangeSourceHandles(),
                    exchangeSourceHandles);
        }

        @Override
        public void noMoreSplits()
        {
            // Only a single split is expected when external exchange is used.
            // Thus the assumption of "noMoreSplit" is made on construction.
        }

        @Override
        public OperatorInfo getInfo()
        {
            return null;
        }

        @Override
        public synchronized void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            try {
                exchangeSource.close();
            }
            catch (RuntimeException e) {
                log.warn(e, "error closing exchange source");
            }
            finally {
                exchangeSource = null;
                systemMemoryContext.setBytes(0);
            }
        }
    }
}
