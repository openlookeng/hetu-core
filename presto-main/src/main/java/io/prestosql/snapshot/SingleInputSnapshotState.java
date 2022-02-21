/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.snapshot;

import com.google.common.base.Stopwatch;
import io.airlift.log.Logger;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.snapshot.Restorable;

import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * This is a utility class used by non-source operators, which only receive inputs from a single source.
 * When an input is received from addInput(Page), the operator first calls the processPage() method to perform snapshot related processing.
 * When getOutput() is called, the operator calls the pollMarker() method to determine if a marker page needs to be returned.
 * Any instance of this class created through forOperator needs to be closed
 */
public class SingleInputSnapshotState
{
    private static final Logger LOG = Logger.get(SingleInputSnapshotState.class);

    private final Restorable restorable;
    private final String restorableId; // Only used in logs
    private final TaskSnapshotManager snapshotManager;
    private final PagesSerde pagesSerde;
    // For a given snapshot, generate a unique id, used to locate the saved state of this restorable object for that snapshot
    // For an operator, this is typically /query-id/snapshot-id/stage-id/task-id/pipeline-id/driver-id/operator-id
    private final Function<Long, SnapshotStateId> snapshotStateIdGenerator;
    // For a given snapshot, generate a unique id, used to locate the folder to store spilled files of this restorable object for that snapshot
    // For an operator, this is typically /query-id/snapshot-id/stage-id/task-id/pipeline-id/driver-id/operator-id-spill/
    private final Function<Long, SnapshotStateId> spillStateIdGenerator;
    // Markers to be returned to the restorable object. The "nextMarker" method polls this list.
    private final Queue<MarkerPage> markers = new LinkedList<>();
    // For recording snapshot memory usage
    private final LocalMemoryContext snapshotMemoryContext;

    public static SingleInputSnapshotState forOperator(Operator operator, OperatorContext operatorContext)
    {
        return new SingleInputSnapshotState(
                operator,
                operatorContext.getDriverContext().getPipelineContext().getTaskContext().getSnapshotManager(),
                operatorContext.getDriverContext().getSerde(),
                snapshotId -> SnapshotStateId.forOperator(snapshotId, operatorContext),
                snapshotId -> SnapshotStateId.forDriverComponent(snapshotId, operatorContext, operatorContext.getOperatorId() + "-spill"),
                operatorContext.newLocalUserMemoryContext(SingleInputSnapshotState.class.getSimpleName()));
    }

    SingleInputSnapshotState(Restorable restorable,
                             TaskSnapshotManager snapshotManager,
                             PagesSerde pagesSerde,
                             Function<Long, SnapshotStateId> snapshotStateIdGenerator,
                             Function<Long, SnapshotStateId> spillStateIdGenerator,
                             LocalMemoryContext snapshotMemoryContext)
    {
        this.restorable = requireNonNull(restorable, "restorable is null");
        this.restorableId = String.format("%s (%s)", restorable.getClass().getSimpleName(), snapshotStateIdGenerator.apply(0L).getId());
        this.snapshotManager = requireNonNull(snapshotManager, "snapshotManager is null");
        this.snapshotStateIdGenerator = requireNonNull(snapshotStateIdGenerator, "snapshotStateIdGenerator is null");
        this.spillStateIdGenerator = requireNonNull(spillStateIdGenerator, "spillStateIdGenerator is null");
        this.pagesSerde = pagesSerde;
        this.snapshotMemoryContext = snapshotMemoryContext;
    }

    public void close()
    {
        snapshotMemoryContext.close();
    }

    /**
     * Perform marker and snapshot related processing on an incoming input
     *
     * @param input the input, either a Page or a Split
     * @return true if the input has been processed, false otherwise (so operator needs to process it as a regular input)
     */
    public boolean processPage(Page input)
    {
        if (!(input instanceof MarkerPage)) {
            return false;
        }

        MarkerPage marker = (MarkerPage) input;
        long snapshotId = marker.getSnapshotId();
        SnapshotStateId componentId = snapshotStateIdGenerator.apply(snapshotId);
        if (marker.isResuming()) {
            try {
                Optional<Object> state;
                if (restorable.supportsConsolidatedWrites()) {
                    state = snapshotManager.loadConsolidatedState(componentId);
                }
                else {
                    state = snapshotManager.loadState(componentId);
                }
                if (!state.isPresent()) {
                    snapshotManager.failedToRestore(componentId, true);
                    LOG.warn("Can't locate saved state for snapshot %d, component %s", snapshotId, restorableId);
                }
                else if (state.get() == TaskSnapshotManager.NO_STATE) {
                    snapshotManager.failedToRestore(componentId, true);
                    LOG.error("BUG! State of component %s has never been stored successfully before snapshot %d", restorableId, snapshotId);
                }
                else {
                    Stopwatch timer = Stopwatch.createStarted();
                    restorable.restore(state.get(), pagesSerde);
                    timer.stop();
                    boolean successful = true;
                    if (restorable instanceof Spillable && ((Spillable) restorable).isSpilled()) {
                        Boolean result = loadSpilledFiles(snapshotId, (Spillable) restorable);
                        if (result == null) {
                            snapshotManager.failedToRestore(componentId, true);
                            LOG.error("BUG! Spilled file of component %s has never been stored successfully before snapshot %d", restorableId, snapshotId);
                            successful = false;
                        }
                        else if (!result) {
                            snapshotManager.failedToRestore(componentId, true);
                            LOG.warn("Can't locate spilled file for snapshot %d, component %s", snapshotId, restorableId);
                            successful = false;
                        }
                    }
                    if (successful) {
                        LOG.debug("Successfully restored state to snapshot %d for %s", snapshotId, restorableId);
                        snapshotManager.succeededToRestore(componentId, timer.elapsed(TimeUnit.MILLISECONDS));
                    }
                }
                // Previous pending snapshots no longer need to be carried out
                markers.clear();
            }
            catch (Exception e) {
                LOG.warn(e, "Failed to restore snapshot state for %s: %s", componentId, e.getMessage());
                snapshotManager.failedToRestore(componentId, false);
            }
        }
        else {
            captureState(snapshotId, true);
        }
        markers.add(marker);
        return true;
    }

    // Exposed only to be used by HashBuilderOperator
    public void captureExtraState(long snapshotId)
    {
        captureState(snapshotId, false);
    }

    private void captureState(long snapshotId, boolean record)
    {
        SnapshotStateId componentId = snapshotStateIdGenerator.apply(snapshotId);
        long stateMemory = restorable.getUsedMemory();
        if (!snapshotMemoryContext.trySetBytes(stateMemory)) {
            LOG.warn("Insufficient memory on worker node to take snapshot");
            snapshotManager.failedToCapture(componentId);
            return;
        }
        try {
            storeState(componentId);
            if (restorable instanceof Spillable && ((Spillable) restorable).isSpilled()) {
                storeSpilledFiles(snapshotId, (Spillable) restorable);
            }
            if (record) {
                snapshotManager.succeededToCapture(componentId);
                LOG.debug("Successfully saved state to snapshot %d for %s", snapshotId, restorableId);
            }
            else {
                LOG.debug("Successfully saved EXTRA state to snapshot %d for %s", snapshotId, restorableId);
            }
        }
        catch (Exception e) {
            LOG.warn(e, "Failed to capture and store snapshot state");
            snapshotManager.failedToCapture(componentId);
        }
        finally {
            snapshotMemoryContext.setBytes(0);
        }
    }

    private void storeState(SnapshotStateId componentId)
            throws Exception
    {
        Stopwatch timer = Stopwatch.createStarted();
        Object state = restorable.capture(pagesSerde);
        timer.stop();
        long serTime = timer.elapsed(TimeUnit.MILLISECONDS);

        if (restorable.supportsConsolidatedWrites()) {
            snapshotManager.storeConsolidatedState(componentId, state, serTime);
        }
        else {
            snapshotManager.storeState(componentId, state, serTime);
        }
    }

    public boolean hasMarker()
    {
        return !markers.isEmpty();
    }

    /**
     * Retrieves the next marker page if available
     *
     * @return next marker page if available, null otherwise
     */
    public MarkerPage nextMarker()
    {
        MarkerPage marker = markers.poll();
        if (marker != null) {
            LOG.debug("Sending marker '%s' to target '%s'", marker.toString(), restorableId);
        }
        return marker;
    }

    private void storeSpilledFiles(long snapshotId, Spillable spillable)
            throws Exception
    {
        List<Path> filePaths = spillable.getSpilledFilePaths();
        SnapshotStateId spillId = spillStateIdGenerator.apply(snapshotId);
        for (Path path : filePaths) {
            snapshotManager.storeFile(spillId, path);
        }
    }

    // true: loaded
    // false: failed to load due to incomplete snapshot
    // null: bug, failed to load due to missing file
    private Boolean loadSpilledFiles(long snapshotId, Spillable spillable)
            throws Exception
    {
        List<Path> filePaths = spillable.getSpilledFilePaths();
        SnapshotStateId spillId = spillStateIdGenerator.apply(snapshotId);
        for (Path path : filePaths) {
            Boolean result = snapshotManager.loadFile(spillId, path);
            if (result == null || !result) {
                return result;
            }
        }
        return true;
    }
}
