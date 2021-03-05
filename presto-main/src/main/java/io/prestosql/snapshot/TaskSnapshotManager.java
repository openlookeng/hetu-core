/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.execution.TaskId;
import io.prestosql.operator.Operator;
import io.prestosql.operator.exchange.LocalMergeSourceOperator;

import javax.inject.Inject;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

/**
 * TaskSnapshotManager keeps track of snapshot status of task components
 */
public class TaskSnapshotManager
{
    private static final Logger LOG = Logger.get(TaskSnapshotManager.class);

    private final TaskId taskId;
    private final QuerySnapshotManager querySnapshotManager;

    private int totalComponents = -1;
    // LinkedHashMap can be used to keep ordering
    private final Map<Long, SnapshotComponentCounter<SnapshotStateId>> captureComponentCounters = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<Long, SnapshotResult> captureResults = new LinkedHashMap<>();
    private final Map<Long, SnapshotComponentCounter<SnapshotStateId>> restoreComponentCounters = Collections.synchronizedMap(new LinkedHashMap<>());
    private final RestoreResult restoreResult = new RestoreResult();

    @Inject
    public TaskSnapshotManager(TaskId taskId, QuerySnapshotManager querySnapshotManager)
    {
        this.taskId = taskId;
        this.querySnapshotManager = querySnapshotManager;
    }

    /**
     * Store the state of snapshotStateId in snapshot store
     */
    public void storeState(SnapshotStateId snapshotStateId, Object state)
            throws Exception
    {
        querySnapshotManager.storeState(snapshotStateId, state);
    }

    /**
     * Load the state of snapshotStateId from snapshot store
     *
     * @return state of snapshotStateId; Optional.empty() if the state doesn't exist
     */
    public Optional<Object> loadState(SnapshotStateId snapshotStateId)
            throws Exception
    {
        return querySnapshotManager.loadState(snapshotStateId);
    }

    public void storeFile(SnapshotStateId snapshotStateId, Path sourceFile)
            throws Exception
    {
        querySnapshotManager.storeFile(snapshotStateId, sourceFile);
    }

    public Boolean loadFile(SnapshotStateId snapshotStateId, Path targetFile)
            throws Exception
    {
        return querySnapshotManager.loadFile(snapshotStateId, targetFile);
    }

    public void setTotalComponents(int totalComponents)
    {
        this.totalComponents = totalComponents;
    }

    public void succeededToCapture(SnapshotStateId componentId)
    {
        updateCapture(componentId, SnapshotComponentCounter.ComponentState.SUCCESSFUL);
    }

    public void failedToCapture(SnapshotStateId componentId)
    {
        LOG.debug("Failed to capture snapshot %d for component %s", componentId.getSnapshotId(), componentId);
        updateCapture(componentId, SnapshotComponentCounter.ComponentState.FAILED);
    }

    public void succeededToRestore(SnapshotStateId componentId)
    {
        updateRestore(componentId, SnapshotComponentCounter.ComponentState.SUCCESSFUL);
    }

    public void failedToRestore(SnapshotStateId componentId, boolean fatal)
    {
        LOG.debug("Failed (fatal=%b) to restore snapshot %d for component %s", fatal, componentId.getSnapshotId(), componentId);
        if (fatal) {
            updateRestore(componentId, SnapshotComponentCounter.ComponentState.FAILED_FATAL);
        }
        else {
            updateRestore(componentId, SnapshotComponentCounter.ComponentState.FAILED);
        }
    }

    public Map<Long, SnapshotResult> getSnapshotCaptureResult()
    {
        if (totalComponents == 0) {
            // Special case: don't expect any more markers from this task.
            // It's as if this task has finished.
            // Use -1 to indicate "all snapshots".
            return ImmutableMap.of(-1L, SnapshotResult.SUCCESSFUL);
        }
        // Need to make a copy, otherwise there may be concurrent modification errors
        return ImmutableMap.copyOf(captureResults);
    }

    public RestoreResult getSnapshotRestoreResult()
    {
        return restoreResult;
    }

    private void updateCapture(SnapshotStateId componentId, SnapshotComponentCounter.ComponentState componentState)
    {
        TaskId taskId = componentId.getTaskId();
        checkState(totalComponents > 0);
        final long snapshotId = componentId.getSnapshotId();
        // update capturedSnapshotComponentCounterMap
        SnapshotComponentCounter<SnapshotStateId> counter =
                captureComponentCounters.computeIfAbsent(snapshotId, k -> new SnapshotComponentCounter<>(totalComponents));

        if (counter.updateComponent(componentId, componentState)) {
            // update capturedSnapshotResultMap
            SnapshotResult snapshotResult = counter.getSnapshotResult();
            SnapshotResult oldResult;
            synchronized (captureResults) {
                oldResult = captureResults.put(snapshotId, snapshotResult);
            }
            if (snapshotResult != oldResult && snapshotResult.isDone()) {
                if (querySnapshotManager.isCoordinator()) {
                    // Results on coordinator won't be reported through remote task. Send to the query side.
                    querySnapshotManager.updateQueryCapture(taskId, captureResults);
                }
                LOG.debug("Finished capturing snapshot %d for task %s. Result is %s.", snapshotId, taskId, snapshotResult);
            }
        }
    }

    private void updateRestore(SnapshotStateId componentId, SnapshotComponentCounter.ComponentState componentState)
    {
        TaskId taskId = componentId.getTaskId();
        checkState(totalComponents > 0);
        long snapshotId = componentId.getSnapshotId();
        // update restoredSnapshotComponentCounterMap
        SnapshotComponentCounter<SnapshotStateId> counter =
                restoreComponentCounters.computeIfAbsent(snapshotId, k -> new SnapshotComponentCounter<>(totalComponents));

        if (counter.updateComponent(componentId, componentState)) {
            SnapshotResult snapshotResult = counter.getSnapshotResult();
            boolean changed;
            synchronized (restoreResult) {
                changed = restoreResult.setSnapshotResult(snapshotId, snapshotResult);
            }
            if (changed && snapshotResult.isDone()) {
                if (querySnapshotManager.isCoordinator()) {
                    // Results on coordinator won't be reported through remote task. Send to the query side.
                    querySnapshotManager.updateQueryRestore(taskId, Optional.of(restoreResult));
                }
                LOG.debug("Finished restoring snapshot %d for task %s. Result is %s.", snapshotId, taskId.toString(), snapshotResult);
            }
        }
    }

    public void updateFinishedComponents(Collection<Operator> finishedOperators)
    {
        checkState(totalComponents > 0);

        synchronized (captureComponentCounters) {
            // Update ongoing snapshots
            for (Long snapshotId : captureComponentCounters.keySet()) {
                for (Operator operator : finishedOperators) {
                    SnapshotStateId operatorId = SnapshotStateId.forOperator(snapshotId, operator.getOperatorContext());
                    if (operator instanceof LocalMergeSourceOperator) {
                        // For local merge, MultiInputSnapshotState is associated with the local-exchange,
                        // so "updateCapture" needs to use the same operatorId as what's used by local-exchange,
                        // i.e. based on dthe plan node id
                        operatorId = SnapshotStateId.forTaskComponent(snapshotId, operator.getOperatorContext().getDriverContext().getPipelineContext().getTaskContext(), ((LocalMergeSourceOperator) operator).getPlanNodeId());
                    }
                    updateCapture(operatorId, SnapshotComponentCounter.ComponentState.SUCCESSFUL);
                }
            }

            // Updated expected total count for future snapshots
            totalComponents -= finishedOperators.size();
        }
    }

    @Override
    public String toString()
    {
        return String.format("%s, with total component %d", taskId, totalComponents);
    }
}
