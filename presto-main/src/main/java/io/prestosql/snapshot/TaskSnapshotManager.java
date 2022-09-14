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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.execution.TaskId;
import io.prestosql.operator.Operator;
import io.prestosql.operator.exchange.LocalMergeSourceOperator;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * TaskSnapshotManager keeps track of snapshot status of task components
 */
public class TaskSnapshotManager
        implements SnapshotDataCollector
{
    private static final Logger LOG = Logger.get(TaskSnapshotManager.class);
    public static final Object NO_STATE = new Object();
    private static final String CONSOLIDATED_STATE_COMPONENT = "ConsolidatedState";

    private final TaskId taskId;
    private final long resumeCount;
    private final RecoveryUtils recoveryUtils;

    private int totalComponents = -1;
    // LinkedHashMap can be used to keep ordering
    private final Map<Long, SnapshotComponentCounter<SnapshotStateId>> captureComponentCounters = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<Long, SnapshotInfo> captureResults = new LinkedHashMap<>();
    private final Map<Long, SnapshotComponentCounter<SnapshotStateId>> restoreComponentCounters = Collections.synchronizedMap(new LinkedHashMap<>());
    private final RestoreResult restoreResult = new RestoreResult();

    // We cannot use Maps.newConcurrentMap() since that doesn't allow null values
    // For simple operators, storeCache stores the actual values. For complex, it stores key as value
    // for each entry that is saved.
    private final Map<Long, Map<String, Object>> storeCache = Collections.synchronizedMap(new HashMap<>());
    private final Map<Long, Map<String, Object>> loadCache = Collections.synchronizedMap(new HashMap<>());

    private Set<String> createdConsolidatedFiles;

    public TaskSnapshotManager(TaskId taskId, long resumeCount, RecoveryUtils recoveryUtils)
    {
        this.taskId = taskId;
        this.resumeCount = resumeCount;
        this.recoveryUtils = recoveryUtils;
    }

    public long getResumeCount()
    {
        return resumeCount;
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    public QuerySnapshotManager getQuerySnapshotManager()
    {
        return recoveryUtils.getQuerySnapshotManager(taskId.getQueryId());
    }

    public static SnapshotStateId createConsolidatedId(long snapshotId, TaskId taskId)
    {
        return SnapshotStateId.forTaskComponent(snapshotId, taskId, CONSOLIDATED_STATE_COMPONENT);
    }

    public void storeConsolidatedState(SnapshotStateId snapshotStateId, Object state, long serCpuTime)
    {
        Map<String, Object> map = storeCache.computeIfAbsent(snapshotStateId.getSnapshotId(), (x) -> Collections.synchronizedMap(new HashMap<>()));
        map.put(snapshotStateId.toString(), state);
        updateSnapshotCaptureCpuTime(snapshotStateId.getSnapshotId(), serCpuTime);
    }

    /**
     * Store the state of snapshotStateId in snapshot store
     */
    public void storeState(SnapshotStateId snapshotStateId, Object state, long serCpuTime)
            throws Exception
    {
        recoveryUtils.storeState(snapshotStateId, state, this);

        // store dummy value
        Map<String, Object> map = storeCache.computeIfAbsent(snapshotStateId.getSnapshotId(), (x) -> Collections.synchronizedMap(new HashMap<>()));
        map.put(snapshotStateId.toString(), snapshotStateId.toString());
        updateSnapshotCaptureCpuTime(snapshotStateId.getSnapshotId(), serCpuTime);
    }

    private void loadMapIfNecessary(long snapshotId, TaskId taskId)
            throws Exception
    {
        if (!loadCache.containsKey(snapshotId)) {
            synchronized (loadCache) {
                // double-check to make sure only 1 thread attempts load
                if (!loadCache.containsKey(snapshotId)) {
                    String queryId = taskId.getQueryId().getId();
                    SnapshotStateId stateId = createConsolidatedId(snapshotId, taskId);
                    Optional<Object> loadedState = recoveryUtils.loadState(stateId, this);
                    if (createdConsolidatedFiles == null) {
                        createdConsolidatedFiles = recoveryUtils.loadConsolidatedFiles(queryId);
                    }
                    // if it is still null after loading, that means it is deleted, and we need to fail
                    if (createdConsolidatedFiles == null || (createdConsolidatedFiles.contains(stateId.toString()) && !loadedState.isPresent())) {
                        // we created the consolidated file, but it has been deleted. non-recoverable failure
                        failedToRestore(stateId, true);
                        // continue so that the failure can be detected
                    }
                    Object map = loadedState.orElse(Collections.emptyMap());
                    loadCache.put(snapshotId, (Map<String, Object>) map);
                }
            }
        }
    }

    private Optional<Object> loadWithBacktrack(SnapshotStateId snapshotStateId)
            throws Exception
    {
        SnapshotStateId newSnapshotStateId = snapshotStateId;
        TaskId snapshotStateIdTaskId = newSnapshotStateId.getTaskId();
        long snapshotId = newSnapshotStateId.getSnapshotId();

        // Operators may have finished when a snapshot is taken, then in the snapshot the operator won't have a corresponding state,
        // but they still needs to be restored to rebuild their internal states.
        // Need to check previous snapshots for their stored states.
        Optional<Object> state;
        loadMapIfNecessary(snapshotId, snapshotStateIdTaskId);
        state = Optional.ofNullable(loadCache.get(snapshotId).get(newSnapshotStateId.toString()));
        Map<Long, SnapshotInfo> snapshotToSnapshotResultMap = null;
        while (!state.isPresent()) {
            // Snapshot is complete but no entry for this id, then the component must have finished
            // before the snapshot was taken. Look at previous complete snapshots for last saved state.
            if (snapshotToSnapshotResultMap == null) {
                snapshotToSnapshotResultMap = recoveryUtils.loadSnapshotResult(newSnapshotStateId.getTaskId().getQueryId().getId());
            }
            OptionalLong prevSnapshotId = getPreviousSnapshotIdIfComplete(snapshotToSnapshotResultMap, newSnapshotStateId.getSnapshotId());
            if (!prevSnapshotId.isPresent()) {
                return state;
            }
            if (prevSnapshotId.getAsLong() == 0) {
                // We reached the beginning. This should not happen.
                // We should either have hit an incomplete snapshot (so empty should be returned),
                // or we should have found a snapshot that includes this component.
                // Return empty so an error can be reported.
                return Optional.of(NO_STATE);
            }
            snapshotId = prevSnapshotId.getAsLong();
            newSnapshotStateId = newSnapshotStateId.withSnapshotId(snapshotId);
            loadMapIfNecessary(snapshotId, snapshotStateIdTaskId);
            state = Optional.ofNullable(loadCache.get(snapshotId).get(newSnapshotStateId.toString()));
        }
        return state;
    }

    public Optional<Object> loadConsolidatedState(SnapshotStateId snapshotStateId)
            throws Exception
    {
        return loadWithBacktrack(snapshotStateId);
    }

    /**
     * Load the state of snapshotStateId from snapshot store. Returns:
     * - Empty: state file doesn't exist
     * - NO_STATE: bug situation
     * - Other object: previously saved state
     */
    public Optional<Object> loadState(SnapshotStateId snapshotStateId)
            throws Exception
    {
        Optional<Object> loadedValue = loadWithBacktrack(snapshotStateId);
        if (loadedValue.isPresent() && loadedValue.get() != NO_STATE) {
            return recoveryUtils.loadState(SnapshotStateId.fromString((String) loadedValue.get()), this);
        }
        return loadedValue;
    }

    public void storeFile(SnapshotStateId snapshotStateId, Path sourceFile, long skipBytes)
            throws Exception
    {
        recoveryUtils.storeFile(snapshotStateId, sourceFile, this, skipBytes);
        // store dummy value
        Map<String, Object> map = storeCache.computeIfAbsent(snapshotStateId.getSnapshotId(), (x) -> Collections.synchronizedMap(new HashMap<>()));
        map.put(snapshotStateId.toString(), snapshotStateId.toString());
    }

    public Boolean loadFile(SnapshotStateId snapshotStateId, Path targetFile)
            throws Exception
    {
        requireNonNull(targetFile);

        Optional<Object> loadedValue = loadWithBacktrack(snapshotStateId);
        if (!loadedValue.isPresent()) {
            return false;
        }
        if (loadedValue.get() == NO_STATE) {
            return null;
        }
        return recoveryUtils.loadFile(SnapshotStateId.fromString((String) loadedValue.get()), targetFile, this);
    }

    public Boolean loadFiles(Map<Path, List<SnapshotStateId>> snapshotSpillMap)
            throws Exception
    {
        return recoveryUtils.loadFiles(snapshotSpillMap, this);
    }

    public SnapshotStateId loadSpilledFile(SnapshotStateId snapshotStateId)
            throws Exception
    {
        Optional<Object> loadedValue = loadWithBacktrack(snapshotStateId);
        if (!loadedValue.isPresent() || loadedValue.get() == NO_STATE) {
            return null;
        }
        return SnapshotStateId.fromString((String) loadedValue.get());
    }

    private OptionalLong getPreviousSnapshotIdIfComplete(Map<Long, SnapshotInfo> snapshotToSnapshotResultMap, long snapshotId)
    {
        try {
            List<Map.Entry<Long, SnapshotInfo>> entryList = new ArrayList<>(snapshotToSnapshotResultMap.entrySet());
            for (int i = entryList.size() - 1; i >= 0; i--) {
                long sId = entryList.get(i).getKey();
                SnapshotResult snapshotRestoreResult = entryList.get(i).getValue().getSnapshotResult();
                if (sId < snapshotId) {
                    if (snapshotRestoreResult == SnapshotResult.SUCCESSFUL) {
                        return OptionalLong.of(sId);
                    }
                    // Skip over NA entries
                    else if (snapshotRestoreResult != SnapshotResult.NA) {
                        return OptionalLong.empty();
                    }
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        // We reach the beginning. Return 0 to indicate that.
        return OptionalLong.of(0);
    }

    public void setTotalComponents(int totalComponents)
    {
        this.totalComponents = totalComponents;
        if (totalComponents == 0) {
            // If this task only has table-scan pipelines, then "restore" is implicitly done
            restoreResult.setSnapshotResult(-1, SnapshotResult.SUCCESSFUL);
        }
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

    public void succeededToRestore(SnapshotStateId componentId, long deserCpuTime)
    {
        updateSnapshotRestoreCpuTime(deserCpuTime);
        updateRestore(componentId, SnapshotComponentCounter.ComponentState.SUCCESSFUL);
    }

    public void failedToRestore(SnapshotStateId componentId, boolean fatal)
    {
        LOG.warn("Failed (fatal=%b) to restore snapshot %d for component %s", fatal, componentId.getSnapshotId(), componentId);
        if (fatal) {
            updateRestore(componentId, SnapshotComponentCounter.ComponentState.FAILED_FATAL);
        }
        else {
            updateRestore(componentId, SnapshotComponentCounter.ComponentState.FAILED);
        }
    }

    public Map<Long, SnapshotInfo> getSnapshotCaptureResult()
    {
        if (totalComponents == 0) {
            // Special case: don't expect any more markers from this task.
            // It's as if this task has finished.
            // Use -1 to indicate "all snapshots".
            return ImmutableMap.of(-1L, SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL));
        }
        // Need to make a copy, otherwise there may be concurrent modification errors
        synchronized (captureResults) {
            return ImmutableMap.copyOf(captureResults);
        }
    }

    public RestoreResult getSnapshotRestoreResult()
    {
        return restoreResult;
    }

    private void updateCapture(SnapshotStateId componentId, SnapshotComponentCounter.ComponentState componentState)
    {
        TaskId componentIdTaskId = componentId.getTaskId();
        checkState(totalComponents > 0);
        final long snapshotId = componentId.getSnapshotId();
        // update capturedSnapshotComponentCounterMap
        SnapshotComponentCounter<SnapshotStateId> counter =
                captureComponentCounters.computeIfAbsent(snapshotId, k -> new SnapshotComponentCounter<>(totalComponents));

        if (counter.updateComponent(componentId, componentState)) {
            // update capturedSnapshotResultMap
            SnapshotResult snapshotResult = counter.getSnapshotResult();
            synchronized (captureResults) {
                SnapshotResult oldResult = updateSnapshotStatus(snapshotId, snapshotResult);
                if (snapshotResult != oldResult && snapshotResult.isDone()) {
                    if (snapshotResult == SnapshotResult.SUCCESSFUL) {
                        // All components for the task have captured their states successfully.
                        // Save the consolidated state.
                        SnapshotStateId newId = createConsolidatedId(snapshotId, componentIdTaskId);
                        try {
                            Map<String, Object> inputMap = storeCache.remove(snapshotId);
                            Map<String, Object> map = new HashMap<>();
                            if (inputMap != null) {
                                map.putAll(inputMap);
                            }
                            else {
                                map = Collections.emptyMap();
                            }
                            recoveryUtils.storeState(newId, map, this);
                        }
                        catch (Exception e) {
                            LOG.error(e, "Failed to store state for %s", newId.toString());
                            snapshotResult = SnapshotResult.FAILED;
                            updateSnapshotStatus(snapshotId, snapshotResult);
                        }
                    }
                    if (recoveryUtils.isCoordinator()) {
                        // Results on coordinator won't be reported through remote task. Send to the query side.
                        QuerySnapshotManager querySnapshotManager = recoveryUtils.getQuerySnapshotManager(componentIdTaskId.getQueryId());
                        if (querySnapshotManager != null) {
                            if (snapshotResult == SnapshotResult.SUCCESSFUL) {
                                querySnapshotManager.addConsolidatedFileToList(createConsolidatedId(snapshotId, componentIdTaskId).toString());
                            }
                            SnapshotInfo snapshotInfo = captureResults.get(snapshotId);
                            querySnapshotManager.updateQueryCapture(componentIdTaskId, snapshotId, snapshotInfo);
                        }
                    }
                    LOG.debug("Finished capturing snapshot %d for task %s. Result is %s.", snapshotId, componentIdTaskId, snapshotResult);
                }
            }
        }
    }

    private void updateRestore(SnapshotStateId componentId, SnapshotComponentCounter.ComponentState componentState)
    {
        TaskId componentIdTaskId = componentId.getTaskId();
        checkState(totalComponents > 0);
        long snapshotId = componentId.getSnapshotId();
        // update restoredSnapshotComponentCounterMap
        SnapshotComponentCounter<SnapshotStateId> counter =
                restoreComponentCounters.computeIfAbsent(snapshotId, k -> new SnapshotComponentCounter<>(totalComponents));

        if (counter.updateComponent(componentId, componentState)) {
            SnapshotResult snapshotResult = counter.getSnapshotResult();
            synchronized (restoreResult) {
                if (restoreResult.setSnapshotResult(snapshotId, snapshotResult) && snapshotResult.isDone()) {
                    if (recoveryUtils.isCoordinator()) {
                        // Results on coordinator won't be reported through remote task. Send to the query side.
                        QuerySnapshotManager querySnapshotManager = recoveryUtils.getQuerySnapshotManager(componentIdTaskId.getQueryId());
                        if (querySnapshotManager != null) {
                            querySnapshotManager.updateQueryRestore(componentIdTaskId, Optional.of(restoreResult));
                        }
                    }
                    // All components for the task have restored their states successfully.
                    // The loadCache won't be used again.
                    loadCache.clear();
                    LOG.debug("Finished restoring snapshot %d for task %s. Result is %s.", snapshotId, componentIdTaskId.toString(), snapshotResult);
                }
            }
        }
    }

    public void updateFinishedComponents(Collection<Operator> finishedOperators)
    {
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

        checkState(totalComponents >= 0);
    }

    @Override
    public void updateSnapshotCaptureSize(long snapshotId, long sizeBytes)
    {
        synchronized (captureResults) {
            SnapshotInfo snapshotInfo = captureResults.computeIfAbsent(snapshotId, k -> getNewSnapshotInfo(k));
            snapshotInfo.updateSizeBytes(sizeBytes);
        }
    }

    @Override
    public void updateSnapshotCaptureCpuTime(long snapshotId, long time)
    {
        synchronized (captureResults) {
            SnapshotInfo snapshotInfo = captureResults.computeIfAbsent(snapshotId, k -> getNewSnapshotInfo(k));
            snapshotInfo.updateCpuTime(time);
        }
    }

    @Override
    public void updateSnapshotRestoreSize(long sizeBytes)
    {
        synchronized (restoreResult) {
            SnapshotInfo snapshotInfo = restoreResult.getSnapshotInfo();
            snapshotInfo.updateSizeBytes(sizeBytes);
        }
    }

    @Override
    public void updateSnapshotRestoreCpuTime(long time)
    {
        synchronized (restoreResult) {
            SnapshotInfo snapshotInfo = restoreResult.getSnapshotInfo();
            snapshotInfo.updateCpuTime(time);
        }
    }

    private SnapshotResult updateSnapshotStatus(long snapshotId, SnapshotResult newStatus)
    {
        SnapshotInfo snapshotInfo = captureResults.computeIfAbsent(snapshotId, k -> getNewSnapshotInfo(k));
        SnapshotResult oldStatus = snapshotInfo.getSnapshotResult();
        snapshotInfo.setSnapshotResult(newStatus);
        return oldStatus;
    }

    private SnapshotInfo getNewSnapshotInfo(long snapshotId)
    {
        return new SnapshotInfo(0, 0, 0, 0, SnapshotResult.IN_PROGRESS);
    }

    @Override
    public String toString()
    {
        return String.format(Locale.ENGLISH, "%s, with total component %d", taskId, totalComponents);
    }

    public void storeSpilledPathInfo(SnapshotStateId spillId, Object snapshotSpillPaths)
            throws Exception
    {
        recoveryUtils.storeSpilledPathInfo(spillId, snapshotSpillPaths);
        Map<String, Object> map = storeCache.computeIfAbsent(spillId.getSnapshotId(), (x) -> Collections.synchronizedMap(new HashMap<>()));
        map.put(spillId.toString(), spillId.toString());
    }

    public Map<Long, List<String>> loadSpilledPathInfo(SnapshotStateId spillId)
            throws Exception
    {
        Optional<Object> loadedValue = loadWithBacktrack(spillId);
        if (!loadedValue.isPresent() || loadedValue.get() == NO_STATE) {
            return null;
        }
        else {
            return recoveryUtils.loadSpilledPathInfo(SnapshotStateId.fromString((String) loadedValue.get()));
        }
    }
}
