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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.execution.TaskId;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.TOO_MANY_RESUMES;
import static java.util.Objects.requireNonNull;

/**
 * - On the coordinator, it keeps track of snapshot status of all queries
 * - On workers, it serves as a bridge between TaskSnapshotManager instances and the SnapshotStoreClient
 */
public class QuerySnapshotManager
{
    private static final Logger LOG = Logger.get(QuerySnapshotManager.class);
    public static final Object NO_STATE = new Object();

    //TODO-cp-I2D63N remove DEBUG after testing is done
    private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("snapshot.debug", "false"));

    private final QueryId queryId;
    private final SnapshotUtils snapshotUtils;

    private final Set<TaskId> unfinishedTasks = Sets.newConcurrentHashSet();
    // LinkedHashMap can be used to keep ordering
    private final Map<Long, SnapshotComponentCounter<TaskId>> captureComponentCounters = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<Long, SnapshotResult> captureResults = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<String, SnapshotComponentCounter<TaskId>> restoreComponentCounters = Collections.synchronizedMap(new LinkedHashMap<>());
    private final RestoreResult restoreResult = new RestoreResult();
    private final List<Consumer<RestoreResult>> restoreCompleteListeners = Collections.synchronizedList(new ArrayList<>());

    private final long maxRetry;
    private final long retryTimeout;

    // The snapshot id used for current resume. It's cleared when a resume is successful.
    private OptionalLong lastTriedId = OptionalLong.empty();
    // If a resume is not finished before the timer expires, it's considered a failure.
    private Optional<Timer> retryTimer = Optional.empty();
    // How many numbers resume has been attempted for this query
    private long retryCount;
    private Runnable rescheduler = () -> {}; // No-op by default. SqlQueryScheduler will set it.

    public QuerySnapshotManager(QueryId queryId, SnapshotUtils snapshotUtils, Session session)
    {
        this.queryId = requireNonNull(queryId);
        this.snapshotUtils = requireNonNull(snapshotUtils);
        if (session == null) {
            maxRetry = 0;
            retryTimeout = 0;
        }
        else {
            this.maxRetry = SystemSessionProperties.getSnapshotMaxRetries(session);
            this.retryTimeout = SystemSessionProperties.getSnapshotRetryTimeout(session).toMillis();
        }
    }

    public void setRescheduler(Runnable rescheduler)
    {
        this.rescheduler = rescheduler;
    }

    public boolean isCoordinator()
    {
        return snapshotUtils.isCoordinator();
    }

    /**
     * Store the state of snapshotStateId in snapshot store
     */
    public void storeState(SnapshotStateId snapshotStateId, Object state)
            throws Exception
    {
        snapshotUtils.storeState(snapshotStateId, state);
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
        // Operators may have finished when a snapshot is taken, then in the snapshot the operator won't have a corresponding state,
        // but they still needs to be restored to rebuild their internal states.
        // Need to check previous snapshots for their stored states.
        Optional<Object> state = snapshotUtils.loadState(snapshotStateId);
        Map<Long, SnapshotResult> snapshotToSnapshotResultMap = null;
        while (!state.isPresent()) {
            if (snapshotToSnapshotResultMap == null) {
                snapshotToSnapshotResultMap = snapshotUtils.loadSnapshotResult(queryId.getId());
            }
            // Snapshot is complete but no entry for this id, then the component must have finished
            // before the snapshot was taken. Look at previous complete snapshots for last saved state.
            OptionalLong prevSnapshotId = getPreviousSnapshotIdIfComplete(snapshotToSnapshotResultMap, snapshotStateId.getSnapshotId());
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
            snapshotStateId = snapshotStateId.withSnapshotId(prevSnapshotId.getAsLong());
            state = snapshotUtils.loadState(snapshotStateId);
        }
        return state;
    }

    public void storeFile(SnapshotStateId snapshotStateId, Path sourceFile)
            throws Exception
    {
        snapshotUtils.storeFile(snapshotStateId, sourceFile);
    }

    public Boolean loadFile(SnapshotStateId snapshotStateId, Path targetFile)
            throws Exception
    {
        requireNonNull(targetFile);

        // Logic of this function is very similar to that of "loadState"
        boolean loadResult = snapshotUtils.loadFile(snapshotStateId, targetFile);
        Map<Long, SnapshotResult> snapshotToSnapshotResultMap = null;
        while (!loadResult) {
            if (snapshotToSnapshotResultMap == null) {
                snapshotToSnapshotResultMap = snapshotUtils.loadSnapshotResult(queryId.getId());
            }
            OptionalLong prevSnapshotId = getPreviousSnapshotIdIfComplete(snapshotToSnapshotResultMap, snapshotStateId.getSnapshotId());
            if (!prevSnapshotId.isPresent()) {
                return false;
            }
            if (prevSnapshotId.getAsLong() == 0) {
                return null;
            }
            snapshotStateId = snapshotStateId.withSnapshotId(prevSnapshotId.getAsLong());
            loadResult = snapshotUtils.loadFile(snapshotStateId, targetFile);
        }
        return true;
    }

    private OptionalLong getPreviousSnapshotIdIfComplete(Map<Long, SnapshotResult> snapshotToSnapshotResultMap, long snapshotId)
    {
        try {
            List<Map.Entry<Long, SnapshotResult>> entryList = new ArrayList<>(snapshotToSnapshotResultMap.entrySet());
            for (int i = entryList.size() - 1; i >= 0; i--) {
                long sId = entryList.get(i).getKey();
                SnapshotResult restoreResult = entryList.get(i).getValue();
                if (sId < snapshotId) {
                    if (restoreResult == SnapshotResult.SUCCESSFUL) {
                        return OptionalLong.of(sId);
                    }
                    // Skip over NA entries
                    else if (restoreResult != SnapshotResult.NA) {
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

    // coordinator specified functions

    public void addNewTask(TaskId taskId)
    {
        unfinishedTasks.add(taskId);
    }

    /**
     * Get the successful and complete snapshot id to resume.
     *
     * @return The previous successful and complete snapshot id of beforeThis to resume if beforeThis is specified.
     * If beforeThis is not specified, then return the last complete and successful snapshot id.
     * @throws PrestoException if reached max number of resumes
     */
    public OptionalLong getResumeSnapshotId()
            throws PrestoException
    {
        if (retryCount >= maxRetry) {
            throw new PrestoException(TOO_MANY_RESUMES, "Tried to recover query execution for too many times");
        }
        retryCount++;

        lastTriedId = getResumeSnapshotId(lastTriedId);
        if (lastTriedId.isPresent()) {
            startSnapshotRestoreTimer();
        }

        // Clear entries that are no longer needed after resuming.
        // In particular, unfinishedTasks needs to be cleared in case it contains table-scan tasks that won't be restored.
        unfinishedTasks.clear();
        captureComponentCounters.clear();
        restoreComponentCounters.clear();

        return lastTriedId;
    }

    private OptionalLong getResumeSnapshotId(OptionalLong beforeThis)
    {
        OptionalLong result = OptionalLong.empty();
        if (captureResults.isEmpty()) {
            LOG.debug("Can't find a suitable snapshot to resume for query '%s'", queryId.getId());
            return result;
        }

        if (!beforeThis.isPresent()) {
            beforeThis = OptionalLong.of(Long.MAX_VALUE);
        }

        synchronized (captureResults) {
            List<Map.Entry<Long, SnapshotResult>> entryList = new ArrayList<>(captureResults.entrySet());
            // iterate in reverse order
            for (int i = entryList.size() - 1; i >= 0; i--) {
                long snapshotId = entryList.get(i).getKey();
                SnapshotResult restoreResult = entryList.get(i).getValue();
                // Update the snapshot result to n/a where snapshotId > resumeSnapshotId && snapshotId <= beforeThis
                if (snapshotId == beforeThis.getAsLong()) {
                    captureResults.put(snapshotId, SnapshotResult.NA);
                }
                else if (snapshotId < beforeThis.getAsLong()) {
                    if (restoreResult == SnapshotResult.SUCCESSFUL) {
                        result = OptionalLong.of(snapshotId);
                        break;
                    }
                    captureResults.put(snapshotId, SnapshotResult.NA);
                }
            }

            saveQuerySnapshotResult();
        }

        if (result.isPresent()) {
            LOG.debug("About to resume from snapshot %d for query '%s'", result.getAsLong(), queryId.getId());
        }
        else {
            LOG.debug("Can't find a suitable snapshot to resume for query '%s'", queryId.getId());
        }
        return result;
    }

    private void queryRestoreComplete(RestoreResult restoreResult)
    {
        if (!retryTimer.isPresent()) {
            return;
        }

        synchronized (this) {
            if (!retryTimer.isPresent()) {
                return;
            }
            retryTimer.get().cancel();
            retryTimer = Optional.empty();
        }

        if (restoreResult.getSnapshotResult() == SnapshotResult.SUCCESSFUL) {
            lastTriedId = OptionalLong.empty();
        }
        else {
            LOG.warn("Failed to restore snapshot for %s, snapshot %d", queryId.getId(), restoreResult.getSnapshotId());
            rescheduler.run();
        }
    }

    private void startSnapshotRestoreTimer()
    {
        // start timer for restoring snapshot
        TimerTask task = new TimerTask()
        {
            public void run()
            {
                LOG.warn("Snapshot restore timed out, failed to restore snapshot for %s, snapshot %d", queryId.getId(), lastTriedId.getAsLong());
                synchronized (this) {
                    if (retryTimer.isPresent()) {
                        retryTimer = Optional.empty();
                    }
                    else {
                        // We must have received the "queryRestoreComplete" signal while the time is triggerd
                        return;
                    }
                }
                rescheduler.run();
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, retryTimeout);

        synchronized (this) {
            if (retryTimer.isPresent()) {
                retryTimer.get().cancel();
            }
            retryTimer = Optional.of(timer);
        }
    }

    public void doneQuery()
    {
        LOG.debug("query will be removed with queryId = %s,%n" +
                        "captureComponentCounters = %s,%n" +
                        "captureResults = %s,%n" +
                        "restoreComponentCounters = %s,%n" +
                        "restoreResult = %s",
                queryId,
                captureComponentCounters,
                captureResults,
                restoreComponentCounters,
                restoreResult);

        resetForQuery();

        if (!DEBUG) {
            // clear all stored states for this query
            try {
                snapshotUtils.deleteAll(queryId.getId());
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void resetForQuery()
    {
        // clear all maps related to this query
        unfinishedTasks.clear();
        captureComponentCounters.clear();
        captureResults.clear();
        restoreComponentCounters.clear();
        restoreResult.setSnapshotResult(0, 0, SnapshotResult.IN_PROGRESS);
        restoreCompleteListeners.clear();
    }

    public void addQueryRestoreCompleteListeners(Consumer<RestoreResult> listener)
    {
        restoreCompleteListeners.add(listener);
    }

    // Update capture results based on TaskInfo
    public void updateQueryCapture(TaskId taskId, Map<Long, SnapshotResult> captureResult)
    {
        for (Map.Entry<Long, SnapshotResult> entry : captureResult.entrySet()) {
            Long snapshotId = entry.getKey();
            SnapshotResult result = entry.getValue();
            if (snapshotId < 0) {
                // Special case. Task will never receive any marker. Add it to the "finished" list
                checkArgument(result == SnapshotResult.SUCCESSFUL);
                updateFinishedQueryComponents(ImmutableList.of(taskId));
            }
            else if (result == SnapshotResult.FAILED) {
                updateQueryCapture(snapshotId, taskId, SnapshotComponentCounter.ComponentState.FAILED);
            }
            else if (result == SnapshotResult.SUCCESSFUL) {
                updateQueryCapture(snapshotId, taskId, SnapshotComponentCounter.ComponentState.SUCCESSFUL);
            }
        }
    }

    // Update restore results based on TaskInfo
    public void updateQueryRestore(TaskId taskId, Optional<RestoreResult> restoreResult)
    {
        if (restoreResult.isPresent()) {
            SnapshotResult result = restoreResult.get().getSnapshotResult();
            long snapshotId = restoreResult.get().getSnapshotId();
            int resumeId = restoreResult.get().getResumeId();
            if (result == SnapshotResult.FAILED) {
                updateQueryRestore(snapshotId, taskId, resumeId, SnapshotComponentCounter.ComponentState.FAILED);
                LOG.debug("[FATAL] Failed to resume for: " + taskId + ", snapshot " + snapshotId);
            }
            else if (result == SnapshotResult.FAILED_FATAL) {
                updateQueryRestore(snapshotId, taskId, resumeId, SnapshotComponentCounter.ComponentState.FAILED_FATAL);
                LOG.debug("Failed to resume for: " + taskId + ", snapshot " + snapshotId);
            }
            else if (result == SnapshotResult.SUCCESSFUL) {
                updateQueryRestore(snapshotId, taskId, resumeId, SnapshotComponentCounter.ComponentState.SUCCESSFUL);
            }
        }
    }

    private void saveQuerySnapshotResult()
    {
        if (!captureResults.isEmpty()) {
            Map<Long, SnapshotResult> doneResult = captureResults.entrySet()
                    .stream()
                    .filter(e -> e.getValue().isDone())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

            try {
                snapshotUtils.storeSnapshotResult(queryId.getId(), doneResult);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void updateQueryCapture(long snapshotId, TaskId taskId, SnapshotComponentCounter.ComponentState componentState)
    {
        SnapshotComponentCounter<TaskId> counter = captureComponentCounters.computeIfAbsent(snapshotId, k ->
                // A snapshot is considered complete if tasks either finished their snapshots or have completed
                new SnapshotComponentCounter<>(ids -> ids.containsAll(unfinishedTasks)));

        if (counter.updateComponent(taskId, componentState)) {
            LOG.debug("Finished capturing snapshot %d for task %s", snapshotId, taskId);

            SnapshotResult snapshotResult = counter.getSnapshotResult();
            synchronized (captureResults) {
                if (captureResults.get(snapshotId) != SnapshotResult.NA) {
                    SnapshotResult oldResult = captureResults.put(snapshotId, snapshotResult);
                    if (snapshotResult != oldResult && snapshotResult.isDone()) {
                        LOG.debug("Finished capturing snapshot %d for query %s. Result is %s.", snapshotId, queryId.getId(), snapshotResult);
                        // Store snapshot information for this query in filesystem, so it can be accessed by tasks, e.g. during backtrack state loading.
                        saveQuerySnapshotResult();
                    }
                }
            }
        }
    }

    private void updateQueryRestore(long snapshotId, TaskId taskId, int resumeId, SnapshotComponentCounter.ComponentState componentState)
    {
        String snapshotResumeId = snapshotId + "-" + resumeId;

        // update queryToRestoredSnapshotComponentCounterMap
        SnapshotComponentCounter<TaskId> counter = restoreComponentCounters.computeIfAbsent(snapshotResumeId, k ->
                // A snapshot is considered complete if tasks either finished their snapshots or have completed
                new SnapshotComponentCounter<>(ids -> ids.containsAll(unfinishedTasks)));

        if (counter.updateComponent(taskId, componentState)) {
            LOG.debug("Finished restoring snapshot %d for task %s", snapshotId, taskId);

            // update queryToRestoreReportMap;
            SnapshotResult snapshotResult = counter.getSnapshotResult();
            boolean changed;
            synchronized (restoreResult) {
                changed = restoreResult.setSnapshotResult(snapshotId, resumeId, snapshotResult);
            }
            if (changed && snapshotResult.isDone()) {
                LOG.debug("Finished restoring snapshot %d for query %s. Result is %s.", snapshotId, queryId.getId(), snapshotResult);
                // inform the listeners(ie schedulers) if query snapshot result is finished
                queryRestoreComplete(restoreResult);
            }
        }
    }

    public void updateFinishedQueryComponents(Collection<TaskId> finishedTasks)
    {
        if (unfinishedTasks.removeAll(finishedTasks)) {
            LOG.debug("Some tasks finished for query %s.%n  Remaining tasks: %s.%n  Snapshot result: %s", queryId.getId(), unfinishedTasks, captureResults);
        }
    }

    public RestoreResult getQuerySnapshotRestoreResult()
    {
        return restoreResult;
    }
}
