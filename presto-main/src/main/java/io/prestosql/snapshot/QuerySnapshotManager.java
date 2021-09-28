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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.execution.QueryState;
import io.prestosql.execution.TaskId;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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

    private final QueryId queryId;
    private final SnapshotUtils snapshotUtils;

    private final Set<TaskId> unfinishedTasks = Sets.newConcurrentHashSet();
    // LinkedHashMap can be used to keep ordering
    private final Map<Long, SnapshotComponentCounter<TaskId>> captureComponentCounters = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<Long, SnapshotResult> captureResults = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Set<String> consolidatedFilePaths = Collections.synchronizedSet(new HashSet<>());
    private final Map<Long, SnapshotComponentCounter<TaskId>> restoreComponentCounters = Collections.synchronizedMap(new LinkedHashMap<>());
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
    private Runnable rescheduler; // SqlQueryScheduler will set it.

    // Keeps track of relevant snapshotIds.
    // Whenever MarkerAnnouncer decides to initiate a new snapshot, it needs to inform the QuerySnapshotManager about its action.
    // QuerySnapshotManager will keep track of all snapshots because snapshotId won't be able to tell us the absolute index after
    // restore. (eg. have completed snapshot 1-10, restored to 1, snapshot 11 that's generated after restore is actually 2nd snapshot)
    // It is important for the management of snapshot sub-files written by TableWriterOperator.
    private List<Long> initiatedSnapshotId;

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
        this.initiatedSnapshotId = Collections.synchronizedList(new ArrayList<>());
        initiatedSnapshotId.add(0L);
    }

    public void setRescheduler(Runnable rescheduler)
    {
        this.rescheduler = rescheduler;
    }

    public boolean isCoordinator()
    {
        return snapshotUtils.isCoordinator();
    }

    // coordinator specified functions

    public void addNewTask(TaskId taskId)
    {
        unfinishedTasks.add(taskId);
    }

    public void snapshotInitiated(long snapshotId)
    {
        captureResults.put(snapshotId, SnapshotResult.IN_PROGRESS);
        initiatedSnapshotId.add(snapshotId);
    }

    public long getResumeCount()
    {
        return retryCount;
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
        startSnapshotRestoreTimer();

        // Clear entries that are no longer needed after resuming.
        // In particular, unfinishedTasks needs to be cleared in case it contains table-scan tasks that won't be restored.
        unfinishedTasks.clear();
        captureComponentCounters.clear();
        restoreComponentCounters.clear();

        if (!lastTriedId.isPresent()) {
            // resume to 0, all current snapshotIds are invalidated
            initiatedSnapshotId.clear();
            initiatedSnapshotId.add(0L);
        }
        else {
            // all snapshotIds including lastTriedId can be reused.
            initiatedSnapshotId = initiatedSnapshotId.subList(0, initiatedSnapshotId.indexOf(lastTriedId.getAsLong()) + 1);
        }

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

            try {
                saveQuerySnapshotResult();
            }
            catch (Exception e) {
                LOG.warn(e, "Failed to save query snapshot state for %s: %s", queryId, e.getMessage());
                invalidateAllSnapshots();
                result = OptionalLong.empty();
            }
        }

        if (result.isPresent()) {
            LOG.debug("About to resume from snapshot %d for query '%s'", result.getAsLong(), queryId.getId());
        }
        else {
            LOG.debug("Can't find a suitable snapshot to resume for query '%s'", queryId.getId());
        }
        return result;
    }

    public void invalidateAllSnapshots()
    {
        synchronized (captureResults) {
            for (Long snapshotId : captureResults.keySet()) {
                captureResults.put(snapshotId, SnapshotResult.NA);
            }
        }
    }

    public boolean hasPendingResume()
    {
        if (retryTimer.isPresent()) {
            LOG.warn("Query %s finished after resume, but resume was not done", queryId.getId());
            return true;
        }
        return false;
    }

    private synchronized boolean cancelRestoreTimer()
    {
        if (!retryTimer.isPresent()) {
            return false;
        }

        retryTimer.get().cancel();
        retryTimer = Optional.empty();
        return true;
    }

    private void queryRestoreComplete(RestoreResult restoreResult)
    {
        if (!retryTimer.isPresent()) {
            return;
        }

        if (!cancelRestoreTimer()) {
            return;
        }

        if (restoreResult.getSnapshotResult() == SnapshotResult.SUCCESSFUL) {
            if (lastTriedId.isPresent()) {
                // Successfully resumed from this snapshot id. Avoid resuming from it again.
                // See HashBuilderOperator#finish(), which depends on this behavior.
                captureResults.put(lastTriedId.getAsLong(), SnapshotResult.FAILED);
                lastTriedId = OptionalLong.empty();
            }
        }
        else {
            LOG.warn("Failed to restore snapshot for %s, snapshot %d", queryId.getId(), restoreResult.getSnapshotId());
            cancelToResume();
        }
    }

    private void startSnapshotRestoreTimer()
    {
        if (!lastTriedId.isPresent()) {
            cancelRestoreTimer();
            return;
        }

        // start timer for restoring snapshot
        TimerTask task = new TimerTask()
        {
            public void run()
            {
                synchronized (this) {
                    if (retryTimer.isPresent()) {
                        LOG.warn("Snapshot restore timed out, failed to restore snapshot for %s, snapshot %s", queryId.getId(), lastTriedId.toString());
                        retryTimer = Optional.empty();
                    }
                    else {
                        // We must have received the "queryRestoreComplete" signal while the time is triggerd
                        return;
                    }
                }
                cancelToResume();
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, retryTimeout);

        synchronized (this) {
            cancelRestoreTimer();
            retryTimer = Optional.of(timer);
        }
    }

    public void doneQuery(QueryState state)
    {
        LOG.debug("query will be removed with queryId = %s,%n" +
                        "state = %s,%n" +
                        "captureComponentCounters = %s,%n" +
                        "captureResults = %s,%n" +
                        "restoreComponentCounters = %s,%n" +
                        "restoreResult = %s",
                queryId,
                state,
                captureComponentCounters,
                captureResults,
                restoreComponentCounters,
                restoreResult);

        resetForQuery();

        snapshotUtils.removeQuerySnapshotManager(queryId);
    }

    private void resetForQuery()
    {
        // clear all maps related to this query
        unfinishedTasks.clear();
        captureComponentCounters.clear();
        captureResults.clear();
        restoreComponentCounters.clear();
        restoreResult.setSnapshotResult(0, SnapshotResult.IN_PROGRESS);
        restoreCompleteListeners.clear();
        cancelRestoreTimer();
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
                updateCapturedComponents(ImmutableList.of(taskId), false);
            }
            else {
                if (updateQueryCapture(taskId, entry.getKey(), entry.getValue())) {
                    // if the capture works, then that means a consolidated file was created and we need to add it to the list
                    addConsolidatedFileToList(TaskSnapshotManager.createConsolidatedId(snapshotId, taskId).toString());
                }
            }
        }
    }

    // Update capture results based on TaskSnapshotManager running on coordinator
    public boolean updateQueryCapture(TaskId taskId, long snapshotId, SnapshotResult result)
    {
        if (result == SnapshotResult.FAILED) {
            return updateQueryCapture(snapshotId, taskId, SnapshotComponentCounter.ComponentState.FAILED);
        }
        else if (result == SnapshotResult.SUCCESSFUL) {
            return updateQueryCapture(snapshotId, taskId, SnapshotComponentCounter.ComponentState.SUCCESSFUL);
        }
        return false;
    }

    // Update restore results based on TaskInfo
    public void updateQueryRestore(TaskId taskId, Optional<RestoreResult> restoreResult)
    {
        if (restoreResult.isPresent()) {
            SnapshotResult result = restoreResult.get().getSnapshotResult();
            long snapshotId = restoreResult.get().getSnapshotId();
            if (snapshotId < 0) {
                synchronized (restoreComponentCounters) {
                    // Special case. Task will never receive any marker. Treat as finished.
                    checkArgument(result == SnapshotResult.SUCCESSFUL);
                    for (Long sid : restoreComponentCounters.keySet()) {
                        updateQueryRestore(sid, taskId, SnapshotComponentCounter.ComponentState.SUCCESSFUL);
                    }
                }
            }
            else {
                if (result == SnapshotResult.FAILED) {
                    LOG.debug("[FATAL] Failed to resume for: " + taskId + ", snapshot " + snapshotId);
                    updateQueryRestore(snapshotId, taskId, SnapshotComponentCounter.ComponentState.FAILED);
                }
                else if (result == SnapshotResult.FAILED_FATAL) {
                    LOG.debug("Failed to resume for: " + taskId + ", snapshot " + snapshotId);
                    updateQueryRestore(snapshotId, taskId, SnapshotComponentCounter.ComponentState.FAILED_FATAL);
                }
                else if (result == SnapshotResult.SUCCESSFUL) {
                    updateQueryRestore(snapshotId, taskId, SnapshotComponentCounter.ComponentState.SUCCESSFUL);
                }
            }
        }
    }

    public void addConsolidatedFileToList(String path)
    {
        consolidatedFilePaths.add(path);
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
                snapshotUtils.storeConsolidatedFileList(queryId.getId(), consolidatedFilePaths);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private boolean updateQueryCapture(long snapshotId, TaskId taskId, SnapshotComponentCounter.ComponentState componentState)
    {
        SnapshotComponentCounter<TaskId> counter = captureComponentCounters.computeIfAbsent(snapshotId, k ->
                // A snapshot is considered complete if tasks either finished their snapshots or have completed
                new SnapshotComponentCounter<>(ids -> ids.containsAll(unfinishedTasks)));

        if (counter.updateComponent(taskId, componentState)) {
            SnapshotResult snapshotResult = counter.getSnapshotResult();
            synchronized (captureResults) {
                if (captureResults.get(snapshotId) != SnapshotResult.NA) {
                    LOG.debug("Finished capturing snapshot %d for task %s", snapshotId, taskId);
                    SnapshotResult oldResult = captureResults.put(snapshotId, snapshotResult);
                    if (snapshotResult != oldResult && snapshotResult.isDone()) {
                        LOG.debug("Finished capturing snapshot %d for query %s. Result is %s.", snapshotId, queryId.getId(), snapshotResult);
                    }
                    return true;
                }
            }
        }
        return false;
    }

    private void updateQueryRestore(long snapshotId, TaskId taskId, SnapshotComponentCounter.ComponentState componentState)
    {
        // update queryToRestoredSnapshotComponentCounterMap
        SnapshotComponentCounter<TaskId> counter = restoreComponentCounters.computeIfAbsent(snapshotId, k ->
                // A snapshot is considered complete if tasks either finished their snapshots or have completed
                new SnapshotComponentCounter<>(ids -> ids.containsAll(unfinishedTasks)));

        if (counter.updateComponent(taskId, componentState)) {
            LOG.debug("Finished restoring snapshot %d for task %s", snapshotId, taskId);

            // update queryToRestoreReportMap;
            SnapshotResult snapshotResult = counter.getSnapshotResult();
            boolean changed;
            synchronized (restoreResult) {
                changed = restoreResult.setSnapshotResult(snapshotId, snapshotResult);
            }
            if (changed) {
                if (snapshotResult.isDone()) {
                    LOG.debug("Finished restoring snapshot %d for query %s. Result is %s.", snapshotId, queryId.getId(), snapshotResult);
                    // inform the listeners(ie schedulers) if query snapshot result is finished
                    queryRestoreComplete(restoreResult);
                }
                else if (snapshotResult == SnapshotResult.IN_PROGRESS_FAILED || snapshotResult == SnapshotResult.IN_PROGRESS_FAILED_FATAL) {
                    LOG.debug("Failed to restore snapshot %d for query %s. Result is %s.", snapshotId, queryId.getId(), snapshotResult);
                    // inform the listeners(ie schedulers) if query snapshot result is finished
                    queryRestoreComplete(restoreResult);
                }
            }
        }
    }

    public int computeSnapshotIndex(OptionalLong snapshotId)
    {
        if (!snapshotId.isPresent()) {
            return 0;
        }
        else {
            return initiatedSnapshotId.indexOf(snapshotId.getAsLong());
        }
    }

    public void updateFinishedQueryComponents(Collection<TaskId> finishedTasks)
    {
        updateCapturedComponents(finishedTasks, true);
    }

    public void updateCapturedComponents(Collection<TaskId> capturedTasks, boolean finished)
    {
        // For future snapshots
        if (unfinishedTasks.removeAll(capturedTasks)) {
            if (finished) {
                LOG.debug("Some tasks finished for query %s.%n  Finished tasks: %s.%n  Remaining tasks: %s.%n  Snapshot result: %s",
                        queryId.getId(), capturedTasks, unfinishedTasks, captureResults);
            }
            else {
                LOG.debug("Some tasks are fully captured for query %s.%n  Captured tasks: %s.%n  Remaining tasks: %s.%n  Snapshot result: %s",
                        queryId.getId(), capturedTasks, unfinishedTasks, captureResults);
            }

            synchronized (captureComponentCounters) {
                // Update ongoing snapshots
                for (Long snapshotId : captureComponentCounters.keySet()) {
                    for (TaskId taskId : capturedTasks) {
                        updateQueryCapture(taskId, ImmutableMap.of(snapshotId, SnapshotResult.SUCCESSFUL));
                    }
                }
            }
        }
    }

    @VisibleForTesting
    RestoreResult getQuerySnapshotRestoreResult()
    {
        return restoreResult;
    }

    public SnapshotUtils getSnapshotUtils()
    {
        return snapshotUtils;
    }

    // Returns true if cancel-to-resume is triggered; returns false if this was a no-op
    public synchronized void cancelToResume()
    {
        // If rescheduler is null, then the query must be in the process o being (re)scheduled
        if (rescheduler != null) {
            rescheduler.run();
            rescheduler = null;
        }
    }
}
