/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.execution.QueryState;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.StandardWarningCode;

import javax.annotation.Nonnull;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.Timer;
import java.util.TimerTask;

import static io.prestosql.snapshot.RecoveryState.DEFAULT;
import static io.prestosql.snapshot.RecoveryState.RECOVERY_FAILED;
import static io.prestosql.snapshot.RecoveryState.RESCHEDULING;
import static io.prestosql.snapshot.RecoveryState.STOPPING_FOR_RESCHEDULE;
import static io.prestosql.snapshot.RecoveryState.SUSPENDED;
import static io.prestosql.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static io.prestosql.spi.StandardErrorCode.TOO_MANY_RESUMES;

public class QueryRecoveryManager
{
    private static final Logger LOG = Logger.get(QueryRecoveryManager.class);
    private RecoveryState recoveryState;
    private Runnable cancelToResume;
    private RecoveryUtils recoveryUtils;
    private Session session;
    private long retryCount;
    private final long maxRetry;
    private Optional<Timer> retryTimer = Optional.empty();
    private final long retryTimeout;
    private Rescheduler rescheduler;
    private QueryId queryId;
    private QuerySnapshotManager querySnapshotManager;
    private WarningCollector warningCollector;

    public QueryRecoveryManager(@Nonnull RecoveryUtils recoveryUtils, Session session, QueryId queryId)
    {
        this.recoveryUtils = recoveryUtils;
        this.session = session;
        this.maxRetry = SystemSessionProperties.getRecoveryMaxRetries(session);
        this.retryTimeout = SystemSessionProperties.getRecoveryRetryTimeout(session).toMillis();
        this.queryId = queryId;
        this.retryCount = 0;
        this.querySnapshotManager = recoveryUtils.getOrCreateQuerySnapshotManager(queryId, session);
        this.recoveryState = DEFAULT;
    }

    public void setCancelToResumeCb(Runnable cancelToResume)
    {
        this.cancelToResume = cancelToResume;
    }

    public synchronized void startRecovery()
    {
        LOG.debug("startRecovery() is called!, recoveryState: " + recoveryState);
        if (recoveryState == DEFAULT || recoveryState == RECOVERY_FAILED) {
            this.recoveryState = STOPPING_FOR_RESCHEDULE;
            if (cancelToResume != null) {
                cancelToResume.run();
            }
        }
        else {
            LOG.info("Failure is reported already!, Ignoring..");
        }
    }

    public synchronized void suspendQuery()
    {
        LOG.debug("Suspending query is called!, recoveryState: " + recoveryState);
        if (recoveryState == DEFAULT || recoveryState == RECOVERY_FAILED) {
            this.recoveryState = SUSPENDED;
            if (cancelToResume != null) {
                cancelToResume.run();
            }
        }
        else {
            LOG.info("Failure is reported already!, Ignoring..");
        }
    }

    public synchronized void resumeQuery()
    {
        LOG.debug("Suspending query is called!, recoveryState: " + recoveryState);
        if (recoveryState == SUSPENDED) {
            this.recoveryState = STOPPING_FOR_RESCHEDULE;
        }
        else {
            LOG.info("Failure is reported already!, Ignoring..");
        }
    }

    public boolean isCoordinator()
    {
        return recoveryUtils.isCoordinator();
    }

    public RecoveryState getState()
    {
        return recoveryState;
    }

    public boolean isRecovering()
    {
        return (recoveryState == STOPPING_FOR_RESCHEDULE || recoveryState == SUSPENDED);
    }

    public synchronized void rescheduleQuery() throws Throwable
    {
        LOG.debug("rescheduleQuery() is called!, retryCount: " + retryCount + ", recoveryState: " + recoveryState);
        if (recoveryState == STOPPING_FOR_RESCHEDULE) {
            recoveryState = RESCHEDULING;
            if (retryCount >= maxRetry) {
                throw new PrestoException(TOO_MANY_RESUMES, "Tried to recover query execution for too many times");
            }
            retryCount++;
            startRecoveryRestoreTimer();

            querySnapshotManager.setRecoveryCallbacks(this::onRecoveryComplete, this::isRecoveryInProgress);
            String warningMessage = "Query encountered failures. Attempting query recovery.";
            OptionalLong snapshotId = querySnapshotManager.getResumeSnapshotId();
            if (snapshotId.isPresent()) {
                LOG.debug("Available snapshotId: " + snapshotId.getAsLong());
                if (rescheduler != null) {
                    warningCollector.add(new PrestoWarning(StandardWarningCode.SNAPSHOT_RECOVERY, warningMessage));
                    try {
                        rescheduler.apply(snapshotId);
                    }
                    catch (Throwable e) {
                        if (e instanceof PrestoException) {
                            PrestoException ex = (PrestoException) e;
                            if (ex.getErrorCode() == NO_NODES_AVAILABLE.toErrorCode()) {
                                // Not enough worker to resume all tasks. Retrying from any saved snapshot likely wont' work either.
                                // Clear ongoing and existing snapshots and restart.
                                LOG.debug("Restarting query %s due to insufficient number of nodes", queryId);
                                querySnapshotManager.invalidateAllSnapshots();
                                snapshotId = querySnapshotManager.getResumeSnapshotId();
                                rescheduler.apply(snapshotId);
                            }
                            else {
                                throw e;
                            }
                        }
                        else {
                            throw e;
                        }
                    }
                }
            }
            else {
                // Go for restart if snapshot is not available
                warningCollector.add(new PrestoWarning(StandardWarningCode.SNAPSHOT_RECOVERY, warningMessage));
                rescheduler.apply(snapshotId);
            }
        }
        else {
            LOG.warn("Invalid recovery manager state..");
        }
    }

    void onRecoveryComplete(boolean success)
    {
        LOG.debug("onRecoveryComplete() is called!, success: " + success);
        cancelRestoreTimer();
        if (success) {
            recoveryState = DEFAULT;
        }
        else {
            recoveryState = RECOVERY_FAILED;
            startRecovery();
        }
    }

    private void startRecoveryRestoreTimer()
    {
        LOG.debug("startRecoveryRestoreTimer() is called!");
        // start timer for restoring snapshot
        TimerTask task = new TimerTask()
        {
            public void run()
            {
                synchronized (this) {
                    if (retryTimer.isPresent()) {
                        retryTimer = Optional.empty();
                    }
                    else {
                        LOG.warn("Timer is not set");
                        return;
                    }
                }
                recoveryState = RECOVERY_FAILED;
                startRecovery();
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, retryTimeout);

        synchronized (this) {
            cancelRestoreTimer();
            retryTimer = Optional.of(timer);
        }
    }

    public synchronized boolean cancelRestoreTimer()
    {
        if (!retryTimer.isPresent()) {
            return false;
        }
        retryTimer.get().cancel();
        retryTimer = Optional.empty();
        return true;
    }

    public void setRescheduler(Rescheduler<OptionalLong> rescheduler, WarningCollector warningCollector)
    {
        if (this.rescheduler == null) {
            this.rescheduler = rescheduler;
            this.warningCollector = warningCollector;
        }
    }

    public void doneQuery(QueryState state)
    {
        cancelRestoreTimer();
        querySnapshotManager.doneQuery(state);
    }

    public Boolean isRecoveryInProgress(Void unused)
    {
        return hasPendingRecovery();
    }

    public long getResumeCount()
    {
        return retryCount;
    }

    public boolean hasPendingRecovery()
    {
        if (retryTimer.isPresent()) {
            LOG.warn("Query recovery in progress..");
            return true;
        }
        return false;
    }
}
