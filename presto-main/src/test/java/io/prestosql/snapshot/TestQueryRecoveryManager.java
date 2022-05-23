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

import com.google.common.collect.ImmutableList;
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;

import static io.prestosql.SessionTestUtils.TEST_SNAPSHOT_SESSION;
import static io.prestosql.spi.StandardErrorCode.TOO_MANY_RESUMES;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.lang.Thread.sleep;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestQueryRecoveryManager
{
    private static final String SNAPSHOT_FILE_SYSTEM_DIR = "/tmp/test_recovery_manager";
    QueryId queryId;
    RecoveryConfig recoveryConfig;
    RecoveryUtils recoveryUtils;
    FileSystemClientManager fileSystemClientManager;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        // Set up snapshot config
        recoveryConfig = new RecoveryConfig();
        fileSystemClientManager = mock(FileSystemClientManager.class);
        when(fileSystemClientManager.getFileSystemClient(any(Path.class))).thenReturn(new HetuLocalFileSystemClient(new LocalConfig(new Properties()), Paths.get(SNAPSHOT_FILE_SYSTEM_DIR)));

        recoveryUtils = new RecoveryUtils(fileSystemClientManager, recoveryConfig, new InMemoryNodeManager());
        recoveryUtils.rootPath = SNAPSHOT_FILE_SYSTEM_DIR;
        recoveryUtils.initialize();
    }

    @AfterMethod
    public void teardown()
            throws Exception
    {
        // Cleanup files
        recoveryUtils.removeQuerySnapshotManager(queryId);
    }

    @Test
    public void testStartRecovery()
    {
        queryId = new QueryId("startrecovery");
        QueryRecoveryManager queryRecoveryManager = new QueryRecoveryManager(recoveryUtils, TEST_SNAPSHOT_SESSION, queryId);
        assertEquals(queryRecoveryManager.getState(), RecoveryState.DEFAULT);

        Runnable cancelToResume = mock(Runnable.class);
        queryRecoveryManager.setCancelToResumeCb(cancelToResume);
        queryRecoveryManager.startRecovery();
        verify(cancelToResume).run();
        assertEquals(queryRecoveryManager.getState(), RecoveryState.STOPPING_FOR_RESCHEDULE);
    }

    @Test
    public void testSuspendQuery()
    {
        queryId = new QueryId("suspend_query");
        QueryRecoveryManager recoveryManager = new QueryRecoveryManager(recoveryUtils, TEST_SNAPSHOT_SESSION, queryId);
        assertEquals(recoveryManager.getState(), RecoveryState.DEFAULT);

        Runnable cancelToResume = mock(Runnable.class);
        recoveryManager.setCancelToResumeCb(cancelToResume);
        recoveryManager.suspendQuery();
        verify(cancelToResume).run();
        assertEquals(recoveryManager.getState(), RecoveryState.SUSPENDED);
    }

    @Test
    public void testRescheduleSuccess()
    {
        recoverTestSuccess(false);
    }

    @Test
    public void testSuspendResumeSuccess()
    {
        recoverTestSuccess(true);
    }

    private void recoverTestSuccess(boolean useSuspendResume)
    {
        queryId = new QueryId("reschedule_success");

        // Create QuerySnapshotManager
        recoveryUtils.getOrCreateQuerySnapshotManager(queryId, TEST_SNAPSHOT_SESSION);
        QuerySnapshotManager querySnapshotManager = recoveryUtils.getQuerySnapshotManager(queryId);

        TaskId taskId1 = new TaskId(queryId.getId(), 0, 0);
        TaskId taskId2 = new TaskId(queryId.getId(), 1, 0);
        querySnapshotManager.addNewTask(taskId1);
        querySnapshotManager.addNewTask(taskId2);

        QueryRecoveryManager queryRecoveryManager = new QueryRecoveryManager(recoveryUtils, TEST_SNAPSHOT_SESSION, queryId);
        assertEquals(queryRecoveryManager.getState(), RecoveryState.DEFAULT);

        Runnable cancelToResume = mock(Runnable.class);
        Rescheduler rescheduler = mock(Rescheduler.class);
        WarningCollector warningCollector = mock(WarningCollector.class);
        queryRecoveryManager.setCancelToResumeCb(cancelToResume);
        queryRecoveryManager.setRescheduler(rescheduler, warningCollector);

        // capture snapshot 1
        querySnapshotManager.snapshotInitiated(1L);
        querySnapshotManager.updateFinishedQueryComponents(ImmutableList.of(taskId2));
        querySnapshotManager.updateQueryCapture(taskId1, Collections.singletonMap(1L, SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL)));

        // worker failure
        if (useSuspendResume) {
            queryRecoveryManager.suspendQuery();
            assertEquals(queryRecoveryManager.getState(), RecoveryState.SUSPENDED);
        }
        else {
            queryRecoveryManager.startRecovery();
            assertEquals(queryRecoveryManager.getState(), RecoveryState.STOPPING_FOR_RESCHEDULE);
        }

        querySnapshotManager.addNewTask(new TaskId(queryId.getId(), 0, 0));
        querySnapshotManager.addNewTask(new TaskId(queryId.getId(), 1, 0));
        // Initiate reschedule after stopping current execution
        try {
            if (useSuspendResume) {
                queryRecoveryManager.resumeQuery();
            }
            queryRecoveryManager.rescheduleQuery();
        }
        catch (Throwable t) {
            fail("Failed to reschedule");
        }
        assertEquals(queryRecoveryManager.getState(), RecoveryState.RESCHEDULING);
        try {
            verify(rescheduler).apply(OptionalLong.of(1L));
        }
        catch (Throwable t) {
            fail("Failed to verify");
        }
        assertEquals(queryRecoveryManager.getResumeCount(), 1);
        // Finish recovery
        assertTrue(queryRecoveryManager.hasPendingRecovery());
        SnapshotInfo restoreInfo = SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL);
        querySnapshotManager.updateQueryRestore(new TaskId(queryId.getId(), 0, 0), Optional.of(new RestoreResult(1, restoreInfo)));
        querySnapshotManager.updateQueryRestore(new TaskId(queryId.getId(), 1, 0), Optional.of(new RestoreResult(1, restoreInfo)));

        //check final state
        assertFalse(queryRecoveryManager.hasPendingRecovery());
        assertEquals(queryRecoveryManager.getState(), RecoveryState.DEFAULT);
    }

    @Test
    public void testRescheduleRetry()
    {
        queryId = new QueryId("reschedule_retry");

        // Create QuerySnapshotManager
        recoveryUtils.getOrCreateQuerySnapshotManager(queryId, TEST_SNAPSHOT_SESSION);
        QuerySnapshotManager querySnapshotManager = recoveryUtils.getQuerySnapshotManager(queryId);

        TaskId taskId1 = new TaskId(queryId.getId(), 0, 0);
        TaskId taskId2 = new TaskId(queryId.getId(), 1, 0);
        querySnapshotManager.addNewTask(taskId1);
        querySnapshotManager.addNewTask(taskId2);

        QueryRecoveryManager queryRecoveryManager = new QueryRecoveryManager(recoveryUtils, TEST_SNAPSHOT_SESSION, queryId);
        assertEquals(queryRecoveryManager.getState(), RecoveryState.DEFAULT);

        Runnable cancelToResume = mock(Runnable.class);
        Rescheduler rescheduler = mock(Rescheduler.class);
        WarningCollector warningCollector = mock(WarningCollector.class);
        queryRecoveryManager.setCancelToResumeCb(cancelToResume);
        queryRecoveryManager.setRescheduler(rescheduler, warningCollector);

        // capture snapshot 1
        querySnapshotManager.snapshotInitiated(1L);
        querySnapshotManager.updateFinishedQueryComponents(ImmutableList.of(taskId2));
        querySnapshotManager.updateQueryCapture(taskId1, Collections.singletonMap(1L, SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL)));

        // worker failure
        queryRecoveryManager.startRecovery();
        assertEquals(queryRecoveryManager.getState(), RecoveryState.STOPPING_FOR_RESCHEDULE);
        // Initiate reschedule after stopping current execution
        try {
            queryRecoveryManager.rescheduleQuery();
        }
        catch (Throwable t) {
            fail("Failed to reschedule");
        }
        assertEquals(queryRecoveryManager.getState(), RecoveryState.RESCHEDULING);
        try {
            verify(rescheduler).apply(OptionalLong.of(1L));
        }
        catch (Throwable t) {
            fail("Failed to verify");
        }
        assertEquals(queryRecoveryManager.getResumeCount(), 1);
        // Finish recovery
        assertTrue(queryRecoveryManager.hasPendingRecovery());

        querySnapshotManager.addNewTask(new TaskId(queryId.getId(), 0, 0));
        querySnapshotManager.addNewTask(new TaskId(queryId.getId(), 1, 0));
        SnapshotInfo restoreInfo = SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL);
        SnapshotInfo restoreFailedInfo = SnapshotInfo.withStatus(SnapshotResult.FAILED);
        querySnapshotManager.updateQueryRestore(new TaskId(queryId.getId(), 0, 0), Optional.of(new RestoreResult(1, restoreInfo)));
        querySnapshotManager.updateQueryRestore(new TaskId(queryId.getId(), 1, 0), Optional.of(new RestoreResult(1, restoreFailedInfo)));

        //check if retry is triggered
        assertEquals(queryRecoveryManager.getState(), RecoveryState.STOPPING_FOR_RESCHEDULE);

        // Initiate reschedule after stopping current execution
        try {
            queryRecoveryManager.rescheduleQuery();
        }
        catch (Throwable t) {
            fail("Failed to reschedule");
            fail();
        }

        assertEquals(queryRecoveryManager.getState(), RecoveryState.DEFAULT);
        assertEquals(queryRecoveryManager.getResumeCount(), 2);
        assertFalse(queryRecoveryManager.hasPendingRecovery());
    }

    @Test
    public void testMaxRetry()
    {
        queryId = new QueryId("max_recovery_retry");

        Session session = Session.builder(TEST_SNAPSHOT_SESSION)
                .setSystemProperty(SystemSessionProperties.RECOVERY_MAX_RETRIES, "1")
                .build();

        // Create QuerySnapshotManager
        recoveryUtils.getOrCreateQuerySnapshotManager(queryId, TEST_SNAPSHOT_SESSION);
        QuerySnapshotManager querySnapshotManager = recoveryUtils.getQuerySnapshotManager(queryId);

        TaskId taskId1 = new TaskId(queryId.getId(), 0, 0);
        TaskId taskId2 = new TaskId(queryId.getId(), 1, 0);
        querySnapshotManager.addNewTask(taskId1);
        querySnapshotManager.addNewTask(taskId2);

        QueryRecoveryManager queryRecoveryManager = new QueryRecoveryManager(recoveryUtils, session, queryId);
        assertEquals(queryRecoveryManager.getState(), RecoveryState.DEFAULT);

        Runnable cancelToResume = mock(Runnable.class);
        Rescheduler rescheduler = mock(Rescheduler.class);
        WarningCollector warningCollector = mock(WarningCollector.class);
        queryRecoveryManager.setCancelToResumeCb(cancelToResume);
        queryRecoveryManager.setRescheduler(rescheduler, warningCollector);

        // capture snapshot 1
        querySnapshotManager.snapshotInitiated(1L);
        querySnapshotManager.updateFinishedQueryComponents(ImmutableList.of(taskId2));
        querySnapshotManager.updateQueryCapture(taskId1, Collections.singletonMap(1L, SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL)));

        // worker failure
        queryRecoveryManager.startRecovery();
        assertEquals(queryRecoveryManager.getState(), RecoveryState.STOPPING_FOR_RESCHEDULE);
        // Initiate reschedule after stopping current execution
        try {
            queryRecoveryManager.rescheduleQuery();
        }
        catch (Throwable t) {
            fail("Failed to reschedule");
        }
        assertEquals(queryRecoveryManager.getState(), RecoveryState.RESCHEDULING);
        try {
            verify(rescheduler).apply(OptionalLong.of(1L));
        }
        catch (Throwable t) {
            fail("Failed to verify" + t);
        }
        assertEquals(queryRecoveryManager.getResumeCount(), 1);
        // Finish recovery
        assertTrue(queryRecoveryManager.hasPendingRecovery());

        querySnapshotManager.addNewTask(new TaskId(queryId.getId(), 0, 0));
        querySnapshotManager.addNewTask(new TaskId(queryId.getId(), 1, 0));
        SnapshotInfo restoreInfo = SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL);
        SnapshotInfo restoreFailedInfo = SnapshotInfo.withStatus(SnapshotResult.FAILED);
        querySnapshotManager.updateQueryRestore(new TaskId(queryId.getId(), 0, 0), Optional.of(new RestoreResult(1, restoreInfo)));
        querySnapshotManager.updateQueryRestore(new TaskId(queryId.getId(), 1, 0), Optional.of(new RestoreResult(1, restoreFailedInfo)));

        //check if retry is triggered
        assertEquals(queryRecoveryManager.getState(), RecoveryState.STOPPING_FOR_RESCHEDULE);

        // Initiate reschedule after stopping current execution
        try {
            queryRecoveryManager.rescheduleQuery();
        }
        catch (Throwable t) {
            if (t instanceof PrestoException) {
                PrestoException ex = (PrestoException) t;
                assertEquals(ex.getErrorCode(), TOO_MANY_RESUMES.toErrorCode());
            }
        }
    }

    @Test
    public void testRecoveryTimeout()
    {
        queryId = new QueryId("recovery_time_out");

        Session session = Session.builder(TEST_SNAPSHOT_SESSION)
                .setSystemProperty(SystemSessionProperties.RECOVERY_RETRY_TIMEOUT, "2s")
                .build();

        // Create QuerySnapshotManager
        recoveryUtils.getOrCreateQuerySnapshotManager(queryId, TEST_SNAPSHOT_SESSION);
        QuerySnapshotManager querySnapshotManager = recoveryUtils.getQuerySnapshotManager(queryId);

        TaskId taskId1 = new TaskId(queryId.getId(), 0, 0);
        TaskId taskId2 = new TaskId(queryId.getId(), 1, 0);
        querySnapshotManager.addNewTask(taskId1);
        querySnapshotManager.addNewTask(taskId2);

        QueryRecoveryManager queryRecoveryManager = new QueryRecoveryManager(recoveryUtils, session, queryId);
        assertEquals(queryRecoveryManager.getState(), RecoveryState.DEFAULT);

        Runnable cancelToResume = mock(Runnable.class);
        Rescheduler rescheduler = mock(Rescheduler.class);
        WarningCollector warningCollector = mock(WarningCollector.class);
        queryRecoveryManager.setCancelToResumeCb(cancelToResume);
        queryRecoveryManager.setRescheduler(rescheduler, warningCollector);

        // capture snapshot 1
        querySnapshotManager.snapshotInitiated(1L);
        querySnapshotManager.updateFinishedQueryComponents(ImmutableList.of(taskId2));
        querySnapshotManager.updateQueryCapture(taskId1, Collections.singletonMap(1L, SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL)));

        // worker failure
        queryRecoveryManager.startRecovery();
        assertEquals(queryRecoveryManager.getState(), RecoveryState.STOPPING_FOR_RESCHEDULE);
        // Initiate reschedule after stopping current execution
        try {
            queryRecoveryManager.rescheduleQuery();
        }
        catch (Throwable t) {
            fail("Failed to reschedule");
        }
        assertEquals(queryRecoveryManager.getState(), RecoveryState.RESCHEDULING);
        try {
            verify(rescheduler).apply(OptionalLong.of(1L));
        }
        catch (Throwable t) {
            fail("Failed to verify");
        }
        assertEquals(queryRecoveryManager.getResumeCount(), 1);
        // Finish recovery
        assertTrue(queryRecoveryManager.hasPendingRecovery());

        try {
            // Keep restore pending till timer expires
            sleep(2000);
        }
        catch (Throwable t) {
            fail("Failed to sleep for 2s!");
        }
        //check if retry is triggered
        assertEquals(queryRecoveryManager.getState(), RecoveryState.STOPPING_FOR_RESCHEDULE);
    }
}
