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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.execution.StageId;
import io.prestosql.execution.TaskId;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.prestosql.SessionTestUtils.TEST_SNAPSHOT_SESSION;
import static io.prestosql.spi.StandardErrorCode.TOO_MANY_RESUMES;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestQuerySnapshotManager
{
    private static final String SNAPSHOT_FILE_SYSTEM_DIR = "/tmp/test_snapshot_manager";

    SnapshotConfig snapshotConfig;
    FileSystemClientManager fileSystemClientManager;
    SnapshotUtils snapshotUtils;
    QueryId queryId;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        // Set up snapshot config
        snapshotConfig = new SnapshotConfig();

        // Set up mock file system client manager
        fileSystemClientManager = mock(FileSystemClientManager.class);
        when(fileSystemClientManager.getFileSystemClient(any(Path.class))).thenReturn(new HetuLocalFileSystemClient(new LocalConfig(new Properties()), Paths.get(SNAPSHOT_FILE_SYSTEM_DIR)));

        snapshotUtils = new SnapshotUtils(fileSystemClientManager, snapshotConfig, new InMemoryNodeManager());
        snapshotUtils.rootPath = SNAPSHOT_FILE_SYSTEM_DIR;
        snapshotUtils.initialize();
    }

    @AfterMethod
    public void teardown()
            throws Exception
    {
        // Cleanup files
        snapshotUtils.removeQuerySnapshotManager(queryId);
    }

    @Test
    public void testGetResumeSnapshotId()
    {
        queryId = new QueryId("resumeid");
        QuerySnapshotManager snapshotManager = new QuerySnapshotManager(queryId, snapshotUtils, TEST_SNAPSHOT_SESSION);
        TaskId taskId = new TaskId(queryId.getId(), 2, 3);
        SnapshotInfo info = SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL);

        // Try1: no id is available yet
        snapshotManager.addNewTask(taskId);
        OptionalLong sid = snapshotManager.getResumeSnapshotId();
        assertFalse(sid.isPresent());

        // Try2: setup some successful snapshots
        snapshotManager.addNewTask(taskId);
        snapshotManager.snapshotInitiated(1L);
        snapshotManager.updateQueryCapture(taskId, Collections.singletonMap(1L, info));
        snapshotManager.snapshotInitiated(2L);
        snapshotManager.updateQueryCapture(taskId, Collections.singletonMap(2L, info));
        sid = snapshotManager.getResumeSnapshotId();
        assertEquals(sid.getAsLong(), 2);

        // Try3: get available snapshot before 2
        snapshotManager.addNewTask(taskId);
        snapshotManager.updateQueryCapture(taskId, Collections.singletonMap(1L, info));
        snapshotManager.updateQueryCapture(taskId, Collections.singletonMap(2L, info));
        sid = snapshotManager.getResumeSnapshotId();
        assertEquals(sid.getAsLong(), 1);

        // Try4: get available snapshot before 1
        snapshotManager.addNewTask(taskId);
        snapshotManager.updateQueryCapture(taskId, Collections.singletonMap(1L, info));
        snapshotManager.updateQueryCapture(taskId, Collections.singletonMap(2L, info));
        sid = snapshotManager.getResumeSnapshotId();
        assertFalse(sid.isPresent());
    }

    @Test
    public void testResumeFailure()
    {
        queryId = new QueryId("resumefailure");
        QuerySnapshotManager snapshotManager = new QuerySnapshotManager(queryId, snapshotUtils, TEST_SNAPSHOT_SESSION);
        TaskId taskId = new TaskId(queryId.getId(), 2, 3);
        Runnable rescheduler = mock(Runnable.class);
        snapshotManager.setRescheduler(rescheduler);

        snapshotManager.addNewTask(taskId);
        snapshotManager.snapshotInitiated(1L);
        snapshotManager.updateQueryCapture(taskId, Collections.singletonMap(1L, SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL)));
        OptionalLong sid = snapshotManager.getResumeSnapshotId();
        snapshotManager.updateQueryRestore(taskId, Optional.of(new RestoreResult(sid.getAsLong(), SnapshotInfo.withStatus(SnapshotResult.FAILED))));

        verify(rescheduler).run();
    }

    @Test
    public void testResumeTimeout()
            throws Exception
    {
        queryId = new QueryId("resumetimeout");
        Session session = Session.builder(TEST_SNAPSHOT_SESSION)
                .setSystemProperty(SystemSessionProperties.SNAPSHOT_RETRY_TIMEOUT, "1ms")
                .build();
        QuerySnapshotManager snapshotManager = new QuerySnapshotManager(queryId, snapshotUtils, session);
        TaskId taskId = new TaskId(queryId.getId(), 2, 3);
        SettableFuture<?> future = SettableFuture.create();
        snapshotManager.setRescheduler(() -> future.set(null));

        snapshotManager.addNewTask(taskId);
        snapshotManager.snapshotInitiated(1L);
        snapshotManager.updateQueryCapture(taskId, Collections.singletonMap(1L, SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL)));
        snapshotManager.getResumeSnapshotId();

        future.get(1, TimeUnit.SECONDS);
        assertTrue(future.isDone());
    }

    @Test
    public void testResumeMaxRetries()
    {
        queryId = new QueryId("resumemaxretry");
        Session session = Session.builder(TEST_SNAPSHOT_SESSION)
                .setSystemProperty(SystemSessionProperties.SNAPSHOT_MAX_RETRIES, "1")
                .build();
        QuerySnapshotManager snapshotManager = new QuerySnapshotManager(queryId, snapshotUtils, session);
        TaskId taskId = new TaskId(queryId.getId(), 2, 3);
        Runnable rescheduler = mock(Runnable.class);
        snapshotManager.setRescheduler(rescheduler);

        snapshotManager.addNewTask(taskId);
        snapshotManager.snapshotInitiated(1L);
        snapshotManager.updateQueryCapture(taskId, Collections.singletonMap(1L, SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL)));
        snapshotManager.snapshotInitiated(2L);
        snapshotManager.updateQueryCapture(taskId, Collections.singletonMap(2L, SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL)));
        assertTrue(snapshotManager.getResumeSnapshotId().isPresent());
        try {
            snapshotManager.getResumeSnapshotId();
            fail();
        }
        catch (PrestoException e) {
            // Expected
            assertEquals(e.getErrorCode(), TOO_MANY_RESUMES.toErrorCode());
        }
    }

    @Test
    public void testUpdateQueryRestore()
    {
        queryId = new QueryId("updaterestore");
        QuerySnapshotManager snapshotManager = new QuerySnapshotManager(queryId, snapshotUtils, TEST_SNAPSHOT_SESSION);
        TaskId taskId = new TaskId(queryId.getId(), 2, 3);
        snapshotManager.addNewTask(taskId);
        snapshotManager.snapshotInitiated(1L);

        snapshotManager.updateQueryRestore(taskId, Optional.of(new RestoreResult(1, SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL))));
        assertEquals(snapshotManager.getQuerySnapshotRestoreResult().getSnapshotInfo().getSnapshotResult(), SnapshotResult.SUCCESSFUL);

        snapshotManager.snapshotInitiated(2L);
        snapshotManager.updateQueryRestore(taskId, Optional.of(new RestoreResult(2, SnapshotInfo.withStatus(SnapshotResult.FAILED))));
        assertEquals(snapshotManager.getQuerySnapshotRestoreResult().getSnapshotInfo().getSnapshotResult(), SnapshotResult.FAILED);

        snapshotManager.snapshotInitiated(3L);
        snapshotManager.updateQueryRestore(taskId, Optional.of(new RestoreResult(3, SnapshotInfo.withStatus(SnapshotResult.FAILED_FATAL))));
        assertEquals(snapshotManager.getQuerySnapshotRestoreResult().getSnapshotInfo().getSnapshotResult(), SnapshotResult.FAILED_FATAL);
    }

    @Test
    public void testUpdateFinishedQueryComponents()
    {
        queryId = new QueryId("updatefinished");
        QuerySnapshotManager snapshotManager = new QuerySnapshotManager(queryId, snapshotUtils, TEST_SNAPSHOT_SESSION);

        TaskId taskId1 = new TaskId(queryId.getId(), 2, 3);
        TaskId taskId2 = new TaskId(queryId.getId(), 3, 4);
        snapshotManager.addNewTask(taskId1);
        snapshotManager.addNewTask(taskId2);
        snapshotManager.snapshotInitiated(1L);

        snapshotManager.updateFinishedQueryComponents(ImmutableList.of(taskId2));
        snapshotManager.updateQueryCapture(taskId1, Collections.singletonMap(1L, SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL)));
        assertEquals(snapshotManager.getResumeSnapshotId().getAsLong(), 1);
    }

    @Test
    public void test2Resumes()
    {
        queryId = new QueryId("updatefinished");
        QuerySnapshotManager snapshotManager = new QuerySnapshotManager(queryId, snapshotUtils, TEST_SNAPSHOT_SESSION);

        TaskId taskId1 = new TaskId(queryId.getId(), 2, 3);
        snapshotManager.addNewTask(taskId1);
        snapshotManager.snapshotInitiated(1L);
        snapshotManager.updateQueryCapture(taskId1, Collections.singletonMap(1L, SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL)));

        assertTrue(snapshotManager.getResumeSnapshotId().isPresent());
        snapshotManager.invalidateAllSnapshots();
        assertFalse(snapshotManager.getResumeSnapshotId().isPresent());
        assertFalse(snapshotManager.hasPendingResume());
    }

    @Test
    public void testCaptureStats()
    {
        AtomicLong totalCpuTimeMillis = new AtomicLong(0L);
        AtomicLong allSnapshotsSizeBytes = new AtomicLong(0L);
        AtomicLong totalWallTimeMillis = new AtomicLong(0L);
        AtomicLong lastSnapshotId = new AtomicLong(0L);
        int stageCount = 3;
        int taskCountPerStage = 2;
        int finishedStageCount = 2;

        queryId = new QueryId("capturestats");
        QuerySnapshotManager snapshotManager = new QuerySnapshotManager(queryId, snapshotUtils, TEST_SNAPSHOT_SESSION);
        for (int i = 0; i < stageCount; i++) {
            for (int j = 0; j < taskCountPerStage; j++) {
                snapshotManager.addNewTask(new TaskId(queryId.getId(), i, j));
            }
        }
        snapshotManager.snapshotInitiated(1L);
        List<TaskId> finishedList = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < finishedStageCount; i++) {
            for (int j = 0; j < taskCountPerStage; j++) {
                finishedList.add(new TaskId(queryId.getId(), i, j));
            }
        }
        snapshotManager.updateFinishedQueryComponents(finishedList);
        SnapshotInfo captureInfo = SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL);
        captureInfo.updateCpuTime(10);
        captureInfo.updateSizeBytes(1000);
        // Finish snapshot 1 capture
        snapshotManager.updateQueryCapture(new TaskId(queryId.getId(), 2, 0), Collections.singletonMap(1L, captureInfo));
        snapshotManager.updateQueryCapture(new TaskId(queryId.getId(), 2, 1), Collections.singletonMap(1L, captureInfo));

        List<Long> capturedSnapshots = new ArrayList<Long>();
        List<Long> capturingSnapshots = new ArrayList<Long>();
        Map<Long, SnapshotInfo> captureResults = snapshotManager.getCaptureResults();
        captureResults.forEach((snapshotId, snapshotInfo) -> {
            if (snapshotId > 0 && snapshotInfo.isCompleteSnapshot()) {
                if (snapshotId.compareTo(lastSnapshotId.get()) > 0) {
                    // Get last successful snapshot id
                    lastSnapshotId.set(snapshotId);
                }
                long wallTime = snapshotInfo.getEndTime() - snapshotInfo.getBeginTime();
                capturedSnapshots.add(snapshotId);
                allSnapshotsSizeBytes.addAndGet(snapshotInfo.getSizeBytes());
                totalWallTimeMillis.addAndGet(wallTime);
                totalCpuTimeMillis.addAndGet(snapshotInfo.getCpuTime());
            }
            else if (snapshotInfo.getSnapshotResult() == SnapshotResult.IN_PROGRESS) {
                capturingSnapshots.add(snapshotId);
            }
        });

        assertEquals(lastSnapshotId.get(), 1);
        assertEquals(allSnapshotsSizeBytes.get(), 2000);
        assertEquals(totalCpuTimeMillis.get(), 20);
        assertEquals(capturedSnapshots.size(), 1);
        assertEquals(capturingSnapshots.size(), 0);

        SnapshotInfo lastSnapshotInfo = captureResults.get(lastSnapshotId.get());
        long wallTime = lastSnapshotInfo.getEndTime() - lastSnapshotInfo.getBeginTime();

        // Only 1 snapshot, all and last stats should be same
        assertEquals(lastSnapshotInfo.getSizeBytes(), allSnapshotsSizeBytes.get());
        assertEquals(lastSnapshotInfo.getCpuTime(), totalCpuTimeMillis.get());
        assertEquals(wallTime, totalWallTimeMillis.get());
    }

    @Test
    public void testRestoreStats()
    {
        int restoreCount = 0;
        long lastRestoreSnapshotId = 0;
        AtomicLong totalCpuTimeMillis = new AtomicLong(0L);
        AtomicLong allSnapshotsSizeBytes = new AtomicLong(0L);
        AtomicLong totalWallTimeMillis = new AtomicLong(0L);
        AtomicLong totalRestoreWallTime = new AtomicLong(0L);
        AtomicLong totalRestoreCpuTime = new AtomicLong(0L);
        AtomicLong totalRestoreSize = new AtomicLong(0L);
        AtomicLong lastSnapshotId = new AtomicLong(0L);
        int stageCount = 1;
        int taskCountPerStage = 3;

        queryId = new QueryId("restorestats");
        QuerySnapshotManager snapshotManager = new QuerySnapshotManager(queryId, snapshotUtils, TEST_SNAPSHOT_SESSION);
        for (int i = 0; i < stageCount; i++) {
            for (int j = 0; j < taskCountPerStage; j++) {
                snapshotManager.addNewTask(new TaskId(queryId.getId(), i, j));
            }
        }

        snapshotManager.snapshotInitiated(1L);
        SnapshotInfo captureInfo = SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL);
        captureInfo.updateCpuTime(10);
        captureInfo.updateSizeBytes(1000);
        // Finish snapshot 1 capture
        for (int i = 0; i < stageCount; i++) {
            for (int j = 0; j < taskCountPerStage; j++) {
                snapshotManager.updateQueryCapture(new TaskId(queryId.getId(), i, j), Collections.singletonMap(1L, captureInfo));
            }
        }

        List<Long> capturedSnapshots = new ArrayList<Long>();
        List<Long> capturingSnapshots = new ArrayList<Long>();
        Map<Long, SnapshotInfo> captureResults = snapshotManager.getCaptureResults();
        captureResults.forEach((snapshotId, snapshotInfo) -> {
            if (snapshotId > 0 && snapshotInfo.isCompleteSnapshot()) {
                if (snapshotId.compareTo(lastSnapshotId.get()) > 0) {
                    // Get last successful snapshot id
                    lastSnapshotId.set(snapshotId);
                }
                long wallTime = snapshotInfo.getEndTime() - snapshotInfo.getBeginTime();
                capturedSnapshots.add(snapshotId);
                allSnapshotsSizeBytes.addAndGet(snapshotInfo.getSizeBytes());
                totalWallTimeMillis.addAndGet(wallTime);
                totalCpuTimeMillis.addAndGet(snapshotInfo.getCpuTime());
            }
            else if (snapshotInfo.getSnapshotResult() == SnapshotResult.IN_PROGRESS) {
                capturingSnapshots.add(snapshotId);
            }
        });

        assertEquals(lastSnapshotId.get(), 1);
        assertEquals(allSnapshotsSizeBytes.get(), 3000);
        assertEquals(totalCpuTimeMillis.get(), 30);

        SnapshotInfo lastSnapshotInfo = captureResults.get(lastSnapshotId.get());
        long wallTime = lastSnapshotInfo.getEndTime() - lastSnapshotInfo.getBeginTime();

        // Only 1 snapshot, all and last stats should be same
        assertEquals(lastSnapshotInfo.getSizeBytes(), allSnapshotsSizeBytes.get());
        assertEquals(lastSnapshotInfo.getCpuTime(), totalCpuTimeMillis.get());
        assertEquals(wallTime, totalWallTimeMillis.get());
        assertEquals(capturedSnapshots.size(), 1);
        assertEquals(capturingSnapshots.size(), 0);

        long resumeSnapshotId = snapshotManager.getResumeSnapshotId().getAsLong();
        assertEquals(resumeSnapshotId, 1);

        for (int i = 0; i < stageCount; i++) {
            for (int j = 0; j < taskCountPerStage; j++) {
                snapshotManager.addNewTask(new TaskId(queryId.getId(), i, j));
            }
        }
        // Initiate restore
        SnapshotInfo restoreInfo = SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL);
        restoreInfo.updateCpuTime(2);
        restoreInfo.updateSizeBytes(1000);
        for (int i = 0; i < stageCount; i++) {
            for (int j = 0; j < taskCountPerStage; j++) {
                snapshotManager.updateQueryRestore(new TaskId(queryId.getId(), i, j), Optional.of(new RestoreResult(1, restoreInfo)));
            }
        }

        List<RestoreResult> restoreStats = snapshotManager.getRestoreStats();
        restoreCount = restoreStats.size();
        assertEquals(restoreCount, 1);
        lastRestoreSnapshotId = restoreStats.get(restoreCount - 1).getSnapshotId();
        restoreStats.forEach(restoreResult -> {
            SnapshotInfo info = restoreResult.getSnapshotInfo();
            totalRestoreWallTime.addAndGet(info.getEndTime() - info.getBeginTime());
            totalRestoreCpuTime.addAndGet(info.getCpuTime());
            totalRestoreSize.addAndGet(info.getSizeBytes());
        });
        assertEquals(lastRestoreSnapshotId, 1L);
        assertEquals(totalRestoreSize.get(), 3000);
        assertEquals(totalRestoreCpuTime.get(), 6);
    }

    @Test
    public void testStageCaptureCompleteCallbacks()
    {
        AtomicLong callbackCount = new AtomicLong();
        AtomicLong totalCpuTimeMillis = new AtomicLong(0L);
        AtomicLong allSnapshotsSizeBytes = new AtomicLong(0L);
        AtomicLong totalWallTimeMillis = new AtomicLong(0L);
        AtomicLong lastSnapshotId = new AtomicLong(0L);

        queryId = new QueryId("stagecapturecallback");
        StageId stageId0 = new StageId(queryId, 0);
        StageId stageId1 = new StageId(queryId, 1);
        StageId stageId2 = new StageId(queryId, 2);

        QuerySnapshotManager snapshotManager = new QuerySnapshotManager(queryId, snapshotUtils, TEST_SNAPSHOT_SESSION);
        TaskId taskId00 = new TaskId(queryId.getId(), 0, 0);
        TaskId taskId01 = new TaskId(queryId.getId(), 0, 1);
        TaskId taskId10 = new TaskId(queryId.getId(), 1, 0);
        TaskId taskId11 = new TaskId(queryId.getId(), 1, 1);
        TaskId taskId20 = new TaskId(queryId.getId(), 2, 0);
        TaskId taskId21 = new TaskId(queryId.getId(), 2, 1);
        snapshotManager.addNewTask(taskId00);
        snapshotManager.addNewTask(taskId01);
        snapshotManager.addNewTask(taskId10);
        snapshotManager.addNewTask(taskId11);
        snapshotManager.addNewTask(taskId20);
        snapshotManager.addNewTask(taskId21);

        SnapshotInfo captureInfo = SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL);
        captureInfo.updateCpuTime(10);
        captureInfo.updateSizeBytes(1000);

        snapshotManager.setStageCompleteListener(stageId0, (isCapture, snapshotId) -> {
            if (isCapture && snapshotId == 1) {
                callbackCount.getAndIncrement();
            }
        });
        snapshotManager.setStageCompleteListener(stageId1, (isCapture, snapshotId) -> {
            if (isCapture && snapshotId == 1) {
                callbackCount.getAndIncrement();
            }
        });
        snapshotManager.setStageCompleteListener(stageId2, (isCapture, snapshotId) -> {
            if (isCapture && snapshotId == 1) {
                callbackCount.getAndIncrement();
            }
        });

        // Finish snapshot 1 capture
        snapshotManager.snapshotInitiated(1L);
        snapshotManager.updateFinishedQueryComponents(ImmutableList.of(taskId00, taskId10, taskId20));
        snapshotManager.updateQueryCapture(taskId01, Collections.singletonMap(1L, SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL)));
        snapshotManager.updateQueryCapture(taskId11, Collections.singletonMap(1L, SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL)));
        snapshotManager.updateQueryCapture(taskId21, Collections.singletonMap(1L, SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL)));

        // Check if all callbacks received with correct state and id
        assertEquals(callbackCount.get(), 3);
        callbackCount.set(0);

        List<Long> capturedSnapshots = new ArrayList<Long>();
        List<Long> capturingSnapshots = new ArrayList<Long>();
        Map<Long, SnapshotInfo> captureResults = snapshotManager.getCaptureResults();
        captureResults.forEach((snapshotId, snapshotInfo) -> {
            if (snapshotId > 0 && snapshotInfo.isCompleteSnapshot()) {
                if (snapshotId.compareTo(lastSnapshotId.get()) > 0) {
                    // Get last successful snapshot id
                    lastSnapshotId.set(snapshotId);
                }
                long wallTime = snapshotInfo.getEndTime() - snapshotInfo.getBeginTime();
                capturedSnapshots.add(snapshotId);
                allSnapshotsSizeBytes.addAndGet(snapshotInfo.getSizeBytes());
                totalWallTimeMillis.addAndGet(wallTime);
                totalCpuTimeMillis.addAndGet(snapshotInfo.getCpuTime());
            }
            else if (snapshotInfo.getSnapshotResult() == SnapshotResult.IN_PROGRESS) {
                capturingSnapshots.add(snapshotId);
            }
        });
        assertEquals(lastSnapshotId.get(), 1);
        assertEquals(allSnapshotsSizeBytes.get(), 0);
        assertEquals(totalCpuTimeMillis.get(), 0);
    }

    @Test
    public void testStageRestoreCompleteCallbacks()
    {
        AtomicLong captureCallbackCount = new AtomicLong();
        AtomicLong restoreCallbackCount = new AtomicLong();
        AtomicLong totalCpuTimeMillis = new AtomicLong(0L);
        AtomicLong allSnapshotsSizeBytes = new AtomicLong(0L);
        AtomicLong totalWallTimeMillis = new AtomicLong(0L);
        AtomicLong lastSnapshotId = new AtomicLong(0L);
        AtomicLong totalRestoreWallTime = new AtomicLong(0L);
        AtomicLong totalRestoreCpuTime = new AtomicLong(0L);
        AtomicLong totalRestoreSize = new AtomicLong(0L);

        queryId = new QueryId("stagerestorecallback");
        StageId stageId0 = new StageId(queryId, 0);
        StageId stageId1 = new StageId(queryId, 1);
        StageId stageId2 = new StageId(queryId, 2);

        QuerySnapshotManager snapshotManager = new QuerySnapshotManager(queryId, snapshotUtils, TEST_SNAPSHOT_SESSION);
        TaskId taskId00 = new TaskId(queryId.getId(), 0, 0);
        TaskId taskId01 = new TaskId(queryId.getId(), 0, 1);
        TaskId taskId10 = new TaskId(queryId.getId(), 1, 0);
        TaskId taskId11 = new TaskId(queryId.getId(), 1, 1);
        TaskId taskId20 = new TaskId(queryId.getId(), 2, 0);
        TaskId taskId21 = new TaskId(queryId.getId(), 2, 1);
        snapshotManager.addNewTask(taskId00);
        snapshotManager.addNewTask(taskId01);
        snapshotManager.addNewTask(taskId10);
        snapshotManager.addNewTask(taskId11);
        snapshotManager.addNewTask(taskId20);
        snapshotManager.addNewTask(taskId21);

        SnapshotInfo captureInfo = SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL);
        captureInfo.updateCpuTime(10);
        captureInfo.updateSizeBytes(1000);

        snapshotManager.setStageCompleteListener(stageId0, (isCapture, snapshotId) -> {
            if (isCapture) {
                captureCallbackCount.getAndIncrement();
            }
            else {
                restoreCallbackCount.getAndIncrement();
            }
        });
        snapshotManager.setStageCompleteListener(stageId1, (isCapture, snapshotId) -> {
            if (isCapture) {
                captureCallbackCount.getAndIncrement();
            }
            else {
                restoreCallbackCount.getAndIncrement();
            }
        });
        snapshotManager.setStageCompleteListener(stageId2, (isCapture, snapshotId) -> {
            if (isCapture) {
                captureCallbackCount.getAndIncrement();
            }
            else {
                restoreCallbackCount.getAndIncrement();
            }
        });

        // Finish snapshot 1 capture
        snapshotManager.snapshotInitiated(1L);
        snapshotManager.updateFinishedQueryComponents(ImmutableList.of(taskId00, taskId01, taskId10, taskId11, taskId20));
        snapshotManager.updateQueryCapture(taskId21, Collections.singletonMap(1L, SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL)));

        // Check if all callbacks received with correct state and id
        assertEquals(captureCallbackCount.get(), 1);
        assertEquals(restoreCallbackCount.get(), 0);
        captureCallbackCount.set(0);

        List<Long> capturedSnapshots = new ArrayList<Long>();
        List<Long> capturingSnapshots = new ArrayList<Long>();
        Map<Long, SnapshotInfo> captureResults = snapshotManager.getCaptureResults();
        captureResults.forEach((snapshotId, snapshotInfo) -> {
            if (snapshotId > 0 && snapshotInfo.isCompleteSnapshot()) {
                if (snapshotId.compareTo(lastSnapshotId.get()) > 0) {
                    // Get last successful snapshot id
                    lastSnapshotId.set(snapshotId);
                }
                long wallTime = snapshotInfo.getEndTime() - snapshotInfo.getBeginTime();
                capturedSnapshots.add(snapshotId);
                allSnapshotsSizeBytes.addAndGet(snapshotInfo.getSizeBytes());
                totalWallTimeMillis.addAndGet(wallTime);
                totalCpuTimeMillis.addAndGet(snapshotInfo.getCpuTime());
            }
            else if (snapshotInfo.getSnapshotResult() == SnapshotResult.IN_PROGRESS) {
                capturingSnapshots.add(snapshotId);
            }
        });
        assertEquals(lastSnapshotId.get(), 1);
        assertEquals(allSnapshotsSizeBytes.get(), 0);
        assertEquals(totalCpuTimeMillis.get(), 0);

        OptionalLong resumeSnapshotId = snapshotManager.getResumeSnapshotId();
        assertEquals(resumeSnapshotId.getAsLong(), 1);
        // Add tasks which need to be restored
        snapshotManager.addNewTask(taskId00);
        snapshotManager.addNewTask(taskId01);
        snapshotManager.addNewTask(taskId10);
        snapshotManager.addNewTask(taskId11);
        snapshotManager.addNewTask(taskId20);
        snapshotManager.addNewTask(taskId21);

        // Set callbacks before restoring
        snapshotManager.setStageCompleteListener(stageId0, (isCapture, snapshotId) -> {
            if (isCapture) {
                captureCallbackCount.getAndIncrement();
            }
            else {
                restoreCallbackCount.getAndIncrement();
            }
        });
        snapshotManager.setStageCompleteListener(stageId1, (isCapture, snapshotId) -> {
            if (isCapture) {
                captureCallbackCount.getAndIncrement();
            }
            else {
                restoreCallbackCount.getAndIncrement();
            }
        });
        snapshotManager.setStageCompleteListener(stageId2, (isCapture, snapshotId) -> {
            if (isCapture) {
                captureCallbackCount.getAndIncrement();
            }
            else {
                restoreCallbackCount.getAndIncrement();
            }
        });

        snapshotManager.updateFinishedQueryComponents(ImmutableList.of(taskId00, taskId10, taskId20));
        SnapshotInfo restoreInfo = SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL);
        restoreInfo.updateCpuTime(2);
        restoreInfo.updateSizeBytes(1000);

        RestoreResult taskResult = new RestoreResult(resumeSnapshotId.getAsLong(), restoreInfo);

        snapshotManager.updateQueryRestore(taskId01, Optional.of(taskResult));
        snapshotManager.updateQueryRestore(taskId11, Optional.of(taskResult));
        snapshotManager.updateQueryRestore(taskId21, Optional.of(taskResult));

        assertEquals(captureCallbackCount.get(), 0);
        assertEquals(restoreCallbackCount.get(), 3);

        List<RestoreResult> restoreStats = snapshotManager.getRestoreStats();
        int restoreCount = restoreStats.size();
        assertEquals(restoreCount, 1);

        List<Long> restoredSnapshotList = new ArrayList<Long>();
        restoreStats.forEach(restoreResult -> {
            SnapshotInfo info = restoreResult.getSnapshotInfo();
            // Wall Time and CPU Time
            totalRestoreWallTime.addAndGet(info.getEndTime() - info.getBeginTime());
            totalRestoreCpuTime.addAndGet(info.getCpuTime());
            totalRestoreSize.addAndGet(info.getSizeBytes());
            // Collect list of restored snapshots
            if (restoreResult.getSnapshotId() > 0) {
                restoredSnapshotList.add(restoreResult.getSnapshotId());
            }
        });

        assertEquals(restoredSnapshotList.size(), 1);
        assertEquals(restoredSnapshotList.get(0).longValue(), 1L);
        assertEquals(totalRestoreCpuTime.get(), 6);
        assertEquals(totalRestoreSize.get(), 3000);
    }
}
