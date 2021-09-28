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
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static io.prestosql.SessionTestUtils.TEST_SNAPSHOT_SESSION;
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

        // Try1: no id is available yet
        snapshotManager.addNewTask(taskId);
        OptionalLong sid = snapshotManager.getResumeSnapshotId();
        assertFalse(sid.isPresent());

        // Try2: setup some successful snapshots
        snapshotManager.addNewTask(taskId);
        snapshotManager.updateQueryCapture(taskId, Collections.singletonMap(1L, SnapshotResult.SUCCESSFUL));
        snapshotManager.updateQueryCapture(taskId, Collections.singletonMap(2L, SnapshotResult.SUCCESSFUL));
        sid = snapshotManager.getResumeSnapshotId();
        assertEquals(sid.getAsLong(), 2);

        // Try3: get available snapshot before 2
        snapshotManager.addNewTask(taskId);
        snapshotManager.updateQueryCapture(taskId, Collections.singletonMap(1L, SnapshotResult.SUCCESSFUL));
        snapshotManager.updateQueryCapture(taskId, Collections.singletonMap(2L, SnapshotResult.SUCCESSFUL));
        sid = snapshotManager.getResumeSnapshotId();
        assertEquals(sid.getAsLong(), 1);

        // Try4: get available snapshot before 1
        snapshotManager.addNewTask(taskId);
        snapshotManager.updateQueryCapture(taskId, Collections.singletonMap(1L, SnapshotResult.SUCCESSFUL));
        snapshotManager.updateQueryCapture(taskId, Collections.singletonMap(2L, SnapshotResult.SUCCESSFUL));
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
        snapshotManager.updateQueryCapture(taskId, Collections.singletonMap(1L, SnapshotResult.SUCCESSFUL));
        OptionalLong sid = snapshotManager.getResumeSnapshotId();
        snapshotManager.updateQueryRestore(taskId, Optional.of(new RestoreResult(sid.getAsLong(), SnapshotResult.FAILED)));

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
        snapshotManager.updateQueryCapture(taskId, Collections.singletonMap(1L, SnapshotResult.SUCCESSFUL));
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
        snapshotManager.updateQueryCapture(taskId, Collections.singletonMap(1L, SnapshotResult.SUCCESSFUL));
        snapshotManager.updateQueryCapture(taskId, Collections.singletonMap(2L, SnapshotResult.SUCCESSFUL));
        assertTrue(snapshotManager.getResumeSnapshotId().isPresent());
        try {
            snapshotManager.getResumeSnapshotId();
            fail();
        }
        catch (PrestoException e) {
            // Expected
        }
    }

    @Test
    public void testUpdateQueryRestore()
    {
        queryId = new QueryId("updaterestore");
        QuerySnapshotManager snapshotManager = new QuerySnapshotManager(queryId, snapshotUtils, TEST_SNAPSHOT_SESSION);
        TaskId taskId = new TaskId(queryId.getId(), 2, 3);
        snapshotManager.addNewTask(taskId);

        snapshotManager.updateQueryRestore(taskId, Optional.of(new RestoreResult(1, SnapshotResult.SUCCESSFUL)));
        assertEquals(snapshotManager.getQuerySnapshotRestoreResult().getSnapshotResult(), SnapshotResult.SUCCESSFUL);

        snapshotManager.updateQueryRestore(taskId, Optional.of(new RestoreResult(2, SnapshotResult.FAILED)));
        assertEquals(snapshotManager.getQuerySnapshotRestoreResult().getSnapshotResult(), SnapshotResult.FAILED);

        snapshotManager.updateQueryRestore(taskId, Optional.of(new RestoreResult(3, SnapshotResult.FAILED_FATAL)));
        assertEquals(snapshotManager.getQuerySnapshotRestoreResult().getSnapshotResult(), SnapshotResult.FAILED_FATAL);
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

        snapshotManager.updateFinishedQueryComponents(ImmutableList.of(taskId2));
        snapshotManager.updateQueryCapture(taskId1, Collections.singletonMap(1L, SnapshotResult.SUCCESSFUL));
        assertEquals(snapshotManager.getResumeSnapshotId().getAsLong(), 1);
    }

    @Test
    public void test2Resumes()
    {
        queryId = new QueryId("updatefinished");
        QuerySnapshotManager snapshotManager = new QuerySnapshotManager(queryId, snapshotUtils, TEST_SNAPSHOT_SESSION);

        TaskId taskId1 = new TaskId(queryId.getId(), 2, 3);
        snapshotManager.addNewTask(taskId1);
        snapshotManager.updateQueryCapture(taskId1, Collections.singletonMap(1L, SnapshotResult.SUCCESSFUL));

        assertTrue(snapshotManager.getResumeSnapshotId().isPresent());
        snapshotManager.invalidateAllSnapshots();
        assertFalse(snapshotManager.getResumeSnapshotId().isPresent());
        assertFalse(snapshotManager.hasPendingResume());
    }
}
