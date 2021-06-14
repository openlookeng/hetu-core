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
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.prestosql.execution.StageId;
import io.prestosql.execution.TaskId;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.operator.Operator;
import io.prestosql.spi.QueryId;
import io.prestosql.testing.assertions.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

import static io.prestosql.SessionTestUtils.TEST_SNAPSHOT_SESSION;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestTaskSnapshotManager
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
    public void TestStoreAndLoad()
            throws Exception
    {
        queryId = new QueryId("saveandload");
        TaskId taskId1 = new TaskId(queryId.getId(), 1, 2);
        TaskSnapshotManager snapshotManager = new TaskSnapshotManager(taskId1, snapshotUtils);

        // Test operator state
        MockState operatorState = new MockState("operator-state");
        SnapshotStateId operatorStateId = SnapshotStateId.forOperator(1L, taskId1, 3, 4, 5);
        snapshotManager.storeState(operatorStateId, operatorState);
        MockState newOperatorState = (MockState) snapshotManager.loadState(operatorStateId).get();
        Assert.assertEquals(operatorState.getState(), newOperatorState.getState());

        // Test task state
        MockState taskState = new MockState("task-state");
        TaskId taskId2 = new TaskId(queryId.getId(), 3, 4);
        SnapshotStateId taskStateId = SnapshotStateId.forOperator(3L, taskId2, 5, 6, 7);
        snapshotManager.storeState(taskStateId, taskState);
        MockState newTaskState = (MockState) snapshotManager.loadState(taskStateId).get();
        Assert.assertEquals(taskState.getState(), newTaskState.getState());
    }

    @Test
    public void TestLoadBacktrack()
            throws Exception
    {
        queryId = new QueryId("loadbacktrack");
        StageId stageId = new StageId(queryId, 0);
        TaskId taskId = new TaskId(stageId, 0);
        TaskSnapshotManager snapshotManager = new TaskSnapshotManager(taskId, snapshotUtils);

        // Save operator state
        MockState state = new MockState("state");
        SnapshotStateId stateId = SnapshotStateId.forOperator(1L, taskId, 3, 4, 5);
        snapshotManager.storeState(stateId, state);

        // Make snapshot manager think that snapshot #1 is complete
        QuerySnapshotManager querySnapshotManager = new QuerySnapshotManager(queryId, snapshotUtils, TEST_SNAPSHOT_SESSION);
        querySnapshotManager.addNewTask(taskId);
        querySnapshotManager.updateQueryCapture(taskId, Collections.singletonMap(1L, SnapshotResult.SUCCESSFUL));
        querySnapshotManager.getResumeSnapshotId();

        SnapshotStateId newStateId = stateId.withSnapshotId(2);
        Optional<Object> loadedState = snapshotManager.loadState(newStateId);
        // Should return state saved for snapshot #1
        assertTrue(loadedState.isPresent());
        Assert.assertEquals(((MockState) loadedState.get()).getState(), state.getState());
    }

    @Test
    public void testStoreAndLoadFile()
            throws Exception
    {
        queryId = new QueryId("file");
        TaskId taskId = new TaskId(queryId.getId(), 1, 5);
        TaskSnapshotManager snapshotManager = new TaskSnapshotManager(taskId, snapshotUtils);

        // Create a file
        Path sourcePath = Paths.get(SNAPSHOT_FILE_SYSTEM_DIR + "/source/spill-test.txt");
        sourcePath.getParent().toFile().mkdirs();

        String fileContent = "Spill Contents";
        FileWriter fileWriter = new FileWriter(SNAPSHOT_FILE_SYSTEM_DIR + "/source/spill-test.txt");
        fileWriter.write(fileContent);
        fileWriter.close();

        SnapshotStateId snapshotStateId = new SnapshotStateId(1, taskId, "component1");
        snapshotManager.storeFile(snapshotStateId, sourcePath);

        Path targetPath = Paths.get(SNAPSHOT_FILE_SYSTEM_DIR + "/target/spill-test.txt");
        assertTrue(snapshotManager.loadFile(snapshotStateId, targetPath));

        String output = Files.readAllLines(targetPath).get(0);
        Assert.assertEquals(output, fileContent);
    }

    @Test
    public void testStoreAndLoadFileBacktrack()
            throws Exception
    {
        queryId = new QueryId("filebacktrack");
        TaskId taskId = new TaskId(queryId.getId(), 2, 3);
        TaskSnapshotManager snapshotManager = new TaskSnapshotManager(taskId, snapshotUtils);

        // Create a file
        Path sourcePath = Paths.get(SNAPSHOT_FILE_SYSTEM_DIR + "/source/spill-test.txt");
        sourcePath.getParent().toFile().mkdirs();

        String fileContent = "Spill Contents";
        FileWriter fileWriter = new FileWriter(SNAPSHOT_FILE_SYSTEM_DIR + "/source/spill-test.txt");
        fileWriter.write(fileContent);
        fileWriter.close();

        QuerySnapshotManager querySnapshotManager = new QuerySnapshotManager(queryId, snapshotUtils, TEST_SNAPSHOT_SESSION);
        querySnapshotManager.addNewTask(taskId);

        SnapshotStateId id4save = new SnapshotStateId(2, taskId, "component1");
        snapshotManager.storeFile(id4save, sourcePath);

        SnapshotStateId id4load = new SnapshotStateId(3, taskId, "component1");
        Path targetPath = Paths.get(SNAPSHOT_FILE_SYSTEM_DIR + "/target/spill-test.txt");

        // Try1: Previous snapshot ids not setup, so load fails
        assertNull(snapshotManager.loadFile(id4load, targetPath));

        // Try2: Previous snapshots are setup, so load should be successful
        querySnapshotManager.updateQueryCapture(taskId, Collections.singletonMap(2L, SnapshotResult.SUCCESSFUL));
        querySnapshotManager.getResumeSnapshotId();
        assertTrue(snapshotManager.loadFile(id4load, targetPath));

        String output = Files.readAllLines(targetPath).get(0);
        Assert.assertEquals(output, fileContent);

        // Try3: Previous snapshot failed
        querySnapshotManager.updateQueryRestore(taskId, Optional.of(new RestoreResult(2, SnapshotResult.SUCCESSFUL)));
        querySnapshotManager.updateQueryCapture(taskId, Collections.singletonMap(2L, SnapshotResult.FAILED));
        querySnapshotManager.updateQueryCapture(taskId, Collections.singletonMap(3L, SnapshotResult.SUCCESSFUL));
        querySnapshotManager.getResumeSnapshotId();
        assertFalse(snapshotManager.loadFile(id4load, targetPath));
    }

    @Test
    public void testCapture()
            throws Exception
    {
        TaskId taskId1 = new TaskId("query", 1, 1);
        TaskId taskId2 = new TaskId("query", 1, 2);
        TaskSnapshotManager snapshotManager1 = new TaskSnapshotManager(taskId1, snapshotUtils);
        TaskSnapshotManager snapshotManager2 = new TaskSnapshotManager(taskId2, snapshotUtils);
        snapshotManager1.setTotalComponents(2);
        snapshotManager2.setTotalComponents(2);

        // Test capture successfully
        snapshotManager1.succeededToCapture(new SnapshotStateId(1, taskId1, "component1"));
        Assert.assertEquals(snapshotManager1.getSnapshotCaptureResult().get(1L), SnapshotResult.IN_PROGRESS);
        snapshotManager1.succeededToCapture(new SnapshotStateId(1, taskId1, "component2"));
        Assert.assertEquals(snapshotManager1.getSnapshotCaptureResult().get(1L), SnapshotResult.SUCCESSFUL);

        // Test capture failed
        snapshotManager2.failedToCapture(new SnapshotStateId(1, taskId2, "component1"));
        Assert.assertEquals(snapshotManager2.getSnapshotCaptureResult().get(1L), SnapshotResult.IN_PROGRESS_FAILED);
        snapshotManager2.failedToCapture(new SnapshotStateId(1, taskId2, "component2"));
        Assert.assertEquals(snapshotManager2.getSnapshotCaptureResult().get(1L), SnapshotResult.FAILED);
    }

    @Test
    public void testRestore()
            throws Exception
    {
        TaskId taskId1 = new TaskId("query", 1, 1);
        TaskId taskId2 = new TaskId("query", 1, 2);
        TaskSnapshotManager snapshotManager1 = new TaskSnapshotManager(taskId1, snapshotUtils);
        TaskSnapshotManager snapshotManager2 = new TaskSnapshotManager(taskId2, snapshotUtils);
        snapshotManager1.setTotalComponents(2);
        snapshotManager2.setTotalComponents(3);

        // Test restore successfully
        snapshotManager1.succeededToRestore(new SnapshotStateId(1, taskId1, "component1"));
        RestoreResult result = snapshotManager1.getSnapshotRestoreResult();
        Assert.assertEquals(result.getSnapshotId(), 1L);
        Assert.assertEquals(result.getSnapshotResult(), SnapshotResult.IN_PROGRESS);
        snapshotManager1.succeededToRestore(new SnapshotStateId(1, taskId1, "component2"));
        Assert.assertEquals(snapshotManager1.getSnapshotRestoreResult().getSnapshotResult(), SnapshotResult.SUCCESSFUL);

        // Test restore failed
        try {
            snapshotManager2.failedToRestore(new SnapshotStateId(1, taskId2, "component1"), false);
        }
        catch (Exception e) { // Ignore
        }
        Assert.assertEquals(snapshotManager2.getSnapshotRestoreResult().getSnapshotResult(), SnapshotResult.IN_PROGRESS_FAILED);
        try {
            snapshotManager2.failedToRestore(new SnapshotStateId(1, taskId2, "component2"), true);
        }
        catch (Exception e) { // Ignore
        }
        Assert.assertEquals(snapshotManager2.getSnapshotRestoreResult().getSnapshotResult(), SnapshotResult.IN_PROGRESS_FAILED_FATAL);
        snapshotManager2.succeededToRestore(new SnapshotStateId(1, taskId2, "component3"));
        Assert.assertEquals(snapshotManager2.getSnapshotRestoreResult().getSnapshotResult(), SnapshotResult.FAILED_FATAL);
    }

    @Test
    public void testUpdateFinishedQueryComponents()
    {
        TaskId taskId = new TaskId("query", 1, 1);
        TaskSnapshotManager sm = new TaskSnapshotManager(taskId, snapshotUtils);
        sm.setTotalComponents(2);

        sm.updateFinishedComponents(ImmutableList.of(mock(Operator.class)));
        sm.succeededToCapture(new SnapshotStateId(1, taskId));
        assertEquals(sm.getSnapshotCaptureResult().get(1L), SnapshotResult.SUCCESSFUL);
    }
}
