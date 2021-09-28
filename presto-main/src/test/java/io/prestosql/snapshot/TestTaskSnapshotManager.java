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
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.prestosql.execution.StageId;
import io.prestosql.execution.TaskId;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.operator.Operator;
import io.prestosql.spi.QueryId;
import io.prestosql.testing.assertions.Assert;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static io.prestosql.SessionTestUtils.TEST_SNAPSHOT_SESSION;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
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
        TaskSnapshotManager snapshotManager = new TaskSnapshotManager(taskId1, 0, snapshotUtils);

        snapshotUtils.getOrCreateQuerySnapshotManager(queryId, TEST_SNAPSHOT_SESSION);

        // Test operator state
        MockState operatorState = new MockState("operator-state");
        SnapshotStateId operatorStateId = SnapshotStateId.forOperator(1L, taskId1, 3, 4, 5);
        snapshotManager.storeState(operatorStateId, operatorState);
        snapshotManager.setTotalComponents(1);
        snapshotManager.succeededToCapture(operatorStateId);
        MockState newOperatorState = (MockState) snapshotManager.loadState(operatorStateId).get();
        Assert.assertEquals(operatorState.getState(), newOperatorState.getState());

        // Test task state
        MockState taskState = new MockState("task-state");
        TaskId taskId2 = new TaskId(queryId.getId(), 3, 4);
        SnapshotStateId taskStateId = SnapshotStateId.forOperator(3L, taskId2, 5, 6, 7);
        snapshotManager.storeState(taskStateId, taskState);
        snapshotManager.succeededToCapture(taskStateId);
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
        TaskSnapshotManager snapshotManager = new TaskSnapshotManager(taskId, 0, snapshotUtils);

        snapshotUtils.getOrCreateQuerySnapshotManager(queryId, TEST_SNAPSHOT_SESSION);

        // Save operator state
        MockState state = new MockState("state");
        SnapshotStateId stateId = SnapshotStateId.forOperator(1L, taskId, 3, 4, 5);
        snapshotManager.storeState(stateId, state);
        snapshotManager.setTotalComponents(1);
        snapshotManager.succeededToCapture(stateId);

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
        TaskSnapshotManager snapshotManager = new TaskSnapshotManager(taskId, 0, snapshotUtils);
        snapshotManager.setTotalComponents(1);

        // Create a file
        Path sourcePath = Paths.get(SNAPSHOT_FILE_SYSTEM_DIR + "/source/spill-test.txt");
        sourcePath.getParent().toFile().mkdirs();

        String fileContent = "Spill Contents";
        FileWriter fileWriter = new FileWriter(SNAPSHOT_FILE_SYSTEM_DIR + "/source/spill-test.txt");
        fileWriter.write(fileContent);
        fileWriter.close();

        SnapshotStateId snapshotStateId = new SnapshotStateId(1, taskId, "component1");
        snapshotManager.storeFile(snapshotStateId, sourcePath);
        snapshotManager.succeededToCapture(snapshotStateId);

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
        TaskSnapshotManager snapshotManager = new TaskSnapshotManager(taskId, 0, snapshotUtils);
        snapshotManager.setTotalComponents(1);

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
        snapshotManager.succeededToCapture(id4save);

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
        queryId = new QueryId("query");
        TaskId taskId1 = new TaskId(queryId.getId(), 1, 1);
        TaskId taskId2 = new TaskId(queryId.getId(), 1, 2);
        TaskSnapshotManager snapshotManager1 = new TaskSnapshotManager(taskId1, 0, snapshotUtils);
        TaskSnapshotManager snapshotManager2 = new TaskSnapshotManager(taskId2, 0, snapshotUtils);
        snapshotManager1.setTotalComponents(2);
        snapshotManager2.setTotalComponents(2);

        snapshotUtils.getOrCreateQuerySnapshotManager(queryId, TEST_SNAPSHOT_SESSION);

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
        TaskSnapshotManager snapshotManager1 = new TaskSnapshotManager(taskId1, 0, snapshotUtils);
        TaskSnapshotManager snapshotManager2 = new TaskSnapshotManager(taskId2, 0, snapshotUtils);
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
        queryId = new QueryId("query");
        TaskId taskId = new TaskId(queryId.getId(), 1, 1);
        TaskSnapshotManager sm = new TaskSnapshotManager(taskId, 0, snapshotUtils);
        sm.setTotalComponents(2);

        snapshotUtils.getOrCreateQuerySnapshotManager(queryId, TEST_SNAPSHOT_SESSION);

        sm.updateFinishedComponents(ImmutableList.of(mock(Operator.class)));
        sm.succeededToCapture(new SnapshotStateId(1, taskId));
        assertEquals(sm.getSnapshotCaptureResult().get(1L), SnapshotResult.SUCCESSFUL);
    }

    @Test
    public void testConsolidatedStoreLoadSimple()
            throws Exception
    {
        queryId = new QueryId("consolidatesimplequery");
        TaskId taskId1 = new TaskId(queryId.getId(), 1, 0);
        TaskSnapshotManager snapshotManager = new TaskSnapshotManager(taskId1, 0, snapshotUtils);
        snapshotManager.setTotalComponents(1);

        snapshotUtils.getOrCreateQuerySnapshotManager(queryId, TEST_SNAPSHOT_SESSION);

        MockState state = new MockState("mockstate");
        SnapshotStateId stateId = SnapshotStateId.forOperator(1L, taskId1, 3, 4, 5);
        snapshotManager.storeConsolidatedState(stateId, state);
        snapshotManager.succeededToCapture(stateId);
        MockState newState = (MockState) snapshotManager.loadConsolidatedState(stateId).get();
        Assert.assertEquals(state.getState(), newState.getState());
    }

    @Test
    public void testConsolidatedBacktrack()
            throws Exception
    {
        queryId = new QueryId("consolidatebacktrackquery");
        TaskId taskId1 = new TaskId(queryId.getId(), 1, 0);
        TaskSnapshotManager snapshotManager = new TaskSnapshotManager(taskId1, 0, snapshotUtils);
        snapshotManager.setTotalComponents(1);

        snapshotUtils.getOrCreateQuerySnapshotManager(queryId, TEST_SNAPSHOT_SESSION);

        QuerySnapshotManager querySnapshotManager = new QuerySnapshotManager(queryId, snapshotUtils, TEST_SNAPSHOT_SESSION);
        querySnapshotManager.addNewTask(taskId1);

        // first store
        long firstSnapshotId = 1L;
        MockState state = new MockState("mockstate");
        SnapshotStateId stateId = SnapshotStateId.forOperator(firstSnapshotId, taskId1, 3, 4, 5);
        snapshotManager.storeConsolidatedState(stateId, state);
        snapshotManager.succeededToCapture(stateId);

        // second store (null)
        SnapshotStateId secondId = SnapshotStateId.forOperator(2L, taskId1, 3, 4, 5);
        snapshotManager.storeConsolidatedState(secondId, null);
        snapshotManager.succeededToCapture(secondId);

        Map<Long, SnapshotResult> queryCaptureResult = new LinkedHashMap<>();
        queryCaptureResult.put(firstSnapshotId, SnapshotResult.SUCCESSFUL);
        snapshotUtils.storeSnapshotResult(queryId.getId(), queryCaptureResult);

        MockState newState = (MockState) snapshotManager.loadConsolidatedState(secondId).get();
        Assert.assertEquals(state.getState(), newState.getState());
    }

    @Test
    public void testConsolidatedOverlap()
            throws Exception
    {
        queryId = new QueryId("consolidateoverlapquery");
        TaskId taskId1 = new TaskId(queryId.getId(), 1, 0);
        TaskId taskId2 = new TaskId(queryId.getId(), 1, 1);
        TaskSnapshotManager snapshotManager1 = new TaskSnapshotManager(taskId1, 0, snapshotUtils);
        TaskSnapshotManager snapshotManager2 = new TaskSnapshotManager(taskId2, 0, snapshotUtils);
        snapshotManager1.setTotalComponents(1);
        snapshotManager2.setTotalComponents(1);

        snapshotUtils.getOrCreateQuerySnapshotManager(queryId, TEST_SNAPSHOT_SESSION);

        // statexy means task x snapshot y
        MockState state11 = new MockState("mockstate1.1");
        MockState state12 = new MockState("mockstate1.2");
        MockState state21 = new MockState("mockstate2.1");
        MockState state22 = new MockState("mockstate2.2");

        // similar naming as above
        SnapshotStateId stateId11 = SnapshotStateId.forOperator(1L, taskId1, 3, 4, 5);
        SnapshotStateId stateId12 = SnapshotStateId.forOperator(2L, taskId1, 3, 4, 5);
        SnapshotStateId stateId21 = SnapshotStateId.forOperator(1L, taskId2, 3, 4, 5);
        SnapshotStateId stateId22 = SnapshotStateId.forOperator(2L, taskId2, 3, 4, 5);

        // save 11 (part of snapshot 1), then 12 and 22 (snapshot 2), then finish snapshot 1 (21)
        snapshotManager1.storeConsolidatedState(stateId11, state11);
        snapshotManager2.storeConsolidatedState(stateId22, state22);
        snapshotManager1.storeConsolidatedState(stateId12, state12);
        snapshotManager1.succeededToCapture(stateId12);
        snapshotManager2.succeededToCapture(stateId22);
        snapshotManager2.storeConsolidatedState(stateId21, state21);
        snapshotManager1.succeededToCapture(stateId11);
        snapshotManager2.succeededToCapture(stateId21);

        // similar naming as above
        MockState newState11 = (MockState) snapshotManager1.loadConsolidatedState(stateId11).get();
        MockState newState12 = (MockState) snapshotManager1.loadConsolidatedState(stateId12).get();
        MockState newState21 = (MockState) snapshotManager2.loadConsolidatedState(stateId21).get();
        MockState newState22 = (MockState) snapshotManager2.loadConsolidatedState(stateId22).get();

        Assert.assertEquals(state11.getState(), newState11.getState());
        Assert.assertEquals(state12.getState(), newState12.getState());
        Assert.assertEquals(state21.getState(), newState21.getState());
        Assert.assertEquals(state22.getState(), newState22.getState());
    }

    @Test
    public void testDeletedFileLoad()
            throws Exception
    {
        queryId = new QueryId("deletedfileloadquery");
        TaskId taskId1 = new TaskId(queryId.getId(), 1, 0);
        TaskSnapshotManager snapshotManager = new TaskSnapshotManager(taskId1, 0, snapshotUtils);
        snapshotManager.setTotalComponents(1);

        QuerySnapshotManager querySnapshotManager = new QuerySnapshotManager(queryId, snapshotUtils, TEST_SNAPSHOT_SESSION);
        querySnapshotManager.addNewTask(taskId1);

        snapshotUtils.getOrCreateQuerySnapshotManager(queryId, TEST_SNAPSHOT_SESSION);

        // first store
        long firstSnapshotId = 1L;
        MockState state = new MockState("mockstate");
        SnapshotStateId stateId = SnapshotStateId.forOperator(firstSnapshotId, taskId1, 3, 4, 5);
        snapshotManager.storeState(stateId, state);
        snapshotManager.succeededToCapture(stateId);

        // second store, then deleted
        MockState secondState = new MockState("secondState");
        SnapshotStateId secondId = SnapshotStateId.forOperator(2L, taskId1, 3, 4, 5);
        snapshotManager.storeState(secondId, secondState);
        snapshotManager.succeededToCapture(secondId);

        querySnapshotManager.updateQueryCapture(taskId1, Collections.singletonMap(firstSnapshotId, SnapshotResult.SUCCESSFUL));

        assertTrue(snapshotManager.loadState(secondId).isPresent());
        File second = new File("/tmp/test_snapshot_manager/" + queryId + "/2/1/0/3/4/5");
        assertTrue(second.exists());
        second.delete();
        assertFalse(second.exists());

        assertFalse(snapshotManager.loadState(secondId).isPresent());

        // delete entire snapshot
        FileUtils.deleteDirectory(new File("/tmp/test_snapshot_manager/" + queryId));
        assertFalse(snapshotManager.loadState(secondId).isPresent());
    }

    @Test
    public void testFailedStoreConsolidated()
            throws Exception
    {
        queryId = new QueryId("failedstoreconsolidatedquery");

        SnapshotUtils faultySnapshotUtils = mock(SnapshotUtils.class);
        doThrow(new NullPointerException()).when(faultySnapshotUtils).storeState(any(), any());

        TaskId taskId1 = new TaskId(queryId.getId(), 1, 0);
        TaskSnapshotManager snapshotManager = new TaskSnapshotManager(taskId1, 0, faultySnapshotUtils);
        snapshotManager.setTotalComponents(1);

        MockState state = new MockState("mockstate");
        SnapshotStateId stateId = SnapshotStateId.forOperator(1L, taskId1, 3, 4, 5);

        snapshotManager.storeConsolidatedState(stateId, state);
        // Error messages will print. This is normal because we are failing the store on purpose
        snapshotManager.succeededToCapture(stateId);

        Assert.assertEquals(snapshotManager.getSnapshotCaptureResult().get(1L), SnapshotResult.FAILED);
    }

    @Test
    public void testSpilledDeleted()
            throws Exception
    {
        queryId = new QueryId("spilleddeletedquery");
        TaskId taskId1 = new TaskId(queryId.getId(), 1, 0);
        TaskSnapshotManager snapshotManager = new TaskSnapshotManager(taskId1, 0, snapshotUtils);
        snapshotManager.setTotalComponents(1);

        QuerySnapshotManager querySnapshotManager = new QuerySnapshotManager(queryId, snapshotUtils, TEST_SNAPSHOT_SESSION);
        querySnapshotManager.addNewTask(taskId1);

        snapshotUtils.getOrCreateQuerySnapshotManager(queryId, TEST_SNAPSHOT_SESSION);

        // first store
        long firstSnapshotId = 1L;
        File dirs = new File("/tmp/test_snapshot_manager/" + queryId + "/");
        File firstFile = new File("/tmp/test_snapshot_manager/" + queryId + "/firstFile");
        dirs.mkdirs();
        firstFile.createNewFile();
        try (FileWriter fw1 = new FileWriter(firstFile)) {
            String firstStr = "first string";
            fw1.write(firstStr);
            SnapshotStateId stateId = SnapshotStateId.forOperator(firstSnapshotId, taskId1, 3, 4, 5);
            snapshotManager.storeFile(stateId, firstFile.toPath());
            snapshotManager.succeededToCapture(stateId);
        }

        // second store, then deleted
        File secondFile = new File("/tmp/test_snapshot_manager/" + queryId + "/secondFile");
        secondFile.createNewFile();
        try (FileWriter fw2 = new FileWriter(secondFile)) {
            String secondStr = "second string";
            fw2.write(secondStr);
            SnapshotStateId secondId = SnapshotStateId.forOperator(2L, taskId1, 3, 4, 5);
            snapshotManager.storeFile(secondId, secondFile.toPath());
            snapshotManager.succeededToCapture(secondId);
            querySnapshotManager.updateQueryCapture(taskId1, Collections.singletonMap(firstSnapshotId, SnapshotResult.SUCCESSFUL));

            File secondFileOperator = new File("/tmp/test_snapshot_manager/" + queryId + "/2/1/0/3/4/5/secondFile");
            assertTrue(secondFileOperator.exists());
            secondFileOperator.delete();
            assertFalse(secondFileOperator.exists());

            assertFalse(snapshotManager.loadFile(secondId, secondFile.toPath()));
        }
    }
}
