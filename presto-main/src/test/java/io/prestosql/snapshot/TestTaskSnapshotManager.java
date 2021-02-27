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
import io.prestosql.execution.TaskId;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.operator.Operator;
import io.prestosql.spi.QueryId;
import io.prestosql.testing.assertions.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestTaskSnapshotManager
{
    SnapshotConfig snapshotConfig;
    FileSystemClientManager fileSystemClientManager;
    QuerySnapshotManager querySnapshotManager;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        // Set up snapshot config
        snapshotConfig = new SnapshotConfig();

        // Set up mock file system client manager
        fileSystemClientManager = mock(FileSystemClientManager.class);

        SnapshotUtils snapshotUtils = new SnapshotUtils(fileSystemClientManager, snapshotConfig, new InMemoryNodeManager());
        snapshotUtils.initialize();
        this.querySnapshotManager = new QuerySnapshotManager(new QueryId("query"), snapshotUtils, null);
    }

    @Test
    public void testCapture()
            throws Exception
    {
        TaskId taskId1 = new TaskId("query", 1, 1);
        TaskId taskId2 = new TaskId("query", 1, 2);
        TaskSnapshotManager snapshotManager1 = new TaskSnapshotManager(taskId1, querySnapshotManager);
        TaskSnapshotManager snapshotManager2 = new TaskSnapshotManager(taskId2, querySnapshotManager);
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
        TaskSnapshotManager snapshotManager1 = new TaskSnapshotManager(taskId1, querySnapshotManager);
        TaskSnapshotManager snapshotManager2 = new TaskSnapshotManager(taskId2, querySnapshotManager);
        snapshotManager1.setTotalComponents(2);
        snapshotManager2.setTotalComponents(3);

        // Test restore successfully
        snapshotManager1.succeededToRestore(new SnapshotStateId(1, taskId1, "component1"), 1);
        RestoreResult result = snapshotManager1.getSnapshotRestoreResult();
        Assert.assertEquals(result.getSnapshotId(), 1L);
        Assert.assertEquals(result.getResumeId(), 1);
        Assert.assertEquals(result.getSnapshotResult(), SnapshotResult.IN_PROGRESS);
        snapshotManager1.succeededToRestore(new SnapshotStateId(1, taskId1, "component2"), 1);
        Assert.assertEquals(snapshotManager1.getSnapshotRestoreResult().getSnapshotResult(), SnapshotResult.SUCCESSFUL);

        // Test restore failed
        try {
            snapshotManager2.failedToRestore(new SnapshotStateId(1, taskId2, "component1"), 1, false);
        }
        catch (Exception e) { // Ignore
        }
        Assert.assertEquals(snapshotManager2.getSnapshotRestoreResult().getSnapshotResult(), SnapshotResult.IN_PROGRESS_FAILED);
        try {
            snapshotManager2.failedToRestore(new SnapshotStateId(1, taskId2, "component2"), 1, true);
        }
        catch (Exception e) { // Ignore
        }
        Assert.assertEquals(snapshotManager2.getSnapshotRestoreResult().getSnapshotResult(), SnapshotResult.IN_PROGRESS_FAILED_FATAL);
        snapshotManager2.succeededToRestore(new SnapshotStateId(1, taskId2, "component3"), 1);
        Assert.assertEquals(snapshotManager2.getSnapshotRestoreResult().getSnapshotResult(), SnapshotResult.FAILED_FATAL);
    }

    @Test
    public void testUpdateFinishedQueryComponents()
    {
        TaskId taskId = new TaskId("query", 1, 1);
        TaskSnapshotManager sm = new TaskSnapshotManager(taskId, querySnapshotManager);
        sm.setTotalComponents(2);

        sm.updateFinishedComponents(ImmutableList.of(mock(Operator.class)));
        sm.succeededToCapture(new SnapshotStateId(1, taskId));
        assertEquals(sm.getSnapshotCaptureResult().get(1L), SnapshotResult.SUCCESSFUL);
    }
}
