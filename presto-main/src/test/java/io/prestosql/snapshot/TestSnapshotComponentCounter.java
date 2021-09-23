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

import io.prestosql.execution.TaskId;
import io.prestosql.testing.assertions.Assert;
import org.testng.annotations.Test;

public class TestSnapshotComponentCounter
{
    private static final SnapshotStateId component1 = new SnapshotStateId(1, new TaskId("query1", 1, 1));
    private static final SnapshotStateId component2 = new SnapshotStateId(1, new TaskId("query1", 1, 2));
    private static final SnapshotStateId component3 = new SnapshotStateId(1, new TaskId("query1", 1, 3));

    @Test
    public void testCaptureStateValidChange()
    {
        // IN_PROGRESS -> SUCCESSFUL -> FAILED
        SnapshotComponentCounter snapshotComponentCounter = new SnapshotComponentCounter(1);
        Assert.assertEquals(snapshotComponentCounter.getSnapshotResult(), SnapshotResult.IN_PROGRESS);
        snapshotComponentCounter.updateComponent(component1, SnapshotComponentCounter.ComponentState.SUCCESSFUL);
        Assert.assertEquals(snapshotComponentCounter.getSnapshotResult(), SnapshotResult.SUCCESSFUL);
        snapshotComponentCounter.updateComponent(component1, SnapshotComponentCounter.ComponentState.FAILED);
        Assert.assertEquals(snapshotComponentCounter.getSnapshotResult(), SnapshotResult.FAILED);
        // IN_PROGRESS -> IN_PROGRESS_FAILED -> FAILED
        snapshotComponentCounter = new SnapshotComponentCounter(2);
        snapshotComponentCounter.updateComponent(component1, SnapshotComponentCounter.ComponentState.FAILED);
        Assert.assertEquals(snapshotComponentCounter.getSnapshotResult(), SnapshotResult.IN_PROGRESS_FAILED);
        snapshotComponentCounter.updateComponent(component2, SnapshotComponentCounter.ComponentState.SUCCESSFUL);
        Assert.assertEquals(snapshotComponentCounter.getSnapshotResult(), SnapshotResult.FAILED);
    }

    @Test
    public void testCaptureStateInvalidChange()
    {
        // IN_PROGRESS_FAILED -> IN_PROGRESS
        // Once result changed to IN_PROGRESS_FAILED, it cannot change back to IN_PROGRESS
        SnapshotComponentCounter snapshotComponentCounter = new SnapshotComponentCounter(3);
        snapshotComponentCounter.updateComponent(component1, SnapshotComponentCounter.ComponentState.FAILED);
        Assert.assertEquals(snapshotComponentCounter.getSnapshotResult(), SnapshotResult.IN_PROGRESS_FAILED);
        snapshotComponentCounter.updateComponent(component2, SnapshotComponentCounter.ComponentState.SUCCESSFUL);
        Assert.assertEquals(snapshotComponentCounter.getSnapshotResult(), SnapshotResult.IN_PROGRESS_FAILED);

        // FAILED -> SUCCESSFUL
        // Once result changed to FAILED, it cannot change back to SUCCESSFUL
        snapshotComponentCounter.updateComponent(component3, SnapshotComponentCounter.ComponentState.SUCCESSFUL);
        Assert.assertEquals(snapshotComponentCounter.getSnapshotResult(), SnapshotResult.FAILED);
        snapshotComponentCounter.updateComponent(component1, SnapshotComponentCounter.ComponentState.SUCCESSFUL);
        Assert.assertEquals(snapshotComponentCounter.getSnapshotResult(), SnapshotResult.FAILED);
    }

    @Test
    public void testRestoreStateValidChange()
    {
        // IN_PROGRESS -> SUCCESSFUL -> FAILED -> FAILED_FATAL
        SnapshotComponentCounter snapshotComponentCounter = new SnapshotComponentCounter(1);
        Assert.assertEquals(snapshotComponentCounter.getSnapshotResult(), SnapshotResult.IN_PROGRESS);
        snapshotComponentCounter.updateComponent(component1, SnapshotComponentCounter.ComponentState.SUCCESSFUL);
        Assert.assertEquals(snapshotComponentCounter.getSnapshotResult(), SnapshotResult.SUCCESSFUL);
        snapshotComponentCounter.updateComponent(component1, SnapshotComponentCounter.ComponentState.FAILED);
        Assert.assertEquals(snapshotComponentCounter.getSnapshotResult(), SnapshotResult.FAILED);
        snapshotComponentCounter.updateComponent(component1, SnapshotComponentCounter.ComponentState.FAILED_FATAL);
        Assert.assertEquals(snapshotComponentCounter.getSnapshotResult(), SnapshotResult.FAILED_FATAL);

        // IN_PROGRESS -> IN_PROGRESS_FATAL -> FAILED_FATAL
        snapshotComponentCounter = new SnapshotComponentCounter(2);
        snapshotComponentCounter.updateComponent(component1, SnapshotComponentCounter.ComponentState.FAILED_FATAL);
        Assert.assertEquals(snapshotComponentCounter.getSnapshotResult(), SnapshotResult.IN_PROGRESS_FAILED_FATAL);
        snapshotComponentCounter.updateComponent(component2, SnapshotComponentCounter.ComponentState.SUCCESSFUL);
        Assert.assertEquals(snapshotComponentCounter.getSnapshotResult(), SnapshotResult.FAILED_FATAL);
    }

    @Test
    public void testRestoreStateInvalidChange()
    {
        // IN_PROGRESS_FAILED_FAILED -> (IN_PROGRESS_FAILED or IN_PROGRESS)
        // Once the result changed to IN_PROGRESS_FAILED_FATAL, it cannot change back to either IN_PROGRESS_FAILED or IN_PROGRESS
        SnapshotComponentCounter snapshotComponentCounter = new SnapshotComponentCounter(4);
        snapshotComponentCounter.updateComponent(component1, SnapshotComponentCounter.ComponentState.FAILED_FATAL);
        Assert.assertEquals(snapshotComponentCounter.getSnapshotResult(), SnapshotResult.IN_PROGRESS_FAILED_FATAL);
        snapshotComponentCounter.updateComponent(component2, SnapshotComponentCounter.ComponentState.FAILED);
        Assert.assertEquals(snapshotComponentCounter.getSnapshotResult(), SnapshotResult.IN_PROGRESS_FAILED_FATAL);
        snapshotComponentCounter.updateComponent(component3, SnapshotComponentCounter.ComponentState.SUCCESSFUL);
        Assert.assertEquals(snapshotComponentCounter.getSnapshotResult(), SnapshotResult.IN_PROGRESS_FAILED_FATAL);

        // FAILED_FATAL -> (FAILED or SUCCESSFUL)
        // Once the result changed to FAILED_FATAL, it cannot change back to either FAILED or SUCCESSFUL
        snapshotComponentCounter = new SnapshotComponentCounter(1);
        snapshotComponentCounter.updateComponent(component1, SnapshotComponentCounter.ComponentState.FAILED_FATAL);
        Assert.assertEquals(snapshotComponentCounter.getSnapshotResult(), SnapshotResult.FAILED_FATAL);
        snapshotComponentCounter.updateComponent(component1, SnapshotComponentCounter.ComponentState.FAILED);
        Assert.assertEquals(snapshotComponentCounter.getSnapshotResult(), SnapshotResult.FAILED_FATAL);
        snapshotComponentCounter.updateComponent(component1, SnapshotComponentCounter.ComponentState.SUCCESSFUL);
        Assert.assertEquals(snapshotComponentCounter.getSnapshotResult(), SnapshotResult.FAILED_FATAL);
    }
}
