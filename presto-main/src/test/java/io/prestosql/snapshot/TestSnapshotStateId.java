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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class TestSnapshotStateId
{
    @Test
    public void testGetId()
    {
        String queryId = "query1";
        long snapshotId = 1L;
        int stageId = 100;
        int taskId = 200;
        int pipelineId = 300;
        int driverId = 400;
        int operatorId = 3000;
        TaskId taskId1 = new TaskId(queryId, stageId, taskId);
        SnapshotStateId stateId = SnapshotStateId.forOperator(snapshotId, taskId1, pipelineId, driverId, operatorId);

        Assert.assertEquals(stateId.getId(), "query1/1/100/200/300/400/3000");
    }

    @Test
    public void testGetHierarchy()
    {
        String queryId = "query1";
        long snapshotId = 1L;
        int stageId = 100;
        int taskId = 200;
        int pipelineId = 300;
        int driverId = 400;
        int operatorId = 3000;
        TaskId taskId1 = new TaskId(queryId, stageId, taskId);
        SnapshotStateId stateId = SnapshotStateId.forOperator(snapshotId, taskId1, pipelineId, driverId, operatorId);

        Assert.assertEquals(stateId.getHierarchy(), new ArrayList<String>()
        {{
                add(queryId);
                add(String.valueOf(snapshotId));
                add(String.valueOf(stageId));
                add(String.valueOf(taskId));
                add(String.valueOf(pipelineId));
                add(String.valueOf(driverId));
                add(String.valueOf(operatorId));
            }});
    }

    @Test
    public void testWithSnapshotId()
    {
        String queryId = "query1";
        long snapshotId = 1L;
        int stageId = 100;
        int taskId = 200;
        TaskId taskId1 = new TaskId(queryId, stageId, taskId);
        SnapshotStateId stateId = new SnapshotStateId(snapshotId, taskId1, "component");
        stateId = stateId.withSnapshotId(2);
        Assert.assertEquals(stateId.getId(), "query1/2/100/200/component");
    }

    @Test
    public void testHashcodeEquals()
    {
        String queryId = "query1";
        long snapshotId = 1L;
        int stageId = 100;
        int taskId = 200;
        int taskId2 = 300;
        SnapshotStateId stateId1 = new SnapshotStateId(snapshotId, new TaskId(queryId, stageId, taskId));
        SnapshotStateId stateId2 = new SnapshotStateId(snapshotId, new TaskId(queryId, stageId, taskId));
        SnapshotStateId stateId3 = new SnapshotStateId(snapshotId, new TaskId(queryId, stageId, taskId2));
        Set<SnapshotStateId> set = new HashSet<>();
        set.add(stateId1);
        Assert.assertTrue(set.contains(stateId2));
        Assert.assertFalse(set.contains(stateId3));
        Assert.assertFalse(stateId1.equals(this));
    }

    @Test
    public void testFromString()
    {
        String testQueryId = "testingquery";
        long snapshotId = 18L;
        int stageId = 500;
        int taskInt = 600;

        TaskId taskId = new TaskId(testQueryId, stageId, taskInt);
        SnapshotStateId notString = new SnapshotStateId(snapshotId, taskId);
        String str = notString.toString();
        SnapshotStateId withString = SnapshotStateId.fromString(str);
        Assert.assertTrue(notString.equals(withString));
    }
}
