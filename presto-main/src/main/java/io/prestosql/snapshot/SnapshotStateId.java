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
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.TaskContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The SnapshotStateId class is used to create stateId.
 * The SnapshotStateId can be one of the following
 * - operator (queryId/snapshotId/stageId/taskId/pipelineId/driverId/operatorId)
 * - driver component (queryId/snapshotId/stageId/taskId/pipelineId/driverId/{component-name})
 * - task (queryId/snapshotId/stageId/taskId)
 * - task component (queryId/snapshotId/stageId/taskId/{component-name})
 */
public class SnapshotStateId
{
    private static final String SLASH = "/";
    private final TaskId taskId;
    private final long snapshotId;
    private final String id;
    // split id into a list
    private final List<String> hierarchy;

    public static SnapshotStateId forTask(long snapshotId, TaskId taskId)
    {
        return new SnapshotStateId(snapshotId, taskId);
    }

    public static SnapshotStateId fromString(String str)
    {
        String[] components = str.split(SLASH);
        // the 4 first components are queryId, snapshotId, stageId, and taskId, according to generateHierarchy
        TaskId taskId = new TaskId(components[0], Integer.parseInt(components[2]), Integer.parseInt(components[3]));
        long snapshotId = Long.parseLong(components[1]);
        List<String> parts = Arrays.asList(components);
        return new SnapshotStateId(snapshotId, taskId, parts);
    }

    public static SnapshotStateId forTaskComponent(long snapshotId, TaskContext taskContext, String component)
    {
        TaskId taskId = taskContext.getTaskId();
        return new SnapshotStateId(snapshotId, taskId, component);
    }

    public static SnapshotStateId forTaskComponent(long snapshotId, TaskId taskId, String component)
    {
        return new SnapshotStateId(snapshotId, taskId, component);
    }

    public static SnapshotStateId forOperator(long snapshotId, OperatorContext operatorContext)
    {
        DriverContext driverContext = operatorContext.getDriverContext();
        TaskId taskId = driverContext.getTaskId();
        int pipelineId = driverContext.getPipelineContext().getPipelineId();
        int driverId = driverContext.getDriverId();
        return new SnapshotStateId(snapshotId, taskId, pipelineId, driverId, operatorContext.getOperatorId());
    }

    public static SnapshotStateId forOperator(long snapshotId, TaskId taskId, int pipelineId, int driverId, int operatorId)
    {
        return new SnapshotStateId(snapshotId, taskId, pipelineId, driverId, operatorId);
    }

    public static SnapshotStateId forDriverComponent(long snapshotId, OperatorContext operatorContext, String component)
    {
        DriverContext driverContext = operatorContext.getDriverContext();
        TaskId taskId = driverContext.getTaskId();
        int pipelineId = driverContext.getPipelineContext().getPipelineId();
        int driverId = driverContext.getDriverId();
        return new SnapshotStateId(snapshotId, taskId, pipelineId, driverId, component);
    }

    public SnapshotStateId(long snapshotId, TaskId taskId, Object... parts)
    {
        this.taskId = taskId;
        this.snapshotId = snapshotId;
        this.hierarchy = generateHierarchy(snapshotId, taskId, parts);
        this.id = String.join(SLASH, hierarchy);
    }

    private SnapshotStateId(long snapshotId, TaskId taskId, List<String> hierarchy)
    {
        this.taskId = taskId;
        this.snapshotId = snapshotId;
        this.hierarchy = hierarchy;
        this.id = String.join(SLASH, hierarchy);
    }

    public SnapshotStateId withSnapshotId(long snapshotId)
    {
        List<String> newHierarchy = new ArrayList<>(hierarchy);
        newHierarchy.set(1, String.valueOf(snapshotId));
        return new SnapshotStateId(snapshotId, taskId, newHierarchy);
    }

    public String getId()
    {
        return id;
    }

    public List<String> getHierarchy()
    {
        return hierarchy;
    }

    public long getSnapshotId()
    {
        return snapshotId;
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    private static List<String> generateHierarchy(long snapshotId, TaskId taskId, Object... ids)
    {
        List<String> output = new ArrayList<>(ids.length + 4);
        output.add(taskId.getQueryId().getId());
        output.add(String.valueOf(snapshotId));
        output.add(String.valueOf(taskId.getStageId().getId()));
        output.add(String.valueOf(taskId.getId()));
        for (Object id : ids) {
            output.add(String.valueOf(id));
        }
        return output;
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof SnapshotStateId) {
            return id.equals(((SnapshotStateId) obj).id);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return id;
    }
}
