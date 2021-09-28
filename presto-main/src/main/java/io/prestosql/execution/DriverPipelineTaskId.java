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
package io.prestosql.execution;

import java.util.Optional;

public class DriverPipelineTaskId
{
    private final Optional<TaskId> taskId;
    private final int driverId;
    private final int pipelineId;

    public DriverPipelineTaskId(Optional<TaskId> taskId, int pipelineId, int driverId)
    {
        this.taskId = taskId;
        this.driverId = driverId;
        this.pipelineId = pipelineId;
    }

    public Optional<TaskId> getTaskId()
    {
        return taskId;
    }

    public int getDriverId()
    {
        return driverId;
    }

    public int getPipelineId()
    {
        return pipelineId;
    }
}
