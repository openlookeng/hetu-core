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
package io.prestosql.resourcemanager;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.execution.StageInfo;
import io.prestosql.execution.StageStats;

import java.util.List;

public interface ResourceManager
{
    boolean setResourceLimit(DataSize memoryLimit, Duration cpuLimit, DataSize ioLimit);

    DataSize getMemoryLimit();

    void updateStats(List<StageStats> stats);

    QueryExecutionModifier updateStats(ImmutableList<StageInfo> stats);

    boolean canScheduleMore();

    void updateStats(Duration totalCpu, DataSize totalMem, DataSize totalIo);
}
