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
package io.prestosql.execution.scheduler.policy;

import io.prestosql.dynamicfilter.DynamicFilterService;
import io.prestosql.execution.SqlStageExecution;

import javax.inject.Inject;

import java.util.Collection;

import static java.util.Objects.requireNonNull;

public class PrioritizeUtilizationExecutionPolicy
        implements ExecutionPolicy
{
    private final DynamicFilterService dynamicFilterService;

    @Inject
    public PrioritizeUtilizationExecutionPolicy(DynamicFilterService dynamicFilterService)
    {
        this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
    }

    @Override
    public ExecutionSchedule createExecutionSchedule(Collection<SqlStageExecution> stages)
    {
        return PrioritizeUtilizationExecutionSchedule.forStages(stages, dynamicFilterService);
    }
}
