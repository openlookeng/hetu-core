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
package io.prestosql.plugin.jdbc.optimization;

import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.ConnectorPlanOptimizer;
import io.prestosql.spi.connector.ConnectorPlanOptimizerProvider;

import java.util.Set;

public class JdbcPlanOptimizerProvider
        implements ConnectorPlanOptimizerProvider
{
    private final ConnectorPlanOptimizer planOptimizer;

    public JdbcPlanOptimizerProvider(ConnectorPlanOptimizer planOptimizer)
    {
        this.planOptimizer = planOptimizer;
    }

    @Override
    public Set<ConnectorPlanOptimizer> getLogicalPlanOptimizers()
    {
        return ImmutableSet.of(planOptimizer);
    }

    @Override
    public Set<ConnectorPlanOptimizer> getPhysicalPlanOptimizers()
    {
        return ImmutableSet.of();
    }
}
