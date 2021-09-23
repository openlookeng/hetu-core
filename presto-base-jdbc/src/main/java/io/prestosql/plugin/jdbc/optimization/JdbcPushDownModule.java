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
package io.prestosql.plugin.jdbc.optimization;

import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.TopNNode;

import java.util.Set;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;

/**
 * Jdbc Query Push Down Module
 */
public enum JdbcPushDownModule
{
    /**
     * Default Module
     */
    DEFAULT,
    /**
     * Push down all supported PlanNodes to TableScan
     */
    FULL_PUSHDOWN,
    /**
     * Only push down filter, aggregation, limit, topN, project to tableScan
     */
    BASE_PUSHDOWN;

    private static final Set<Class<? extends PlanNode>> BASE_PUSH_DOWN_NODE = ImmutableSet.of(
            FilterNode.class,
            AggregationNode.class,
            LimitNode.class,
            TopNNode.class,
            ProjectNode.class,
            TableScanNode.class);

    public boolean isAvailable(PlanNode node)
    {
        switch (this) {
            case FULL_PUSHDOWN:
                return true;
            case BASE_PUSHDOWN:
                return BASE_PUSH_DOWN_NODE.contains(node.getClass());
            default:
                throw new PrestoException(NOT_SUPPORTED, "Unsupported push down module");
        }
    }
}
