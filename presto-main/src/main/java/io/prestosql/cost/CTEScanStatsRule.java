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
package io.prestosql.cost;

import io.prestosql.Session;
import io.prestosql.matching.Pattern;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Lookup;

import java.util.Optional;

import static io.prestosql.sql.planner.plan.Patterns.cteScan;

public class CTEScanStatsRule
        extends SimpleStatsRule<CTEScanNode>
{
    private static final Pattern<CTEScanNode> PATTERN = cteScan();

    public CTEScanStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<CTEScanNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNodeStatsEstimate> doCalculate(CTEScanNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        // Its stats is same as the source node stats
        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(node.getSource());
        return Optional.of(sourceStats.mapOutputRowCount(sourceRowCount -> sourceStats.getOutputRowCount()));
    }
}
