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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.sql.planner.iterative.Rule;

import java.util.Optional;

import static io.prestosql.SystemSessionProperties.isCTEReuseEnabled;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.prestosql.sql.planner.plan.ExchangeNode.partitionedExchange;
import static io.prestosql.sql.planner.plan.Patterns.cteScan;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.source;

public class AddExchangeAboveCTENode
        implements Rule<FilterNode>
{
    private static final Pattern<FilterNode> PATTERN = filter()
            .with(source().matching(cteScan()));

    @Override
    public boolean isEnabled(Session session)
    {
        return isCTEReuseEnabled(session);
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context)
    {
        PlanNode source = node.getSource();
        source = partitionedExchange(
                context.getIdAllocator().getNextId(),
                REMOTE,
                source,
                source.getOutputSymbols(),
                Optional.empty());
        PlanNode newFilterNode = node.replaceChildren(ImmutableList.of(source));
        return Result.ofPlanNode(newFilterNode);
    }
}
