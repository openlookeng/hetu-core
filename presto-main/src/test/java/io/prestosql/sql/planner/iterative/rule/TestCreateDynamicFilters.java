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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.TestingColumnHandle;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestCreateDynamicFilters
        extends BaseRuleTest
{
    private static final String PROBE_SYMBOL_NAME = "probe";
    private static final String BUILD_SYMBOL_NAME = "build";

    @Test
    public void testNoPredicate()
    {
        tester().assertThat(new CreateDynamicFilters(tester().getMetadata()))
                .on(p -> {
                    Symbol probeSymbol = p.symbol(PROBE_SYMBOL_NAME);
                    Symbol buildSymbol = p.symbol(BUILD_SYMBOL_NAME);
                    ColumnHandle probeColumn = new TestingColumnHandle(PROBE_SYMBOL_NAME);
                    ColumnHandle buildColumn = new TestingColumnHandle(BUILD_SYMBOL_NAME);
                    return buildJoin(
                            p,
                            p.tableScan(ImmutableList.of(probeSymbol), ImmutableMap.of(probeSymbol, probeColumn)),
                            p.tableScan(ImmutableList.of(buildSymbol), ImmutableMap.of(buildSymbol, buildColumn)));
                })
                .doesNotFire();
    }

    private static PlanNode buildJoin(PlanBuilder p, PlanNode left, PlanNode right)
    {
        Symbol leftKey = p.symbol(PROBE_SYMBOL_NAME);
        Symbol rightKey = p.symbol(BUILD_SYMBOL_NAME);
        List<Symbol> outputs = ImmutableList.of(leftKey, rightKey);
        return p.join(
                JoinNode.Type.INNER,
                left,
                right,
                ImmutableList.of(new JoinNode.EquiJoinClause(leftKey, rightKey)),
                outputs,
                Optional.of(expression("leftValue > 5")),
                Optional.of(leftKey),
                Optional.of(rightKey));
    }
}
