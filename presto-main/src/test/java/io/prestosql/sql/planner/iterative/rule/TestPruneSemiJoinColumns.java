/*
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
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneSemiJoinColumns
        extends BaseRuleTest
{
    @Test
    public void testSemiJoinNotNeeded()
    {
        tester().assertThat(new PruneSemiJoinColumns())
                .on(p -> buildProjectedSemiJoin(p, symbol -> symbol.getName().equals("leftValue")))
                .matches(
                        strictProject(
                                ImmutableMap.of("leftValue", expression("leftValue")),
                                values("leftKey", "leftKeyHash", "leftValue")));
    }

    @Test
    public void testAllColumnsNeeded()
    {
        tester().assertThat(new PruneSemiJoinColumns())
                .on(p -> buildProjectedSemiJoin(p, symbol -> true))
                .doesNotFire();
    }

    @Test
    public void testKeysNotNeeded()
    {
        tester().assertThat(new PruneSemiJoinColumns())
                .on(p -> buildProjectedSemiJoin(p, symbol -> (symbol.getName().equals("leftValue") || symbol.getName().equals("match"))))
                .doesNotFire();
    }

    @Test
    public void testValueNotNeeded()
    {
        tester().assertThat(new PruneSemiJoinColumns())
                .on(p -> buildProjectedSemiJoin(p, symbol -> symbol.getName().equals("match")))
                .matches(
                        strictProject(
                                ImmutableMap.of("match", expression("match")),
                                semiJoin("leftKey", "rightKey", "match",
                                        strictProject(
                                                ImmutableMap.of(
                                                        "leftKey", expression("leftKey"),
                                                        "leftKeyHash", expression("leftKeyHash")),
                                                values("leftKey", "leftKeyHash", "leftValue")),
                                        values("rightKey"))));
    }

    private static PlanNode buildProjectedSemiJoin(PlanBuilder p, Predicate<Symbol> projectionFilter)
    {
        Symbol match = p.symbol("match");
        Symbol leftKey = p.symbol("leftKey");
        Symbol leftKeyHash = p.symbol("leftKeyHash");
        Symbol leftValue = p.symbol("leftValue");
        Symbol rightKey = p.symbol("rightKey");
        List<Symbol> outputs = ImmutableList.of(match, leftKey, leftKeyHash, leftValue);
        return p.project(
                Assignments.copyOf(
                        outputs.stream()
                                .filter(projectionFilter)
                                .collect(Collectors.toMap(v -> v, v -> p.variable(v.getName(), BIGINT)))),

                p.semiJoin(
                        leftKey,
                        rightKey,
                        match,
                        Optional.of(leftKeyHash),
                        Optional.empty(),
                        p.values(leftKey, leftKeyHash, leftValue),
                        p.values(rightKey)));
    }
}
