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
import io.prestosql.spi.plan.Symbol;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.SystemSessionProperties.FILTERING_SEMI_JOIN_TO_INNER;
import static io.prestosql.spi.plan.AggregationNode.Step.SINGLE;
import static io.prestosql.spi.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestTransformFilteringSemiJoinToInnerJoin
        extends BaseRuleTest
{
    @Test
    public void testTransformSemiJoinToInnerJoin()
    {
        tester().assertThat(new TransformFilteringSemiJoinToInnerJoin())
                .setSystemProperty(FILTERING_SEMI_JOIN_TO_INNER, "true")
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol aInB = p.symbol("a_in_b");
                    return p.filter(
                            expression("a_in_b AND a > 5"),
                            p.semiJoin(
                                    p.values(a),
                                    p.values(b),
                                    a,
                                    b,
                                    aInB,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()));
                })
                .matches(project(
                        ImmutableMap.of("a", PlanMatchPattern.expression("a"), "a_in_b", PlanMatchPattern.expression("true")),
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("a", "b")),
                                Optional.of("a > 5"),
                                values("a"),
                                aggregation(
                                        singleGroupingSet("b"),
                                        ImmutableMap.of(),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        values("b")))));
    }

    @Test
    public void testRemoveRedundantFilter()
    {
        tester().assertThat(new TransformFilteringSemiJoinToInnerJoin())
                .setSystemProperty(FILTERING_SEMI_JOIN_TO_INNER, "true")
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol aInB = p.symbol("a_in_b");
                    return p.filter(
                            expression("a_in_b"),
                            p.semiJoin(
                                    p.values(a),
                                    p.values(b),
                                    a,
                                    b,
                                    aInB,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()));
                })
                .matches(project(
                        ImmutableMap.of("a", PlanMatchPattern.expression("a"), "a_in_b", PlanMatchPattern.expression("true")),
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("a", "b")),
                                Optional.empty(),
                                values("a"),
                                aggregation(
                                        singleGroupingSet("b"),
                                        ImmutableMap.of(),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        values("b")))));
    }

    @Test
    public void testFilterNotMatching()
    {
        tester().assertThat(new TransformFilteringSemiJoinToInnerJoin())
                .setSystemProperty(FILTERING_SEMI_JOIN_TO_INNER, "true")
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol aInB = p.symbol("a_in_b");
                    return p.filter(
                            expression("a > 5"),
                            p.semiJoin(
                                    p.values(a),
                                    p.values(b),
                                    a,
                                    b,
                                    aInB,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()));
                })
                .doesNotFire();
    }
}
