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

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.sql.planner.assertions.ExpressionMatcher;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.castToRowExpression;

public class TestInlineProjections
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        tester().assertThat(new InlineProjections())
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("identity"), castToRowExpression("symbol")) // identity
                                        .put(p.symbol("multi_complex_1"), castToRowExpression("complex + 1")) // complex expression referenced multiple times
                                        .put(p.symbol("multi_complex_2"), castToRowExpression("complex + 2")) // complex expression referenced multiple times
                                        .put(p.symbol("multi_literal_1"), castToRowExpression("literal + 1")) // literal referenced multiple times
                                        .put(p.symbol("multi_literal_2"), castToRowExpression("literal + 2")) // literal referenced multiple times
                                        .put(p.symbol("single_complex"), castToRowExpression("complex_2 + 2")) // complex expression reference only once
                                        .put(p.symbol("try"), castToRowExpression("try(complex / literal)"))
                                        .build(),
                                p.project(Assignments.builder()
                                                .put(p.symbol("symbol"), castToRowExpression("x"))
                                                .put(p.symbol("complex"), castToRowExpression("x * 2"))
                                                .put(p.symbol("literal"), castToRowExpression("1"))
                                                .put(p.symbol("complex_2"), castToRowExpression("x - 1"))
                                                .build(),
                                        p.values(p.symbol("x")))))
                .matches(
                        project(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("out1", PlanMatchPattern.expression("x"))
                                        .put("out2", PlanMatchPattern.expression("y + 1"))
                                        .put("out3", PlanMatchPattern.expression("y + 2"))
                                        .put("out4", PlanMatchPattern.expression("1 + 1"))
                                        .put("out5", PlanMatchPattern.expression("1 + 2"))
                                        .put("out6", PlanMatchPattern.expression("x - 1 + 2"))
                                        .put("out7", PlanMatchPattern.expression("try(y / 1)"))
                                        .build(),
                                project(
                                        ImmutableMap.of(
                                                "x", PlanMatchPattern.expression("x"),
                                                "y", PlanMatchPattern.expression("x * 2")),
                                        values(ImmutableMap.of("x", 0)))));
    }

    @Test
    public void testIdentityProjections()
    {
        tester().assertThat(new InlineProjections())
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("output"), castToRowExpression("value")),
                                p.project(
                                        Assignments.of(p.symbol("value"), p.variable("value")),
                                        p.values(p.symbol("value")))))
                .doesNotFire();
    }

    @Test
    public void testSubqueryProjections()
    {
        tester().assertThat(new InlineProjections())
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("fromOuterScope"), p.variable("fromOuterScope"), p.symbol("value"), p.variable("value")),
                                p.project(
                                        Assignments.of(p.symbol("value"), p.variable("value")),
                                        p.values(p.symbol("value")))))
                .doesNotFire();
    }
}
