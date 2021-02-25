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
import io.prestosql.spi.plan.Symbol;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.offset;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.relational.Expressions.constant;

public class TestPushOffsetThroughProject
        extends BaseRuleTest
{
    @Test
    public void testPushdownOffsetNonIdentityProjection()
    {
        tester().assertThat(new PushOffsetThroughProject())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    return p.offset(
                            5,
                            p.project(
                                    Assignments.of(a, constant(true, BOOLEAN)),
                                    p.values()));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("b", expression("true")),
                                offset(5, values())));
    }

    @Test
    public void testDoNotPushdownOffsetThroughIdentityProjection()
    {
        tester().assertThat(new PushOffsetThroughProject())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    return p.offset(
                            5,
                            p.project(
                                    Assignments.of(a, p.variable(a.getName())),
                                    p.values(a)));
                }).doesNotFire();
    }
}
