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
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.originalExpressions;

public class TestTablePushdown
        extends BaseRuleTest
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();

    @Test
    public void testDoesNotFireWhenJoinTypeNotInner()
    {
        tester().assertThat(new TablePushdown(METADATA))
                .on(p -> p.join(JoinNode.Type.LEFT,
                        p.values(ImmutableList.of(p.symbol("COL1")), ImmutableList.of(originalExpressions("10"))),
                        p.values(new Symbol("COL2"), new Symbol("COL3")),
                        ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol("COL1"), new Symbol("COL2"))),
                        ImmutableList.of(new Symbol("COL1"), new Symbol("COL2")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenJoinInBothChildren()
    {
        tester().assertThat(new TablePushdown(METADATA))
                .on(p -> p.join(
                        JoinNode.Type.INNER,
                        p.join(JoinNode.Type.INNER,
                                p.values(ImmutableList.of(p.symbol("COL1")), ImmutableList.of(originalExpressions("10"))),
                                p.values(new Symbol("COL2"), new Symbol("COL3")),
                                ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol("COL1"), new Symbol("COL2"))),
                                ImmutableList.of(new Symbol("COL1"), new Symbol("COL2")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        p.join(JoinNode.Type.INNER,
                                p.values(ImmutableList.of(p.symbol("COL1")), ImmutableList.of(originalExpressions("10"))),
                                p.values(new Symbol("COL2"), new Symbol("COL3")),
                                ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol("COL1"), new Symbol("COL2"))),
                                ImmutableList.of(new Symbol("COL1"), new Symbol("COL2")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol("COL1"), new Symbol("COL2"))),
                        ImmutableList.of(new Symbol("COL1"), new Symbol("COL2")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenGroupByInBothChildren()
    {
        tester().assertThat(new TablePushdown(METADATA))
                .on(p -> p.join(
                        JoinNode.Type.INNER,
                        p.aggregation(a -> a.source(
                                p.join(
                                        JoinNode.Type.INNER,
                                        p.values(ImmutableList.of(p.symbol("COL1")), ImmutableList.of(originalExpressions("10"))),
                                        p.values(ImmutableList.of(p.symbol("COL2")), ImmutableList.of(originalExpressions("20"))),
                                        ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol("COL1"), new Symbol("COL2"))),
                                        ImmutableList.of(new Symbol("COL1"), new Symbol("COL2")),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()))
                                .addAggregation(new Symbol("SUM"), PlanBuilder.expression("sum(COL1)"), ImmutableList.of(DOUBLE))
                                .singleGroupingSet(new Symbol("COL1"))),
                        p.aggregation(a -> a.source(
                                p.join(
                                        JoinNode.Type.INNER,
                                        p.values(ImmutableList.of(p.symbol("COL1")), ImmutableList.of(originalExpressions("10"))),
                                        p.values(ImmutableList.of(p.symbol("COL2")), ImmutableList.of(originalExpressions("20"))),
                                        ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol("COL1"), new Symbol("COL2"))),
                                        ImmutableList.of(new Symbol("COL1"), new Symbol("COL2")),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()))
                                .addAggregation(new Symbol("SUM"), PlanBuilder.expression("sum(COL1)"), ImmutableList.of(DOUBLE))
                                .singleGroupingSet(new Symbol("COL1"))),
                        ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol("COL1"), new Symbol("COL2"))),
                        ImmutableList.of(new Symbol("COL1")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()))
                .doesNotFire();
    }
}
