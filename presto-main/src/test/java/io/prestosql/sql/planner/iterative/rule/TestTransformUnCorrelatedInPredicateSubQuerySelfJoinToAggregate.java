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
import io.prestosql.plugin.tpch.TpchColumnHandle;
import io.prestosql.plugin.tpch.TpchTableHandle;
import io.prestosql.plugin.tpch.TpchTransactionHandle;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.sql.planner.SymbolUtils;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.iterative.rule.test.RuleTester;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.ExistsPredicate;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.SymbolReference;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.prestosql.SystemSessionProperties.TRANSFORM_SELF_JOIN_TO_GROUPBY;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCALE_FACTOR;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static java.util.Collections.emptyList;

public class TestTransformUnCorrelatedInPredicateSubQuerySelfJoinToAggregate
        extends BaseRuleTest
{
    private RuleTester tester;

    @BeforeClass
    public void setup()
    {
        tester = new RuleTester(
                ImmutableList.of(),
                ImmutableMap.of(TRANSFORM_SELF_JOIN_TO_GROUPBY, "true"));
    }

    @AfterClass
    public void teardown()
    {
        closeAllRuntimeException(tester);
        tester = null;
    }

    @Test
    public void testDoesNotFire()
    {
        tester.assertThat(new TransformUnCorrelatedInPredicateSubQuerySelfJoinToAggregate())
                .on(p -> p.apply(
                        Assignments.of(p.symbol("x"), OriginalExpressionUtils.castToRowExpression(new ExistsPredicate(new LongLiteral("1")))),
                        emptyList(),
                        p.values(),
                        p.values()))
                .doesNotFire();
    }

    @Test
    public void testDoesFireOnInPredicate()
    {
        tester.assertThat(new TransformUnCorrelatedInPredicateSubQuerySelfJoinToAggregate())
                .on(p -> {
                    Symbol o1 = p.symbol("o1", DATE);
                    Symbol t1 = p.symbol("t1", DOUBLE);
                    Symbol o2 = p.symbol("o2", DATE);
                    Symbol t2 = p.symbol("t2", DOUBLE);
                    SymbolReference o1ref = new SymbolReference("o1");
                    SymbolReference o2ref = new SymbolReference("o2");
                    SymbolReference t1ref = new SymbolReference("t1");
                    SymbolReference t2ref = new SymbolReference("t2");
                    TableHandle tableHandle = new TableHandle(
                            new CatalogName("local"),
                            new TpchTableHandle("orders", TINY_SCALE_FACTOR),
                            TpchTransactionHandle.INSTANCE,
                            Optional.empty());
                    TableScanNode leftTableScan = p.tableScan(
                            tableHandle,
                            ImmutableList.of(o1, t1),
                            ImmutableMap.of(
                                    o1, new TpchColumnHandle("orderdate", DATE),
                                    t1, new TpchColumnHandle("totalprice", DOUBLE)));
                    TableScanNode rightTableScan = p.tableScan(
                            tableHandle,
                            ImmutableList.of(o2, t2),
                            ImmutableMap.of(
                                    o2, new TpchColumnHandle("orderdate", DATE),
                                    t2, new TpchColumnHandle("totalprice", DOUBLE)));
                    return p.apply(
                            Assignments.of(
                                    p.symbol("x", BooleanType.BOOLEAN),
                                    OriginalExpressionUtils.castToRowExpression(new InPredicate(new SymbolReference("y"), o1ref))),
                            emptyList(),
                            p.values(p.symbol("y", DATE)),
                            p.project(Assignments.of(o1, OriginalExpressionUtils.castToRowExpression(SymbolUtils.toSymbolReference(o1))),
                                    p.filter(new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND,
                                                    new ComparisonExpression(ComparisonExpression.Operator.EQUAL, o1ref, o2ref),
                                                    new ComparisonExpression(ComparisonExpression.Operator.NOT_EQUAL, t1ref, t2ref)),
                                            p.join(JoinNode.Type.INNER, leftTableScan, rightTableScan,
                                                    new JoinNode.EquiJoinClause(o1, o2)))));
                })
                .matches(node(ApplyNode.class, values("y"),
                        project(node(FilterNode.class,
                                node(AggregationNode.class,
                                        node(TableScanNode.class))))));
    }
}
