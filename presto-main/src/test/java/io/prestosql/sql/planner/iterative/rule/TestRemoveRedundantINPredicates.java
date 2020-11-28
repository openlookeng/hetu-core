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
import io.prestosql.connector.CatalogName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.plugin.tpch.TpchColumnHandle;
import io.prestosql.plugin.tpch.TpchTableHandle;
import io.prestosql.plugin.tpch.TpchTransactionHandle;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.ExistsPredicate;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.SymbolReference;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCALE_FACTOR;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static java.util.Collections.emptyList;

public class TestRemoveRedundantINPredicates
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new RemoveRedundantInPredicates())
                .on(p -> {
                    Symbol o1 = p.symbol("o1", DATE);
                    Symbol e1 = p.symbol("e1", BooleanType.BOOLEAN);
                    Symbol e2 = p.symbol("e2", BooleanType.BOOLEAN);
                    SymbolReference e1Ref = e1.toSymbolReference();
                    SymbolReference e2Ref = e2.toSymbolReference();
                    return p.project(Assignments.identity(o1),
                            p.filter(new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND,
                                            e1Ref, e2Ref),
                                    p.apply(
                                            Assignments.of(o1, new ExistsPredicate(new LongLiteral("1"))),
                                            emptyList(),
                                            p.values(),
                                            p.values())));
                }).doesNotFire();
    }

    @Test
    public void testDoesFireOnRedundantINPredicates()
    {
        tester().assertThat(new RemoveRedundantInPredicates())
                .on(p -> {
                    Symbol o = p.symbol("o", DATE);
                    Symbol o1 = p.symbol("o1", DATE);
                    Symbol o2 = p.symbol("o2", DATE);
                    Symbol v = p.symbol("v", DATE);
                    Symbol e1 = p.symbol("e1", BooleanType.BOOLEAN);
                    Symbol e2 = p.symbol("e2", BooleanType.BOOLEAN);
                    SymbolReference oRef = o.toSymbolReference();
                    SymbolReference o1ref = o1.toSymbolReference();
                    SymbolReference vRef = v.toSymbolReference();
                    SymbolReference e1Ref = e1.toSymbolReference();
                    SymbolReference e2Ref = e2.toSymbolReference();
                    TableHandle tableHandle = new TableHandle(
                            new CatalogName("local"),
                            new TpchTableHandle("orders", TINY_SCALE_FACTOR),
                            TpchTransactionHandle.INSTANCE,
                            Optional.empty());
                    TableHandle nationTable = new TableHandle(
                            new CatalogName("local"),
                            new TpchTableHandle("nation", TINY_SCALE_FACTOR),
                            TpchTransactionHandle.INSTANCE,
                            Optional.empty());
                    TableScanNode tableScan = p.tableScan(
                            tableHandle,
                            ImmutableList.of(o),
                            ImmutableMap.of(
                                    o, new TpchColumnHandle("orderdate", DATE)));
                    TableScanNode leftTableScan = p.tableScan(
                            tableHandle,
                            ImmutableList.of(o1),
                            ImmutableMap.of(
                                    o1, new TpchColumnHandle("orderdate", DATE)));
                    TableScanNode rightTableScan = p.tableScan(
                            nationTable,
                            ImmutableList.of(o2),
                            ImmutableMap.of(
                                    o2, new TpchColumnHandle("orderdate", DATE)));
                    return p.project(Assignments.identity(o1),
                            p.filter(new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, e1Ref, e2Ref),
                                    p.apply(
                                            Assignments.of(e1, new InPredicate(vRef, oRef)),
                                            emptyList(),
                                            p.apply(Assignments.of(e2, new InPredicate(vRef, o1ref)),
                                                    emptyList(),
                                                    p.values(v),
                                                    p.project(Assignments.identity(o1),
                                                            p.filter(BooleanLiteral.TRUE_LITERAL,
                                                                    p.join(JoinNode.Type.INNER,
                                                                            p.project(Assignments.identity(o1), leftTableScan),
                                                                            p.project(Assignments.identity(o2), rightTableScan),
                                                                            new JoinNode.EquiJoinClause(o1, o2))))),
                                            p.project(Assignments.identity(o),
                                                    p.filter(BooleanLiteral.TRUE_LITERAL, tableScan)))));
                })
                .matches(
                        node(ProjectNode.class,
                                node(FilterNode.class,
                                        node(ApplyNode.class, values("v"),
                                                node(ProjectNode.class,
                                                        node(FilterNode.class,
                                                                node(JoinNode.class,
                                                                        node(ProjectNode.class, node(TableScanNode.class)),
                                                                        node(ProjectNode.class, node(TableScanNode.class)))))))));
    }
}
