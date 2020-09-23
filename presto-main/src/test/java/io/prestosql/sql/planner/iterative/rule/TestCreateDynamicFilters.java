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
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.prestosql.SystemSessionProperties.DYNAMIC_FILTERING_MAX_SIZE;
import static io.prestosql.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCALE_FACTOR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestCreateDynamicFilters
        extends BaseRuleTest
{
    private static final TableHandle customerTH = new TableHandle(new CatalogName("local"), new TpchTableHandle("customer", TINY_SCALE_FACTOR), TpchTransactionHandle.INSTANCE, Optional.empty());
    private static final TableHandle ordersTH = new TableHandle(new CatalogName("local"), new TpchTableHandle("orders", TINY_SCALE_FACTOR), TpchTransactionHandle.INSTANCE, Optional.empty());

    @Test
    public void testNoPredicate()
    {
        tester().assertThat(new CreateDynamicFilters(tester().getMetadata()))
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "true")
                .on(p -> {
                    TableScanNode left = createTableScan(customerTH, p, BIGINT, "custkey", "nationkey");
                    TableScanNode right = createTableScan(ordersTH, p, BIGINT, "custkey");
                    return buildJoin(p, left, right);
                })
                .matches(PlanMatchPattern.join(JoinNode.Type.INNER,
                        ImmutableList.of(equiJoinClause("custkey", "custkey")),
                        Optional.empty(),
                        PlanMatchPattern.tableScan("customer", ImmutableMap.of("custkey", "custkey", "nationkey", "nationkey")),
                        PlanMatchPattern.tableScan("orders", ImmutableMap.of("custkey", "custkey"))));
    }

    @Test
    public void testWithPredicateTooSelective()
    {
        tester().assertThat(new CreateDynamicFilters(tester().getMetadata()))
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "true")
                .setSystemProperty(DYNAMIC_FILTERING_MAX_SIZE, "100000")
                .on(p -> {
                    TableScanNode left = createTableScan(customerTH, p, BIGINT, "custkey", "nationkey");
                    TableScanNode source = createTableScan(ordersTH, p, BIGINT, "custkey");
                    FilterNode right = p.filter(expression("custkey < 1000"), source);
                    return buildJoin(p, left, right);
                })
                .matches(PlanMatchPattern.join(JoinNode.Type.INNER,
                        ImmutableList.of(equiJoinClause("custkey", "custkey")),
                        Optional.empty(),
                        PlanMatchPattern.tableScan("customer", ImmutableMap.of("custkey", "custkey", "nationkey", "nationkey")),
                        PlanMatchPattern.filter(expression("custkey < 1000"), PlanMatchPattern.tableScan("orders", ImmutableMap.of("custkey", "custkey")))));
    }

    @Test
    public void testWithPredicateExceedMaxSize()
    {
        tester().assertThat(new CreateDynamicFilters(tester().getMetadata()))
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "true")
                .setSystemProperty(DYNAMIC_FILTERING_MAX_SIZE, "10")
                .on(p -> {
                    TableScanNode left = createTableScan(customerTH, p, BIGINT, "custkey", "nationkey");
                    TableScanNode source = createTableScan(ordersTH, p, BIGINT, "custkey");
                    FilterNode right = p.filter(expression("custkey < 200"), source);
                    return buildJoin(p, left, right);
                })
                .matches(PlanMatchPattern.join(JoinNode.Type.INNER,
                        ImmutableList.of(equiJoinClause("custkey", "custkey")),
                        Optional.empty(),
                        PlanMatchPattern.tableScan("customer", ImmutableMap.of("custkey", "custkey", "nationkey", "nationkey")),
                        PlanMatchPattern.filter(expression("custkey < 200"), PlanMatchPattern.tableScan("orders", ImmutableMap.of("custkey", "custkey")))));
    }

    private static PlanNode buildJoin(PlanBuilder p, PlanNode left, PlanNode right)
    {
        Symbol leftKey = p.symbol("custkey", BIGINT);
        Symbol rightKey = p.symbol("custkey", BIGINT);
        Symbol nationKey = p.symbol("nationkey", BIGINT);
        List<Symbol> outputs = ImmutableList.of(leftKey, rightKey, nationKey);

        return p.join(
                JoinNode.Type.INNER,
                left,
                right,
                ImmutableList.of(new JoinNode.EquiJoinClause(leftKey, rightKey)),
                outputs,
                Optional.empty(),
                Optional.of(leftKey),
                Optional.of(rightKey));
    }

    private static TableScanNode createTableScan(TableHandle th, PlanBuilder p, Type type, String... names)
    {
        List<Symbol> symbols = new ArrayList<>();
        for (String name : names) {
            symbols.add(p.symbol(name, type));
        }

        ImmutableMap.Builder<Symbol, ColumnHandle> builder = ImmutableMap.builder();
        for (Symbol sym : symbols) {
            builder.put(sym, new TpchColumnHandle(sym.getName(), type));
        }

        return p.tableScan(th, ImmutableList.copyOf(symbols.stream().toArray(Symbol[]::new)), builder.build());
    }
}
