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

package io.prestosql.cube;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.hetu.core.spi.cube.CubeStatement;
import io.prestosql.plugin.tpch.TpchColumnHandle;
import io.prestosql.plugin.tpch.TpchTableHandle;
import io.prestosql.plugin.tpch.TpchTableLayoutHandle;
import io.prestosql.plugin.tpch.TpchTransactionHandle;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import org.testng.annotations.BeforeClass;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.prestosql.metadata.AbstractMockMetadata.dummyMetadata;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;

public class TestCubeStatementGenerator
{
    private PlanBuilder planBuilder;
    private PlanSymbolAllocator symbolAllocator;
    private CubeStatement.Builder builder;

    private Symbol columnOrderkey;
    private Symbol columnAvgPrice;
    private Symbol columnTotalprice;
    private TpchColumnHandle orderkeyHandle;
    private TpchColumnHandle custkeyHandle;
    private TpchColumnHandle totalpriceHandle;
    private TableScanNode baseTableScan;
    private Map<String, Object> columnMapping;

    @BeforeClass
    public void setup()
    {
        planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), dummyMetadata());
        symbolAllocator = new PlanSymbolAllocator();
        builder = CubeStatement.newBuilder();

        columnOrderkey = symbolAllocator.newSymbol("orderkey", BIGINT);
        columnTotalprice = symbolAllocator.newSymbol("totalprice", DOUBLE);
        columnAvgPrice = symbolAllocator.newSymbol("avgprice", DOUBLE);
        orderkeyHandle = new TpchColumnHandle("orderkey", BIGINT);
        totalpriceHandle = new TpchColumnHandle("totalprice", DOUBLE);

        columnMapping = new HashMap<>();
        columnMapping.put("orderkey", orderkeyHandle);
        columnMapping.put("totalprice", totalpriceHandle);
        columnMapping.put("avgprice", columnAvgPrice);

        Map<Symbol, ColumnHandle> assignments = ImmutableMap.<Symbol, ColumnHandle>builder()
                .put(columnOrderkey, orderkeyHandle)
                .put(columnTotalprice, totalpriceHandle)
                .build();

        TpchTableHandle orders = new TpchTableHandle("orders", 1.0);
        TableHandle ordersTableHandle = new TableHandle(new CatalogName("test"),
                orders, TpchTransactionHandle.INSTANCE,
                Optional.of(new TpchTableLayoutHandle(orders, TupleDomain.all())));
        baseTableScan = new TableScanNode(
                new PlanNodeId(UUID.randomUUID().toString()),
                ordersTableHandle,
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                Optional.empty(),
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT,
                new UUID(0, 0),
                0,
                false);
    }
}
