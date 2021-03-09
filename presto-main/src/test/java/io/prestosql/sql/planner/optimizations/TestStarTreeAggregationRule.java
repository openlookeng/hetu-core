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

package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import io.hetu.core.spi.cube.CubeMetadata;
import io.hetu.core.spi.cube.CubeStatement;
import io.hetu.core.spi.cube.io.CubeMetaStore;
import io.prestosql.Session;
import io.prestosql.cube.CubeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.TableMetadata;
import io.prestosql.plugin.tpch.TpchColumnHandle;
import io.prestosql.plugin.tpch.TpchTableHandle;
import io.prestosql.plugin.tpch.TpchTableLayoutHandle;
import io.prestosql.plugin.tpch.TpchTransactionHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.cube.CubeProvider;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.util.DateTimeUtils;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.planner.FunctionCallBuilder;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.SymbolUtils;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.QualifiedName;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.Returns;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.prestosql.SystemSessionProperties.ENABLE_STAR_TREE_INDEX;
import static io.prestosql.metadata.AbstractMockMetadata.dummyMetadata;
import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.plan.AggregationNode.Step.SINGLE;
import static io.prestosql.spi.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestStarTreeAggregationRule
        extends BaseRuleTest
{
    private final PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), dummyMetadata());
    private Symbol output;
    private Symbol columnOrderkey;
    private Symbol columnOrderDate;
    private Symbol columnCustkey;
    private Symbol columnTotalprice;
    private TpchColumnHandle orderkeyHandle;
    private TpchColumnHandle orderdateHandle;
    private TpchColumnHandle custkeyHandle;
    private TpchColumnHandle totalpriceHandle;
    private Map<Symbol, ColumnHandle> assignments;
    private TableHandle ordersTableHandle;
    private TableScanNode baseTableScan;
    private FeaturesConfig config;
    private CubeManager cubeManager;
    private CubeProvider provider;
    private CubeMetaStore cubeMetaStore;
    private CubeMetadata cubeMetadata;

    private TableHandle ordersCubeHandle;
    private Symbol columnCountAll;
    private Symbol columnSumTotalPrice;
    private Symbol columnCountOrderKey;
    private Symbol columnGroupingBitSet;
    private Symbol cubeColumnOrderDate;
    private Symbol cubeColumnCustKey;

    private TpchColumnHandle countAllHandle;
    private TpchColumnHandle sumTotalPriceHandle;
    private TpchColumnHandle countOrderKeyHandle;
    private TpchColumnHandle groupingBitSetHandle;
    private TpchColumnHandle orderDateCubeColumnHandle;
    private TpchColumnHandle custKeyCubeColumnHandle;
    private Map<String, ColumnHandle> ordersCubeColumnHandles = new HashMap<>();

    private ColumnMetadata countAllColumnMetadata;
    private ColumnMetadata sumTotalPriceColumnMetadata;
    private ColumnMetadata countOrderKeyColumnMetadata;
    private ColumnMetadata groupingBitSetColumnMetadata;
    private ColumnMetadata orderDateCubeColumnMetadata;
    private ColumnMetadata custKeyCubeColumnMetadata;

    private Matcher<TableHandle> ordersTableHandleMatcher;
    private Matcher<TableHandle> ordersCubeHandleMatcher;
    private Matcher<ColumnHandle> countAllColumnHandleMatcher;
    private Matcher<ColumnHandle> sumTotalPriceColumnHandleMatcher;
    private Matcher<ColumnHandle> countOrderKeyColumnHandleMatcher;
    private Matcher<ColumnHandle> groupingBitSetColumnHandleMatcher;
    private Matcher<ColumnHandle> orderDateCubeColumnHandleMatcher;
    private Matcher<ColumnHandle> custKeyCubeColumnHandleMatcher;

    @BeforeClass
    public void setupBeforeClass()
    {
        PlanSymbolAllocator symbolAllocator = new PlanSymbolAllocator();
        columnOrderkey = symbolAllocator.newSymbol("orderkey", BIGINT);
        columnOrderDate = symbolAllocator.newSymbol("orderdate", DATE);
        columnCustkey = symbolAllocator.newSymbol("custkey", BIGINT);
        columnTotalprice = symbolAllocator.newSymbol("totalprice", DOUBLE);
        orderkeyHandle = new TpchColumnHandle("orderkey", BIGINT);
        orderdateHandle = new TpchColumnHandle("orderdate", DATE);
        custkeyHandle = new TpchColumnHandle("custkey", BIGINT);
        totalpriceHandle = new TpchColumnHandle("totalprice", DOUBLE);

        output = symbolAllocator.newSymbol("output", DOUBLE);
        assignments = ImmutableMap.<Symbol, ColumnHandle>builder()
                .put(columnOrderkey, orderkeyHandle)
                .put(columnOrderDate, orderdateHandle)
                .put(columnCustkey, custkeyHandle)
                .put(columnTotalprice, totalpriceHandle)
                .build();

        TpchTableHandle orders = new TpchTableHandle("orders", 1.0);
        ordersTableHandle = new TableHandle(tester().getCurrentConnectorId(),
                orders, TpchTransactionHandle.INSTANCE,
                Optional.of(new TpchTableLayoutHandle(orders, TupleDomain.all())));
        baseTableScan = new TableScanNode(
                newId(),
                ordersTableHandle,
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.all(),
                Optional.empty(),
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT,
                new UUID(0, 0),
                0,
                false);

        columnCountAll = symbolAllocator.newSymbol("count_all", BIGINT);
        columnSumTotalPrice = symbolAllocator.newSymbol("sum_totalprice", DOUBLE);
        columnCountOrderKey = symbolAllocator.newSymbol("count_orderkey", BIGINT);
        columnGroupingBitSet = symbolAllocator.newSymbol("grouping_bit_set", BIGINT);
        cubeColumnCustKey = symbolAllocator.newSymbol("custkey", BIGINT);
        cubeColumnOrderDate = symbolAllocator.newSymbol("orderdate", DATE);

        countAllHandle = new TpchColumnHandle("count_all", BIGINT);
        sumTotalPriceHandle = new TpchColumnHandle("sum_totalprice", DOUBLE);
        countOrderKeyHandle = new TpchColumnHandle("count_orderkey", BIGINT);
        groupingBitSetHandle = new TpchColumnHandle("grouping_bit_set", BIGINT);
        custKeyCubeColumnHandle = new TpchColumnHandle("custkey", BIGINT);
        orderDateCubeColumnHandle = new TpchColumnHandle("orderdate", DATE);

        ordersCubeColumnHandles.put(countAllHandle.getColumnName(), countAllHandle);
        ordersCubeColumnHandles.put(sumTotalPriceHandle.getColumnName(), sumTotalPriceHandle);
        ordersCubeColumnHandles.put(countOrderKeyHandle.getColumnName(), countOrderKeyHandle);
        ordersCubeColumnHandles.put(groupingBitSetHandle.getColumnName(), groupingBitSetHandle);
        ordersCubeColumnHandles.put(custKeyCubeColumnHandle.getColumnName(), custKeyCubeColumnHandle);
        ordersCubeColumnHandles.put(orderDateCubeColumnHandle.getColumnName(), orderDateCubeColumnHandle);

        TpchTableHandle ordersCube = new TpchTableHandle("orders_cube", 1.0);
        ordersCubeHandle = new TableHandle(tester().getCurrentConnectorId(),
                ordersCube, TpchTransactionHandle.INSTANCE,
                Optional.of(new TpchTableLayoutHandle(ordersCube, TupleDomain.all())));

        countAllColumnMetadata = new ColumnMetadata(countAllHandle.getColumnName(), countAllHandle.getType());
        sumTotalPriceColumnMetadata = new ColumnMetadata(sumTotalPriceHandle.getColumnName(), sumTotalPriceHandle.getType());
        countOrderKeyColumnMetadata = new ColumnMetadata(countOrderKeyHandle.getColumnName(), countOrderKeyHandle.getType());
        groupingBitSetColumnMetadata = new ColumnMetadata(groupingBitSetHandle.getColumnName(), groupingBitSetHandle.getType());
        custKeyCubeColumnMetadata = new ColumnMetadata(custKeyCubeColumnHandle.getColumnName(), custKeyCubeColumnHandle.getType());
        orderDateCubeColumnMetadata = new ColumnMetadata(orderDateCubeColumnHandle.getColumnName(), orderDateCubeColumnHandle.getType());

        config = new FeaturesConfig();
        config.setEnableStarTreeIndex(true);

        cubeManager = Mockito.mock(CubeManager.class);
        provider = Mockito.mock(CubeProvider.class);
        cubeMetaStore = Mockito.mock(CubeMetaStore.class);
        cubeMetadata = Mockito.mock(CubeMetadata.class);

        ordersTableHandleMatcher = new BaseMatcher<TableHandle>() {
            @Override
            public boolean matches(Object o)
            {
                if (!(o instanceof TableHandle)) {
                    return false;
                }
                TableHandle th = (TableHandle) o;
                return th.getFullyQualifiedName().equals(ordersTableHandle.getFullyQualifiedName());
            }

            @Override
            public void describeTo(Description description)
            {
            }
        };

        ordersCubeHandleMatcher = new BaseMatcher<TableHandle>() {
            @Override
            public boolean matches(Object o)
            {
                if (!(o instanceof TableHandle)) {
                    return false;
                }
                TableHandle th = (TableHandle) o;
                return th.getFullyQualifiedName().equals(ordersCubeHandle.getFullyQualifiedName());
            }

            @Override
            public void describeTo(Description description)
            {
            }
        };

        countAllColumnHandleMatcher = new BaseMatcher<ColumnHandle>() {
            @Override
            public void describeTo(Description description)
            {
            }

            @Override
            public boolean matches(Object o)
            {
                if (!(o instanceof ColumnHandle)) {
                    return false;
                }
                ColumnHandle ch = (ColumnHandle) o;
                return ch.getColumnName().equalsIgnoreCase(countAllHandle.getColumnName());
            }
        };

        sumTotalPriceColumnHandleMatcher = new BaseMatcher<ColumnHandle>() {
            @Override
            public void describeTo(Description description)
            {
            }

            @Override
            public boolean matches(Object o)
            {
                if (!(o instanceof ColumnHandle)) {
                    return false;
                }
                ColumnHandle ch = (ColumnHandle) o;
                return ch.getColumnName().equalsIgnoreCase(sumTotalPriceHandle.getColumnName());
            }
        };

        countOrderKeyColumnHandleMatcher = new BaseMatcher<ColumnHandle>() {
            @Override
            public boolean matches(Object o)
            {
                if (!(o instanceof ColumnHandle)) {
                    return false;
                }
                ColumnHandle ch = (ColumnHandle) o;
                return ch.getColumnName().equalsIgnoreCase(countOrderKeyHandle.getColumnName());
            }

            @Override
            public void describeTo(Description description)
            {
            }
        };

        groupingBitSetColumnHandleMatcher = new BaseMatcher<ColumnHandle>() {
            @Override
            public boolean matches(Object o)
            {
                if (!(o instanceof ColumnHandle)) {
                    return false;
                }
                ColumnHandle ch = (ColumnHandle) o;
                return ch.getColumnName().equalsIgnoreCase(groupingBitSetHandle.getColumnName());
            }

            @Override
            public void describeTo(Description description)
            {
            }
        };

        orderDateCubeColumnHandleMatcher = new BaseMatcher<ColumnHandle>() {
            @Override
            public boolean matches(Object o)
            {
                if (!(o instanceof ColumnHandle)) {
                    return false;
                }
                ColumnHandle ch = (ColumnHandle) o;
                return ch.getColumnName().equalsIgnoreCase(orderDateCubeColumnHandle.getColumnName());
            }

            @Override
            public void describeTo(Description description)
            {
            }
        };

        custKeyCubeColumnHandleMatcher = new BaseMatcher<ColumnHandle>() {
            @Override
            public boolean matches(Object o)
            {
                if (!(o instanceof ColumnHandle)) {
                    return false;
                }
                ColumnHandle ch = (ColumnHandle) o;
                return ch.getColumnName().equalsIgnoreCase(custKeyCubeColumnHandle.getColumnName());
            }

            @Override
            public void describeTo(Description description)
            {
            }
        };
    }

    @Test
    public void testSupportedProjectNode()
    {
        //node is projectNode and expression instance of cast
        Assignments assignment1 = Assignments.builder()
                .put(columnCustkey, new VariableReferenceExpression(columnCustkey.getName(), custkeyHandle.getType()))
                .build();
        Optional<PlanNode> planNode1 = Optional.of(new ProjectNode(
                newId(),
                baseTableScan,
                assignment1));
        assertTrue(StarTreeAggregationRule.supportedProjectNode(planNode1));

        //not projectNode
        ListMultimap<Symbol, Symbol> mappings = ImmutableListMultimap.<Symbol, Symbol>builder()
                .put(output, columnOrderkey)
                .put(output, columnOrderkey)
                .build();
        Optional<PlanNode> planNode2 = Optional.of(new UnionNode(
                newId(),
                ImmutableList.of(baseTableScan, baseTableScan),
                mappings,
                ImmutableList.copyOf(mappings.keySet())));
        assertFalse(StarTreeAggregationRule.supportedProjectNode(planNode2));

        //expression not instance of Cast
        Assignments assignment2 = Assignments.builder()
                .put(columnCustkey, new InputReferenceExpression(1, IntegerType.INTEGER))
                .build();
        Optional<PlanNode> planNode3 = Optional.of(new ProjectNode(
                newId(),
                baseTableScan,
                assignment2));
        assertFalse(StarTreeAggregationRule.supportedProjectNode(planNode3));

        //expression is instance of SymbolReference OR Literal
        Assignments assignment3 = Assignments.builder()
                .put(columnCustkey, new VariableReferenceExpression(columnCustkey.getName(), custkeyHandle.getType())) // should be INTEGER
                .build();
        Optional<PlanNode> planNode4 = Optional.of(new ProjectNode(
                newId(),
                baseTableScan,
                assignment3));
        assertTrue(StarTreeAggregationRule.supportedProjectNode(planNode4));

        //empty node
        Optional<PlanNode> emptyPlanNode = Optional.empty();
        assertTrue(StarTreeAggregationRule.supportedProjectNode(emptyPlanNode));
    }

    @Test
    public void testSupportedAggregations()
    {
        //function supported
        AggregationNode countAgg = new AggregationNode(
                newId(),
                baseTableScan,
                ImmutableMap.of(columnOrderkey, new AggregationNode.Aggregation(
                        new Signature("count", AGGREGATE, DOUBLE.getTypeSignature()),
                        ImmutableList.of(planBuilder.variable(columnOrderkey.getName(), orderkeyHandle.getType())),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(columnOrderkey)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());
        assertTrue(StarTreeAggregationRule.isSupportedAggregation(countAgg));

        AggregationNode countDistinctNode = new AggregationNode(
                newId(),
                baseTableScan,
                ImmutableMap.of(columnOrderkey, new AggregationNode.Aggregation(
                        new Signature("count", AGGREGATE, DOUBLE.getTypeSignature()),
                        ImmutableList.of(planBuilder.variable(columnOrderkey.getName(), orderkeyHandle.getType())),
                        true,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(columnOrderkey)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());
        assertTrue(StarTreeAggregationRule.isSupportedAggregation(countDistinctNode));

        AggregationNode sumAgg = new AggregationNode(
                newId(),
                baseTableScan,
                ImmutableMap.of(columnOrderkey, new AggregationNode.Aggregation(
                        new Signature("sum", AGGREGATE, DOUBLE.getTypeSignature()),
                        ImmutableList.of(planBuilder.variable(columnOrderkey.getName(), orderkeyHandle.getType())),
                        true,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(columnOrderkey)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());
        assertFalse(StarTreeAggregationRule.isSupportedAggregation(sumAgg));

        AggregationNode avgAgg = new AggregationNode(
                newId(),
                baseTableScan,
                ImmutableMap.of(columnOrderkey, new AggregationNode.Aggregation(
                        new Signature("avg", AGGREGATE, DOUBLE.getTypeSignature()),
                        ImmutableList.of(planBuilder.variable(columnOrderkey.getName(), orderkeyHandle.getType())),
                        true,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(columnOrderkey)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());
        assertFalse(StarTreeAggregationRule.isSupportedAggregation(avgAgg));

        AggregationNode minAgg = new AggregationNode(
                newId(),
                baseTableScan,
                ImmutableMap.of(columnOrderkey, new AggregationNode.Aggregation(
                        new Signature("min", AGGREGATE, DOUBLE.getTypeSignature()),
                        ImmutableList.of(planBuilder.variable(columnOrderkey.getName(), orderkeyHandle.getType())),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(columnOrderkey)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());
        assertTrue(StarTreeAggregationRule.isSupportedAggregation(minAgg));

        AggregationNode maxAgg = new AggregationNode(
                newId(),
                baseTableScan,
                ImmutableMap.of(columnOrderkey, new AggregationNode.Aggregation(
                        new Signature("max", AGGREGATE, DOUBLE.getTypeSignature()),
                        ImmutableList.of(planBuilder.variable(columnOrderkey.getName(), orderkeyHandle.getType())),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(columnOrderkey)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());
        assertTrue(StarTreeAggregationRule.isSupportedAggregation(maxAgg));

        //function not supported
        AggregationNode lagAgg = new AggregationNode(
                newId(),
                baseTableScan,
                ImmutableMap.of(columnOrderkey, new AggregationNode.Aggregation(
                        new Signature("lag", AGGREGATE, DOUBLE.getTypeSignature()),
                        ImmutableList.of(planBuilder.variable(columnOrderkey.getName(), orderkeyHandle.getType())),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(columnOrderkey)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());
        assertFalse(StarTreeAggregationRule.isSupportedAggregation(lagAgg));

        //empty node
        AggregationNode emptyAggNode = planBuilder.aggregation(PlanBuilder.AggregationBuilder::singleGroupingSet);
        assertFalse(StarTreeAggregationRule.isSupportedAggregation(emptyAggNode));
    }

    @Test
    public void testBuildSymbolMappings()
    {
        //expression instanceof SymbolReference
        RowExpression rowExpression = planBuilder.variable(columnTotalprice.getName(), totalpriceHandle.getType());
        FilterNode filterNode = new FilterNode(newId(), baseTableScan, rowExpression);

        Assignments projectionAssignments = Assignments.builder()
                .put(columnCustkey, planBuilder.variable(columnCustkey.getName(), custkeyHandle.getType()))
                .build();
        ProjectNode projectNode1 = new ProjectNode(newId(), filterNode, projectionAssignments);
        List<ProjectNode> projectNodes = ImmutableList.of(projectNode1);
        AggregationNode countAggNode = new AggregationNode(
                newId(),
                projectNode1,
                ImmutableMap.of(columnCustkey, new AggregationNode.Aggregation(
                        new Signature("count", AGGREGATE, DOUBLE.getTypeSignature()),
                        ImmutableList.of(planBuilder.variable(columnCustkey.getName(), custkeyHandle.getType())),
                        true,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(columnOrderkey, columnOrderDate)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());
        Map<String, Object> columnMapping = StarTreeAggregationRule.buildSymbolMappings(countAggNode, projectNodes, Optional.of(filterNode), baseTableScan);
        assertEquals(columnMapping.size(), 4);
        assertEquals(new ArrayList<>(columnMapping.keySet()).toString(), "[orderkey, custkey, totalprice, orderdate]");
        assertTrue(columnMapping.values().containsAll(assignments.values()) && assignments.values().containsAll(columnMapping.values()));

        //expression instanceof Literal
        DoubleLiteral testDouble = new DoubleLiteral("1.0");
        Assignments assignment2 = Assignments.builder()
                .put(columnCustkey, OriginalExpressionUtils.castToRowExpression(testDouble))
                .build();
        ProjectNode projectNode2 = new ProjectNode(newId(), filterNode, assignment2);
        List<ProjectNode> projections2 = ImmutableList.of(projectNode2);
        columnMapping = StarTreeAggregationRule.buildSymbolMappings(countAggNode, projections2, Optional.of(filterNode), baseTableScan);
        assertEquals(new ArrayList(columnMapping.values()).get(1), testDouble);

        //expression instanceof Cast/SymbolReference
        Cast testCast = new Cast(SymbolUtils.toSymbolReference(columnCustkey), StandardTypes.INTEGER);
        Assignments assignment3 = Assignments.builder()
                .put(columnCustkey, OriginalExpressionUtils.castToRowExpression(testCast))
                .build();
        ProjectNode planNode3 = new ProjectNode(newId(), baseTableScan, assignment3);
        List<ProjectNode> projections3 = ImmutableList.of(planNode3);
        columnMapping = StarTreeAggregationRule.buildSymbolMappings(countAggNode, projections3, Optional.of(filterNode), baseTableScan);
        assertEquals(new ArrayList(columnMapping.values()).get(1), custkeyHandle);

        //expression instanceof Cast/Literal
        Cast testCast2 = new Cast(testDouble, StandardTypes.DOUBLE);
        Assignments assignment4 = Assignments.builder()
                .put(columnCustkey, OriginalExpressionUtils.castToRowExpression(testCast2))
                .build();
        ProjectNode planNode4 = new ProjectNode(newId(), baseTableScan, assignment4);
        List<ProjectNode> projections4 = ImmutableList.of(planNode4);
        columnMapping = StarTreeAggregationRule.buildSymbolMappings(countAggNode, projections4, Optional.of(filterNode), baseTableScan);
        assertEquals(new ArrayList(columnMapping.values()).get(1), testDouble);
    }

    @Test
    public void testDoNotFireWhenFeatureIsDisabled()
    {
        Mockito.when(cubeManager.getCubeProvider(anyString())).then(new Returns(Optional.of(provider)));
        Mockito.when(cubeManager.getMetaStore(anyString())).then(new Returns(Optional.of(cubeMetaStore)));

        TpchTableHandle orders = new TpchTableHandle("orders", 1.0);
        TableHandle ordersTableHandle = new TableHandle(tester().getCurrentConnectorId(),
                orders, TpchTransactionHandle.INSTANCE,
                Optional.of(new TpchTableLayoutHandle(orders, TupleDomain.all())));

        StarTreeAggregationRule starTreeAggregationRule = new StarTreeAggregationRule(cubeManager, tester().getMetadata());
        tester().assertThat(starTreeAggregationRule)
                .setSystemProperty(ENABLE_STAR_TREE_INDEX, "false")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("count", BIGINT), new FunctionCallBuilder(tester().getMetadata()).setName(QualifiedName.of("count")).build(),
                                ImmutableList.of(BIGINT))
                        .step(SINGLE)
                        .source(
                                p.project(Assignments.builder().put(p.symbol("custkey"), p.variable("custkey", custkeyHandle.getType())).build(),
                                        p.tableScan(ordersTableHandle, ImmutableList.of(p.symbol("orderkey", BIGINT)),
                                                ImmutableMap.of(p.symbol("orderkey", BIGINT), new TpchColumnHandle("orderkey", BIGINT)))))))
                .doesNotFire();
        Mockito.verify(cubeMetaStore, Mockito.never()).getMetadataList(eq("local.sf1.0.orders"));
    }

    @Test
    public void testDoNotFireWhenWithoutCube()
    {
        Mockito.when(cubeManager.getCubeProvider(anyString())).then(new Returns(Optional.of(provider)));
        Mockito.when(cubeManager.getMetaStore(anyString())).then(new Returns(Optional.of(cubeMetaStore)));

        TpchTableHandle orders = new TpchTableHandle("orders", 1.0);
        TableHandle ordersTableHandle = new TableHandle(tester().getCurrentConnectorId(),
                orders, TpchTransactionHandle.INSTANCE,
                Optional.of(new TpchTableLayoutHandle(orders, TupleDomain.all())));

        Mockito.when(cubeMetaStore.getMetadataList(eq("local.sf1.0.orders"))).then(new Returns(ImmutableList.of()));

        StarTreeAggregationRule starTreeAggregationRule = new StarTreeAggregationRule(cubeManager, tester().getMetadata());
        tester().assertThat(starTreeAggregationRule)
                .setSystemProperty(ENABLE_STAR_TREE_INDEX, "true")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("count", BIGINT), new FunctionCallBuilder(tester().getMetadata()).setName(QualifiedName.of("count")).build(),
                                ImmutableList.of(BIGINT))
                        .step(SINGLE)
                        .source(
                                p.project(Assignments.builder().put(p.symbol("custkey"), OriginalExpressionUtils.castToRowExpression(SymbolUtils.toSymbolReference(p.symbol("custkey")))).build(),
                                        p.tableScan(ordersTableHandle, ImmutableList.of(p.symbol("orderkey", BIGINT)),
                                                ImmutableMap.of(p.symbol("orderkey", BIGINT), new TpchColumnHandle("orderkey", BIGINT)))))))
                .doesNotFire();
        Mockito.verify(cubeMetaStore, Mockito.atLeastOnce()).getMetadataList(eq("local.sf1.0.orders"));
    }

    @Test
    public void testDoNotFireWhenMatchingCubeNotFound()
    {
        Mockito.when(cubeManager.getCubeProvider(anyString())).then(new Returns(Optional.of(provider)));
        Mockito.when(cubeManager.getMetaStore(anyString())).then(new Returns(Optional.of(cubeMetaStore)));

        TpchTableHandle orders = new TpchTableHandle("orders", 1.0);
        TableHandle ordersTableHandle = new TableHandle(tester().getCurrentConnectorId(),
                orders, TpchTransactionHandle.INSTANCE,
                Optional.of(new TpchTableLayoutHandle(orders, TupleDomain.all())));

        List<CubeMetadata> metadataList = ImmutableList.of(cubeMetadata);
        Mockito.when(cubeMetaStore.getMetadataList(eq("local.sf1.0.orders"))).then(new Returns(metadataList));
        Mockito.when(cubeMetadata.matches(any(CubeStatement.class))).thenReturn(false);

        StarTreeAggregationRule starTreeAggregationRule = new StarTreeAggregationRule(cubeManager, tester().getMetadata());
        tester().assertThat(starTreeAggregationRule)
                .setSystemProperty(ENABLE_STAR_TREE_INDEX, "true")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("count", BIGINT), new FunctionCallBuilder(tester().getMetadata()).setName(QualifiedName.of("count")).build(),
                                ImmutableList.of(BIGINT))
                        .step(SINGLE)
                        .source(
                                p.project(Assignments.builder().put(p.symbol("custkey"), OriginalExpressionUtils.castToRowExpression(SymbolUtils.toSymbolReference(p.symbol("custkey")))).build(),
                                        p.filter(expression("orderkey=1"),
                                                p.tableScan(ordersTableHandle, ImmutableList.of(p.symbol("orderkey", BIGINT)),
                                                        ImmutableMap.of(p.symbol("orderkey", BIGINT), new TpchColumnHandle("orderkey", BIGINT))))))))
                .doesNotFire();
        Mockito.verify(cubeMetaStore, Mockito.atLeastOnce()).getMetadataList(eq("local.sf1.0.orders"));
        Mockito.verify(cubeMetadata, Mockito.atLeastOnce()).matches(any(CubeStatement.class));
    }

    @Test
    public void testDoNotUseCubeIfOriginalTableUpdatedAfterCubeCreated()
    {
        Mockito.when(cubeManager.getCubeProvider(anyString())).then(new Returns(Optional.of(provider)));
        Mockito.when(cubeManager.getMetaStore(anyString())).then(new Returns(Optional.of(cubeMetaStore)));

        Metadata metadata = Mockito.mock(Metadata.class);
        TableMetadata ordersTableMetadata = Mockito.mock(TableMetadata.class);
        QualifiedObjectName objectName = new QualifiedObjectName("local", "sf1.0", "orders");
        Mockito.when(metadata.getTableHandle(any(Session.class), eq(objectName))).thenReturn(Optional.of(ordersTableHandle));
        Mockito.when(metadata.getTableLastModifiedTimeSupplier(any(Session.class), any(TableHandle.class)))
                .thenReturn(() -> DateTimeUtils.parseTimestampWithoutTimeZone("2020-01-02 12:00:00"));
        Mockito.when(metadata.getTableMetadata(any(Session.class), eq(ordersTableHandle))).thenReturn(ordersTableMetadata);
        Mockito.when(ordersTableMetadata.getQualifiedName()).thenReturn(objectName);

        List<CubeMetadata> metadataList = ImmutableList.of(cubeMetadata);
        Mockito.when(cubeMetaStore.getMetadataList(eq("local.sf1.0.orders"))).then(new Returns(metadataList));
        Mockito.when(cubeMetadata.matches(any(CubeStatement.class))).thenReturn(true);
        Mockito.when(cubeMetadata.getLastUpdated()).thenReturn(DateTimeUtils.parseTimestampWithoutTimeZone("2020-01-01 12:00:00"));

        StarTreeAggregationRule starTreeAggregationRule = new StarTreeAggregationRule(cubeManager, metadata);
        tester().assertThat(starTreeAggregationRule)
                .setSystemProperty(ENABLE_STAR_TREE_INDEX, "true")
                .on(p -> p.aggregation(builder -> builder
                        .step(SINGLE)
                        .addAggregation(new Symbol("count_orderkey"), PlanBuilder.expression("count(orderkey)"), ImmutableList.of(BIGINT))
                        .singleGroupingSet(new Symbol("orderdate"))
                        .source(
                                p.project(
                                        Assignments.builder()
                                                .put(p.symbol("orderdate", DATE), p.variable("orderdate", DATE))
                                                .put(p.symbol("orderkey", BIGINT), p.variable("orderkey", BIGINT))
                                                .build(),
                                        p.tableScan(ordersTableHandle,
                                                ImmutableList.of(p.symbol("orderdate", DATE), p.symbol("orderkey", BIGINT)),
                                                ImmutableMap.of(p.symbol("orderkey", BIGINT), new TpchColumnHandle("orderkey", BIGINT),
                                                        p.symbol("orderdate", DATE), new TpchColumnHandle("orderdate", DATE)))))))
                .doesNotFire();
        Mockito.verify(cubeMetaStore, Mockito.atLeastOnce()).getMetadataList(eq("local.sf1.0.orders"));
        Mockito.verify(cubeMetadata, Mockito.atLeastOnce()).matches(any(CubeStatement.class));
    }

    private static PlanNodeId newId()
    {
        return new PlanNodeId(UUID.randomUUID().toString());
    }
}
