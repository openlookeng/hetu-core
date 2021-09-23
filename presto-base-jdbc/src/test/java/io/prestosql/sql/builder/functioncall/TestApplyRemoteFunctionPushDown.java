/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.sql.builder.functioncall;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.optimization.BaseJdbcRowExpressionConverter;
import io.prestosql.plugin.jdbc.optimization.JdbcConverterContext;
import io.prestosql.plugin.jdbc.optimization.TesterParameter;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.SqlFunctionHandle;
import io.prestosql.spi.function.SqlFunctionId;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestApplyRemoteFunctionPushDown
{
    private ApplyRemoteFunctionPushDown applyRemoteFunctionPushDown;
    private BaseJdbcRowExpressionConverter baseJdbcRowExpressionConverter;
    private static final FunctionAndTypeManager FUNCTION_MANAGER = new MockMetadata().getFunctionAndTypeManager();
    private static final FunctionHandle SUM = FUNCTION_MANAGER.lookupFunction("sum", fromTypes(DOUBLE));
    private static final FunctionHandle EXTERNAL_JDBC_V1 = new SqlFunctionHandle(
            new SqlFunctionId(new QualifiedObjectName("jdbc", "v1", "foo"), fromTypes(DOUBLE).stream().map(TypeSignatureProvider::getTypeSignature).collect(Collectors.toList())),
            "v1");
    private static final FunctionHandle EXTERNAL_FOO_V1 = new SqlFunctionHandle(
            new SqlFunctionId(new QualifiedObjectName("foo", "v1", "foo"), fromTypes(DOUBLE).stream().map(TypeSignatureProvider::getTypeSignature).collect(Collectors.toList())),
            "v1");
    private Symbol columnA;
    private PlanSymbolAllocator planSymbolAllocator = new PlanSymbolAllocator();

    @BeforeTest
    public void setup()
    {
        BaseJdbcConfig baseJdbcConfig = new BaseJdbcConfig();
        baseJdbcConfig.setPushDownExternalFunctionNamespace("jdbc.v1");
        TesterParameter testerParameter = TesterParameter.getTesterParameter();
        this.baseJdbcRowExpressionConverter = new BaseJdbcRowExpressionConverter(testerParameter.getMetadata().getFunctionAndTypeManager(), testerParameter.getFunctionResolution(), testerParameter.getRowExpressionService(), testerParameter.getDeterminismEvaluator());
        this.applyRemoteFunctionPushDown = new TestingApplyRemoteFunctionPushDown(baseJdbcConfig, "foo");
        columnA = planSymbolAllocator.newSymbol("a", BIGINT);
    }

    @Test
    public void testRewriteRemoteFunction()
    {
        CallExpression builtin = new CallExpression("sum",
                SUM,
                DOUBLE,
                ImmutableList.of());
        Optional<String> result1 = this.applyRemoteFunctionPushDown.rewriteRemoteFunction(builtin, baseJdbcRowExpressionConverter, new JdbcConverterContext());
        assertFalse(result1.isPresent());

        CallExpression mockExternal = new CallExpression("foo",
                EXTERNAL_JDBC_V1,
                DOUBLE,
                ImmutableList.of());
        Optional<String> result2 = this.applyRemoteFunctionPushDown.rewriteRemoteFunction(mockExternal, baseJdbcRowExpressionConverter, new JdbcConverterContext());
        assertTrue(result2.isPresent());
    }

    @Test
    public void testIsConnectorSupportedRemoteFunction()
    {
        assertFalse(this.applyRemoteFunctionPushDown.isConnectorSupportedRemoteFunction(null));
        CallExpression mockExternalJdbcV1 = new CallExpression("foo",
                EXTERNAL_JDBC_V1,
                DOUBLE,
                ImmutableList.of(castToRowExpression(toSymbolReference(columnA))));
        assertTrue(this.applyRemoteFunctionPushDown.isConnectorSupportedRemoteFunction(mockExternalJdbcV1));
        CallExpression externalFooV1 = new CallExpression("foo",
                EXTERNAL_FOO_V1,
                DOUBLE,
                ImmutableList.of(castToRowExpression(toSymbolReference(columnA))));
        assertFalse(this.applyRemoteFunctionPushDown.isConnectorSupportedRemoteFunction(externalFooV1));
    }
}
