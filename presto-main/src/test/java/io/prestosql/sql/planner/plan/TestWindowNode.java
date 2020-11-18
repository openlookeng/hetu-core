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
package io.prestosql.sql.planner.plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonModule;
import io.airlift.slice.Slice;
import io.prestosql.metadata.HandleJsonModule;
import io.prestosql.server.SliceDeserializer;
import io.prestosql.server.SliceSerializer;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.plan.OrderingScheme;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.sql.expression.Types;
import io.prestosql.spi.type.TestingTypeDeserializer;
import io.prestosql.spi.type.TestingTypeManager;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.sql.Serialization;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.relational.Expressions.call;
import static org.testng.Assert.assertEquals;

public class TestWindowNode
{
    private PlanSymbolAllocator planSymbolAllocator;
    private ValuesNode sourceNode;
    private Symbol columnA;
    private Symbol columnB;
    private Symbol columnC;

    private final JsonCodec<WindowNode> codec;

    public TestWindowNode()
            throws Exception
    {
        codec = getJsonCodec();
    }

    @BeforeClass
    public void setUp()
    {
        planSymbolAllocator = new PlanSymbolAllocator();
        columnA = planSymbolAllocator.newSymbol("a", BIGINT);
        columnB = planSymbolAllocator.newSymbol("b", BIGINT);
        columnC = planSymbolAllocator.newSymbol("c", BIGINT);

        sourceNode = new ValuesNode(
                newId(),
                ImmutableList.of(columnA, columnB, columnC),
                ImmutableList.of());
    }

    @Test
    public void testSerializationRoundtrip()
            throws Exception
    {
        Symbol windowSymbol = planSymbolAllocator.newSymbol("sum", BIGINT);
        FunctionHandle functionHandle = createTestMetadataManager().getFunctionAndTypeManager().lookupFunction("sum", fromTypes(BIGINT));

        WindowNode.Frame frame = new WindowNode.Frame(
                Types.WindowFrameType.RANGE,
                Types.FrameBoundType.UNBOUNDED_PRECEDING,
                Optional.empty(),
                Types.FrameBoundType.UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        PlanNodeId id = newId();
        WindowNode.Specification specification = new WindowNode.Specification(
                ImmutableList.of(columnA),
                Optional.of(new OrderingScheme(
                        ImmutableList.of(columnB),
                        ImmutableMap.of(columnB, SortOrder.ASC_NULLS_FIRST))));
        CallExpression call = call("sum", functionHandle, BIGINT, ImmutableList.of(new VariableReferenceExpression(columnC.getName(), BIGINT)));
        Map<Symbol, WindowNode.Function> functions = ImmutableMap.of(windowSymbol, new WindowNode.Function(call, ImmutableList.of(new VariableReferenceExpression(columnC.getName(), BIGINT)), frame));
        Optional<Symbol> hashSymbol = Optional.of(columnB);
        Set<Symbol> prePartitionedInputs = ImmutableSet.of(columnA);
        WindowNode windowNode = new WindowNode(
                id,
                sourceNode,
                specification,
                functions,
                hashSymbol,
                prePartitionedInputs,
                0);

        String json = codec.toJson(windowNode);

        WindowNode actualNode = codec.fromJson(json);

        assertEquals(actualNode.getId(), windowNode.getId());
        assertEquals(actualNode.getSpecification(), windowNode.getSpecification());
        assertEquals(actualNode.getWindowFunctions(), windowNode.getWindowFunctions());
        assertEquals(actualNode.getFrames(), windowNode.getFrames());
        assertEquals(actualNode.getHashSymbol(), windowNode.getHashSymbol());
        assertEquals(actualNode.getPrePartitionedInputs(), windowNode.getPrePartitionedInputs());
        assertEquals(actualNode.getPreSortedOrderPrefix(), windowNode.getPreSortedOrderPrefix());
    }

    private static PlanNodeId newId()
    {
        return new PlanNodeId(UUID.randomUUID().toString());
    }

    private JsonCodec<WindowNode> getJsonCodec()
            throws Exception
    {
        Module module = binder -> {
            SqlParser sqlParser = new SqlParser();
            TypeManager typeManager = new TestingTypeManager();
            binder.install(new JsonModule());
            binder.install(new HandleJsonModule());
            binder.bind(SqlParser.class).toInstance(sqlParser);
            binder.bind(TypeManager.class).toInstance(typeManager);
            configBinder(binder).bindConfig(FeaturesConfig.class);
            newSetBinder(binder, Type.class);
            jsonBinder(binder).addSerializerBinding(Slice.class).to(SliceSerializer.class);
            jsonBinder(binder).addDeserializerBinding(Slice.class).to(SliceDeserializer.class);
            jsonBinder(binder).addDeserializerBinding(Type.class).to(TestingTypeDeserializer.class);
            jsonBinder(binder).addSerializerBinding(Expression.class).to(Serialization.ExpressionSerializer.class);
            jsonBinder(binder).addDeserializerBinding(Expression.class).to(Serialization.ExpressionDeserializer.class);
            jsonBinder(binder).addDeserializerBinding(FunctionCall.class).to(Serialization.FunctionCallDeserializer.class);
            jsonBinder(binder).addKeySerializerBinding(VariableReferenceExpression.class).to(Serialization.VariableReferenceExpressionSerializer.class);
            jsonBinder(binder).addKeyDeserializerBinding(VariableReferenceExpression.class).to(Serialization.VariableReferenceExpressionDeserializer.class);
            jsonCodecBinder(binder).bindJsonCodec(WindowNode.class);
        };
        Bootstrap app = new Bootstrap(ImmutableList.of(module));
        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        return injector.getInstance(new Key<JsonCodec<WindowNode>>() {});
    }
}
