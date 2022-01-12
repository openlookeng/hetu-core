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
package io.prestosql.operator.aggregation.listagg;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.metadata.SqlAggregationFunction;
import io.prestosql.operator.aggregation.AccumulatorCompiler;
import io.prestosql.operator.aggregation.AggregationMetadata;
import io.prestosql.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata;
import io.prestosql.operator.aggregation.GenericAccumulatorFactoryBinder;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorState;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.prestosql.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.prestosql.spi.StandardErrorCode.EXCEEDED_FUNCTION_MEMORY_LIMIT;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.util.Reflection.methodHandle;
import static java.lang.String.format;

public class ListaggAggregationFunction
        extends SqlAggregationFunction
{
    public static final ListaggAggregationFunction LISTAGG = new ListaggAggregationFunction();
    public static final String NAME = "listagg";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(ListaggAggregationFunction.class, "input", Type.class, ListaggAggregationState.class, Block.class, int.class, Slice.class, boolean.class, Slice.class, boolean.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(ListaggAggregationFunction.class, "combine", Type.class, ListaggAggregationState.class, ListaggAggregationState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(ListaggAggregationFunction.class, "output", Type.class, ListaggAggregationState.class, BlockBuilder.class);

    private static final int MAX_OUTPUT_LENGTH = DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
    private static final int MAX_OVERFLOW_FILLER_LENGTH = 65_536;

    private ListaggAggregationFunction()
    {
        super(
                NAME,
                ImmutableList.of(),
                ImmutableList.of(),
                VARCHAR.getTypeSignature(),
                ImmutableList.of(
                        new TypeSignature(StandardTypes.VARCHAR, TypeSignatureParameter.of("v")),
                        new TypeSignature(StandardTypes.VARCHAR, TypeSignatureParameter.of("d")),
                        BOOLEAN.getTypeSignature(),
                        new TypeSignature(StandardTypes.VARCHAR, TypeSignatureParameter.of("f")),
                        BOOLEAN.getTypeSignature()));
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        return generateAggregation(VARCHAR);
    }

    private static InternalAggregationFunction generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(ListaggAggregationFunction.class.getClassLoader());

        AccumulatorStateSerializer<?> stateSerializer = new ListaggAggregationStateSerializer(type);
        AccumulatorStateFactory<?> stateFactory = new ListaggAggregationStateFactory(type);

        List<Type> inputTypes = ImmutableList.of(VARCHAR, VARCHAR, BOOLEAN, VARCHAR, BOOLEAN);
        Type outputType = VARCHAR;
        Type intermediateType = stateSerializer.getSerializedType();
        List<ParameterMetadata> inputParameterMetadata = ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, type),
                new ParameterMetadata(BLOCK_INDEX),
                new ParameterMetadata(INPUT_CHANNEL, VARCHAR),
                new ParameterMetadata(INPUT_CHANNEL, BOOLEAN),
                new ParameterMetadata(INPUT_CHANNEL, VARCHAR),
                new ParameterMetadata(INPUT_CHANNEL, BOOLEAN));

        MethodHandle inputFunction = INPUT_FUNCTION.bindTo(type);
        MethodHandle combineFunction = COMBINE_FUNCTION.bindTo(type);
        MethodHandle outputFunction = OUTPUT_FUNCTION.bindTo(type);
        Class<? extends AccumulatorState> stateInterface = ListaggAggregationState.class;

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, type.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                inputParameterMetadata,
                inputFunction,
                combineFunction,
                outputFunction,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        stateInterface,
                        stateSerializer,
                        stateFactory)),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, ImmutableList.of(intermediateType), outputType, true, true, factory);
    }

    public static void input(Type type, ListaggAggregationState state, Block value, int position, Slice separator, boolean overflowError, Slice overflowFiller, boolean showOverflowEntryCount)
    {
        if (state.isEmpty()) {
            if (overflowFiller.length() > MAX_OVERFLOW_FILLER_LENGTH) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Overflow filler length %d exceeds maximum length %d", overflowFiller.length(), MAX_OVERFLOW_FILLER_LENGTH));
            }
            // Set the parameters of the LISTAGG command within the state so that
            // they can be used within the `output` function
            state.setSeparator(separator);
            state.setOverflowError(overflowError);
            state.setOverflowFiller(overflowFiller);
            state.setShowOverflowEntryCount(showOverflowEntryCount);
        }
        if (!value.isNull(position)) {
            state.add(value, position);
        }
    }

    public static void combine(Type type, ListaggAggregationState state, ListaggAggregationState otherState)
    {
        Slice previousSeparator = state.getSeparator();
        if (previousSeparator == null) {
            state.setSeparator(otherState.getSeparator());
            state.setOverflowError(otherState.isOverflowError());
            state.setOverflowFiller(otherState.getOverflowFiller());
            state.setShowOverflowEntryCount(otherState.showOverflowEntryCount());
        }

        state.merge(otherState);
    }

    public static void output(Type type, ListaggAggregationState state, BlockBuilder out)
    {
        if (state.isEmpty()) {
            out.appendNull();
        }
        else {
            outputState(state, out, MAX_OUTPUT_LENGTH);
        }
    }

    @VisibleForTesting
    protected static void outputState(ListaggAggregationState state, BlockBuilder out, int maxOutputLength)
    {
        Slice separator = state.getSeparator();
        int separatorLength = separator.length();
        OutputContext context = new OutputContext();
        state.forEach((block, position) -> {
            int entryLength = block.getSliceLength(position);
            int spaceRequired = entryLength + (context.emittedEntryCount > 0 ? separatorLength : 0);

            if (context.outputLength + spaceRequired > maxOutputLength) {
                context.overflow = true;
                return false;
            }

            if (context.emittedEntryCount > 0) {
                out.writeBytes(separator, 0, separatorLength);
                context.outputLength += separatorLength;
            }

            block.writeBytesTo(position, 0, entryLength, out);
            context.outputLength += entryLength;
            context.emittedEntryCount++;

            return true;
        });

        if (context.overflow) {
            if (state.isOverflowError()) {
                throw new PrestoException(EXCEEDED_FUNCTION_MEMORY_LIMIT, format("Concatenated string has the length in bytes larger than the maximum output length %d", maxOutputLength));
            }

            if (context.emittedEntryCount > 0) {
                out.writeBytes(separator, 0, separatorLength);
            }
            out.writeBytes(state.getOverflowFiller(), 0, state.getOverflowFiller().length());

            if (state.showOverflowEntryCount()) {
                out.writeBytes(Slices.utf8Slice("("), 0, 1);
                Slice count = Slices.utf8Slice(Integer.toString(state.getEntryCount() - context.emittedEntryCount));
                out.writeBytes(count, 0, count.length());
                out.writeBytes(Slices.utf8Slice(")"), 0, 1);
            }
        }

        out.closeEntry();
    }

    @Override
    public String getDescription()
    {
        return "Function listagg()";
    }

    private static class OutputContext
    {
        long outputLength;
        int emittedEntryCount;
        boolean overflow;
    }
}
