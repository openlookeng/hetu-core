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
package io.prestosql.sql;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.planner.FunctionCallBuilder;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SymbolReference;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.sql.RowExpressionUtils.extractConjuncts;
import static io.prestosql.spi.type.StandardTypes.BOOLEAN;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.relational.Expressions.call;
import static java.util.Objects.requireNonNull;

public final class DynamicFilters
{
    private DynamicFilters() {}

    public static Expression createDynamicFilterExpression(Metadata metadata, String id, Type inputType, SymbolReference input)
    {
        return new FunctionCallBuilder(metadata)
                .setName(QualifiedName.of(Function.NAME))
                .addArgument(VarcharType.VARCHAR, new StringLiteral(id))
                .addArgument(inputType, input)
                .build();
    }

    public static RowExpression createDynamicFilterRowExpression(Metadata metadata, TypeManager typeManager, String id, Type inputType, SymbolReference input)
    {
        ConstantExpression string = new ConstantExpression(utf8Slice(id), VarcharType.VARCHAR);
        VariableReferenceExpression expression = new VariableReferenceExpression(input.getName(), inputType);
        Signature signature = metadata.resolveFunction(QualifiedName.of(Function.NAME), fromTypes(VarcharType.VARCHAR, inputType));
        return call(signature, typeManager.getType(signature.getReturnType()), string, expression);
    }

    public static ExtractResult extractDynamicFilters(RowExpression expression)
    {
        List<RowExpression> conjuncts = extractConjuncts(expression);

        ImmutableList.Builder<RowExpression> staticConjuncts = ImmutableList.builder();
        ImmutableList.Builder<Descriptor> dynamicConjuncts = ImmutableList.builder();

        for (RowExpression conjunct : conjuncts) {
            Optional<Descriptor> descriptor = getDescriptor(conjunct);
            if (descriptor.isPresent()) {
                dynamicConjuncts.add(descriptor.get());
            }
            else {
                staticConjuncts.add(conjunct);
            }
        }

        return new ExtractResult(staticConjuncts.build(), dynamicConjuncts.build());
    }

    public static boolean isDynamicFilter(RowExpression expression)
    {
        return getDescriptor(expression).isPresent();
    }

    public static Optional<Descriptor> getDescriptor(RowExpression expression)
    {
        if (!(expression instanceof CallExpression)) {
            return Optional.empty();
        }

        CallExpression callExpression = (CallExpression) expression;

        if (!callExpression.getSignature().getName().contains(Function.NAME)) {
            return Optional.empty();
        }

        List<RowExpression> arguments = callExpression.getArguments();
        checkArgument(arguments.size() == 2, "invalid arguments count: %s", arguments.size());

        RowExpression firstArgument = arguments.get(0);
        checkArgument(firstArgument instanceof ConstantExpression, "firstArgument is expected to be an instance of ConstantExpression: %s", firstArgument.getClass().getSimpleName());
        Object firstArgumentValue = ((ConstantExpression) firstArgument).getValue();
        String id = (firstArgumentValue instanceof String) ? (String) (firstArgumentValue) : ((Slice) (firstArgumentValue)).toStringUtf8();
        return Optional.of(new Descriptor(id, arguments.get(1)));
    }

    public static class ExtractResult
    {
        private final List<RowExpression> staticConjuncts;
        private final List<Descriptor> dynamicConjuncts;

        public ExtractResult(List<RowExpression> staticConjuncts, List<Descriptor> dynamicConjuncts)
        {
            this.staticConjuncts = ImmutableList.copyOf(requireNonNull(staticConjuncts, "staticConjuncts is null"));
            this.dynamicConjuncts = ImmutableList.copyOf(requireNonNull(dynamicConjuncts, "dynamicConjuncts is null"));
        }

        public List<RowExpression> getStaticConjuncts()
        {
            return staticConjuncts;
        }

        public List<Descriptor> getDynamicConjuncts()
        {
            return dynamicConjuncts;
        }
    }

    public static final class Descriptor
    {
        private final String id;
        private final RowExpression input;

        public Descriptor(String id, RowExpression input)
        {
            this.id = requireNonNull(id, "id is null");
            this.input = requireNonNull(input, "input is null");
        }

        public String getId()
        {
            return id;
        }

        public RowExpression getInput()
        {
            return input;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Descriptor that = (Descriptor) o;
            return Objects.equals(id, that.id) &&
                    Objects.equals(input, that.input);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, input);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("id", id)
                    .add("input", input)
                    .toString();
        }
    }

    @ScalarFunction(value = Function.NAME, hidden = true, deterministic = true)
    public static final class Function
    {
        private Function() {}

        public static final String NAME = "$internal$dynamic_filter_function";

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType(VARCHAR) Slice id, @SqlType("T") Block input)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType(VARCHAR) Slice id, @SqlType("T") Slice input)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType(VARCHAR) Slice id, @SqlType("T") long input)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType(VARCHAR) Slice id, @SqlType("T") boolean input)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType(VARCHAR) Slice id, @SqlType("T") double input)
        {
            throw new UnsupportedOperationException();
        }
    }
}
