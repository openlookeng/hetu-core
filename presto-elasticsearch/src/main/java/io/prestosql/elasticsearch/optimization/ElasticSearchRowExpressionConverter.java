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
package io.prestosql.elasticsearch.optimization;

import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.LambdaDefinitionExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.sql.RowExpressionConverter;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;

public class ElasticSearchRowExpressionConverter
        implements RowExpressionConverter<ElasticSearchConverterContext>
{
    private static final String EMPTY_STRING = "";
    private static final String DEREFERENCE_SEPERATOR = "|";

    @Override
    public String visitCall(CallExpression call, ElasticSearchConverterContext context)
    {
        return RowExpressionConverter.super.visitCall(call, context);
    }

    @Override
    public String visitSpecialForm(SpecialForm specialForm, ElasticSearchConverterContext context)
    {
        SpecialForm.Form form = specialForm.getForm();
        List<RowExpression> specialFormArguments = specialForm.getArguments();
        switch (form) {
            case IS_NULL:
                return String.format("NOT _exists_:%s",
                        specialFormArguments.get(0).accept(this, context));
            case BETWEEN:
                return String.format("(%s: (>=%s AND <=%s))",
                        specialFormArguments.get(0).accept(this, context),
                        specialFormArguments.get(1).accept(this, context),
                        specialFormArguments.get(2).accept(this, context));
            case IN:
                RowExpression variable = specialFormArguments.get(0);
                return specialFormArguments.stream().skip(1)
                        .map(arg -> String.format("%s:%s", variable.accept(this, context), arg.accept(this, context)))
                        .collect(Collectors.joining(" OR "));
            case AND:
            case OR:
                return String.format("%s %s %s",
                        specialFormArguments.get(0).accept(this, context),
                        form,
                        specialFormArguments.get(1).accept(this, context));
            case DEREFERENCE:
                int index;
                try {
                    index = Integer.parseInt(specialFormArguments.get(1).accept(this, context));
                }
                catch (NumberFormatException e) {
                    return handleUnsupportedOptimize(context);
                }
                context.stepInDeference();
                String innerName = specialFormArguments.get(0).accept(this, context);
                context.stepOutDeference();

                String[] splitNames = innerName.split(Pattern.quote(DEREFERENCE_SEPERATOR));
                Type type = specialForm.getType();
                return constructTypedNames(type, splitNames[index]);
            default:
                return handleUnsupportedOptimize(context);
        }
    }

    private String constructTypedNames(Type type, String splitName)
    {
        if (!(type instanceof RowType)) {
            return splitName;
        }
        RowType rowType = (RowType) type;
        return rowType.getFields().stream().filter(typeField -> typeField.getName().isPresent())
                .map(typeField -> typeField.getName().get())
                .map(typeFieldName -> String.format("%s.%s", splitName, typeFieldName))
                .collect(Collectors.joining(DEREFERENCE_SEPERATOR));
    }

    @Override
    public String visitConstant(ConstantExpression literal, ElasticSearchConverterContext context)
    {
        Type type = literal.getType();

        if (literal.getValue() == null) {
            return "null";
        }
        if (type instanceof BooleanType) {
            return String.valueOf(((Boolean) literal.getValue()).booleanValue());
        }
        if (type instanceof BigintType || type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType) {
            Number number = (Number) literal.getValue();
            return format("%d", number.longValue());
        }
        if (type instanceof DoubleType) {
            return literal.getValue().toString();
        }
        if (type instanceof RealType) {
            Long number = (Long) literal.getValue();
            return format("%f", intBitsToFloat(number.intValue()));
        }

        if (type instanceof VarcharType) {
            return "'" + ((Slice) literal.getValue()).toStringUtf8() + "'";
        }

        if (type instanceof TimestampType) {
            DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
            Long time = (Long) literal.getValue();
            return formatter.format(Instant.ofEpochMilli(time).atZone(UTC).toLocalDateTime());
        }
        throw new PrestoException(NOT_SUPPORTED, String.format("Cannot handle the constant expression %s with value of type %s", literal.getValue(), type));
    }

    @Override
    public String visitVariableReference(VariableReferenceExpression reference, ElasticSearchConverterContext context)
    {
        String name = reference.getName();
        if (!context.isInDeference()) {
            return name;
        }
        return constructTypedNames(reference.getType(), name);
    }

    @Override
    public String visitInputReference(InputReferenceExpression reference, ElasticSearchConverterContext context)
    {
        return handleUnsupportedOptimize(context);
    }

    @Override
    public String visitLambda(LambdaDefinitionExpression lambda, ElasticSearchConverterContext context)
    {
        return handleUnsupportedOptimize(context);
    }

    public String handleUnsupportedOptimize(ElasticSearchConverterContext context)
    {
        context.setConversionFailed();
        return EMPTY_STRING;
    }
}
