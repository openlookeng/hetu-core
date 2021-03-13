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
package io.prestosql.sql.planner.planprinter;

import io.prestosql.metadata.Metadata;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.LambdaDefinitionExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.RowExpressionVisitor;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.LiteralInterpreter;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RowExpressionFormatter
{
    private final Metadata metadata;

    public RowExpressionFormatter(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public String formatRowExpression(RowExpression expression)
    {
        return expression.accept(new Formatter(), null);
    }

    private List<String> formatRowExpressions(List<RowExpression> rowExpressions)
    {
        return rowExpressions.stream().map(rowExpression -> formatRowExpression(rowExpression)).collect(toList());
    }

    public class Formatter
            implements RowExpressionVisitor<String, Void>
    {
        @Override
        public String visitCall(CallExpression node, Void context)
        {
            String functionName = metadata.getFunctionAndTypeManager().getFunctionMetadata(node.getFunctionHandle()).getName().getObjectName();
            if (functionName.contains("$operator$")) {
                OperatorType operatorType = Signature.unmangleOperator(functionName);
                if (operatorType.isArithmeticOperator() || operatorType.isComparisonOperator()) {
                    String operation = operatorType.getOperator();
                    return String.join(" " + operation + " ", formatRowExpressions(node.getArguments()).stream().map(e -> "(" + e + ")").collect(toImmutableList()));
                }
                else if (operatorType.equals(OperatorType.CAST)) {
                    return format("CAST(%s AS %s)", formatRowExpression(node.getArguments().get(0)), node.getType().getDisplayName());
                }
                else if (operatorType.equals(OperatorType.NEGATION)) {
                    return "-(" + formatRowExpression(node.getArguments().get(0)) + ")";
                }
                else if (operatorType.equals(OperatorType.SUBSCRIPT)) {
                    return formatRowExpression(node.getArguments().get(0)) + "[" + formatRowExpression(node.getArguments().get(1)) + "]";
                }
                else if (operatorType.equals(OperatorType.BETWEEN)) {
                    List<String> formattedExpresions = formatRowExpressions(node.getArguments());
                    return format("%s BETWEEN (%s) AND (%s)", formattedExpresions.get(0), formattedExpresions.get(1), formattedExpresions.get(2));
                }
            }
            return node.getDisplayName() + "(" + String.join(", ", formatRowExpressions(node.getArguments())) + ")";
        }

        @Override
        public String visitSpecialForm(SpecialForm node, Void context)
        {
            if (node.getForm().equals(SpecialForm.Form.AND) || node.getForm().equals(SpecialForm.Form.OR)) {
                return String.join(" " + node.getForm() + " ", formatRowExpressions(node.getArguments()).stream().map(e -> "(" + e + ")").collect(toImmutableList()));
            }
            return node.getForm().name() + "(" + String.join(", ", formatRowExpressions(node.getArguments())) + ")";
        }

        @Override
        public String visitInputReference(InputReferenceExpression node, Void context)
        {
            return node.toString();
        }

        @Override
        public String visitLambda(LambdaDefinitionExpression node, Void context)
        {
            return "(" + String.join(", ", node.getArguments()) + ") -> " + formatRowExpression(node.getBody());
        }

        @Override
        public String visitVariableReference(VariableReferenceExpression node, Void context)
        {
            return node.getName();
        }

        @Override
        public String visitConstant(ConstantExpression node, Void context)
        {
            Object value = LiteralInterpreter.evaluate(node);

            if (value == null) {
                return String.valueOf((Object) null);
            }

            Type type = node.getType();
            if (node.getType().getJavaType() == Block.class) {
                Block block = (Block) value;
                // TODO: format block
                return format("[Block: position count: %s; size: %s bytes]", block.getPositionCount(), block.getRetainedSizeInBytes());
            }
            return type.getDisplayName().toUpperCase() + " " + value.toString();
        }
    }
}
