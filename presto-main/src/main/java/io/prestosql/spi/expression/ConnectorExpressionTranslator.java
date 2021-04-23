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
package io.prestosql.spi.expression;

import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.LambdaDefinitionExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.RowExpressionVisitor;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.RowType;
import io.prestosql.sql.planner.LiteralEncoder;
import io.prestosql.sql.tree.Identifier;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.sql.relational.Expressions.constant;
import static java.util.Objects.requireNonNull;

public final class ConnectorExpressionTranslator
{
    private ConnectorExpressionTranslator()
    {
    }

    public static RowExpression translate(ConnectorExpression expression, Map<String, Symbol> variableMappings, LiteralEncoder literalEncoder)
    {
        return new ConnectorToSqlExpressionTranslator(variableMappings, literalEncoder).translate(expression);
    }

    public static ConnectorExpression translate(RowExpression expression)
    {
        return expression.accept(new SqlToConnectorExpressionTranslator(), null);
    }

    private static class ConnectorToSqlExpressionTranslator
    {
        private final Map<String, Symbol> variableMappings;
        private final LiteralEncoder literalEncoder;

        public ConnectorToSqlExpressionTranslator(Map<String, Symbol> variableMappings, LiteralEncoder literalEncoder)
        {
            this.variableMappings = requireNonNull(variableMappings, "variableMappings is null");
            this.literalEncoder = requireNonNull(literalEncoder, "literalEncoder is null");
        }

        public RowExpression translate(ConnectorExpression expression)
        {
            if (expression instanceof Variable) {
                return new VariableReferenceExpression(variableMappings.get(((Variable) expression).getName()).getName(), expression.getType());
            }

            if (expression instanceof Constant) {
                return literalEncoder.toRowExpression(((Constant) expression).getValue(), expression.getType());
            }

            if (expression instanceof FieldDereference) {
                FieldDereference dereference = (FieldDereference) expression;

                RowType type = (RowType) expression.getType();
                String name = type.getFields().get(dereference.getField()).getName().get();
                List<RowType.Field> fields = type.getFields();
                int index = -1;
                for (int i = 0; i < fields.size(); i++) {
                    RowType.Field field = fields.get(i);
                    if (field.getName().isPresent() && field.getName().get().equalsIgnoreCase(name)) {
                        checkArgument(index < 0, "Ambiguous field %s in type %s", field, type.getDisplayName());
                        index = i;
                    }
                }
                checkState(index >= 0, "could not find field name:%s", new Identifier(name));
                return new SpecialForm(SpecialForm.Form.DEREFERENCE, type.getFields().get(index).getType(), translate(dereference.getTarget()), constant(index, INTEGER));
            }

            throw new UnsupportedOperationException("Expression type not supported: " + expression.getClass().getName());
        }
    }

    private static class SqlToConnectorExpressionTranslator
            implements RowExpressionVisitor<ConnectorExpression, Void>
    {
        private SqlToConnectorExpressionTranslator()
        {
        }

        @Override
        public ConnectorExpression visitConstant(ConstantExpression node, Void context)
        {
            return new Constant(node.getValue(), node.getType());
        }

        @Override
        public ConnectorExpression visitVariableReference(VariableReferenceExpression node, Void context)
        {
            return new Variable(node.getName(), node.getType());
        }

        @Override
        public ConnectorExpression visitSpecialForm(SpecialForm node, Void context)
        {
            switch (node.getForm()) {
                case DEREFERENCE: {
                    checkArgument(node.getArguments().size() == 2);

                    ConnectorExpression base = node.getArguments().get(0).accept(this, context);
                    checkArgument(node.getArguments().get(1) instanceof ConstantExpression);
                    int index = ((Number) ((ConstantExpression) node.getArguments().get(1)).getValue()).intValue();

                    // if the base part is evaluated to be null, the dereference expression should also be null
                    if (base == null) {
                        return null;
                    }

                    return new FieldDereference(node.getType(), base, index);
                }
                default:
                    throw new UnsupportedOperationException("not yet implemented: expression translator for " + node.getClass().getName());
            }
        }

        @Override
        public ConnectorExpression visitCall(CallExpression node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: expression translator for " + node.getClass().getName());
        }

        @Override
        public ConnectorExpression visitInputReference(InputReferenceExpression node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: expression translator for " + node.getClass().getName());
        }

        @Override
        public ConnectorExpression visitLambda(LambdaDefinitionExpression node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: expression translator for " + node.getClass().getName());
        }
    }
}
