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
package io.prestosql.sql.relational;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.prestosql.Session;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.operator.scalar.TryFunction;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.LambdaDefinitionExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.SpecialForm.Form;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalParseResult;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.RowType.Field;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.UnknownType;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.analyzer.SemanticErrorCode;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.planner.SymbolUtils;
import io.prestosql.sql.relational.optimizer.ExpressionOptimizer;
import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.ArithmeticUnaryExpression;
import io.prestosql.sql.tree.ArrayConstructor;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.BinaryLiteral;
import io.prestosql.sql.tree.BindExpression;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.CharLiteral;
import io.prestosql.sql.tree.CoalesceExpression;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FieldReference;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.IfExpression;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.IntervalLiteral;
import io.prestosql.sql.tree.IsNotNullPredicate;
import io.prestosql.sql.tree.IsNullPredicate;
import io.prestosql.sql.tree.LambdaArgumentDeclaration;
import io.prestosql.sql.tree.LambdaExpression;
import io.prestosql.sql.tree.LikePredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.NotExpression;
import io.prestosql.sql.tree.NullIfExpression;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.Row;
import io.prestosql.sql.tree.SearchedCaseExpression;
import io.prestosql.sql.tree.SimpleCaseExpression;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SubscriptExpression;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.TimeLiteral;
import io.prestosql.sql.tree.TimestampLiteral;
import io.prestosql.sql.tree.TryExpression;
import io.prestosql.sql.tree.WhenClause;
import io.prestosql.transaction.TransactionId;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.metadata.CastType.CAST;
import static io.prestosql.metadata.CastType.TRY_CAST;
import static io.prestosql.metadata.FunctionAndTypeManager.qualifyObjectName;
import static io.prestosql.spi.function.OperatorType.NEGATION;
import static io.prestosql.spi.function.OperatorType.SUBSCRIPT;
import static io.prestosql.spi.relation.SpecialForm.Form.AND;
import static io.prestosql.spi.relation.SpecialForm.Form.BETWEEN;
import static io.prestosql.spi.relation.SpecialForm.Form.BIND;
import static io.prestosql.spi.relation.SpecialForm.Form.COALESCE;
import static io.prestosql.spi.relation.SpecialForm.Form.DEREFERENCE;
import static io.prestosql.spi.relation.SpecialForm.Form.IF;
import static io.prestosql.spi.relation.SpecialForm.Form.IN;
import static io.prestosql.spi.relation.SpecialForm.Form.IS_NULL;
import static io.prestosql.spi.relation.SpecialForm.Form.NULL_IF;
import static io.prestosql.spi.relation.SpecialForm.Form.OR;
import static io.prestosql.spi.relation.SpecialForm.Form.ROW_CONSTRUCTOR;
import static io.prestosql.spi.relation.SpecialForm.Form.SWITCH;
import static io.prestosql.spi.relation.SpecialForm.Form.WHEN;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.LikePatternType.LIKE_PATTERN;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.spi.util.DateTimeUtils.parseTimeWithTimeZone;
import static io.prestosql.spi.util.DateTimeUtils.parseTimeWithoutTimeZone;
import static io.prestosql.spi.util.DateTimeUtils.parseTimestampLiteral;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.relational.Expressions.call;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.sql.relational.Expressions.constantNull;
import static io.prestosql.sql.relational.Expressions.field;
import static io.prestosql.type.JsonType.JSON;
import static io.prestosql.util.DateTimePeriodUtils.parseDayTimeInterval;
import static io.prestosql.util.DateTimePeriodUtils.parseYearMonthInterval;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class SqlToRowExpressionTranslator
{
    private SqlToRowExpressionTranslator() {}

    public static RowExpression translate(
            Expression expression,
            FunctionKind functionKind,
            Map<NodeRef<Expression>, Type> types,
            Map<Symbol, Integer> layout,
            FunctionAndTypeManager functionAndTypeManager,
            Session session,
            boolean optimize)
    {
        return translate(expression, functionKind, types, layout, functionAndTypeManager, session, session.getTransactionId(), optimize);
    }

    public static RowExpression translate(
            Expression expression,
            FunctionKind functionKind,
            Map<NodeRef<Expression>, Type> types,
            Map<Symbol, Integer> layout,
            FunctionAndTypeManager functionAndTypeManager,
            Session session,
            Optional<TransactionId> transactionId,
            boolean optimize)
    {
        Visitor visitor = new Visitor(
                functionKind,
                types,
                layout,
                functionAndTypeManager,
                transactionId);
        RowExpression result = visitor.process(expression, null);

        requireNonNull(result, "translated expression is null");

        if (optimize) {
            ExpressionOptimizer optimizer = new ExpressionOptimizer(functionAndTypeManager, session);
            return optimizer.optimize(result);
        }

        return result;
    }

    private static class Visitor
            extends AstVisitor<RowExpression, Void>
    {
        private final FunctionKind functionKind;
        private final Map<NodeRef<Expression>, Type> types;
        private final Map<Symbol, Integer> layout;
        private final FunctionAndTypeManager functionAndTypeManager;
        private final Optional<TransactionId> transactionId;
        private final FunctionResolution functionResolution;

        private Visitor(
                FunctionKind functionKind,
                Map<NodeRef<Expression>, Type> types,
                Map<Symbol, Integer> layout,
                FunctionAndTypeManager functionAndTypeManager,
                Optional<TransactionId> transactionId)
        {
            this.functionKind = functionKind;
            this.types = ImmutableMap.copyOf(requireNonNull(types, "types is null"));
            this.functionAndTypeManager = functionAndTypeManager;
            this.layout = layout;
            this.transactionId = transactionId;
            this.functionResolution = new FunctionResolution(functionAndTypeManager);
        }

        private Type getType(Expression node)
        {
            return types.get(NodeRef.of(node));
        }

        @Override
        protected RowExpression visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: expression translator for " + node.getClass().getName());
        }

        @Override
        protected RowExpression visitIdentifier(Identifier node, Void context)
        {
            // identifier should never be reachable with the exception of lambda within VALUES (#9711)
            return new VariableReferenceExpression(node.getValue(), getType(node));
        }

        @Override
        protected RowExpression visitFieldReference(FieldReference node, Void context)
        {
            return field(node.getFieldIndex(), getType(node));
        }

        @Override
        protected RowExpression visitNullLiteral(NullLiteral node, Void context)
        {
            return constantNull(UnknownType.UNKNOWN);
        }

        @Override
        protected RowExpression visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return constant(node.getValue(), BOOLEAN);
        }

        @Override
        protected RowExpression visitLongLiteral(LongLiteral node, Void context)
        {
            if (node.getValue() >= Integer.MIN_VALUE && node.getValue() <= Integer.MAX_VALUE) {
                return constant(node.getValue(), INTEGER);
            }
            return constant(node.getValue(), BIGINT);
        }

        @Override
        protected RowExpression visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return constant(node.getValue(), DOUBLE);
        }

        @Override
        protected RowExpression visitDecimalLiteral(DecimalLiteral node, Void context)
        {
            DecimalParseResult parseResult = Decimals.parse(node.getValue());
            return constant(parseResult.getObject(), parseResult.getType());
        }

        @Override
        protected RowExpression visitStringLiteral(StringLiteral node, Void context)
        {
            return constant(node.getSlice(), createVarcharType(countCodePoints(node.getSlice())));
        }

        @Override
        protected RowExpression visitCharLiteral(CharLiteral node, Void context)
        {
            return constant(node.getSlice(), createCharType(node.getValue().length()));
        }

        @Override
        protected RowExpression visitBinaryLiteral(BinaryLiteral node, Void context)
        {
            return constant(node.getValue(), VARBINARY);
        }

        @Override
        protected RowExpression visitGenericLiteral(GenericLiteral node, Void context)
        {
            Type type;
            try {
                type = functionAndTypeManager.getType(TypeSignature.parseTypeSignature(node.getType()));
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unsupported type: " + node.getType());
            }

            try {
                if (TINYINT.equals(type)) {
                    return constant((long) Byte.parseByte(node.getValue()), TINYINT);
                }
                else if (SMALLINT.equals(type)) {
                    return constant((long) Short.parseShort(node.getValue()), SMALLINT);
                }
                else if (BIGINT.equals(type)) {
                    return constant(Long.parseLong(node.getValue()), BIGINT);
                }
            }
            catch (NumberFormatException e) {
                throw new SemanticException(SemanticErrorCode.INVALID_LITERAL, node, format("Invalid formatted generic %s literal: %s", type, node));
            }

            if (JSON.equals(type)) {
                return call(
                        "json_parse",
                        functionAndTypeManager.lookupFunction("json_parse", fromTypes(VARCHAR)),
                        getType(node),
                        constant(utf8Slice(node.getValue()), VARCHAR));
            }

            return call(
                    CAST.name(),
                    functionAndTypeManager.lookupCast(CAST, VARCHAR.getTypeSignature(), getType(node).getTypeSignature()),
                    getType(node),
                    constant(utf8Slice(node.getValue()), VARCHAR));
        }

        @Override
        protected RowExpression visitTimeLiteral(TimeLiteral node, Void context)
        {
            long value;
            if (getType(node).equals(TIME_WITH_TIME_ZONE)) {
                value = parseTimeWithTimeZone(node.getValue());
            }
            else {
                value = parseTimeWithoutTimeZone(node.getValue());
            }
            return constant(value, getType(node));
        }

        @Override
        protected RowExpression visitTimestampLiteral(TimestampLiteral node, Void context)
        {
            long value = parseTimestampLiteral(node.getValue());
            return constant(value, getType(node));
        }

        @Override
        protected RowExpression visitIntervalLiteral(IntervalLiteral node, Void context)
        {
            long value;
            if (node.isYearToMonth()) {
                value = node.getSign().multiplier() * parseYearMonthInterval(node.getValue(), node.getStartField(), node.getEndField());
            }
            else {
                value = node.getSign().multiplier() * parseDayTimeInterval(node.getValue(), node.getStartField(), node.getEndField());
            }
            return constant(value, getType(node));
        }

        @Override
        protected RowExpression visitComparisonExpression(ComparisonExpression node, Void context)
        {
            RowExpression left = process(node.getLeft(), context);
            RowExpression right = process(node.getRight(), context);

            return call(
                    node.getOperator().name(),
                    functionResolution.comparisonFunction(node.getOperator(), left.getType(), right.getType()),
                    BOOLEAN,
                    left,
                    right);
        }

        @Override
        protected RowExpression visitFunctionCall(FunctionCall node, Void context)
        {
            List<RowExpression> arguments = node.getArguments().stream()
                    .map(value -> process(value, context))
                    .collect(toImmutableList());

            List<TypeSignatureProvider> argumentTypes = arguments.stream()
                    .map(RowExpression::getType)
                    .map(Type::getTypeSignature)
                    .map(TypeSignatureProvider::new)
                    .collect(toImmutableList());

            Optional<RowExpression> filter = (node.getFilter().isPresent()) ? Optional.of(process(node.getFilter().get(), context)) : Optional.empty();

            return call(node.getName().toString(), functionAndTypeManager.resolveFunction(transactionId, qualifyObjectName(node.getName()), argumentTypes), getType(node), arguments, filter);
        }

        @Override
        protected RowExpression visitSymbolReference(SymbolReference node, Void context)
        {
            Integer field = layout.get(SymbolUtils.from(node));
            if (field != null) {
                return field(field, getType(node));
            }

            return new VariableReferenceExpression(node.getName(), getType(node));
        }

        @Override
        protected RowExpression visitLambdaExpression(LambdaExpression node, Void context)
        {
            RowExpression body = process(node.getBody(), context);

            Type type = getType(node);
            List<Type> typeParameters = type.getTypeParameters();
            List<Type> argumentTypes = typeParameters.subList(0, typeParameters.size() - 1);
            List<String> argumentNames = node.getArguments().stream()
                    .map(LambdaArgumentDeclaration::getName)
                    .map(Identifier::getValue)
                    .collect(toImmutableList());

            return new LambdaDefinitionExpression(argumentTypes, argumentNames, body);
        }

        @Override
        protected RowExpression visitBindExpression(BindExpression node, Void context)
        {
            ImmutableList.Builder<Type> valueTypesBuilder = ImmutableList.builder();
            ImmutableList.Builder<RowExpression> argumentsBuilder = ImmutableList.builder();
            for (Expression value : node.getValues()) {
                RowExpression valueRowExpression = process(value, context);
                valueTypesBuilder.add(valueRowExpression.getType());
                argumentsBuilder.add(valueRowExpression);
            }
            RowExpression function = process(node.getFunction(), context);
            argumentsBuilder.add(function);

            return new SpecialForm(BIND, getType(node), argumentsBuilder.build());
        }

        @Override
        protected RowExpression visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            RowExpression left = process(node.getLeft(), context);
            RowExpression right = process(node.getRight(), context);

            return call(
                    node.getOperator().name(),
                    functionResolution.arithmeticFunction(node.getOperator(), left.getType(), right.getType()),
                    getType(node),
                    left,
                    right);
        }

        @Override
        protected RowExpression visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
        {
            RowExpression expression = process(node.getValue(), context);

            switch (node.getSign()) {
                case PLUS:
                    return expression;
                case MINUS:
                    return call(
                            NEGATION.name(),
                            functionAndTypeManager.resolveOperatorFunctionHandle(NEGATION, fromTypes(expression.getType())),
                            getType(node),
                            expression);
            }

            throw new UnsupportedOperationException("Unsupported unary operator: " + node.getSign());
        }

        @Override
        protected RowExpression visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            Form form;
            switch (node.getOperator()) {
                case AND:
                    form = AND;
                    break;
                case OR:
                    form = OR;
                    break;
                default:
                    throw new IllegalStateException("Unknown logical operator: " + node.getOperator());
            }
            return new SpecialForm(
                    form,
                    BOOLEAN,
                    process(node.getLeft(), context),
                    process(node.getRight(), context));
        }

        @Override
        protected RowExpression visitCast(Cast node, Void context)
        {
            RowExpression value = process(node.getExpression(), context);

            if (node.isSafe()) {
                return call(TRY_CAST.name(), functionAndTypeManager.lookupCast(TRY_CAST, value.getType().getTypeSignature(), getType(node).getTypeSignature()), getType(node), value);
            }

            return call(CAST.name(), functionAndTypeManager.lookupCast(CAST, value.getType().getTypeSignature(), getType(node).getTypeSignature()), getType(node), value);
        }

        @Override
        protected RowExpression visitCoalesceExpression(CoalesceExpression node, Void context)
        {
            List<RowExpression> arguments = node.getOperands().stream()
                    .map(value -> process(value, context))
                    .collect(toImmutableList());

            return new SpecialForm(COALESCE, getType(node), arguments);
        }

        @Override
        protected RowExpression visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
        {
            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();

            arguments.add(process(node.getOperand(), context));

            for (WhenClause clause : node.getWhenClauses()) {
                arguments.add(new SpecialForm(WHEN,
                        getType(clause),
                        process(clause.getOperand(), context),
                        process(clause.getResult(), context)));
            }

            Type returnType = getType(node);

            arguments.add(node.getDefaultValue()
                    .map((value) -> process(value, context))
                    .orElse(constantNull(returnType)));

            return new SpecialForm(SWITCH, returnType, arguments.build());
        }

        @Override
        protected RowExpression visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
        {
            /*
                Translates an expression like:

                    case when cond1 then value1
                         when cond2 then value2
                         when cond3 then value3
                         else value4
                    end

                To:

                    IF(cond1,
                        value1,
                        IF(cond2,
                            value2,
                                If(cond3,
                                    value3,
                                    value4)))

             */
            RowExpression expression = node.getDefaultValue()
                    .map((value) -> process(value, context))
                    .orElse(constantNull(getType(node)));

            for (WhenClause clause : Lists.reverse(node.getWhenClauses())) {
                expression = new SpecialForm(
                        IF,
                        getType(node),
                        process(clause.getOperand(), context),
                        process(clause.getResult(), context),
                        expression);
            }

            return expression;
        }

        @Override
        protected RowExpression visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            RowType rowType = (RowType) getType(node.getBase());
            String fieldName = node.getField().getValue();
            List<Field> fields = rowType.getFields();
            int index = -1;
            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                if (field.getName().isPresent() && field.getName().get().equalsIgnoreCase(fieldName)) {
                    checkArgument(index < 0, "Ambiguous field %s in type %s", field, rowType.getDisplayName());
                    index = i;
                }
            }

            checkState(index >= 0, "could not find field name: %s", node.getField());
            Type returnType = getType(node);
            return new SpecialForm(DEREFERENCE, returnType, process(node.getBase(), context), constant(index, INTEGER));
        }

        @Override
        protected RowExpression visitIfExpression(IfExpression node, Void context)
        {
            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();

            arguments.add(process(node.getCondition(), context))
                    .add(process(node.getTrueValue(), context));

            if (node.getFalseValue().isPresent()) {
                arguments.add(process(node.getFalseValue().get(), context));
            }
            else {
                arguments.add(constantNull(getType(node)));
            }

            return new SpecialForm(IF, getType(node), arguments.build());
        }

        @Override
        protected RowExpression visitTryExpression(TryExpression node, Void context)
        {
            return call(TryFunction.NAME, functionResolution.tryFunction(getType(node)), getType(node), process(node.getInnerExpression(), context));
        }

        @Override
        protected RowExpression visitInPredicate(InPredicate node, Void context)
        {
            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
            arguments.add(process(node.getValue(), context));
            InListExpression values;
            if (node.getValueList() instanceof InListExpression) {
                values = (InListExpression) node.getValueList();
            }
            else {
                values = new InListExpression(ImmutableList.of(node.getValueList()));
            }
            for (Expression value : values.getValues()) {
                arguments.add(process(value, context));
            }

            return new SpecialForm(IN, BOOLEAN, arguments.build());
        }

        @Override
        protected RowExpression visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            RowExpression expression = process(node.getValue(), context);

            return call(
                    "not",
                    functionResolution.notFunction(),
                    BOOLEAN,
                    Expressions.specialForm(IS_NULL, BOOLEAN, ImmutableList.of(expression)));
        }

        @Override
        protected RowExpression visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            RowExpression expression = process(node.getValue(), context);

            return new SpecialForm(IS_NULL, BOOLEAN, expression);
        }

        @Override
        protected RowExpression visitNotExpression(NotExpression node, Void context)
        {
            return call("not", functionResolution.notFunction(), BOOLEAN, process(node.getValue(), context));
        }

        @Override
        protected RowExpression visitNullIfExpression(NullIfExpression node, Void context)
        {
            RowExpression first = process(node.getFirst(), context);
            RowExpression second = process(node.getSecond(), context);

            return new SpecialForm(
                    NULL_IF,
                    getType(node),
                    first,
                    second);
        }

        @Override
        protected RowExpression visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            RowExpression value = process(node.getValue(), context);
            RowExpression min = process(node.getMin(), context);
            RowExpression max = process(node.getMax(), context);

            return new SpecialForm(
                    BETWEEN,
                    BOOLEAN,
                    value,
                    min,
                    max);
        }

        @Override
        protected RowExpression visitLikePredicate(LikePredicate node, Void context)
        {
            RowExpression value = process(node.getValue(), context);
            RowExpression pattern = process(node.getPattern(), context);

            if (node.getEscape().isPresent()) {
                RowExpression escape = process(node.getEscape().get(), context);
                return likeFunctionCall(value, call("LIKE_PATTERN", functionResolution.likePatternFunction(), LIKE_PATTERN, pattern, escape));
            }

            return likeFunctionCall(value, call(CAST.name(), functionAndTypeManager.lookupCast(CAST, VARCHAR.getTypeSignature(), LIKE_PATTERN.getTypeSignature()), LIKE_PATTERN, pattern));
        }

        private RowExpression likeFunctionCall(RowExpression value, RowExpression pattern)
        {
            if (value.getType() instanceof VarcharType) {
                return call("LIKE", functionResolution.likeVarcharFunction(), BOOLEAN, value, pattern);
            }

            checkState(value.getType() instanceof CharType, "LIKE value type is neither VARCHAR or CHAR");
            return call("LIKE", functionResolution.likeCharFunction(value.getType()), BOOLEAN, value, pattern);
        }

        @Override
        protected RowExpression visitSubscriptExpression(SubscriptExpression node, Void context)
        {
            RowExpression base = process(node.getBase(), context);
            RowExpression index = process(node.getIndex(), context);

            // this block will handle row subscript, converts the ROW_CONSTRUCTOR with subscript to a DEREFERENCE expression
            if (base.getType() instanceof RowType) {
                checkState(index instanceof ConstantExpression, "Subscript expression on ROW requires a ConstantExpression");
                ConstantExpression position = (ConstantExpression) index;
                checkState(position.getValue() instanceof Long, "ConstantExpression should contain a valid integer index into the row");
                Long offset = (Long) position.getValue();
                checkState(
                        offset >= 1 && offset <= base.getType().getTypeParameters().size(),
                        "Subscript index out of bounds %s: should be >= 1 and <= %s",
                        offset,
                        base.getType().getTypeParameters().size());
                return Expressions.specialForm(DEREFERENCE, getType(node), base, Expressions.constant(offset - 1, INTEGER));
            }
            return call(
                    SUBSCRIPT.name(),
                    functionAndTypeManager.resolveOperatorFunctionHandle(SUBSCRIPT, fromTypes(base.getType(), index.getType())),
                    getType(node),
                    base,
                    index);
        }

        @Override
        protected RowExpression visitArrayConstructor(ArrayConstructor node, Void context)
        {
            List<RowExpression> arguments = node.getValues().stream()
                    .map(value -> process(value, context))
                    .collect(toImmutableList());
            List<Type> argumentTypes = arguments.stream()
                    .map(RowExpression::getType)
                    .collect(toImmutableList());
            return call("ARRAY", functionResolution.arrayConstructor(argumentTypes), getType(node), arguments);
        }

        @Override
        protected RowExpression visitRow(Row node, Void context)
        {
            List<RowExpression> arguments = node.getItems().stream()
                    .map(value -> process(value, context))
                    .collect(toImmutableList());
            Type returnType = getType(node);
            return new SpecialForm(ROW_CONSTRUCTOR, returnType, arguments);
        }
    }
}
