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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.scalar.VarbinaryFunctions;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.SqlDate;
import io.prestosql.spi.type.SqlTime;
import io.prestosql.spi.type.SqlTimestamp;
import io.prestosql.spi.type.SqlVarbinary;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeNotFoundException;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.InterpretedFunctionInvoker;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.BinaryLiteral;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.CharLiteral;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.IntervalLiteral;
import io.prestosql.sql.tree.Literal;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.TimeLiteral;
import io.prestosql.sql.tree.TimestampLiteral;
import io.prestosql.type.IntervalDayTimeType;
import io.prestosql.type.IntervalYearMonthType;
import io.prestosql.type.SqlIntervalDayTime;
import io.prestosql.type.SqlIntervalYearMonth;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.metadata.CastType.CAST;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.prestosql.spi.type.Decimals.decodeUnscaledValue;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.util.DateTimeUtils.parseTimeLiteral;
import static io.prestosql.spi.util.DateTimeUtils.parseTimestampLiteral;
import static io.prestosql.sql.analyzer.SemanticErrorCode.INVALID_LITERAL;
import static io.prestosql.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static io.prestosql.type.JsonType.JSON;
import static io.prestosql.util.DateTimePeriodUtils.parseDayTimeInterval;
import static io.prestosql.util.DateTimePeriodUtils.parseYearMonthInterval;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;

public final class LiteralInterpreter
{
    private LiteralInterpreter() {}

    public static Object evaluate(Metadata metadata, ConnectorSession session, Expression node)
    {
        if (!(node instanceof Literal)) {
            throw new IllegalArgumentException("node must be a Literal");
        }
        return new LiteralVisitor(metadata).process(node, session);
    }

    public static Object evaluate(ConstantExpression node)
    {
        Type type = node.getType();

        if (node.getValue() == null) {
            return null;
        }
        if (type instanceof BooleanType) {
            return node.getValue();
        }
        if (type instanceof BigintType || type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType) {
            return node.getValue();
        }
        if (type instanceof DoubleType) {
            return node.getValue();
        }
        if (type instanceof RealType) {
            Long number = (Long) node.getValue();
            return intBitsToFloat(number.intValue());
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (decimalType.isShort()) {
                checkState(node.getValue() instanceof Long);
                return decodeDecimal(BigInteger.valueOf((long) node.getValue()), decimalType);
            }
            checkState(node.getValue() instanceof Slice);
            Slice value = (Slice) node.getValue();
            return decodeDecimal(decodeUnscaledValue(value), decimalType);
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            return (node.getValue() instanceof String) ? node.getValue() : ((Slice) node.getValue()).toStringUtf8();
        }
        if (type instanceof VarbinaryType) {
            return new SqlVarbinary(((Slice) node.getValue()).getBytes());
        }
        if (type instanceof DateType) {
            return new SqlDate(((Long) node.getValue()).intValue());
        }
        if (type instanceof TimeType) {
            return new SqlTime((long) node.getValue());
        }
        if (type instanceof TimestampType) {
            try {
                return new SqlTimestamp((long) node.getValue());
            }
            catch (RuntimeException e) {
                throw new PrestoException(GENERIC_USER_ERROR, format("'%s' is not a valid timestamp literal", (String) node.getValue()));
            }
        }
        if (type instanceof IntervalDayTimeType) {
            return new SqlIntervalDayTime((long) node.getValue());
        }
        if (type instanceof IntervalYearMonthType) {
            return new SqlIntervalYearMonth(((Long) node.getValue()).intValue());
        }
        if (type.getJavaType().equals(Slice.class)) {
            // DO NOT ever remove toBase64. Calling toString directly on Slice whose base is not byte[] will cause JVM to crash.
            return "'" + VarbinaryFunctions.toBase64((Slice) node.getValue()).toStringUtf8() + "'";
        }

        // We should not fail at the moment; just return the raw value (block, regex, etc) to the user
        return node.getValue();
    }

    private static Number decodeDecimal(BigInteger unscaledValue, DecimalType type)
    {
        return new BigDecimal(unscaledValue, type.getScale(), new MathContext(type.getPrecision()));
    }

    private static class LiteralVisitor
            extends AstVisitor<Object, ConnectorSession>
    {
        private final Metadata metadata;
        private final InterpretedFunctionInvoker functionInvoker;

        private LiteralVisitor(Metadata metadata)
        {
            this.metadata = metadata;
            this.functionInvoker = new InterpretedFunctionInvoker(metadata.getFunctionAndTypeManager());
        }

        @Override
        protected Object visitLiteral(Literal node, ConnectorSession session)
        {
            throw new UnsupportedOperationException("Unhandled literal type: " + node);
        }

        @Override
        protected Object visitBooleanLiteral(BooleanLiteral node, ConnectorSession session)
        {
            return node.getValue();
        }

        @Override
        protected Long visitLongLiteral(LongLiteral node, ConnectorSession session)
        {
            return node.getValue();
        }

        @Override
        protected Double visitDoubleLiteral(DoubleLiteral node, ConnectorSession session)
        {
            return node.getValue();
        }

        @Override
        protected Object visitDecimalLiteral(DecimalLiteral node, ConnectorSession context)
        {
            return Decimals.parse(node.getValue()).getObject();
        }

        @Override
        protected Slice visitStringLiteral(StringLiteral node, ConnectorSession session)
        {
            return node.getSlice();
        }

        @Override
        protected Object visitCharLiteral(CharLiteral node, ConnectorSession context)
        {
            return node.getSlice();
        }

        @Override
        protected Slice visitBinaryLiteral(BinaryLiteral node, ConnectorSession session)
        {
            return node.getValue();
        }

        @Override
        protected Object visitGenericLiteral(GenericLiteral node, ConnectorSession session)
        {
            Type type;
            try {
                type = metadata.getType(parseTypeSignature(node.getType()));
            }
            catch (TypeNotFoundException e) {
                throw new SemanticException(TYPE_MISMATCH, node, "Unknown type: " + node.getType());
            }

            if (JSON.equals(type)) {
                FunctionHandle functionHandle = metadata.getFunctionAndTypeManager().lookupFunction("json_parse", TypeSignatureProvider.fromTypes(VARCHAR));
                return functionInvoker.invoke(functionHandle, session, ImmutableList.of(utf8Slice(node.getValue())));
            }

            try {
                FunctionHandle functionHandle = metadata.getFunctionAndTypeManager().lookupCast(CAST, VARCHAR.getTypeSignature(), type.getTypeSignature());
                return functionInvoker.invoke(functionHandle, session, ImmutableList.of(utf8Slice(node.getValue())));
            }
            catch (IllegalArgumentException e) {
                throw new SemanticException(TYPE_MISMATCH, node, "No literal form for type %s", type);
            }
        }

        @Override
        protected Long visitTimeLiteral(TimeLiteral node, ConnectorSession session)
        {
            return parseTimeLiteral(node.getValue());
        }

        @Override
        protected Long visitTimestampLiteral(TimestampLiteral node, ConnectorSession session)
        {
            try {
                return parseTimestampLiteral(node.getValue());
            }
            catch (RuntimeException e) {
                throw new SemanticException(INVALID_LITERAL, node, "'%s' is not a valid timestamp literal", node.getValue());
            }
        }

        @Override
        protected Long visitIntervalLiteral(IntervalLiteral node, ConnectorSession session)
        {
            if (node.isYearToMonth()) {
                return node.getSign().multiplier() * parseYearMonthInterval(node.getValue(), node.getStartField(), node.getEndField());
            }
            else {
                return node.getSign().multiplier() * parseDayTimeInterval(node.getValue(), node.getStartField(), node.getEndField());
            }
        }

        @Override
        protected Object visitNullLiteral(NullLiteral node, ConnectorSession session)
        {
            return null;
        }
    }
}
