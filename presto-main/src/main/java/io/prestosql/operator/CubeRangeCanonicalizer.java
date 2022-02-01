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

package io.prestosql.operator;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.planner.ExpressionDomainTranslator;
import io.prestosql.sql.planner.LiteralEncoder;
import io.prestosql.sql.planner.LiteralInterpreter;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.SymbolReference;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.prestosql.sql.tree.LogicalBinaryExpression.Operator.AND;
import static io.prestosql.sql.tree.LogicalBinaryExpression.Operator.OR;

/**
 * Parse Cube predicates and canonicalize them into Standard formats
 * that can be merged into single Range predicate.
 */
public class CubeRangeCanonicalizer
{
    private static final Logger log = Logger.get(CubeRangeCanonicalizer.class);
    private final Metadata metadata;
    private final Session session;
    private final TypeProvider types;

    public CubeRangeCanonicalizer(Metadata metadata, Session session, TypeProvider types)
    {
        this.metadata = metadata;
        this.session = session;
        this.types = types;
    }

    public Expression mergePredicates(Expression cubePredicate)
    {
        Set<Identifier> predicateColumns = ExpressionUtils.getIdentifiers(cubePredicate);
        if (predicateColumns.size() > 1) {
            //Only single column predicates can be merged
            return cubePredicate;
        }
        cubePredicate = ExpressionUtils.rewriteIdentifiersToSymbolReferences(cubePredicate);
        List<Expression> predicates = ExpressionUtils.extractDisjuncts(cubePredicate);
        CubeRangeVisitor visitor = new CubeRangeVisitor(types, metadata, session.toConnectorSession());
        Expression transformed = ExpressionUtils.or(predicates.stream().map(visitor::process).collect(Collectors.toList()));
        ExpressionDomainTranslator.ExtractionResult result = ExpressionDomainTranslator.fromPredicate(metadata, session, transformed, types);
        if (!result.getRemainingExpression().equals(BooleanLiteral.TRUE_LITERAL)) {
            log.info("Unable to transform predicate %s into tuple domain completely. Cannot merge ranges into single predicate.", transformed);
            return cubePredicate;
        }
        ExpressionDomainTranslator domainTranslator = new ExpressionDomainTranslator(new LiteralEncoder(metadata));
        return domainTranslator.toPredicate(result.getTupleDomain());
    }

    public static class CubeRangeVisitor
            extends AstVisitor<Expression, Void>
    {
        private final TypeProvider types;
        private final Metadata metadata;
        private final ConnectorSession session;
        private final LiteralEncoder encoder;
        private static final Pattern NUMBER_PATTERN = Pattern.compile("\\d+$");
        private static final Set<Type> SUPPORTED_TYPES = ImmutableSet.<Type>builder()
                .add(BIGINT).add(INTEGER).add(SMALLINT).add(TINYINT).add(DATE).build();

        public CubeRangeVisitor(TypeProvider types, Metadata metadata, ConnectorSession session)
        {
            this.types = types;
            this.metadata = metadata;
            this.session = session;
            this.encoder = new LiteralEncoder(metadata);
        }

        @Override
        public Expression visitExpression(Expression expression, Void ignored)
        {
            return expression;
        }

        @Override
        public Expression visitComparisonExpression(ComparisonExpression comparisonExpression, Void ignored)
        {
            ComparisonExpression.Operator operator = comparisonExpression.getOperator();
            Pair<SymbolReference, Expression> symbolAndValuePair = extractSymbolAndValue(comparisonExpression);
            SymbolReference symbolReference = symbolAndValuePair.getFirst();
            Expression valueExpr = symbolAndValuePair.getSecond();
            Type type = types.get(new Symbol(symbolReference.getName()));
            if (!isTypeSupported(type)) {
                return super.visitComparisonExpression(comparisonExpression, ignored);
            }
            Object value = evaluate(valueExpr, type);
            if (value == null) {
                return super.visitComparisonExpression(comparisonExpression, ignored);
            }
            if (operator == EQUAL) {
                return new LogicalBinaryExpression(AND,
                        new ComparisonExpression(GREATER_THAN_OR_EQUAL, symbolReference, encode(value, type)),
                        new ComparisonExpression(LESS_THAN, symbolReference, incrementAndEncode(value, type)));
            }
            else if (operator == LESS_THAN_OR_EQUAL) {
                return new ComparisonExpression(LESS_THAN, symbolReference, incrementAndEncode(value, type));
            }
            else if (operator == GREATER_THAN) {
                return new ComparisonExpression(GREATER_THAN_OR_EQUAL, symbolReference, incrementAndEncode(value, type));
            }
            return super.visitComparisonExpression(comparisonExpression, ignored);
        }

        private Pair<SymbolReference, Expression> extractSymbolAndValue(ComparisonExpression comparisonExpression)
        {
            SymbolReference symbolReference;
            Expression valueExpr;
            if (comparisonExpression.getLeft() instanceof SymbolReference || comparisonExpression.getLeft() instanceof Cast) {
                if (comparisonExpression.getLeft() instanceof Cast) {
                    symbolReference = ((SymbolReference) ((Cast) comparisonExpression.getLeft()).getExpression());
                }
                else {
                    symbolReference = (SymbolReference) comparisonExpression.getLeft();
                }
                valueExpr = comparisonExpression.getRight();
            }
            else {
                if (comparisonExpression.getRight() instanceof Cast) {
                    symbolReference = ((SymbolReference) ((Cast) comparisonExpression.getRight()).getExpression());
                }
                else {
                    symbolReference = (SymbolReference) comparisonExpression.getRight();
                }
                valueExpr = comparisonExpression.getLeft();
            }
            if (valueExpr instanceof Cast) {
                valueExpr = ((Cast) valueExpr).getExpression();
            }
            return new Pair<>(symbolReference, valueExpr);
        }

        private boolean isTypeSupported(Type type)
        {
            return (type instanceof VarcharType) || SUPPORTED_TYPES.contains(type);
        }

        private Object evaluate(Expression valueExpr, Type type)
        {
            if (valueExpr instanceof NullLiteral) {
                return null;
            }
            Object value = LiteralInterpreter.evaluate(metadata, session, valueExpr);
            if (type instanceof VarcharType) {
                String valueString = value instanceof Slice ? ((Slice) value).toStringUtf8() : (String) value;
                Matcher m = NUMBER_PATTERN.matcher(valueString);
                return m.find() ? valueString : null;
            }
            return value instanceof Long ? value : null;
        }

        private Expression encode(Object value, Type type)
        {
            if (type instanceof VarcharType) {
                return encoder.toExpression(Slices.utf8Slice((String) value), type);
            }
            return encoder.toExpression(value, type);
        }

        private Expression incrementAndEncode(Object value, Type type)
        {
            Object incrementedValue;
            if (type instanceof VarcharType) {
                String valueAsString = (String) value;
                Matcher m = NUMBER_PATTERN.matcher(valueAsString);
                //Following must always be true. Pattern has earlier tested against the predicate value.
                m.find();
                String num = m.group();
                int inc = Integer.parseInt(num) + 1;
                String incStr = String.format("%0" + num.length() + "d", inc);
                incrementedValue = Slices.utf8Slice(m.replaceFirst(incStr));
            }
            else {
                incrementedValue = ((Long) value) + 1;
            }
            return encoder.toExpression(incrementedValue, type);
        }

        @Override
        public Expression visitLogicalBinaryExpression(LogicalBinaryExpression expression, Void ignored)
        {
            if (expression.getOperator() == AND) {
                Expression left = process(expression.getLeft(), ignored);
                Expression right = process(expression.getRight(), ignored);
                return new LogicalBinaryExpression(AND, left, right);
            }
            else if (expression.getOperator() == OR) {
                Expression left = process(expression.getLeft(), ignored);
                Expression right = process(expression.getRight(), ignored);
                return new LogicalBinaryExpression(OR, left, right);
            }
            return super.visitLogicalBinaryExpression(expression, ignored);
        }

        @Override
        public Expression visitBetweenPredicate(BetweenPredicate predicate, Void ignored)
        {
            SymbolReference symbolReference;
            if (predicate.getValue() instanceof Cast) {
                symbolReference = ((SymbolReference) ((Cast) predicate.getValue()).getExpression());
            }
            else {
                // instance of SymbolReference
                symbolReference = (SymbolReference) predicate.getValue();
            }
            Type type = types.get(new Symbol(symbolReference.getName()));
            if (!isTypeSupported(type)) {
                return super.visitBetweenPredicate(predicate, ignored);
            }
            Expression lowValueExpr = predicate.getMin();
            if (lowValueExpr instanceof Cast) {
                lowValueExpr = ((Cast) lowValueExpr).getExpression();
            }
            Object lowValue = evaluate(lowValueExpr, type);
            if (lowValue == null) {
                return super.visitBetweenPredicate(predicate, ignored);
            }

            Expression highValueExpr = predicate.getMax();
            if (highValueExpr instanceof Cast) {
                highValueExpr = ((Cast) highValueExpr).getExpression();
            }
            Object highValue = evaluate(highValueExpr, type);
            if (highValue == null) {
                return super.visitBetweenPredicate(predicate, ignored);
            }
            return new LogicalBinaryExpression(AND,
                    new ComparisonExpression(GREATER_THAN_OR_EQUAL, predicate.getValue(), encode(lowValue, type)),
                    new ComparisonExpression(LESS_THAN, predicate.getValue(), incrementAndEncode(highValue, type)));
        }
    }
}
