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

import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.type.Type;
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
import io.prestosql.sql.tree.SymbolReference;

import java.util.List;
import java.util.Set;
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

    private static class CubeRangeVisitor
            extends AstVisitor<Expression, Void>
    {
        private final TypeProvider types;
        private final Metadata metadata;
        private final ConnectorSession session;
        private final LiteralEncoder encoder;

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
            SymbolReference symbolReference;
            Expression value;
            if (comparisonExpression.getLeft() instanceof SymbolReference) {
                symbolReference = (SymbolReference) comparisonExpression.getLeft();
                value = comparisonExpression.getRight();
            }
            else {
                symbolReference = (SymbolReference) comparisonExpression.getRight();
                value = comparisonExpression.getLeft();
            }
            Type type = types.get(new Symbol(symbolReference.getName()));
            if (isTypeNotSupported(type)) {
                return super.visitComparisonExpression(comparisonExpression, ignored);
            }
            if (value instanceof Cast) {
                value = ((Cast) value).getExpression();
            }
            if (LiteralInterpreter.evaluate(metadata, session, value) == null) {
                return super.visitComparisonExpression(comparisonExpression, ignored);
            }
            if (operator == EQUAL) {
                Expression low = encoder.toExpression(LiteralInterpreter.evaluate(metadata, session, value), type);
                Expression high = encoder.toExpression(((Long) LiteralInterpreter.evaluate(metadata, session, value) + 1), type);
                return new LogicalBinaryExpression(AND,
                        new ComparisonExpression(GREATER_THAN_OR_EQUAL, symbolReference, low),
                        new ComparisonExpression(LESS_THAN, symbolReference, high));
            }
            else if (operator == LESS_THAN_OR_EQUAL) {
                value = encoder.toExpression(((Long) LiteralInterpreter.evaluate(metadata, session, value) + 1), type);
                return new ComparisonExpression(LESS_THAN, symbolReference, value);
            }
            else if (operator == GREATER_THAN) {
                value = encoder.toExpression(((Long) LiteralInterpreter.evaluate(metadata, session, value) + 1), type);
                return new ComparisonExpression(GREATER_THAN_OR_EQUAL, symbolReference, value);
            }
            return super.visitComparisonExpression(comparisonExpression, ignored);
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
            SymbolReference symbolReference = (SymbolReference) predicate.getValue();
            Type type = types.get(new Symbol(symbolReference.getName()));
            if (isTypeNotSupported(type)) {
                return super.visitBetweenPredicate(predicate, ignored);
            }
            Expression low = predicate.getMin();
            if (low instanceof Cast) {
                low = ((Cast) low).getExpression();
            }
            low = encoder.toExpression(LiteralInterpreter.evaluate(metadata, session, low), type);
            Expression high = predicate.getMax();
            if (high instanceof Cast) {
                high = ((Cast) high).getExpression();
            }
            high = encoder.toExpression(((Long) LiteralInterpreter.evaluate(metadata, session, high) + 1), type);
            return new LogicalBinaryExpression(AND,
                    new ComparisonExpression(GREATER_THAN_OR_EQUAL, predicate.getValue(), low),
                    new ComparisonExpression(LESS_THAN, predicate.getValue(), high));
        }

        private boolean isTypeNotSupported(Type type)
        {
            return type != BIGINT && type != INTEGER && type != SMALLINT && type != TINYINT && type != DATE;
        }
    }
}
