/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.utils;

import com.google.common.collect.ImmutableList;
import io.prestosql.execution.SqlStageExecution;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.Literal;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.NotExpression;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.TimeLiteral;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.HashMap;

import static org.testng.Assert.assertEquals;

public class TestPredicateExtractor
{
    /**
     * Test that split filter is applicable for different operators
     */
    @Test
    public void testIsSplitFilterApplicableForOperators()
    {
        // EQUAL is supported
        testIsSplitFilterApplicableForOperator(
                new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("a"), new StringLiteral("hello")),
                true);

        // GREATER_THAN is supported
        testIsSplitFilterApplicableForOperator(
                new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN, new SymbolReference("a"), new StringLiteral("hello")),
                true);

        // LESS_THAN is supported
        testIsSplitFilterApplicableForOperator(
                new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN, new SymbolReference("a"), new StringLiteral("hello")),
                true);

        // GREATER_THAN_OR_EQUAL is supported
        testIsSplitFilterApplicableForOperator(
                new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new StringLiteral("hello")),
                true);

        // LESS_THAN_OR_EQUAL is supported
        testIsSplitFilterApplicableForOperator(
                new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, new SymbolReference("a"), new StringLiteral("hello")),
                true);

        // NOT is not supported
        testIsSplitFilterApplicableForOperator(
                new NotExpression(new SymbolReference("a")),
                true);

        // Test multiple supported predicates
        // AND is supported
        ComparisonExpression expr1 = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("a"), new Cast(new StringLiteral("a"), "A"));
        ComparisonExpression expr2 = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("b"), new Cast(new StringLiteral("b"), "B"));
        LogicalBinaryExpression andLbExpression = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, expr1, expr2);
        testIsSplitFilterApplicableForOperator(
                andLbExpression,
                true);

        // OR is supported
        LogicalBinaryExpression orLbExpression = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.OR, expr1, expr2);
        testIsSplitFilterApplicableForOperator(
                orLbExpression,
                true);

        // IN is supported
        InPredicate inExpression = new InPredicate(new SymbolReference("a"), new InListExpression(ImmutableList.of(new StringLiteral("hello"), new StringLiteral("hello2"))));
        testIsSplitFilterApplicableForOperator(
                inExpression,
                true);
    }

    private void testIsSplitFilterApplicableForOperator(Expression expression, boolean expected)
    {
        SqlStageExecution stage = TestUtil.getTestStage(expression);
        assertEquals(PredicateExtractor.isSplitFilterApplicable(stage), expected);
    }

    @Test
    public void testBuildCastPredicate()
    {
        ComparisonExpression expr = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("a"), new Cast(new StringLiteral("a"), "A"));

        SqlStageExecution stage = TestUtil.getTestStage(expr);

        Predicate predicate = PredicateExtractor.processComparisonExpression(expr, PredicateExtractor.getFullyQualifiedName(stage).get(), new HashMap<>());
        assertEquals(predicate.getValue(), "a");
        assertEquals(predicate.getColumnName(), "a");
        assertEquals(predicate.getTableName(), "test.null");
    }

    @Test
    public void testBuildPredicates()
    {
        // tinyint
        testBuildPredicate(new GenericLiteral("TINYINT", "1"), Long.valueOf("1"));
        testBuildPredicate(new GenericLiteral("tinyint", "1"), Long.valueOf("1"));

        // smallint
        testBuildPredicate(new GenericLiteral("SMALLINT", "1"), Long.valueOf("1"));
        testBuildPredicate(new GenericLiteral("smallint", "1"), Long.valueOf("1"));

        // integer
        testBuildPredicate(new LongLiteral("1"), Long.valueOf("1"));

        // bigint
        testBuildPredicate(new GenericLiteral("BIGINT", "1"), Long.valueOf("1"));
        testBuildPredicate(new GenericLiteral("bigint", "1"), Long.valueOf("1"));
        testBuildPredicate(new GenericLiteral("bigint", "1"), Long.valueOf(1));

        // float/real
        testBuildPredicate(new GenericLiteral("REAL", "1.0"), (long) Float.floatToIntBits(Float.parseFloat("1.0")));
        testBuildPredicate(new GenericLiteral("real", "1.0"), (long) Float.floatToIntBits(Float.parseFloat("1.0")));
        testBuildPredicate(new GenericLiteral("real", "1.0"), (long) Float.floatToIntBits(Float.parseFloat("1.0")));
        testBuildPredicate(new GenericLiteral("real", "1"), (long) Float.floatToIntBits(Float.parseFloat("1")));
        testBuildPredicate(new GenericLiteral("real", "1"), (long) Float.floatToIntBits(Float.parseFloat("1")));

        // double
        testBuildPredicate(new DoubleLiteral("1"), Double.valueOf(1));
        testBuildPredicate(new DoubleLiteral("1.0"), Double.valueOf(1.0));
        testBuildPredicate(new DoubleLiteral("1"), Double.valueOf(1.0));

        // decimal
        testBuildPredicate(new DecimalLiteral("1"), BigDecimal.valueOf(1));
        testBuildPredicate(new DecimalLiteral("1.0"), new BigDecimal("1.0")); // string constructor should be used, see BigDecimal docs
        testBuildPredicate(new DecimalLiteral("1"), new BigDecimal("1")); // 1 != 1.0

        // string
        testBuildPredicate(new StringLiteral("hello"), "hello");

        // boolean
        testBuildPredicate(new BooleanLiteral("true"), true);
        testBuildPredicate(new BooleanLiteral("false"), false);

        testBuildPredicate(new TimeLiteral("2018-05-01 05:53:03"), "2018-05-01 05:53:03");
    }

    private void testBuildPredicate(Literal literal, Object expectedValue)
    {
        ComparisonExpression expr = new ComparisonExpression(
                ComparisonExpression.Operator.EQUAL,
                new SymbolReference("a"),
                literal);

        SqlStageExecution stage = TestUtil.getTestStage(expr);

        Predicate predicate = PredicateExtractor.processComparisonExpression(expr, PredicateExtractor.getFullyQualifiedName(stage).get(), new HashMap<>());
        if (predicate == null) {
            assertEquals(null, expectedValue);
        }
        else {
            assertEquals(predicate.getValue(), expectedValue);
            assertEquals(predicate.getColumnName(), "a");
            assertEquals(predicate.getTableName(), "test.null");
        }
    }

    @Test
    public void testBuildNotImplementedGenericPredicate()
    {
        testBuildPredicate(new GenericLiteral("a", "A"), null);
    }

    @Test
    public void testBuildNotImplementedPredicate()
    {
        testBuildPredicate(new NullLiteral(), null);
    }

    @Test
    public void testTwoPredicateExtractor()
    {
        ComparisonExpression expr1 = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("a"), new Cast(new StringLiteral("a"), "A"));
        ComparisonExpression expr2 = new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, new SymbolReference("b"), new Cast(new StringLiteral("b"), "B"));
        LogicalBinaryExpression lbExpression = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, expr1, expr2);

        SqlStageExecution stage = TestUtil.getTestStage(lbExpression);
        Expression left = lbExpression.getLeft();
        Expression right = lbExpression.getRight();
        assertEquals(lbExpression.getOperator(), LogicalBinaryExpression.Operator.AND);

        if (left instanceof ComparisonExpression) {
            Predicate leftPredicate = PredicateExtractor.processComparisonExpression((ComparisonExpression) left, PredicateExtractor.getFullyQualifiedName(stage).get(), new HashMap<>());
            assertEquals(leftPredicate.getValue(), "a");
            assertEquals(leftPredicate.getColumnName(), "a");
            assertEquals(leftPredicate.getTableName(), "test.null");
        }

        if (right instanceof ComparisonExpression) {
            Predicate rightPredicate = PredicateExtractor.processComparisonExpression((ComparisonExpression) right, PredicateExtractor.getFullyQualifiedName(stage).get(), new HashMap<>());
            assertEquals(rightPredicate.getValue(), "b");
            assertEquals(rightPredicate.getColumnName(), "b");
            assertEquals(rightPredicate.getTableName(), "test.null");
        }
    }

    @Test
    public void testMultiPredicateExtractor()
    {
        ComparisonExpression expr1 = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("a"), new Cast(new StringLiteral("a"), "A"));
        ComparisonExpression expr2 = new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN, new SymbolReference("b"), new Cast(new StringLiteral("b"), "B"));
        ComparisonExpression expr3 = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("c"), new Cast(new StringLiteral("c"), "C"));

        LogicalBinaryExpression lbExpression1 = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, expr1, expr2);
        LogicalBinaryExpression lbExpression2 = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, lbExpression1, expr3);

        SqlStageExecution stage = TestUtil.getTestStage(lbExpression2);

        Expression left = lbExpression2.getLeft();
        Expression right = lbExpression2.getRight();
        assertEquals(lbExpression2.getOperator(), LogicalBinaryExpression.Operator.AND);

        if (left instanceof LogicalBinaryExpression) {
            Expression leftChild = ((LogicalBinaryExpression) left).getLeft();
            Expression rightChild = ((LogicalBinaryExpression) left).getRight();
            assertEquals(((LogicalBinaryExpression) left).getOperator(), LogicalBinaryExpression.Operator.AND);

            Predicate leftPredicate = PredicateExtractor.processComparisonExpression((ComparisonExpression) leftChild, PredicateExtractor.getFullyQualifiedName(stage).get(), new HashMap<>());
            assertEquals(leftPredicate.getValue(), "a");
            assertEquals(leftPredicate.getColumnName(), "a");
            assertEquals(leftPredicate.getTableName(), "test.null");

            Predicate rightPredicate = PredicateExtractor.processComparisonExpression((ComparisonExpression) rightChild, PredicateExtractor.getFullyQualifiedName(stage).get(), new HashMap<>());
            assertEquals(rightPredicate.getValue(), "b");
            assertEquals(rightPredicate.getColumnName(), "b");
            assertEquals(rightPredicate.getTableName(), "test.null");
        }

        if (right instanceof ComparisonExpression) {
            Predicate rightPredicate = PredicateExtractor.processComparisonExpression((ComparisonExpression) right, PredicateExtractor.getFullyQualifiedName(stage).get(), new HashMap<>());
            assertEquals(rightPredicate.getValue(), "c");
            assertEquals(rightPredicate.getColumnName(), "c");
            assertEquals(rightPredicate.getTableName(), "test.null");
        }
    }

    @Test
    public void testMultiPredicateExtractorWithMultiOperators()
    {
        ComparisonExpression expr1 = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("a"), new Cast(new StringLiteral("a"), "A"));
        ComparisonExpression expr2 = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("b"), new Cast(new StringLiteral("b"), "B"));
        ComparisonExpression expr3 = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("c"), new Cast(new StringLiteral("c"), "C"));

        LogicalBinaryExpression lbExpression1 = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, expr1, expr2);
        LogicalBinaryExpression lbExpression2 = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.OR, lbExpression1, expr3);

        SqlStageExecution stage = TestUtil.getTestStage(lbExpression2);

        Expression left = lbExpression2.getLeft();
        Expression right = lbExpression2.getRight();
        assertEquals(lbExpression2.getOperator(), LogicalBinaryExpression.Operator.OR);

        if (left instanceof LogicalBinaryExpression) {
            Expression leftChild = ((LogicalBinaryExpression) left).getLeft();
            Expression rightChild = ((LogicalBinaryExpression) left).getRight();
            assertEquals(((LogicalBinaryExpression) left).getOperator(), LogicalBinaryExpression.Operator.AND);

            Predicate leftPredicate = PredicateExtractor.processComparisonExpression((ComparisonExpression) leftChild, PredicateExtractor.getFullyQualifiedName(stage).get(), new HashMap<>());
            assertEquals(leftPredicate.getValue(), "a");
            assertEquals(leftPredicate.getColumnName(), "a");
            assertEquals(leftPredicate.getTableName(), "test.null");

            Predicate rightPredicate = PredicateExtractor.processComparisonExpression((ComparisonExpression) rightChild, PredicateExtractor.getFullyQualifiedName(stage).get(), new HashMap<>());
            assertEquals(rightPredicate.getValue(), "b");
            assertEquals(rightPredicate.getColumnName(), "b");
            assertEquals(rightPredicate.getTableName(), "test.null");
        }

        if (right instanceof ComparisonExpression) {
            Predicate rightPredicate = PredicateExtractor.processComparisonExpression((ComparisonExpression) right, PredicateExtractor.getFullyQualifiedName(stage).get(), new HashMap<>());
            assertEquals(rightPredicate.getValue(), "c");
            assertEquals(rightPredicate.getColumnName(), "c");
            assertEquals(rightPredicate.getTableName(), "test.null");
        }
    }

    @Test
    public void testMultiPredicateExtractorWithUnsupportedExpression()
    {
        // FunctionCall unsupported
        ComparisonExpression expr1 = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("a"), new Cast(new StringLiteral("a"), "A"));
        ComparisonExpression expr2 = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new FunctionCall(QualifiedName.of("count"), ImmutableList.of()), new Cast(new StringLiteral("1"), "BIGINT"));
        LogicalBinaryExpression lbExpression = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, expr1, expr2);

        SqlStageExecution stage = TestUtil.getTestStage(lbExpression);
        Expression right = lbExpression.getRight();

        if (right instanceof ComparisonExpression) {
            Predicate rightPredicate = PredicateExtractor.processComparisonExpression((ComparisonExpression) right, PredicateExtractor.getFullyQualifiedName(stage).get(), new HashMap<>());
            assertEquals(rightPredicate, null);
        }
    }
}
