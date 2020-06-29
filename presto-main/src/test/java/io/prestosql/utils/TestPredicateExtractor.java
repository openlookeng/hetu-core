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
import java.util.List;

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
                false);

        // Test multiple supported predicates
        // AND is supported
        ComparisonExpression expr1 = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("a"), new Cast(new StringLiteral("a"), "A"));
        ComparisonExpression expr2 = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("b"), new Cast(new StringLiteral("b"), "B"));
        LogicalBinaryExpression andLbExpression = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, expr1, expr2);
        testIsSplitFilterApplicableForOperator(
                andLbExpression,
                true);

        // OR is not supported
        LogicalBinaryExpression orLbExpression = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.OR, expr1, expr2);
        testIsSplitFilterApplicableForOperator(
                orLbExpression,
                false);
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

        List<Predicate> predicateList = PredicateExtractor.buildPredicates(stage);
        Predicate predicate = predicateList.get(0);
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

        List<Predicate> predicateList = PredicateExtractor.buildPredicates(stage);
        if (predicateList.size() == 0) {
            assertEquals(null, expectedValue);
        }
        else {
            Predicate predicate = predicateList.get(0);
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

        List<Predicate> predicates = PredicateExtractor.buildPredicates(stage);
        assertEquals(predicates.size(), 2);
        assertEquals(predicates.get(0).getValue(), "a");
        assertEquals(predicates.get(0).getColumnName(), "a");
        assertEquals(predicates.get(0).getTableName(), "test.null");
        assertEquals(predicates.get(1).getValue(), "b");
        assertEquals(predicates.get(1).getColumnName(), "b");
        assertEquals(predicates.get(1).getTableName(), "test.null");
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

        List<Predicate> predicates = PredicateExtractor.buildPredicates(stage);
        assertEquals(predicates.size(), 3);
        assertEquals(predicates.get(0).getValue(), "a");
        assertEquals(predicates.get(0).getColumnName(), "a");
        assertEquals(predicates.get(0).getTableName(), "test.null");
        assertEquals(predicates.get(1).getValue(), "b");
        assertEquals(predicates.get(1).getColumnName(), "b");
        assertEquals(predicates.get(1).getTableName(), "test.null");
    }

    @Test
    public void testMultiPredicateExtractorWithUnsupportedOperators()
    {
        ComparisonExpression expr1 = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("a"), new Cast(new StringLiteral("a"), "A"));
        ComparisonExpression expr2 = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("b"), new Cast(new StringLiteral("b"), "B"));
        ComparisonExpression expr3 = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("c"), new Cast(new StringLiteral("c"), "C"));

        LogicalBinaryExpression lbExpression1 = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, expr1, expr2);
        LogicalBinaryExpression lbExpression2 = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.OR, lbExpression1, expr3);

        SqlStageExecution stage = TestUtil.getTestStage(lbExpression2);

        List<Predicate> predicates = PredicateExtractor.buildPredicates(stage);
        assertEquals(predicates.size(), 0);
    }

    @Test
    public void testMultiPredicateExtractorWithDifferentOperators()
    {
        ComparisonExpression expr1 = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("a"), new Cast(new StringLiteral("a"), "A"));
        ComparisonExpression expr2 = new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN, new SymbolReference("b"), new Cast(new StringLiteral("b"), "B"));
        ComparisonExpression expr3 = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("c"), new Cast(new StringLiteral("c"), "C"));

        LogicalBinaryExpression lbExpression1 = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, expr1, expr2);
        LogicalBinaryExpression lbExpression2 = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, lbExpression1, expr3);

        SqlStageExecution stage = TestUtil.getTestStage(lbExpression2);

        List<Predicate> predicates = PredicateExtractor.buildPredicates(stage);
        assertEquals(predicates.size(), 3);
        assertEquals(predicates.get(0).getValue(), "a");
        assertEquals(predicates.get(0).getColumnName(), "a");
        assertEquals(predicates.get(0).getTableName(), "test.null");
        assertEquals(predicates.get(1).getValue(), "b");
        assertEquals(predicates.get(1).getColumnName(), "b");
        assertEquals(predicates.get(1).getTableName(), "test.null");
        assertEquals(predicates.get(2).getValue(), "c");
        assertEquals(predicates.get(2).getColumnName(), "c");
        assertEquals(predicates.get(2).getTableName(), "test.null");
    }

    @Test
    public void testMultiPredicateExtractorWithUnsupportedExpressions()
    {
        ComparisonExpression expr1 = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("a"), new Cast(new StringLiteral("a"), "A"));
        ComparisonExpression expr2 = new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN, new SymbolReference("b"), new Cast(new StringLiteral("b"), "B"));
        ComparisonExpression expr3 = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new FunctionCall(QualifiedName.of("count"), ImmutableList.of()), new Cast(new StringLiteral("1"), "BIGINT"));

        LogicalBinaryExpression lbExpression1 = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, expr1, expr2);
        LogicalBinaryExpression lbExpression2 = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, lbExpression1, expr3);

        SqlStageExecution stage = TestUtil.getTestStage(lbExpression2);

        List<Predicate> predicates = PredicateExtractor.buildPredicates(stage);
        assertEquals(predicates.size(), 2);
        assertEquals(predicates.get(0).getValue(), "a");
        assertEquals(predicates.get(0).getColumnName(), "a");
        assertEquals(predicates.get(0).getTableName(), "test.null");
        assertEquals(predicates.get(1).getValue(), "b");
        assertEquals(predicates.get(1).getColumnName(), "b");
        assertEquals(predicates.get(1).getTableName(), "test.null");
    }
}
