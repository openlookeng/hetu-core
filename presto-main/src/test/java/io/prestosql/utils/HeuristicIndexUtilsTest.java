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
package io.prestosql.utils;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.LongLiteral;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static org.testng.Assert.assertEquals;

public class HeuristicIndexUtilsTest
{
    @Test
    public void testExtractPartitions()
    {
        Expression equalExpName = new ComparisonExpression(EQUAL, name("a"), new LongLiteral("1"));
        System.out.println(equalExpName);
        assertEquals(HeuristicIndexUtils.extractPartitions(equalExpName), ImmutableList.of("a=1"));

        Expression equalExpNameExp = new ComparisonExpression(EQUAL, nameExp("a"), new LongLiteral("1"));
        System.out.println(equalExpNameExp);
        assertEquals(HeuristicIndexUtils.extractPartitions(equalExpName), ImmutableList.of("a=1"));

        Expression equalExp2Name = new ComparisonExpression(EQUAL, name("a"), new LongLiteral("2"));
        System.out.println(equalExp2Name);

        Expression orExp = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.OR, equalExpName, equalExp2Name);
        System.out.println(orExp);
        assertEquals(HeuristicIndexUtils.extractPartitions(orExp), ImmutableList.of("a=1", "a=2"));

        Expression inExpInteger = new InPredicate(name("a"), new InListExpression(
                ImmutableList.of(new LongLiteral("1"), new LongLiteral("2"), new LongLiteral("3"),
                        new LongLiteral("4"), new LongLiteral("5"), new LongLiteral("6"))));
        System.out.println(inExpInteger);
        assertEquals(HeuristicIndexUtils.extractPartitions(inExpInteger), ImmutableList.of("a=1", "a=2", "a=3", "a=4", "a=5", "a=6"));

        Expression inExpBigInt = new InPredicate(name("a"), new InListExpression(
                ImmutableList.of(bigintLiteral(1), bigintLiteral(2), bigintLiteral(3),
                        bigintLiteral(4), bigintLiteral(5), bigintLiteral(6))));
        System.out.println(inExpBigInt);
        assertEquals(HeuristicIndexUtils.extractPartitions(inExpInteger), ImmutableList.of("a=1", "a=2", "a=3", "a=4", "a=5", "a=6"));
    }

    private static Identifier name(String name)
    {
        return new Identifier(name);
    }

    private static Expression nameExp(String name)
    {
        Symbol nameSymbol = new Symbol(name);
        return toSymbolReference(nameSymbol);
    }

    private static Expression bigintLiteral(long number)
    {
        if (number < Integer.MAX_VALUE && number > Integer.MIN_VALUE) {
            return new GenericLiteral("BIGINT", String.valueOf(number));
        }
        return new LongLiteral(String.valueOf(number));
    }
}
