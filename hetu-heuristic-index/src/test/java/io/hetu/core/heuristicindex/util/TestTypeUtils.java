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

package io.hetu.core.heuristicindex.util;

import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.Literal;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.TimeLiteral;
import org.testng.annotations.Test;

import java.math.BigDecimal;

import static io.hetu.core.heuristicindex.util.TypeUtils.extractSingleValue;
import static org.testng.Assert.assertEquals;

public class TestTypeUtils
{
    @Test
    public void testBuildPredicates()
    {
        // tinyint
        testBuildPredicate(new GenericLiteral("TINYINT", "1"), 1L);
        testBuildPredicate(new GenericLiteral("tinyint", "1"), 1L);

        // smallint
        testBuildPredicate(new GenericLiteral("SMALLINT", "1"), 1L);
        testBuildPredicate(new GenericLiteral("smallint", "1"), 1L);

        // integer
        testBuildPredicate(new LongLiteral("1"), 1L);

        // bigint
        testBuildPredicate(new GenericLiteral("BIGINT", "1"), 1L);
        testBuildPredicate(new GenericLiteral("bigint", "1"), 1L);
        testBuildPredicate(new GenericLiteral("bigint", "1"), 1L);

        // float/real
        testBuildPredicate(new GenericLiteral("REAL", "1.0"), (long) Float.floatToIntBits(Float.parseFloat("1.0")));
        testBuildPredicate(new GenericLiteral("real", "1.0"), (long) Float.floatToIntBits(Float.parseFloat("1.0")));
        testBuildPredicate(new GenericLiteral("real", "1.0"), (long) Float.floatToIntBits(Float.parseFloat("1.0")));
        testBuildPredicate(new GenericLiteral("real", "1"), (long) Float.floatToIntBits(Float.parseFloat("1")));
        testBuildPredicate(new GenericLiteral("real", "1"), (long) Float.floatToIntBits(Float.parseFloat("1")));

        // double
        testBuildPredicate(new DoubleLiteral("1"), 1D);
        testBuildPredicate(new DoubleLiteral("1.0"), 1.0);
        testBuildPredicate(new DoubleLiteral("1"), 1.0);

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

    @Test
    public void testCast()
    {
        Expression exp = new Cast(new StringLiteral("a"), "A");
        assertEquals(extractSingleValue(exp), "a");
    }

    private void testBuildPredicate(Literal literal, Object expectedValue)
    {
        assertEquals(extractSingleValue(literal), expectedValue);
    }
}
