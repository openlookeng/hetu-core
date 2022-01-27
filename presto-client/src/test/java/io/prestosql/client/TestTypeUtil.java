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

package io.prestosql.client;

import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.HyperLogLogType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.P4HyperLogLogType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimeWithTimeZoneType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.UnknownType;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.spi.type.testing.TestingTypeManager;
import org.testng.annotations.Test;

import static io.prestosql.client.util.TypeUtil.parseType;
import static org.testng.Assert.assertTrue;

/**
 * Test the type util function. include type parse.
 */
public class TestTypeUtil
{
    TypeManager typeManager = new TestingTypeManager();

    @Test
    public void testParseType()
    {
        Type type = parseType(typeManager, "bigint");
        assertTrue(type instanceof BigintType);

        type = parseType(typeManager, "integer");
        assertTrue(type instanceof IntegerType);

        type = parseType(typeManager, "smallint");
        assertTrue(type instanceof SmallintType);

        type = parseType(typeManager, "boolean");
        assertTrue(type instanceof BooleanType);

        type = parseType(typeManager, "date");
        assertTrue(type instanceof DateType);

        type = parseType(typeManager, "real");
        assertTrue(type instanceof RealType);

        type = parseType(typeManager, "double");
        assertTrue(type instanceof DoubleType);

        type = parseType(typeManager, "HyperLogLog");
        assertTrue(type instanceof HyperLogLogType);

        type = parseType(typeManager, "P4HyperLogLog");
        assertTrue(type instanceof P4HyperLogLogType);

        type = parseType(typeManager, "timestamp");
        assertTrue(type instanceof TimestampType);

        type = parseType(typeManager, "timestamp with time zone");
        assertTrue(type instanceof TimestampWithTimeZoneType);

        type = parseType(typeManager, "time");
        assertTrue(type instanceof TimeType);

        type = parseType(typeManager, "time with time zone");
        assertTrue(type instanceof TimeWithTimeZoneType);

        type = parseType(typeManager, "varbinary");
        assertTrue(type instanceof VarbinaryType);

        type = parseType(typeManager, "unknown");
        assertTrue(type instanceof UnknownType);
    }

    @Test
    public void testParametricType()
    {
        Type type = parseType(typeManager, "decimal(10,2)");
        assertTrue(type instanceof DecimalType);

        type = parseType(typeManager, "char(100)");
        assertTrue(type instanceof CharType);

        type = parseType(typeManager, "varchar(100)");
        assertTrue(type instanceof VarcharType);

        type = parseType(typeManager, "array(varchar(10))");
        assertTrue(type instanceof ArrayType);

        type = parseType(typeManager, "row(street varchar(10))");
        assertTrue(type instanceof RowType);
    }
}
