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

package io.hetu.core.hive.dynamicfunctions.utils;

import io.hetu.core.hive.dynamicfunctions.type.ArrayParametricType;
import io.hetu.core.hive.dynamicfunctions.type.MapParametricType;
import io.prestosql.spi.type.StandardTypes;
import org.testng.annotations.Test;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.hetu.core.hive.dynamicfunctions.utils.HetuTypeUtil.getType;
import static io.hetu.core.hive.dynamicfunctions.utils.HetuTypeUtil.getTypeSignature;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static org.testng.Assert.assertEquals;

public class TestHetuTypeUtil
{
    @Test
    public void testGetTypeWithBooleanSignature()
    {
        assertEquals(BOOLEAN, getType(StandardTypes.BOOLEAN));
    }

    @Test
    public void testGetTypeWithVarbinary()
    {
        assertEquals(VARBINARY, getType(StandardTypes.VARBINARY));
    }

    @Test
    public void testGetTypeWithHyperLogLogSignature()
    {
        assertEquals(HYPER_LOG_LOG, getType(StandardTypes.HYPER_LOG_LOG));
    }

    @Test
    public void testGetTypeWithDateSignature()
    {
        assertEquals(DATE, getType(StandardTypes.DATE));
    }

    @Test
    public void testGetTypeWithTimestampSignature()
    {
        assertEquals(TIMESTAMP, getType(StandardTypes.TIMESTAMP));
    }

    @Test
    public void testGetTypeWithTimeSignature()
    {
        assertEquals(getType(StandardTypes.TIME), TIME);
    }

    @Test
    public void testGetTypeWithDecimalSignature()
    {
        assertEquals(getType("decimal(10,2)"), createDecimalType(10, 2));
    }

    @Test
    public void testGetTypeWithChar()
    {
        assertEquals(getType("char(10)"), createCharType(10L));
    }

    @Test
    public void testGetTypeWithVarchar()
    {
        assertEquals(getType("varchar(10)"), createVarcharType(10));
    }

    @Test
    public void testGetTypeWithMapSignature()
    {
        assertEquals(getType("map(integer,integer)"), new MapParametricType().createType(Arrays.asList("integer", "integer")));
    }

    @Test
    public void testGetTypeWithArraySignature()
    {
        assertEquals(getType("array(integer)"), new ArrayParametricType().createType(Arrays.asList("integer")));
    }

    @Test
    public void testGetTypeSignatureWithPrimitiveJavaType()
    {
        assertEquals(getTypeSignature(Boolean.class).toString(), StandardTypes.BOOLEAN);
    }

    @Test
    public void testGetTypeSignatureWithListType()
    {
        ParameterizedType listType = new ParameterizedType() {
            @Override
            public Type[] getActualTypeArguments()
            {
                return new Type[] {Integer.class};
            }

            @Override
            public Type getRawType()
            {
                return List.class;
            }

            @Override
            public Type getOwnerType()
            {
                return null;
            }
        };
        assertEquals(getTypeSignature(listType).toString(), "array(integer)");
    }

    @Test
    public void testGetTypeSignatureWithMapType()
    {
        ParameterizedType mapType = new ParameterizedType() {
            @Override
            public Type[] getActualTypeArguments()
            {
                return new Type[] {Integer.class, Integer.class};
            }

            @Override
            public Type getRawType()
            {
                return Map.class;
            }

            @Override
            public Type getOwnerType()
            {
                return null;
            }
        };
        assertEquals(getTypeSignature(mapType).toString(), "map(integer,integer)");
    }
}
