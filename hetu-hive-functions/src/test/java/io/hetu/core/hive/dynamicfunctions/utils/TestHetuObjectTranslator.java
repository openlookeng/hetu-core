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

import io.airlift.slice.Slices;
import io.hetu.core.hive.dynamicfunctions.type.ArrayParametricType;
import io.prestosql.spi.block.IntArrayBlock;
import org.testng.annotations.Test;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.hetu.core.hive.dynamicfunctions.utils.HiveObjectTranslator.translateFromHiveObject;
import static io.hetu.core.hive.dynamicfunctions.utils.HiveObjectTranslator.translateToHiveObject;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestHetuObjectTranslator
{
    @Test
    public void testTranslateToHiveObjectWithNull()
    {
        assertEquals(translateToHiveObject(BOOLEAN, null, boolean.class), null);
    }

    @Test
    public void testTranslateToHiveObjectWithBooleanObject()
    {
        assertEquals(translateToHiveObject(BOOLEAN, true, boolean.class), true);
    }

    @Test
    public void testTranslateToHiveObjectWithBooleanWrapperObject()
    {
        assertEquals(translateToHiveObject(BOOLEAN, true, Boolean.class), true);
    }

    @Test
    public void testTranslateToHiveObjectWithDoubleObject()
    {
        assertEquals(translateToHiveObject(DOUBLE, 1.0, double.class), 1.0);
    }

    @Test
    public void testTranslateToHiveObjectWithDoubleWrapperObject()
    {
        assertEquals(translateToHiveObject(DOUBLE, 1.0, Double.class), 1.0);
    }

    @Test
    public void testTranslateToHiveObjectWithLongObject()
    {
        assertEquals(translateToHiveObject(BIGINT, 1000, long.class), 1000);
    }

    @Test
    public void testTranslateToHiveObjectWithLongWrapperObject()
    {
        assertEquals(translateToHiveObject(BIGINT, 1000, Long.class), 1000);
    }

    @Test
    public void testTranslateToHiveObjectWithIntObject()
    {
        assertEquals(translateToHiveObject(INTEGER, (long) 1, int.class), 1);
    }

    @Test
    public void testTranslateToHiveObjectWithIntWrapperObject()
    {
        assertEquals(translateToHiveObject(INTEGER, (long) 1, Integer.class), 1);
    }

    @Test
    public void testTranslateToHiveObjectWithShortObject()
    {
        assertEquals(translateToHiveObject(SMALLINT, (long) 1, short.class), (short) 1);
    }

    @Test
    public void testTranslateToHiveObjectWithShortWrapperObject()
    {
        assertEquals(translateToHiveObject(SMALLINT, (long) 1, Short.class), (short) 1);
    }

    @Test
    public void testTranslateToHiveObjectWithStringObject()
    {
        assertEquals(translateToHiveObject(VARCHAR, Slices.utf8Slice("a"), String.class), "a");
    }

    @Test
    public void testTranslateToHiveObjectWithListObject()
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
        Object obj = new IntArrayBlock(2, Optional.ofNullable(new boolean[] {true, true}),
                new int[] {1, 2});
        assertEquals(translateToHiveObject(
                new ArrayParametricType().createType(Arrays.asList("integer")), obj, listType), Arrays.asList(1, 2));
    }

    @Test
    public void testTranslateFromHiveObjectWithNull()
    {
        assertEquals(translateFromHiveObject(BOOLEAN, null), null);
    }

    @Test
    public void testTranslateFromHiveObjectWithBooleanObject()
    {
        assertEquals(translateFromHiveObject(BOOLEAN, true), true);
    }

    @Test
    public void testTranslateFromHiveObjectWithDoubleObject()
    {
        assertEquals(translateFromHiveObject(DOUBLE, 1.0), 1.0);
    }

    @Test
    public void testTranslateFromHiveObjectWithLongObject()
    {
        assertEquals(translateFromHiveObject(BIGINT, 1000), 1000);
    }

    @Test
    public void testTranslateFromHiveObjectWithIntObject()
    {
        assertEquals(translateFromHiveObject(INTEGER, 1), (long) 1);
    }

    @Test
    public void testTranslateFromHiveObjectWithShortObject()
    {
        assertEquals(translateFromHiveObject(SMALLINT, (short) 1), (long) 1);
    }

    @Test
    public void testTranslateFromHiveObjectWithStringObject()
    {
        assertEquals(translateFromHiveObject(VARCHAR, "a"), Slices.utf8Slice("a"));
    }

    @Test
    public void testTranslateFromHiveObjectWithListObject()
    {
        Object res = new IntArrayBlock(2, Optional.ofNullable(new boolean[] {true, true}), new int[] {1, 2});
        assertEquals(translateFromHiveObject(new ArrayParametricType().createType(Arrays.asList("integer")), Arrays.asList(1, 2)).toString(),
                res.toString());
    }
}
