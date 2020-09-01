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
import io.prestosql.spi.type.BooleanType;
import org.testng.annotations.Test;

import java.util.Arrays;

import static io.prestosql.spi.type.StandardTypes.BOOLEAN;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.booleanTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getListTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getMapTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.intTypeInfo;
import static org.testng.Assert.assertEquals;

public class TestHetuTypeTranslator
{
    @Test
    public void testTranslateFromHiveTypeInfoWithPrimitiveType()
    {
        assertEquals(HiveTypeTranslator.translateFromHiveTypeInfo(booleanTypeInfo).toString(), BOOLEAN);
    }

    @Test
    public void testTranslateFromHiveTypeInfoWithListType()
    {
        assertEquals(HiveTypeTranslator.translateFromHiveTypeInfo(getListTypeInfo(intTypeInfo)).toString(), "array(integer)");
    }

    @Test
    public void testTranslateFromHiveTypeInfoWithMapType()
    {
        assertEquals(HiveTypeTranslator.translateFromHiveTypeInfo(getMapTypeInfo(intTypeInfo, intTypeInfo)).toString(),
                "map(integer,integer)");
    }

    @Test
    public void testTranslateToHiveTypeInfoWithPrimitiveType()
    {
        assertEquals(HiveTypeTranslator.translateToHiveTypeInfo(BooleanType.BOOLEAN), booleanTypeInfo);
    }

    @Test
    public void testTranslateToHiveTypeInfoWithListType()
    {
        assertEquals(HiveTypeTranslator.translateToHiveTypeInfo(new ArrayParametricType().createType(Arrays.asList("integer"))), getListTypeInfo(intTypeInfo));
    }

    @Test
    public void testTranslateToHiveTypeInfoWithMapType()
    {
        assertEquals(HiveTypeTranslator.translateToHiveTypeInfo(new MapParametricType().createType(Arrays.asList("integer", "integer"))),
                getMapTypeInfo(intTypeInfo, intTypeInfo));
    }
}
