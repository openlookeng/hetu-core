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

package io.hetu.core.hive.dynamicfunctions;

import io.hetu.core.hive.dynamicfunctions.examples.udf.IsEmptyUDF;
import io.prestosql.spi.PrestoException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestFunctionMetadata
{
    @BeforeClass
    public void setUpRecognizedFunctions()
    {
        RecognizedFunctions.addRecognizedFunction(
                "io.hetu.core.hive.dynamicfunctions.examples.udf.MapStringUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.IsEmptyUDF1",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.ListStringUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.IsEmptyUDF");
    }

    @Test
    public void testGetFunctionNameWithOneSpace()
    {
        FunctionMetadata funcMetadata = new FunctionMetadata(
                "isempty io.hetu.core.hive.dynamicfunctions.examples.udf.IsEmptyUDF");
        assertEquals("isempty", funcMetadata.getFunctionName());
    }

    @Test
    public void testGetFunctionNameWithMultiSpaces()
    {
        FunctionMetadata funcMetadata = new FunctionMetadata(
                "isempty     io.hetu.core.hive.dynamicfunctions.examples.udf.IsEmptyUDF");
        assertEquals("isempty", funcMetadata.getFunctionName());
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testGetFunctionNameWithException()
    {
        FunctionMetadata funcMetadata = new FunctionMetadata(
                "isemptyio.hetu.core.hive.dynamicfunctions.examples.udf.IsEmptyUDF");
        assertEquals(funcMetadata.getFunctionName(), "IsEmptyUDF");
    }

    @Test
    public void testGetClassNameWithOneSpace()
    {
        FunctionMetadata funcMetadata = new FunctionMetadata(
                "isempty io.hetu.core.hive.dynamicfunctions.examples.udf.IsEmptyUDF");
        assertEquals("io.hetu.core.hive.dynamicfunctions.examples.udf.IsEmptyUDF",
                funcMetadata.getClassName());
    }

    @Test
    public void testGetClassNameWithMultiSpaces()
    {
        FunctionMetadata funcMetadata = new FunctionMetadata(
                "isempty     io.hetu.core.hive.dynamicfunctions.examples.udf.IsEmptyUDF");
        assertEquals("io.hetu.core.hive.dynamicfunctions.examples.udf.IsEmptyUDF",
                funcMetadata.getClassName());
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testGetClassNameWithException()
    {
        FunctionMetadata funcMetadata = new FunctionMetadata(
                "isemptyio.hetu.core.hive.dynamicfunctions.examples.udf.IsEmptyUDF");
        assertEquals(funcMetadata.getClassName(), "isemptyio.hetu.core.hive.dynamicfunctions.examples.udf");
    }

    @Test
    public void testGetClazz()
    {
        FunctionMetadata funcMetadata = new FunctionMetadata(
                "isempty io.hetu.core.hive.dynamicfunctions.examples.udf.IsEmptyUDF");
        assertEquals(IsEmptyUDF.class, funcMetadata.getClazz());
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testGetClazzWithExcepction()
    {
        new FunctionMetadata(
                "isempty io.hetu.core.hive.dynamicfunctions.examples.udf.IsEmptyUDF1");
    }

    @Test
    public void testGetInstance()
    {
        FunctionMetadata funcMetadata = new FunctionMetadata(
                "isempty io.hetu.core.hive.dynamicfunctions.examples.udf.IsEmptyUDF");
        assertTrue(funcMetadata.getInstance() instanceof IsEmptyUDF);
    }

    @Test
    public void testGetMethodByName()
    {
        FunctionMetadata funcMetadata = new FunctionMetadata(
                "isempty io.hetu.core.hive.dynamicfunctions.examples.udf.IsEmptyUDF");
        assertTrue(funcMetadata.getMethodByName().containsKey("evaluate"));
    }

    @Test
    public void testGetMethod()
    {
        FunctionMetadata funcMetadata = new FunctionMetadata(
                "isempty io.hetu.core.hive.dynamicfunctions.examples.udf.IsEmptyUDF");
        assertEquals("evaluate", funcMetadata.getMethod("evaluate").getName());
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testGetMethodWithException()
    {
        FunctionMetadata funcMetadata = new FunctionMetadata(
                "isempty io.hetu.core.hive.dynamicfunctions.examples.udf.IsEmptyUDF");
        funcMetadata.getMethod("evaluate1");
    }

    @Test
    public void testGetGenericParameterTypePrimitiveJava()
    {
        FunctionMetadata funcMetadata = new FunctionMetadata(
                "isempty io.hetu.core.hive.dynamicfunctions.examples.udf.IsEmptyUDF");
        assertEquals(String.class, funcMetadata.getGenericParameterTypes("evaluate")[0]);
    }

    @Test
    public void testGetGenericParameterTypeList()
    {
        FunctionMetadata funcMetadata = new FunctionMetadata(
                "isempty io.hetu.core.hive.dynamicfunctions.examples.udf.ListStringUDF");
        Type type = funcMetadata.getGenericParameterTypes("evaluate")[0];
        assertTrue(type instanceof ParameterizedType);
        Class<?> elementType = (Class<?>) ((ParameterizedType) type).getActualTypeArguments()[0];
        assertEquals(String.class, elementType);
    }

    @Test
    public void testGetGenericParameterTypeMap()
    {
        FunctionMetadata funcMetadata = new FunctionMetadata(
                "isempty io.hetu.core.hive.dynamicfunctions.examples.udf.MapStringUDF");
        Type type = funcMetadata.getGenericParameterTypes("evaluate")[0];
        assertTrue(type instanceof ParameterizedType);
        Class<?> keyType = (Class<?>) ((ParameterizedType) type).getActualTypeArguments()[0];
        Class<?> valueType = (Class<?>) ((ParameterizedType) type).getActualTypeArguments()[1];
        assertEquals(String.class, keyType);
        assertEquals(String.class, valueType);
    }

    @Test
    public void testGetGenericReturnTypePrimitiveJava()
    {
        FunctionMetadata funcMetadata = new FunctionMetadata(
                "isempty io.hetu.core.hive.dynamicfunctions.examples.udf.IsEmptyUDF");
        assertEquals(Boolean.class, funcMetadata.getGenericReturnType("evaluate"));
    }

    @Test
    public void testGetGenericReturnTypeList()
    {
        FunctionMetadata funcMetadata = new FunctionMetadata(
                "isempty io.hetu.core.hive.dynamicfunctions.examples.udf.ListStringUDF");
        Type type = funcMetadata.getGenericReturnType("evaluate");
        assertTrue(type instanceof ParameterizedType);
        Class<?> elementType = (Class<?>) ((ParameterizedType) type).getActualTypeArguments()[0];
        assertEquals(String.class, elementType);
    }

    @Test
    public void testGetGenericReturnTypeMap()
    {
        FunctionMetadata funcMetadata = new FunctionMetadata(
                "isempty io.hetu.core.hive.dynamicfunctions.examples.udf.MapStringUDF");
        Type type = funcMetadata.getGenericReturnType("evaluate");
        assertTrue(type instanceof ParameterizedType);
        Class<?> keyType = (Class<?>) ((ParameterizedType) type).getActualTypeArguments()[0];
        Class<?> valueType = (Class<?>) ((ParameterizedType) type).getActualTypeArguments()[1];
        assertEquals(String.class, keyType);
        assertEquals(String.class, valueType);
    }
}
