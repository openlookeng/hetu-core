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

package io.hetu.core.hive.dynamicfunctions;

import com.google.common.collect.ImmutableMap;
import io.hetu.core.common.filesystem.TempFolder;
import io.hetu.core.filesystem.HetuFileSystemClientPlugin;
import io.hetu.core.metastore.HetuMetastorePlugin;
import io.prestosql.plugin.memory.MemoryPlugin;
import io.prestosql.tests.DistributedQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDynamicHiveScalarFunction
{
    private DistributedQueryRunner queryRunner;

    @BeforeClass
    public void setUpClass()
            throws Exception
    {
        RecognizedFunctions.addRecognizedFunction(
                "io.hetu.core.hive.dynamicfunctions.examples.udf.ListStringUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.TimeOutUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.BooleanUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.MapDoubleUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.DoubleUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.EvaluateOverloadUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.ShortUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.ArrayListDoubleUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.FloatWrapperUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.LongUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.IntThreeArgsUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.IntUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.IntTwoArgsUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.IntFiveArgsUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.MapIntUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.MapStringUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.EmptyParameterUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.IntWrapperUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.IntSixArgsUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.IntFourArgsUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.BooleanWrappperUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.ListIntUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.LongWrapperUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.ByteWrapperUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.ListDoubleUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.ShortWrapperUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.ByteUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.DoubleWrapperUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.FloatUDF",
                "io.hetu.core.hive.dynamicfunctions.examples.udf.NullInputUDF");
        try {
            queryRunner = DistributedQueryRunner.builder(
                    testSessionBuilder().setCatalog("memory").setSchema("default").build())
                    .setNodeCount(1)
                    .build();
            TempFolder folder = new TempFolder();
            folder.create();
            Runtime.getRuntime().addShutdownHook(new Thread(folder::close));
            queryRunner.installPlugin(new HetuFileSystemClientPlugin()); // need dep on hetu-filesystem
            queryRunner.installPlugin(new HetuMetastorePlugin()); // need dep on hetu-metastore
            queryRunner.installPlugin(new MemoryPlugin());
            HashMap<String, String> metastoreConfig = new HashMap<>();
            metastoreConfig.put("hetu.metastore.type", "hetufilesystem");
            metastoreConfig.put("hetu.metastore.hetufilesystem.profile-name", "default");
            metastoreConfig.put("hetu.metastore.hetufilesystem.path", folder.newFolder("metastore").getAbsolutePath());
            queryRunner.getCoordinator().loadMetastore(metastoreConfig);
            queryRunner.createCatalog("memory", "memory",
                    ImmutableMap.of("memory.spill-path", folder.newFolder("memory-connector").getAbsolutePath()));

            createBooleanTable();
            createByteTable();
            createShortTable();
            createIntTable();
            createLongTable();
            createFloatTable();
            createDoubleTable();
            createListTable();
            createMapTable();
            createMultipleParameterTable();
            createEvaluateOverloadTable();
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @AfterClass
    public void tearDownClass()
    {
    }

    @Test
    public void testDynamicHiveScalarFunctionWithBooleanType()
    {
        assertEquals(
                queryRunner.execute("select booleanudf(booleanType) from table_boolean order by booleanType"),
                queryRunner.execute("select booleanType from table_boolean order by booleanType"));
    }

    private void createBooleanTable()
    {
        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "booleanudf io.hetu.core.hive.dynamicfunctions.examples.udf.BooleanUDF"))));
        queryRunner.execute("drop table if exists table_boolean");
        queryRunner.execute("create table table_boolean(booleanType boolean)");
        queryRunner.execute("insert into table_boolean(values(true))");
        queryRunner.execute("insert into table_boolean(values(false))");

        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "booleanwrapperudf io.hetu.core.hive.dynamicfunctions.examples.udf.BooleanWrappperUDF"))));
        queryRunner.execute("drop table if exists table_boolean_wrapper");
        queryRunner.execute("create table table_boolean_wrapper(booleanType boolean)");
        queryRunner.execute("insert into table_boolean_wrapper(values(true))");
        queryRunner.execute("insert into table_boolean_wrapper(values(false))");
    }

    @Test
    public void testDynamicHiveScalarFunctionWithBooleanWrapperType()
    {
        assertEquals(
                queryRunner.execute("select booleanwrapperudf(booleanType) from table_boolean_wrapper order by booleanType"),
                queryRunner.execute("select booleanType from table_boolean_wrapper order by booleanType"));
    }

    @Test
    public void testDynamicHiveScalarFunctionWithByteType()
    {
        assertEquals(
                queryRunner.execute("select byteudf(byteType) from table_byte order by byteType"),
                queryRunner.execute("select byteType from table_byte order by byteType"));
    }

    private void createByteTable()
    {
        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "byteudf io.hetu.core.hive.dynamicfunctions.examples.udf.ByteUDF"))));
        queryRunner.execute("drop table if exists table_byte");
        queryRunner.execute("create table table_byte(byteType tinyint)");
        queryRunner.execute("insert into table_byte(values(cast(0 as tinyint)))");
        queryRunner.execute("insert into table_byte(values(cast(1 as tinyint)))");

        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "bytewrapperudf io.hetu.core.hive.dynamicfunctions.examples.udf.ByteWrapperUDF"))));
        queryRunner.execute("drop table if exists table_byte_wrapper");
        queryRunner.execute("create table table_byte_wrapper(byteType tinyint)");
        queryRunner.execute("insert into table_byte_wrapper(values(cast(0 as tinyint)))");
        queryRunner.execute("insert into table_byte_wrapper(values(cast(1 as tinyint)))");
    }

    @Test
    public void testDynamicHiveScalarFunctionWithByteWrapperType()
    {
        assertEquals(
                queryRunner.execute("select bytewrapperudf(byteType) from table_byte_wrapper order by byteType"),
                queryRunner.execute("select byteType from table_byte_wrapper order by byteType"));
    }

    @Test
    public void testDynamicHiveScalarFunctionWithShortType()
    {
        assertEquals(
                queryRunner.execute("select shortudf(shortType) from table_short order by shortType"),
                queryRunner.execute("select shortType from table_short order by shortType"));
    }

    private void createShortTable()
    {
        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "shortudf io.hetu.core.hive.dynamicfunctions.examples.udf.ShortUDF"))));
        queryRunner.execute("drop table if exists table_short");
        queryRunner.execute("create table table_short(shortType smallint)");
        queryRunner.execute("insert into table_short(values(cast(0 as smallint)))");
        queryRunner.execute("insert into table_short(values(cast(1 as smallint)))");

        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "shortwrapperudf io.hetu.core.hive.dynamicfunctions.examples.udf.ShortWrapperUDF"))));
        queryRunner.execute("drop table if exists table_short_wrapper");
        queryRunner.execute("create table table_short_wrapper(shortType smallint)");
        queryRunner.execute("insert into table_short_wrapper(values(cast(0 as smallint)))");
        queryRunner.execute("insert into table_short_wrapper(values(cast(1 as smallint)))");
    }

    @Test
    public void testDynamicHiveScalarFunctionWithShortWrapperType()
    {
        assertEquals(
                queryRunner.execute("select shortwrapperudf(shortType) from table_short_wrapper order by shortType"),
                queryRunner.execute("select shortType from table_short_wrapper order by shortType"));
    }

    @Test
    public void testDynamicHiveScalarFunctionWithIntType()
    {
        assertEquals(
                queryRunner.execute("select intudf(intType) from table_int order by intType"),
                queryRunner.execute("select intType from table_int order by intType"));
    }

    private void createIntTable()
    {
        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "intudf io.hetu.core.hive.dynamicfunctions.examples.udf.IntUDF"))));
        queryRunner.execute("drop table if exists table_int");
        queryRunner.execute("create table table_int(intType int)");
        queryRunner.execute("insert into table_int(values(0))");
        queryRunner.execute("insert into table_int(values(1))");

        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "intwrapperudf io.hetu.core.hive.dynamicfunctions.examples.udf.IntWrapperUDF"))));
        queryRunner.execute("drop table if exists table_int_wrapper");
        queryRunner.execute("create table table_int_wrapper(intType int)");
        queryRunner.execute("insert into table_int_wrapper(values(0))");
        queryRunner.execute("insert into table_int_wrapper(values(1))");
    }

    @Test
    public void testDynamicHiveScalarFunctionWithIntWrapperType()
    {
        assertEquals(
                queryRunner.execute("select intwrapperudf(intType) from table_int_wrapper order by intType"),
                queryRunner.execute("select intType from table_int_wrapper order by intType"));
    }

    @Test
    public void testDynamicHiveScalarFunctionWithLongType()
    {
        assertEquals(
                queryRunner.execute("select longudf(longType) from table_long order by longType"),
                queryRunner.execute("select longType from table_long order by longType"));
    }

    private void createLongTable()
    {
        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "longudf io.hetu.core.hive.dynamicfunctions.examples.udf.LongUDF"))));
        queryRunner.execute("drop table if exists table_long");
        queryRunner.execute("create table table_long(longType bigint)");
        queryRunner.execute("insert into table_long(values(cast(1000 as bigint)))");

        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "longwrapperudf io.hetu.core.hive.dynamicfunctions.examples.udf.LongWrapperUDF"))));
        queryRunner.execute("drop table if exists table_long_wrapper");
        queryRunner.execute("create table table_long_wrapper(longType bigint)");
        queryRunner.execute("insert into table_long_wrapper(values(cast(1000 as bigint)))");
    }

    @Test
    public void testDynamicHiveScalarFunctionWithLongWrapperType()
    {
        assertEquals(
                queryRunner.execute("select longwrapperudf(longType) from table_long_wrapper order by longType"),
                queryRunner.execute("select longType from table_long_wrapper order by longType"));
    }

    @Test
    public void testDynamicHiveScalarFunctionWithFloatType()
    {
        assertEquals(
                queryRunner.execute("select floatudf(floatType) from table_float order by floatType"),
                queryRunner.execute("select floatType + 11111111 from table_float order by floatType"));
    }

    private void createFloatTable()
    {
        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "floatudf io.hetu.core.hive.dynamicfunctions.examples.udf.FloatUDF"))));
        queryRunner.execute("drop table if exists table_float");
        queryRunner.execute("create table table_float(floatType float)");
        queryRunner.execute("insert into table_float(values(cast(1.0 as float)))");

        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "floatwrapperudf io.hetu.core.hive.dynamicfunctions.examples.udf.FloatWrapperUDF"))));
        queryRunner.execute("drop table if exists table_float_wrapper");
        queryRunner.execute("create table table_float_wrapper(floatType float)");
        queryRunner.execute("insert into table_float_wrapper(values(cast(1.0 as float)))");
    }

    @Test
    public void testDynamicHiveScalarFunctionWithFloatWrapperType()
    {
        assertEquals(
                queryRunner.execute("select floatwrapperudf(floatType) from table_float_wrapper order by floatType"),
                queryRunner.execute("select floatType from table_float_wrapper order by floatType"));
    }

    @Test
    public void testDynamicHiveScalarFunctionWithDoubleType()
    {
        assertEquals(
                queryRunner.execute("select doubleudf(doubleType) from table_double order by doubleType"),
                queryRunner.execute("select doubleType from table_double order by doubleType"));
    }

    private void createDoubleTable()
    {
        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "doubleudf io.hetu.core.hive.dynamicfunctions.examples.udf.DoubleUDF"))));
        queryRunner.execute("drop table if exists table_double");
        queryRunner.execute("create table table_double(doubleType double)");
        queryRunner.execute("insert into table_double(values(cast(1.0 as double)))");

        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "doublewrapperudf io.hetu.core.hive.dynamicfunctions.examples.udf.DoubleWrapperUDF"))));
        queryRunner.execute("drop table if exists table_double_wrapper");
        queryRunner.execute("create table table_double_wrapper(doubleType double)");
        queryRunner.execute("insert into table_double_wrapper(values(cast(1.0 as double)))");
    }

    @Test
    public void testDynamicHiveScalarFunctionWithDoubleWrapperType()
    {
        assertEquals(
                queryRunner.execute("select doublewrapperudf(doubleType) from table_double_wrapper order by doubleType"),
                queryRunner.execute("select doubleType from table_double_wrapper order by doubleType"));
    }

    @Test
    public void testDynamicHiveScalarFunctionWithListDoubleType()
    {
        assertEquals(
                queryRunner.execute("select listdoubleudf(listDoubleTpye) from table_list_double"),
                queryRunner.execute("select listDoubleTpye from table_list_double"));
    }

    private void createListTable()
    {
        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "listdoubleudf io.hetu.core.hive.dynamicfunctions.examples.udf.ListDoubleUDF"))));
        queryRunner.execute("drop table if exists table_list_double");
        queryRunner.execute("create table table_list_double(listDoubleTpye array<double>)");
        queryRunner.execute("insert into table_list_double(values(array[1.0,2.01]))");

        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "arraylistdoubleudf io.hetu.core.hive.dynamicfunctions.examples.udf.ArrayListDoubleUDF"))));
        queryRunner.execute("drop table if exists table_arraylist_double");
        queryRunner.execute("create table table_arraylist_double(listDoubleTpye array<double>)");
        queryRunner.execute("insert into table_arraylist_double(values(array[1.0,2.01]))");

        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "listintudf io.hetu.core.hive.dynamicfunctions.examples.udf.ListIntUDF"))));
        queryRunner.execute("drop table if exists table_list_int");
        queryRunner.execute("create table table_list_int(listIntType array<int>)");
        queryRunner.execute("insert into table_list_int(values(array[1,2]))");

        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "liststringudf io.hetu.core.hive.dynamicfunctions.examples.udf.ListStringUDF"))));
        queryRunner.execute("drop table if exists table_list_string");
        queryRunner.execute("create table table_list_string(listStringType array<varchar>)");
        queryRunner.execute("insert into table_list_string(values(array['s','ss']))");
    }

    @Test
    public void testDynamicHiveScalarFunctionWithArrayListDoubleType()
    {
        assertEquals(
                queryRunner.execute("select arraylistdoubleudf(listDoubleTpye) from table_arraylist_double"),
                queryRunner.execute("select listDoubleTpye from table_arraylist_double"));
    }

    @Test
    public void testDynamicHiveScalarFunctionWithListIntType()
    {
        assertEquals(
                queryRunner.execute("select listintudf(listIntType) from table_list_int"),
                queryRunner.execute("select listIntType from table_list_int"));
    }

    @Test
    public void testDynamicHiveScalarFunctionWithListStringType()
    {
        assertEquals(
                queryRunner.execute("select liststringudf(listStringType) from table_list_string"),
                queryRunner.execute("select listStringType from table_list_string"));
    }

    @Test
    public void testDynamicHiveScalarFunctionWithMapDoubleType()
    {
        assertEquals(
                queryRunner.execute("select mapdoubleudf(mapDoubleType) from table_map_double"),
                queryRunner.execute("select mapDoubleType from table_map_double"));
    }

    private void createMapTable()
    {
        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "mapdoubleudf io.hetu.core.hive.dynamicfunctions.examples.udf.MapDoubleUDF"))));
        queryRunner.execute("drop table if exists table_map_double");
        queryRunner.execute("create table table_map_double(mapDoubleType map(double, double))");
        queryRunner.execute("insert into table_map_double(values(map(array[1.11,2.22], array[100.11,200.22])))");

        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "mapintudf io.hetu.core.hive.dynamicfunctions.examples.udf.MapIntUDF"))));
        queryRunner.execute("drop table if exists table_map_int");
        queryRunner.execute("create table table_map_int(mapIntType map(int, int))");
        queryRunner.execute("insert into table_map_int(values(map(array[11,22], array[111,222])))");

        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "mapstringudf io.hetu.core.hive.dynamicfunctions.examples.udf.MapStringUDF"))));
        queryRunner.execute("drop table if exists table_map_string");
        queryRunner.execute("create table table_map_string(mapStringType map(varchar, varchar))");
        queryRunner.execute("insert into table_map_string(values(map(array['1.0','2.0'], array['100.0','200.0'])))");
    }

    @Test
    public void testDynamicHiveScalarFunctionWithMapIntType()
    {
        assertEquals(
                queryRunner.execute("select mapintudf(mapIntType) from table_map_int"),
                queryRunner.execute("select mapIntType from table_map_int"));
    }

    @Test
    public void testDynamicHiveScalarFunctionWithMapStringType()
    {
        assertEquals(
                queryRunner.execute("select mapstringudf(mapStringType) from table_map_string"),
                queryRunner.execute("select mapStringType from table_map_string"));
    }

    @Test
    public void testDynamicHiveScalarFunctionWithTwoParameters()
    {
        assertEquals(
                queryRunner.execute("select inttwoargsudf(int22,int2) from table_int_two_args"),
                queryRunner.execute("select int22-int2 from table_int_two_args"));
    }

    private void createMultipleParameterTable()
    {
        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "inttwoargsudf io.hetu.core.hive.dynamicfunctions.examples.udf.IntTwoArgsUDF"))));
        queryRunner.execute("drop table if exists table_int_two_args");
        queryRunner.execute("create table table_int_two_args(int22 int,int2 int)");
        queryRunner.execute("insert into table_int_two_args(values(1,2))");

        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "intthreeargsudf io.hetu.core.hive.dynamicfunctions.examples.udf.IntThreeArgsUDF"))));
        queryRunner.execute("drop table if exists table_int_three_args");
        queryRunner.execute("create table table_int_three_args(int23 int,int2 int,int3 int)");
        queryRunner.execute("insert into table_int_three_args(values(1,2,3))");

        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "intfourargsudf io.hetu.core.hive.dynamicfunctions.examples.udf.IntFourArgsUDF"))));
        queryRunner.execute("drop table if exists table_int_four_args");
        queryRunner.execute("create table table_int_four_args(int1 int,int2 int,int3 int,int4 int)");
        queryRunner.execute("insert into table_int_four_args(values(1,2,3,4))");

        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "intfiveargsudf io.hetu.core.hive.dynamicfunctions.examples.udf.IntFiveArgsUDF"))));
        queryRunner.execute("drop table if exists table_int_five_args");
        queryRunner.execute("create table table_int_five_args(int1 int,int2 int,int3 int,int4 int,int5 int)");
        queryRunner.execute("insert into table_int_five_args(values(1,2,3,4,5))");

        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(
                new DynamicHiveScalarFunction(new FunctionMetadata(
                        "emptyparam io.hetu.core.hive.dynamicfunctions.examples.udf.EmptyParameterUDF"),
                        new Type[] {}, int.class)));
        queryRunner.execute("drop table if exists table_empty_parameters");
        queryRunner.execute("create table table_empty_parameters(val varchar)");
    }

    @Test
    public void testDynamicHiveScalarFunctionWithThreeParameters()
    {
        assertEquals(
                queryRunner.execute("select intthreeargsudf(int23,int2,int3) from table_int_three_args"),
                queryRunner.execute("select int23-int2-int3 from table_int_three_args"));
    }

    @Test
    public void testDynamicHiveScalarFunctionWithFourParameters()
    {
        assertEquals(
                queryRunner.execute("select intfourargsudf(int1,int2,int3,int4) from table_int_four_args"),
                queryRunner.execute("select int1-int2-int3-int4 from table_int_four_args"));
    }

    @Test
    public void testDynamicHiveScalarFunctionWithFiveParameters()
    {
        assertEquals(
                queryRunner.execute("select intfiveargsudf(int1,int2,int3,int4,int5) from table_int_five_args"),
                queryRunner.execute("select int1-int2-int3-int4-int5 from table_int_five_args"));
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testDynamicHiveScalarFunctionWithSixParameters()
    {
        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "intsixargsudf io.hetu.core.hive.dynamicfunctions.examples.udf.IntSixArgsUDF"))));
        queryRunner.execute("drop table if exists table_int_six_args");
        queryRunner.execute("create table table_int_six_args(int1 int,int2 int,int3 int,int4 int,int5 int,int6 int)");
        queryRunner.execute("insert into table_int_six_args(values(1,2,3,4,5,6))");
        queryRunner.execute("select intsixargsudf(int1,int2,int3,int4,int5,int6) from table_int_six_args");
    }

    @Test
    public void testDynamicHiveScalarFunctionWithEvaluateOverload()
    {
        assertEquals(
                queryRunner.execute("select evaluateoverloadudf(int1) from t27"),
                queryRunner.execute("select int1 from t27"));
        assertEquals(
                queryRunner.execute("select evaluateoverloadudf(int1,int2) from t27"),
                queryRunner.execute("select int1-int2 from t27"));
        assertEquals(
                queryRunner.execute("select evaluateoverloadudf(int1,int2,int3) from t27"),
                queryRunner.execute("select int1-int2-int3 from t27"));
        assertEquals(
                queryRunner.execute("select evaluateoverloadudf(int1,int2,int3,int4) from t27"),
                queryRunner.execute("select int1-int2-int3-int4 from t27"));
        assertEquals(
                queryRunner.execute("select evaluateoverloadudf(int1,int2,int3,int4,int5) from t27"),
                queryRunner.execute("select int1-int2-int3-int4-int5 from t27"));
        assertEquals(
                queryRunner.execute("select evaluateoverloadudf(long1,long2) from t27"),
                queryRunner.execute("select long1-long2 from t27"));
        assertEquals(
                queryRunner.execute("select evaluateoverloadudf(str1) from t27"),
                queryRunner.execute("select str1 from t27"));
        assertEquals(
                queryRunner.execute("select evaluateoverloadudf(int1,map1,list1) from t27"),
                queryRunner.execute("select int1-element_at(map1,0)-element_at(list1,1)from t27"));
    }

    private void createEvaluateOverloadTable()
    {
        FunctionMetadata funcMetadata = new FunctionMetadata(
                "evaluateoverloadudf io.hetu.core.hive.dynamicfunctions.examples.udf.EvaluateOverloadUDF");
        ParameterizedType mapType = new ParameterizedType()
        {
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
        ParameterizedType listType = new ParameterizedType()
        {
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
        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(
                new DynamicHiveScalarFunction(funcMetadata, new Type[] {int.class}, int.class),
                new DynamicHiveScalarFunction(funcMetadata, new Type[] {int.class, int.class}, int.class),
                new DynamicHiveScalarFunction(funcMetadata, new Type[] {int.class, int.class, int.class}, int.class),
                new DynamicHiveScalarFunction(funcMetadata, new Type[] {int.class, int.class, int.class, int.class}, int.class),
                new DynamicHiveScalarFunction(funcMetadata, new Type[] {int.class, int.class, int.class, int.class, int.class}, int.class),
                new DynamicHiveScalarFunction(funcMetadata, new Type[] {long.class, long.class}, long.class),
                new DynamicHiveScalarFunction(funcMetadata, new Type[] {String.class}, String.class),
                new DynamicHiveScalarFunction(funcMetadata, new Type[] {Integer.class, mapType, listType}, Integer.class)));
        queryRunner.execute("drop table if exists t27");
        queryRunner.execute("create table t27(int1 int,int2 int,int3 int,int4 int,int5 int,long1 bigint,long2 bigint,str1 varchar,map1 map(int, int),list1 array<int>)");
        queryRunner.execute("insert into t27(values(1,2,3,4,5,cast(4 as bigint),cast(5 as bigint),'6',map(array[0,1], array[0,1]),array[1]))");
    }

    @Test
    public void testDynamicHiveScalarFunctionWithEmptyParameters()
    {
        assertEquals(
                queryRunner.execute("select emptyparam() from table_empty_parameters"),
                queryRunner.execute("select -1 from table_empty_parameters"));
    }

    @Test
    public void testEqualsTrue()
    {
        FunctionMetadata funcMetadata = new FunctionMetadata(
                "evaluateoverloadudf io.hetu.core.hive.dynamicfunctions.examples.udf.EvaluateOverloadUDF");
        DynamicHiveScalarFunction func1 = new DynamicHiveScalarFunction(funcMetadata, new Type[] {int.class}, int.class);
        DynamicHiveScalarFunction func2 = new DynamicHiveScalarFunction(funcMetadata, new Type[] {int.class}, int.class);
        assertTrue(func1.equals(func2));
    }

    @Test
    public void testEqualsFalse()
    {
        FunctionMetadata funcMetadata = new FunctionMetadata(
                "evaluateoverloadudf io.hetu.core.hive.dynamicfunctions.examples.udf.EvaluateOverloadUDF");
        DynamicHiveScalarFunction func1 = new DynamicHiveScalarFunction(funcMetadata, new Type[] {int.class}, int.class);
        DynamicHiveScalarFunction func2 = new DynamicHiveScalarFunction(funcMetadata, new Type[] {int.class, int.class}, int.class);
        assertFalse(func1.equals(func2));
    }

    @Test
    public void testHashCode()
    {
        FunctionMetadata funcMetadata = new FunctionMetadata(
                "evaluateoverloadudf io.hetu.core.hive.dynamicfunctions.examples.udf.EvaluateOverloadUDF");
        DynamicHiveScalarFunction func = new DynamicHiveScalarFunction(funcMetadata, new Type[] {int.class}, int.class);
        assertEquals(func.hashCode(), func.getSignature().hashCode());
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testMaxFunctionRunningTimeLimit()
    {
        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "timeout io.hetu.core.hive.dynamicfunctions.examples.udf.TimeOutUDF"), 1)));
        queryRunner.execute("select timeout(1)");
    }

    @Test
    public void testNullInputUDF()
    {
        queryRunner.getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(Arrays.asList(new DynamicHiveScalarFunction(new FunctionMetadata(
                "evaluateNullInputUDF io.hetu.core.hive.dynamicfunctions.examples.udf.NullInputUDF"), 1)));
        assertEquals(
                queryRunner.execute("select evaluateNullInputUDF('we are ', 1)"),
                queryRunner.execute("select 7"));
        assertEquals(
                queryRunner.execute("select evaluateNullInputUDF('we are ', 2)"),
                queryRunner.execute("select 6"));
        assertEquals(
                queryRunner.execute("select evaluateNullInputUDF(' we are ', 2)"),
                queryRunner.execute("select 6"));
        assertEquals(
                queryRunner.execute("select evaluateNullInputUDF('', null)"),
                queryRunner.execute("select -1"));
    }
}
