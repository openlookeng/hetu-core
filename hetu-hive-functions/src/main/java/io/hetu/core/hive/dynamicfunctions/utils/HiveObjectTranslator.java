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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.MapBlockBuilder;
import io.prestosql.spi.block.SingleMapBlock;
import io.prestosql.spi.block.SingleMapBlockWriter;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.hetu.core.hive.dynamicfunctions.utils.HiveTypeTranslator.translateFromHivePrimitiveTypeInfo;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;

public class HiveObjectTranslator
{
    private HiveObjectTranslator()
    {
    }

    /**
     * Translate a Hetu object to a Hive object with ObjectInspector
     *
     * @param type : Hetu type of the object
     * @param obj: a Hetu object
     * @param javaType: Java type of the object
     * @return a Hive object
     */
    public static Object translateToHiveObject(Type type, Object obj, java.lang.reflect.Type javaType)
    {
        if (obj == null) {
            return null;
        }
        return translateToHiveObject(getInspector(type), obj, javaType);
    }

    private static Object translateToHiveObject(ObjectInspector inspector, Object obj, java.lang.reflect.Type javaType)
    {
        if (inspector instanceof PrimitiveObjectInspector) {
            return translateToPrimitiveHiveObject(inspector, obj);
        }
        if (inspector instanceof ListObjectInspector) {
            return translateToListHiveObject(inspector, obj, javaType);
        }
        if (inspector instanceof MapObjectInspector) {
            return translateToMapHiveObject(inspector, obj, javaType);
        }
        throw new PrestoException(NOT_SUPPORTED, String.format("Unsupported Hive type: %s", inspector.getTypeName()));
    }

    private static Object translateToPrimitiveHiveObject(ObjectInspector inspector, Object obj)
    {
        if (inspector instanceof StringObjectInspector) {
            return ((Slice) obj).toStringUtf8();
        }
        else if (inspector instanceof ByteObjectInspector) {
            return ((Long) obj).byteValue();
        }
        else if (inspector instanceof ShortObjectInspector) {
            return ((Long) obj).shortValue();
        }
        else if (inspector instanceof IntObjectInspector) {
            return ((Long) obj).intValue();
        }
        else if (inspector instanceof FloatObjectInspector) {
            int value = ((Long) obj).intValue();
            return intBitsToFloat(value);
        }
        else if (inspector instanceof BinaryObjectInspector) {
            return ((Slice) obj).toByteBuffer();
        }
        return obj;
    }

    private static Object translateToListHiveObject(ObjectInspector inspector, Object obj, java.lang.reflect.Type type)
    {
        if (!(type instanceof ParameterizedType)) {
            throw new PrestoException(NOT_SUPPORTED, format("Type of %s is not ParameterizedType", obj));
        }
        ObjectInspector eleInspector = ((ListObjectInspector) inspector).getListElementObjectInspector();
        Block block = (Block) obj;
        int positionCount = block.getPositionCount();
        List<Object> result = new ArrayList<>(positionCount);
        for (int i = 0; i < positionCount; i++) {
            result.add(
                    translateToHiveObject(eleInspector, readFromBlock(block, i, eleInspector),
                            ((ParameterizedType) type).getActualTypeArguments()[0]));
        }
        return result;
    }

    private static Object translateToMapHiveObject(ObjectInspector inspector, Object obj, java.lang.reflect.Type type)
    {
        if (!(type instanceof ParameterizedType)) {
            throw new PrestoException(NOT_SUPPORTED, format("Type of Object %s is not ParameterizedType", obj));
        }
        ObjectInspector keyInspector = ((MapObjectInspector) inspector).getMapKeyObjectInspector();
        ObjectInspector valueInspector = ((MapObjectInspector) inspector).getMapValueObjectInspector();
        SingleMapBlock block = (SingleMapBlock) obj;
        int positionCount = block.getPositionCount();
        Map<Object, Object> result = new HashMap<>(positionCount);
        for (int i = 0; i < positionCount; i = i + 2) {
            result.put(
                    translateToHiveObject(keyInspector, readFromBlock(block, i, keyInspector),
                            ((ParameterizedType) type).getActualTypeArguments()[0]),
                    translateToHiveObject(valueInspector, readFromBlock(block, i + 1, valueInspector),
                            ((ParameterizedType) type).getActualTypeArguments()[1]));
        }
        return result;
    }

    private static Object readFromBlock(Block block, int position, ObjectInspector inspector)
    {
        if (inspector instanceof StringObjectInspector) {
            return block.getSlice(position, 0, block.getSliceLength(position));
        }
        if (inspector instanceof IntObjectInspector) {
            return (long) block.getInt(position, 0);
        }
        if (inspector instanceof LongObjectInspector) {
            return block.getLong(position, 0);
        }
        if (inspector instanceof ByteObjectInspector) {
            return (long) block.getByte(position, 0);
        }
        if (inspector instanceof ShortObjectInspector) {
            return (long) block.getShort(position, 0);
        }
        if (inspector instanceof DoubleObjectInspector) {
            return TypeUtils.readNativeValue(io.prestosql.spi.type.DoubleType.DOUBLE, block, position);
        }
        throw new PrestoException(NOT_SUPPORTED,
                String.format("Unsupported Hive type: %s", inspector.getTypeName()));
    }

    /**
     * Translate a Hive object to a Hetu object with ObjectInspector
     *
     * @param type: Object type
     * @param obj: a Hive object
     * @return a Hetu object
     */
    public static Object translateFromHiveObject(Type type, Object obj)
    {
        if (obj == null) {
            return null;
        }
        ObjectInspector inspector = getInspector(type);
        if (inspector instanceof PrimitiveObjectInspector) {
            return translateFromPrimitiveHiveObject(inspector, obj);
        }
        if (inspector instanceof ListObjectInspector) {
            return translateFromListHiveObject(inspector, obj);
        }
        if (inspector instanceof MapObjectInspector) {
            return translateFromMapHiveObject(inspector, obj, type);
        }
        throw new PrestoException(NOT_SUPPORTED,
                String.format("Unsupported Hive Type: %s", inspector.getTypeName()));
    }

    private static Object translateFromPrimitiveHiveObject(ObjectInspector inspector, Object obj)
    {
        if (inspector instanceof BooleanObjectInspector ||
                inspector instanceof LongObjectInspector ||
                inspector instanceof DoubleObjectInspector) {
            return ((PrimitiveObjectInspector) inspector).getPrimitiveJavaObject(obj);
        }
        else if (inspector instanceof IntObjectInspector) {
            return (long) ((IntObjectInspector) inspector).get(obj);
        }
        else if (inspector instanceof ShortObjectInspector) {
            return (long) ((ShortObjectInspector) inspector).get(obj);
        }
        else if (inspector instanceof FloatObjectInspector) {
            float value = ((FloatObjectInspector) inspector).get(obj);
            int intBits = Float.floatToIntBits(value);
            return Long.valueOf(intBits);
        }
        else if (inspector instanceof ByteObjectInspector) {
            return (long) ((ByteObjectInspector) inspector).get(obj);
        }
        else if (inspector instanceof StringObjectInspector) {
            return Slices.utf8Slice(((StringObjectInspector) inspector).getPrimitiveJavaObject(obj));
        }
        else if (inspector instanceof BinaryObjectInspector) {
            return Slices.wrappedBuffer(((BinaryObjectInspector) inspector).getPrimitiveJavaObject(obj));
        }
        throw new PrestoException(NOT_SUPPORTED,
                String.format("Unsupported Hive Type: %s.", inspector.getTypeName()));
    }

    private static Object translateFromListHiveObject(ObjectInspector inspector, Object obj)
    {
        ListObjectInspector listInspector = (ListObjectInspector) inspector;
        List<?> list = listInspector.getList(obj);
        ObjectInspector elementObjectInspector = listInspector.getListElementObjectInspector();
        Type elementType = translateFromHivePrimitiveTypeInfo(
                ((AbstractPrimitiveJavaObjectInspector) elementObjectInspector).getTypeInfo());
        int positionCount = list.size();
        BlockBuilder resultBuilder = elementType.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            writeToBlock(resultBuilder, list.get(position));
        }
        return resultBuilder.build();
    }

    private static Object translateFromMapHiveObject(ObjectInspector inspector, Object obj, Type type)
    {
        MapObjectInspector mapInspector = (MapObjectInspector) inspector;
        BlockBuilder mapBlockBuilder = type.createBlockBuilder(null, mapInspector.getMapSize(obj));
        SingleMapBlockWriter mapBlockWriter = ((MapBlockBuilder) mapBlockBuilder).beginBlockEntry();
        for (Map.Entry<?, ?> entry : mapInspector.getMap(obj).entrySet()) {
            writeToBlock(mapBlockWriter, entry.getKey());
            writeToBlock(mapBlockWriter, entry.getValue());
        }
        mapBlockBuilder.closeEntry();
        return mapBlockBuilder.build().getObject(0, Block.class);
    }

    private static void writeToBlock(BlockBuilder builder, Object obj)
    {
        if (obj instanceof String) {
            String str = (String) obj;
            builder.writeBytes(Slices.utf8Slice(str), 0, str.length()).closeEntry();
        }
        else if (obj instanceof Integer) {
            builder.writeInt((int) obj).closeEntry();
        }
        else if (obj instanceof Long) {
            builder.writeLong((long) obj).closeEntry();
        }
        else if (obj instanceof Double) {
            TypeUtils.writeNativeValue(io.prestosql.spi.type.DoubleType.DOUBLE, builder, obj);
        }
    }

    private static ObjectInspector getInspector(Type type)
    {
        return TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(HiveTypeTranslator.translateToHiveTypeInfo(type));
    }
}
