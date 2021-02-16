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

import io.airlift.slice.Slice;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.type.Type;

import java.math.BigDecimal;
import java.util.Comparator;

public class TypeUtils
{
    private TypeUtils() {}

    public static Object extractSingleValue(ConstantExpression constantExpression)
    {
        Type type = constantExpression.getType();
        Object value = constantExpression.getValue();
        if (value instanceof Slice) {
            return ((Slice) value).toStringUtf8();
        }
        return value;
    }

    public static Object getNativeValue(Object object)
    {
        return object instanceof Slice ? ((Slice) object).toStringUtf8() : object;
    }

    public static String extractType(Object object)
    {
        if (object instanceof Long) {
            return "Long";
        }
        else if (object instanceof String) {
            return "String";
        }
        else if (object instanceof Integer) {
            return "Integer";
        }
        else if (object instanceof Slice) {
            return "String";
        }
        else {
            throw new UnsupportedOperationException("Not a valid type to create index: " + object.getClass());
        }
    }

    public static Comparator<kotlin.Pair<? extends Comparable<?>, ?>> getComparator(String type)
    {
        switch (type) {
            case "long":
            case "Long":
            case "Slice":
            case "String":
            case "int":
            case "Integer":
                return (o1, o2) -> ((Comparable) o1.getFirst()).compareTo(o2.getFirst());
        }

        throw new RuntimeException("Type is not supported");
    }

    /**
     * Double is 8 bytes in java. This returns the 8 bytes of the decimal value of BigInteger
     * @param value
     * @return
     */
    public static byte[] getBytes(BigDecimal value)
    {
        byte[] bytes = new byte[8];
        java.nio.ByteBuffer.wrap(bytes).putDouble(value.doubleValue());
        return bytes;
    }

    /**
     * Double is 8 bytes in java. This returns the 8 bytes of the decimal value of Double
     * @param value
     * @return
     */
    public static byte[] getBytes(Double value)
    {
        byte[] bytes = new byte[8];
        java.nio.ByteBuffer.wrap(bytes).putDouble(value.doubleValue());
        return bytes;
    }
}
