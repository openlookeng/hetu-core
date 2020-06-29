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

package io.hetu.core.hive.dynamicfunctions.type;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.annotation.UsedByGeneratedCode;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.util.Reflection;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.hetu.core.hive.dynamicfunctions.utils.HetuTypeUtil.getType;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.TypeUtils.NULL_HASH_CODE;
import static java.lang.String.format;

public class MapParametricType
        implements ParametricType
{
    private Type keyType;
    private Type valueType;

    @Override
    public String getName()
    {
        return StandardTypes.MAP;
    }

    @Override
    public Type createType(List<String> params)
    {
        checkArgument(params.size() == 2, "Expected two parameters, got %s", params);
        this.keyType = getType(params.get(0));
        this.valueType = getType(params.get(1));
        try {
            MethodHandle keyNativeEquals = MethodHandles.lookup().findVirtual(MapParametricType.class, "equal",
                    MethodType.methodType(void.class));
            MethodHandle keyNativeHashCode = Reflection.methodHandle(MapParametricType.class, "hash", Block.class,
                    int.class).bindTo(this);
            return new MapType(
                    keyType,
                    valueType,
                    keyNativeEquals,
                    keyNativeEquals,
                    keyNativeHashCode,
                    keyNativeHashCode);
        }
        catch (NoSuchMethodException | IllegalAccessException e) {
            throw new PrestoException(NOT_SUPPORTED, format("Map type cannot be supported with exception %s", e));
        }
    }

    @UsedByGeneratedCode
    public void equal()
    {}

    @UsedByGeneratedCode
    public long hash(Block block, int position)
    {
        long result = 0;
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            result += hashPosition(keyType, block, i);
        }
        return result;
    }

    private long hashPosition(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return NULL_HASH_CODE;
        }
        return type.hash(block, position);
    }
}
