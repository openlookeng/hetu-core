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
package io.prestosql.plugin.hive.util;

import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.LongDecimalType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;

public class HivePartitionUtils
{
    private HivePartitionUtils() {}

    public static Object getPartitionValue(String partitionValue, Type type)
    {
        if (type instanceof BooleanType) {
            return new Boolean(partitionValue);
        }
        if (type instanceof IntegerType) {
            return new Integer(partitionValue);
        }
        if (type instanceof BigintType) {
            return new Long(partitionValue);
        }
        if (type instanceof SmallintType) {
            return new Short(partitionValue);
        }
        if (type instanceof TinyintType) {
            return new Byte(partitionValue);
        }
        if (type instanceof DoubleType) {
            return new Double(partitionValue);
        }
        if (type instanceof RealType) {
            return new Float(partitionValue);
        }
        if (type instanceof DecimalType || type instanceof LongDecimalType) {
            return HiveDecimal.create(partitionValue);
        }
        if (type instanceof TimestampType) {
            return Timestamp.valueOf(partitionValue);
        }
        if (type instanceof DateType) {
            return Date.valueOf(partitionValue);
        }
        return partitionValue;
    }
}
