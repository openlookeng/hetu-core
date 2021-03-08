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

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.type.SqlDate;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Global constants used in hetu-heuristic-index
 */
public class IndexConstants
{
    /**
     * The lastModified file prefix.
     * lastModified file is created under the same directory of the index file so that the coordinator knows
     * whether the index file is out of date.
     */
    public static final String LAST_MODIFIED_FILE_PREFIX = "lastModified=";

    // The canonical names of Java type classes that the supported Presto types link to
    public static final String[] TYPES_WHITELIST = ImmutableList.of(
            Number.class.getCanonicalName(),
            Byte.class.getCanonicalName(),
            Short.class.getCanonicalName(),
            Integer.class.getCanonicalName(),
            Long.class.getCanonicalName(),
            Float.class.getCanonicalName(),
            Double.class.getCanonicalName(),
            Character.class.getCanonicalName(),
            String.class.getCanonicalName(),
            Boolean.class.getCanonicalName(),
            SqlDate.class.getCanonicalName(),
            BigDecimal.class.getCanonicalName(),
            BigInteger.class.getCanonicalName(),
            byte[].class.getName()) // name [B but not canonical name byte will be used when deserializing
            .toArray(new String[0]);

    private IndexConstants()
    {
    }
}
