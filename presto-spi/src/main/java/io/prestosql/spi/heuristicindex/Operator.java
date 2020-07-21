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

package io.prestosql.spi.heuristicindex;

import java.util.Locale;

/**
 * Enum class containing the query operators supported by the abstract Indexer
 * Note: Specific implementation of indexer will not support all of the listed operators
 * ie, BloomFilter only supports = , it does not support <, >, =<, =>, <>
 *
 * @since 2019-11-21
 */
public enum Operator
{
    /**
     * Equal Operator
     */
    EQUAL("="),
    /**
     * Not equal operator
     */
    NOT_EQUAL("<>"),
    /**
     * Less than operator
     */
    LESS_THAN("<"),
    /**
     * Less than or equal operator
     */
    LESS_THAN_OR_EQUAL("<="),
    /**
     * Greater than operator
     */
    GREATER_THAN(">"),
    /**
     * Greater than or equal to operator
     */
    GREATER_THAN_OR_EQUAL(">=");

    private final String value;

    Operator(String value)
    {
        this.value = value;
    }

    public String getValue()
    {
        return value;
    }

    /**
     * Turns string operator (ie, '>') into an Operator Enum
     *
     * @param value operator in string format
     * @return Operator corresponding to that string
     * @throws IllegalArgumentException If string does not match any operator
     */
    public static Operator fromValue(String value)
    {
        for (Operator o : Operator.values()) {
            if (o.getValue().equalsIgnoreCase(value)) {
                return o;
            }
        }

        throw new IllegalArgumentException(String.format(Locale.ENGLISH, "No Operator found with value %s.", value));
    }
}
