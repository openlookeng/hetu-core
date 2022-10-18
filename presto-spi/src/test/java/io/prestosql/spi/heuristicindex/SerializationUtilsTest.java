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
package io.prestosql.spi.heuristicindex;

import org.checkerframework.checker.units.qual.K;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.testng.Assert.assertEquals;

public class SerializationUtilsTest
{
    @Test
    public void testSerializeMap()
    {
        // Setup
        final Map<?, ?> input = new HashMap<>();

        // Run the test
        final String result = SerializationUtils.serializeMap(input);

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testDeserializeMap() throws Exception
    {
        // Setup
        final Function<String, K> parseKey = val -> {
            return null;
        };
        final Function<String, Object> parseValue = val -> {
            return null;
        };

        // Run the test
        SerializationUtils.deserializeMap("property", parseKey, parseValue);

        // Verify the results
    }

    @Test
    public void testSerializeStripeSymbol() throws Exception
    {
        assertEquals("result", SerializationUtils.serializeStripeSymbol("filepath", 0L, 0L));
    }

    @Test
    public void testDeserializeStripeSymbol() throws Exception
    {
        // Setup
        // Run the test
        final SerializationUtils.LookUpResult result = SerializationUtils.deserializeStripeSymbol("lookUpResult");

        // Verify the results
    }
}
