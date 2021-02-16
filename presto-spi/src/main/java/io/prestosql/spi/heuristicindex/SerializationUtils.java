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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class SerializationUtils
{
    public static final String DELIMITER = ":::";

    private SerializationUtils()
    {
    }

    public static String serializeMap(Map<?, ?> input)
    {
        StringBuilder stringBuilder = new StringBuilder();
        input.forEach((key, value) -> {
            stringBuilder.append(key);
            stringBuilder.append("->");
            stringBuilder.append(value);
            stringBuilder.append(",");
        });
        return stringBuilder.toString();
    }

    /**
     * Deserialize a property to map, converting the keys and values to K, V type respectively according to the parsing functions
     */
    public static <K, V> Map<K, V> deserializeMap(String property, Function<String, K> parseKey, Function<String, V> parseValue)
    {
        String[] keyValPairs = property.split(",");
        Map<K, V> result = new HashMap<>();
        for (String pair : keyValPairs) {
            if (pair.contains("->")) {
                String[] keyVal = pair.split("->");
                result.put(parseKey.apply(keyVal[0]), parseValue.apply(keyVal[1]));
            }
        }
        return result;
    }

    public static String serializeStripeSymbol(String filepath, long stripeStart, long stripeEnd)
    {
        return filepath + DELIMITER + stripeStart + DELIMITER + stripeEnd;
    }

    public static LookUpResult deserializeStripeSymbol(String lookUpResult)
    {
        String[] splitResult = lookUpResult.split(DELIMITER);
        return new LookUpResult(splitResult[0],
                Long.parseLong(splitResult[1]),
                Long.parseLong(splitResult[2]));
    }

    public static class LookUpResult
    {
        public final String filepath;
        public final Pair<Long, Long> stripe;

        public LookUpResult(String filepath, long stripeStart, long stripeEnd)
        {
            this.filepath = filepath;
            this.stripe = new Pair<>(stripeStart, stripeEnd);
        }
    }
}
