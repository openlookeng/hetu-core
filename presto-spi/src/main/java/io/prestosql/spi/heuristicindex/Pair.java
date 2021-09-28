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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Pair<K, V>
{
    private final K first;
    private final V second;

    @JsonCreator
    public Pair(@JsonProperty("first") K first, @JsonProperty("second") V second)
    {
        this.first = first;
        this.second = second;
    }

    @JsonProperty
    public K getFirst()
    {
        return this.first;
    }

    @JsonProperty
    public V getSecond()
    {
        return this.second;
    }
}
