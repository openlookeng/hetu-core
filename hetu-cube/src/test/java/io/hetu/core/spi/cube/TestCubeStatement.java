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

package io.hetu.core.spi.cube;

import io.hetu.core.spi.cube.aggregator.AggregationSignature;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestCubeStatement
{
    @Test
    public void testCubeStatement()
    {
        CubeStatement statement = CubeStatement.newBuilder()
                .select("name", "address", "nationkey")
                .aggregate(AggregationSignature.count())
                .from("tpch.tiny.customer")
                .groupByAddString("address")
                .groupByAddStringList("name", "nationkey")
                .build();

        assertEquals(statement.getFrom(), "tpch.tiny.customer", "incorrect from table");
        assertEquals(statement.getSelection(), new HashSet<>(Arrays.asList("name", "address", "nationkey")), "incorrect selection");
        assertEquals(statement.getGroupBy(), new HashSet<>(Arrays.asList("name", "address", "nationkey")), "incorrect address");
        assertEquals(statement.getAggregations(), Collections.singletonList(AggregationSignature.count()), "incorrect aggregations");
    }

    @Test
    public void testCubeEquality()
    {
        CubeStatement statement1 = CubeStatement.newBuilder()
                .select("name", "address", "nationkey")
                .aggregate(AggregationSignature.count())
                .from("tpch.tiny.customer")
                .build();

        CubeStatement statement2 = CubeStatement.newBuilder()
                .select("name", "address", "nationkey")
                .aggregate(AggregationSignature.count())
                .from("tpch.tiny.customer")
                .build();

        CubeStatement statement3 = CubeStatement.newBuilder()
                .select("name", "address")
                .aggregate(AggregationSignature.count())
                .from("tpch.tiny.customer")
                .build();

        assertEquals(statement1, statement2, "statements are not equal");
        assertNotEquals(statement1, statement3, "statements are not equal");
    }
}
