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

package io.hetu.core.cube.startree.tree;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.hetu.core.spi.cube.CubeMetadata;
import io.hetu.core.spi.cube.CubeStatement;
import io.hetu.core.spi.cube.CubeStatus;
import org.testng.annotations.Test;

import static io.hetu.core.spi.cube.aggregator.AggregationSignature.avg;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestStarTreeMetadata
{
    private final CubeMetadata metadata = new StarTreeMetadata(
            "memory.default.cube1",
            "tpch.tiny.lineitem",
            100,
            ImmutableList.of(
                    new DimensionColumn("suppkey", "suppkey"),
                    new DimensionColumn("returnflag", "returnflag"),
                    new DimensionColumn("linestatus", "linestatus"),
                    new DimensionColumn("shipdate", "shipdate"),
                    new DimensionColumn("discount", "discount"),
                    new DimensionColumn("quantity", "quantity"),
                    new AggregateColumn("sum_quantity", "sum", "quantity", false),
                    new AggregateColumn("sum_extendedprice", "sum", "extendedprice", false),
                    new AggregateColumn("count_quantity", "count", "quantity", false),
                    new AggregateColumn("count_extendprice", "count", "extendprice", false),
                    new AggregateColumn("count_discount", "count", "discount", false)),
            ImmutableList.of(ImmutableSet.of("returnflag", "linestatus")),
            null,
            1000,
            CubeStatus.READY);

    private final CubeMetadata emptyGroupMetadata = new StarTreeMetadata(
            "memory.default.empty_group_cube",
            "tpch.tiny.lineitem",
            100,
            ImmutableList.of(
                    new DimensionColumn("suppkey", "suppkey"),
                    new DimensionColumn("returnflag", "returnflag"),
                    new DimensionColumn("linestatus", "linestatus"),
                    new DimensionColumn("shipdate", "shipdate"),
                    new DimensionColumn("discount", "discount"),
                    new DimensionColumn("quantity", "quantity"),
                    new AggregateColumn("sum_quantity", "sum", "quantity", false),
                    new AggregateColumn("sum_extendedprice", "sum", "extendedprice", false),
                    new AggregateColumn("count_quantity", "count", "quantity", false),
                    new AggregateColumn("count_extendprice", "count", "extendprice", false),
                    new AggregateColumn("count_discount", "count", "discount", false)),
            ImmutableList.of(ImmutableSet.of()),
            null,
            1000,
            CubeStatus.READY);

    @Test
    public void testCubeMetadata()
    {
        assertEquals(metadata.getCubeName(), "memory.default.cube1", "incorrect name");
        assertEquals(metadata.getSourceTableName(), "tpch.tiny.lineitem", "incorrect table name");
        assertEquals(metadata.getLastUpdatedTime(), 1000, "incorrect updating time");
        assertEquals(metadata.getAggregations(), ImmutableList.of("sum_quantity",
                "sum_extendedprice", "count_quantity", "count_extendprice", "count_discount"));
        assertEquals(metadata.getGroup(), ImmutableSet.of("returnflag", "linestatus"));
        assertEquals(metadata.getDimensions(), ImmutableList.of("suppkey", "returnflag", "linestatus", "shipdate", "discount", "quantity"));
    }

    @Test
    public void testMetadataMatchesCubeStatement()
    {
        CubeStatement statement = CubeStatement.newBuilder()
                .select("returnflag", "linestatus")
                .aggregate(avg("quantity", false))
                .from("tpch.tiny.lineitem")
                .groupBy("returnflag", "linestatus")
                .build();
        assertTrue(metadata.matches(statement), "failed to match a valid cube statement");
    }

    @Test
    public void testMetadataNotMatchesCubeStatement()
    {
        CubeStatement statement = CubeStatement.newBuilder()
                .select("returnflag", "linestatus")
                .aggregate(avg("unknown", false))
                .from("tpch.tiny.lineitem")
                .groupBy("returnflag", "linestatus")
                .build();
        assertFalse(metadata.matches(statement), "failed to detect an invalid cube statement");
    }

    @Test
    public void testMatchEmptyGroupMetadata()
    {
        CubeStatement statement = CubeStatement.newBuilder()
                .select("returnflag", "linestatus")
                .aggregate(avg("quantity", false))
                .from("tpch.tiny.lineitem")
                .build();
        assertTrue(emptyGroupMetadata.matches(statement));
    }
}
