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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.LongArrayBlockBuilder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.dynamicfilter.BloomFilterDynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.util.BloomFilter;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HivePageSource.filter;
import static io.prestosql.plugin.hive.HiveType.HIVE_INT;
import static io.prestosql.spi.type.StandardTypes.INTEGER;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static org.testng.Assert.assertEquals;

public class TestHivePageSource
{
    @DataProvider(name = "data")
    public static Object[][] primeNumbers()
    {
        return new Object[][] {
                {0, 10000, 10, "Both columns filter same rows"},
                {0, 10010, 0, "Both columns filter different rows"},
                {0, 10005, 5, "Both column filter partial of same rows"},
                {1024, 0, 0, "Both columns filter all rows"}};
    }

    @Test(dataProvider = "data")
    public void testFilterRows(int columnOffset1, int columnOffset2, int expectedPositionCount, String message)
    {
        final Type[] types = new Type[] {BigintType.BIGINT, BigintType.BIGINT};
        final int numValues = 1024;
        BlockBuilder builder = new LongArrayBlockBuilder(null, numValues);
        for (int i = 0; i < numValues; i++) {
            builder.writeLong(i);
        }
        Block dayBlock = builder.build();
        builder = new LongArrayBlockBuilder(null, numValues);
        for (int i = 0; i < numValues; i++) {
            builder.writeLong(10000 + i);
        }
        Block appBlock = builder.build();

        Page page = new Page(dayBlock, appBlock);

        Map<ColumnHandle, DynamicFilter> dynamicFilters = new HashMap<>();
        ColumnHandle dayColumn = new HiveColumnHandle("pt_d", HIVE_INT, parseTypeSignature(INTEGER), 0, REGULAR, Optional.empty());
        ColumnHandle appColumn = new HiveColumnHandle("app_d", HIVE_INT, parseTypeSignature(INTEGER), 1, REGULAR, Optional.empty());

        BloomFilter dayFilter = new BloomFilter(1024 * 1024, 0.01);
        BloomFilter appFilter = new BloomFilter(1024 * 1024, 0.01);

        for (int i = 0; i < 10; i++) {
            dayFilter.add(columnOffset1 + i);
            appFilter.add(columnOffset2 + i);
        }
        dynamicFilters.put(dayColumn, new BloomFilterDynamicFilter("1", dayColumn, dayFilter, DynamicFilter.Type.GLOBAL));
        dynamicFilters.put(appColumn, new BloomFilterDynamicFilter("2", appColumn, appFilter, DynamicFilter.Type.GLOBAL));

        Map<Integer, ColumnHandle> eligibleColumns = ImmutableMap.of(0, dayColumn, 1, appColumn);

        Page filteredPage = filter(dynamicFilters, page, eligibleColumns, types);

        assertEquals(filteredPage.getPositionCount(), expectedPositionCount, message);
    }
}
