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
package io.hetu.core.plugin.hbase.utils;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.SortedRangeSet;
import io.prestosql.spi.predicate.ValueSet;

import java.util.ArrayList;
import java.util.List;

import static io.prestosql.spi.predicate.Range.equal;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

/**
 * ValueSetUtils
 *
 * @since 2020-03-20
 */
public class ValueSetUtils
{
    private ValueSetUtils() {}

    /**
     * createValueSet
     */
    public static ValueSet createValueSet(int num)
    {
        List<Range> rangeList = new ArrayList<>();
        if (num == 1) {
            return SortedRangeSet.copyOf(
                    VARCHAR, ImmutableList.of(equal(VARCHAR, TestSliceUtils.createSlice("rowkey"))));
        }
        else if (num == 2) {
            rangeList.add(
                    Range.range(
                            VARCHAR,
                            TestSliceUtils.createSlice("rowkey1"),
                            false,
                            TestSliceUtils.createSlice("rowkey2"),
                            false));
        }
        else if (num == 3) {
            rangeList.add(equal(VARCHAR, TestSliceUtils.createSlice("rowkey1")));
            rangeList.add(equal(VARCHAR, TestSliceUtils.createSlice("rowkey2")));
        }
        else {
            rangeList.add(
                    Range.range(
                            VARCHAR,
                            TestSliceUtils.createSlice("rowkey1"),
                            true,
                            TestSliceUtils.createSlice("rowkey2"),
                            true));
        }
        return SortedRangeSet.copyOf(VARCHAR, rangeList);
    }
}
