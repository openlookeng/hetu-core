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

import io.prestosql.plugin.hive.HiveVacuumTableHandle.Range;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;

public class TestHiveVacuumTableHandle
{
    List<Range> ranges = new ArrayList<>();

    @Test
    public void testAddRange()
    {
        ranges.clear();
        addRange(new Range(22, 22));
        addRange(new Range(22, 23));
        assertEquals(ranges.size(), 1);
        addRange(new Range(24, 26));
        addRange(new Range(25, 26));
        assertEquals(ranges.size(), 2);
        addRange(new Range(25, 27));
        assertEquals(ranges.size(), 2);
        addRange(new Range(21, 27));
        assertEquals(ranges.size(), 1);
    }

    @Test
    public void testAddRange2()
    {
        ranges.clear();
        addRange(new Range(2, 2));
        addRange(new Range(3, 11));
        assertEquals(ranges.size(), 2);
        addRange(new Range(1, 10));
        assertEquals(ranges.size(), 1);
        assertEquals(getOnlyElement(ranges), new Range(1, 11));
    }

    @Test
    public void testAddRange3()
    {
        ranges.clear();
        addRange(new Range(5, 10));
        addRange(new Range(1, 4));
        addRange(new Range(2, 5));
        assertEquals(ranges.size(), 1);
        addRange(new Range(1, 7));
        assertEquals(ranges.size(), 1);
        assertEquals(getOnlyElement(ranges), new Range(1, 10));
    }

    void addRange(Range range)
    {
        HiveVacuumTableHandle.addRange(range, ranges);
    }

    @Test
    public void testDeleteDeltaStatement()
    {
        assertEquals(BackgroundHiveSplitLoader.getStatementId("delete_delta_000001_000002_0000").getAsInt(), 0);
        assertEquals(BackgroundHiveSplitLoader.getStatementId("delete_delta_000001_000002_0001").getAsInt(), 1);
        assertEquals(BackgroundHiveSplitLoader.getStatementId("delete_delta_000001_000002").getAsInt(), -1);
    }
}
