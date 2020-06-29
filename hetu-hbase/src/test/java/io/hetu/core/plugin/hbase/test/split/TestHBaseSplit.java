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
package io.hetu.core.plugin.hbase.test.split;

import io.hetu.core.plugin.hbase.client.TestUtils;
import io.hetu.core.plugin.hbase.split.HBaseSplit;
import io.prestosql.spi.HostAddress;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;

import static org.testng.Assert.assertEquals;

/**
 * TestHBaseSplit
 *
 * @since 2020-03-20
 */
public class TestHBaseSplit
{
    /**
     * testJsonSplit
     */
    @Test
    public void testJsonSplit()
    {
        HBaseSplit split =
                new HBaseSplit(
                        "rowKey",
                        TestUtils.createHBaseTableHandle(),
                        new ArrayList<HostAddress>(1),
                        "startrow",
                        "endrow",
                        new HashMap<>(),
                        null,
                        true);

        assertEquals(0, split.getRanges().size());
        assertEquals(TestUtils.createHBaseTableHandle(), split.getTableHandle());
        String className = "io.hetu.core.plugin.hbase.split.HBaseSplit";
        assertEquals(className, split.getInfo().toString().substring(0, className.length()));
        assertEquals(true, split.isRemotelyAccessible());
        assertEquals("rowKey", split.getRowKeyName());
        assertEquals("endrow", split.getEndRow());
        assertEquals(true, split.isRandomSplit());
        assertEquals(null, split.getRegionInfo());
        assertEquals(0, split.getAddresses().size());
    }
}
