/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.hetu.core.plugin.singledata.tidrange;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static org.testng.Assert.assertEquals;

public class TestTidRangeConfig
{
    @Test
    public void testDefaultConfig()
    {
        TidRangeConfig defaultConfig = new TidRangeConfig();
        assertEquals(defaultConfig.getConnectionTimeout(), 0);
        assertEquals(defaultConfig.getMaxLifetime(), 1800000L);
        assertEquals(defaultConfig.getMaxConnectionCountPerNode(), 100);
        assertEquals(defaultConfig.getMaxTableSplitCountPerNode(), 50);
    }

    @Test
    public void testTidRangeConfig()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("tidrange.max-connection-count-per-node", "10")
                .put("tidrange.max-table-split-count-per-node", "5")
                .put("tidrange.connection-timeout", "600")
                .put("tidrange.max-lifetime", "50000")
                .build();

        TidRangeConfig expected = new TidRangeConfig()
                .setMaxConnectionCountPerNode(10)
                .setMaxTableSplitCountPerNode(5)
                .setConnectionTimeout(600L)
                .setMaxLifetime(50000L);

        assertFullMapping(properties, expected);
    }
}
