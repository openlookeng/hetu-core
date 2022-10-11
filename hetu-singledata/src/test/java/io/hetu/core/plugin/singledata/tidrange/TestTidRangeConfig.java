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
import io.airlift.units.DataSize;
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
        assertEquals(defaultConfig.getMaxSplitCount(), 100);
        assertEquals(defaultConfig.getPageSize(), new DataSize(8, DataSize.Unit.KILOBYTE));
        assertEquals(defaultConfig.getDefaultSplitSize(), new DataSize(32, DataSize.Unit.MEGABYTE));
    }

    @Test
    public void testTidRangeConfig()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("tidrange.max-split-count", "50")
                .put("tidrange.page-size", "10kB")
                .put("tidrange.default-split-size", "50MB")
                .build();

        TidRangeConfig expected = new TidRangeConfig()
                .setMaxSplitCount(50)
                .setPageSize(new DataSize(10, DataSize.Unit.KILOBYTE))
                .setDefaultSplitSize(new DataSize(50, DataSize.Unit.MEGABYTE));

        assertFullMapping(properties, expected);
    }
}
