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
package io.prestosql.configmanager;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;

public class TestDefaultUdfRewriteConfigSupplier
{
    @Test
    public void testDefaultUdfRewriteConfigSupplier()
    {
        Map<String, String> propertiesMap = new ImmutableMap.Builder<String, String>()
                .put("k1", "v1")
                .put("k2", "v2")
                .build();
        DefaultUdfRewriteConfigSupplier defaultUdfRewriteConfigSupplier = new DefaultUdfRewriteConfigSupplier(propertiesMap);
        Map<String, String> map = defaultUdfRewriteConfigSupplier.getConfigKeyValueMap();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            assertEquals(propertiesMap.get(key), value);
        }

        assertEquals(defaultUdfRewriteConfigSupplier.getConfigValue("k1").get(), "v1");
        assertEquals(defaultUdfRewriteConfigSupplier.getConfigValue("k2").get(), "v2");
    }
}
