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
package io.hetu.core.plugin.vdm;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

/**
 * TestVdmConfig
 *
 * @since 2019-07-08
 */
public class TestVdmConfig
{
    /**
     * testDefaults
     */
    @Test
    public void testDefaults()
    {
        final long defaultMaximumSize = 10000L;
        assertRecordedDefaults(recordDefaults(VdmConfig.class)
                .setMetadataCacheEnabled(true)
                .setMetadataCacheTtl(new Duration(1, TimeUnit.SECONDS))
                .setMetadataCacheMaximumSize(defaultMaximumSize));
    }

    /**
     * testExplicitPropertyMappings
     */
    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("vdm.metadata-cache-enabled", "false")
                .put("vdm.metadata-cache-ttl", "2s")
                .put("vdm.metadata-cache-maximum-size", "2000").build();

        final int ttl = 2;
        final int defaultMaximumSize = 2000;
        VdmConfig expected = new VdmConfig()
                .setMetadataCacheEnabled(false)
                .setMetadataCacheTtl(new Duration(ttl, TimeUnit.SECONDS))
                .setMetadataCacheMaximumSize(defaultMaximumSize);

        assertFullMapping(properties, expected);
    }
}
