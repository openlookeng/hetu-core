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

package io.prestosql;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import io.prestosql.vacuum.AutoVacuumConfig;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertTrue;

public class TestAutoVacuumConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(AutoVacuumConfig.class).setAutoVacuumEnabled(false)
                .setVacuumScanInterval(Duration.valueOf("10m")).setVacuumScanThreads(3));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("auto-vacuum.enabled", "true")
                .put("auto-vacuum.scan.interval", "15m")
                .put("auto-vacuum.scan.threads", "0").build();
        AutoVacuumConfig expected = new AutoVacuumConfig();

        try {
            newInstance(expected.getClass(), properties);
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("auto-vacuum.scan.threads: must be greater than or equal to 1"));
        }

        properties = new ImmutableMap.Builder<String, String>()
                .put("auto-vacuum.enabled", "true")
                .put("auto-vacuum.scan.interval", "14s")
                .put("auto-vacuum.scan.threads", "1").build();
        try {
            newInstance(expected.getClass(), properties);
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Invalid configuration property auto-vacuum.scan.interval"));
        }
    }

    private static <T> T newInstance(Class<T> configClass, Map<String, String> properties)
    {
        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        return configurationFactory.build(configClass);
    }
}
