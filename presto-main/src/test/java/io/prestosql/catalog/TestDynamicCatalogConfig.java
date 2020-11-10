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

package io.prestosql.catalog;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestDynamicCatalogConfig
{
    @Test
    public void testDefaultConfig()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(DynamicCatalogConfig.class)
                .setDynamicCatalogEnabled(false)
                .setCatalogConfigurationDir(null)
                .setCatalogShareConfigurationDir(null)
                .setCatalogScannerInterval(Duration.valueOf("5s"))
                .setCatalogMaxFileSize(new DataSize(128, KILOBYTE))
                .setCatalogMaxFileNumber(10)
                .setCatalogValidFileSuffixes(null)
                .setShareFileSystemProfile("hdfs-config-default"));
    }

    @Test
    public void testExplicitPropertiesMap()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("catalog.dynamic-enabled", "true")
                .put("catalog.local.config-dir", "/etc/catalog")
                .put("catalog.share.config-dir", "hdfs://etc/catalog")
                .put("catalog.scanner-interval", "5m")
                .put("catalog.max-file-size", "5MB")
                .put("catalog.max-file-number", "1")
                .put("catalog.valid-file-suffixes", "jks,xml,keystore")
                .put("catalog.share.filesystem.profile", "local-config-default")
                .build();

        DynamicCatalogConfig expected = new DynamicCatalogConfig()
                .setDynamicCatalogEnabled(true)
                .setCatalogConfigurationDir("/etc/catalog")
                .setCatalogShareConfigurationDir("hdfs://etc/catalog")
                .setCatalogScannerInterval(Duration.valueOf("5m"))
                .setCatalogMaxFileSize(new DataSize(5, MEGABYTE))
                .setCatalogMaxFileNumber(1)
                .setCatalogValidFileSuffixes("jks,xml,keystore")
                .setShareFileSystemProfile("local-config-default");

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
