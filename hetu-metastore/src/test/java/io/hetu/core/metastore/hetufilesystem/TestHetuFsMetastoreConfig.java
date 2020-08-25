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

package io.hetu.core.metastore.hetufilesystem;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestHetuFsMetastoreConfig
{
    @Test
    public void testDefaultConfig()
    {
        assertRecordedDefaults(recordDefaults(HetuFsMetastoreConfig.class)
                .setHetuFileSystemMetastorePath(null)
                .setHetuFileSystemMetastoreProfileName(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hetu.metastore.hetufilesystem.path", "/etc/hetu/metastore")
                .put("hetu.metastore.hetufilesystem.profile-name", "default")
                .build();
        HetuFsMetastoreConfig config = new HetuFsMetastoreConfig()
                .setHetuFileSystemMetastorePath("/etc/hetu/metastore")
                .setHetuFileSystemMetastoreProfileName("default");
        assertFullMapping(properties, config);
    }
}
