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
package io.hetu.core.metastore.jdbc;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

/**
 * test config
 *
 * @since 2020-03-18
 */
public class TestJdbcMetastoreConfig
{
    /**
     * testDefaults
     */
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(JdbcMetastoreConfig.class)
                .setDbUrl(null)
                .setDbUser(null)
                .setDbPassword(null));
    }

    /**
     * testExplicitPropertyMappings
     */
    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hetu.metastore.db.url", "jdbc:mysql://127.0.0.1:3306/metastore")
                .put("hetu.metastore.db.user", "root")
                .put("hetu.metastore.db.password", "12456")
                .build();

        JdbcMetastoreConfig expected = new JdbcMetastoreConfig()
                .setDbUrl("jdbc:mysql://127.0.0.1:3306/metastore")
                .setDbUser("root")
                .setDbPassword("12456");

        assertFullMapping(properties, expected);
    }
}
