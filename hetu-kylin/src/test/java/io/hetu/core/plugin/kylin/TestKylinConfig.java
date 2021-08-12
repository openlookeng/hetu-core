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
package io.hetu.core.plugin.kylin;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import org.testng.annotations.Test;

import java.math.RoundingMode;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestKylinConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(KylinConfig.class)
                .setQueryPushDownEnabled(true)
                .setRoundingMode(null)
                .setEnablePredicatePushdownWithOrOperator(false)
                .setConnectionUrl(null)
                .setConnectionUser(null)
                .setConnectionPassword(null)
                .setUserCredentialName(null)
                .setPasswordCredentialName(null)
                .setCaseInsensitiveNameMatching(false)
                .setCaseInsensitiveNameMatchingCacheTtl(new Duration(1, MINUTES))
                .setFetchSize(0));
    }

    @Test
    public void testPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hetu.query.pushdown.enabled", "false")
                .put("number.rounding-mode", "UP")
                .put("validate-cubes", "false")
                .put("enable-predicate-pushdown-with-or-operator", "true")
                .put("connection-url", "jdbc:h2:mem:config")
                .put("connection-user", "user")
                .put("connection-password", "password")
                .put("user-credential-name", "foo")
                .put("password-credential-name", "bar")
                .put("case-insensitive-name-matching", "true")
                .put("case-insensitive-name-matching.cache-ttl", "1s")
                .put("fetch-size", "1")
                .put("datasource-users", "user")
                .put("jdbc-types-mapped-to-varchar", "varchar")
                .put("metadata.cache-ttl", "1m")
                .put("special-user-mapping", "true")
                .put("metadata.cache-missing", "true")
                .put("multiple-cnn-enabled", "true")
                .build();

        BaseJdbcConfig expected = new KylinConfig()
                .setQueryPushDownEnabled(false)
                .setRoundingMode(RoundingMode.UP)
                .setEnablePredicatePushdownWithOrOperator(true)
                .setConnectionUrl("jdbc:h2:mem:config")
                .setConnectionUser("user")
                .setConnectionPassword("password")
                .setUserCredentialName("foo")
                .setPasswordCredentialName("bar")
                .setCaseInsensitiveNameMatching(true)
                .setCaseInsensitiveNameMatchingCacheTtl(new Duration(1, SECONDS))
                .setFetchSize(1);

        assertFullMapping(properties, expected);
    }
}
