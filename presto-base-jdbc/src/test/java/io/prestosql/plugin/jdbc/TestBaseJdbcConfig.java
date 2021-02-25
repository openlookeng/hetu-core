/*
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
package io.prestosql.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.prestosql.plugin.jdbc.optimization.JdbcPushDownModule.BASE_PUSHDOWN;
import static io.prestosql.plugin.jdbc.optimization.JdbcPushDownModule.DEFAULT;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestBaseJdbcConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(BaseJdbcConfig.class)
                .setConnectionUrl(null)
                .setConnectionUser(null)
                .setConnectionPassword(null)
                .setUserCredentialName(null)
                .setPasswordCredentialName(null)
                .setCaseInsensitiveNameMatching(false)
                .setFetchSize(0)
                .setUseConnectionPool(false)
                .setBlockWhenExhausted(false)
                .setFairness(false)
                .setJmxEnabled(true)
                .setLifo(true)
                .setTestWhileIdle(true)
                .setTestOnReturn(false)
                .setTestOnBorrow(true)
                .setTestOnCreate(false)
                .setSoftMinEvictableIdleTimeMillis(1000L * 60L * 30L)
                .setBlockWhenExhausted(true)
                .setMaxIdle(30)
                .setMaxTotal(50)
                .setMinIdle(10)
                .setNumTestsPerEvictionRun(3)
                .setTimeBetweenEvictionRunsMillis(-1L)
                .setMaxWaitMillis(-1L)
                .setCaseInsensitiveNameMatchingCacheTtl(new Duration(1, MINUTES))
                .setPushDownEnable(true)
                .setPushDownModule(DEFAULT));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("connection-url", "jdbc:h2:mem:config")
                .put("connection-user", "user")
                .put("connection-password", "password")
                .put("user-credential-name", "foo")
                .put("password-credential-name", "bar")
                .put("case-insensitive-name-matching", "true")
                .put("case-insensitive-name-matching.cache-ttl", "1s")
                .put("fetch-size", "1000")
                .put("jdbc.connection.pool.lifo", "false")
                .put("jdbc.connection.pool.fairness", "true")
                .put("jdbc.connection.pool.maxWaitMillis", "1000")
                .put("jdbc.connection.pool.softMinEvictableIdleTimeMillis", "1000")
                .put("jdbc.connection.pool.numTestsPerEvictionRun", "100")
                .put("jdbc.connection.pool.testOnCreate", "true")
                .put("jdbc.connection.pool.testOnBorrow", "false")
                .put("jdbc.connection.pool.testOnReturn", "true")
                .put("jdbc.connection.pool.testWhileIdle", "false")
                .put("jdbc.connection.pool.timeBetweenEvictionRunsMillis", "1000")
                .put("jdbc.connection.pool.blockWhenExhausted", "false")
                .put("jdbc.connection.pool.jmxEnabled", "false")
                .put("jdbc.connection.pool.maxTotal", "200")
                .put("jdbc.connection.pool.maxIdle", "20")
                .put("jdbc.connection.pool.minIdle", "12")
                .put("jdbc.pushdown-enabled", "false")
                .put("use-connection-pool", "true")
                .put("jdbc.pushdown-module", "BASE_PUSHDOWN")
                .build();

        BaseJdbcConfig expected = new BaseJdbcConfig()
                .setConnectionUrl("jdbc:h2:mem:config")
                .setConnectionUser("user")
                .setConnectionPassword("password")
                .setUserCredentialName("foo")
                .setPasswordCredentialName("bar")
                .setCaseInsensitiveNameMatching(true)
                .setFetchSize(1000)
                .setUseConnectionPool(true)
                .setBlockWhenExhausted(false)
                .setFairness(true)
                .setJmxEnabled(false)
                .setLifo(false)
                .setTestWhileIdle(false)
                .setTestOnReturn(true)
                .setTestOnBorrow(false)
                .setTestOnCreate(true)
                .setSoftMinEvictableIdleTimeMillis(1000)
                .setMaxIdle(20)
                .setMaxTotal(200)
                .setMinIdle(12)
                .setNumTestsPerEvictionRun(100)
                .setTimeBetweenEvictionRunsMillis(1000)
                .setMaxWaitMillis(1000)
                .setCaseInsensitiveNameMatchingCacheTtl(new Duration(1, SECONDS))
                .setPushDownEnable(false)
                .setPushDownModule(BASE_PUSHDOWN);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
