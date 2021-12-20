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
package io.prestosql.server;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestServerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(ServerConfig.class)
                .setCoordinator(true)
                .setAdmins(null)
                .setPrestoVersion(null)
                .setIncludeExceptionInResponse(true)
                .setGracePeriod(new Duration(2, MINUTES))
                .setEnhancedErrorReporting(true)
                .setHttpClientIdleTimeout(new Duration(30, SECONDS))
                .setHttpClientRequestTimeout(new Duration(10, SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("coordinator", "false")
                .put("openlookeng.admins", "user1,user2")
                .put("presto.version", "test")
                .put("http.include-exception-in-response", "false")
                .put("shutdown.grace-period", "5m")
                .put("sql.parser.enhanced-error-reporting", "false")
                .put("http.client.idle-timeout", "5h")
                .put("http.client.request-timeout", "30m")
                .build();

        ServerConfig expected = new ServerConfig()
                .setCoordinator(false)
                .setAdmins("user1,user2")
                .setPrestoVersion("test")
                .setIncludeExceptionInResponse(false)
                .setGracePeriod(new Duration(5, MINUTES))
                .setEnhancedErrorReporting(false)
                .setHttpClientIdleTimeout(new Duration(5, HOURS))
                .setHttpClientRequestTimeout(new Duration(30, MINUTES));

        assertFullMapping(properties, expected);
    }
}
