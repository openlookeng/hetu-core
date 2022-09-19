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
package io.prestosql.elasticsearch;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.prestosql.elasticsearch.ElasticsearchConfig.Security.PASSWORD;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestElasticsearchConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ElasticsearchConfig.class)
                .setHost(null)
                .setPort(9200)
                .setDefaultSchema("default")
                .setScrollSize(1000)
                .setScrollTimeout(new Duration(1, MINUTES))
                .setRequestTimeout(new Duration(10, SECONDS))
                .setConnectTimeout(new Duration(1, SECONDS))
                .setMaxRetryTime(new Duration(30, SECONDS))
                .setNodeRefreshInterval(new Duration(1, MINUTES))
                .setMaxHttpConnections(25)
                .setHttpThreadCount(Runtime.getRuntime().availableProcessors())
                .setTlsEnabled(false)
                .setKeystorePath(null)
                .setKeystorePassword(null)
                .setTrustStorePath(null)
                .setTruststorePassword(null)
                .setVerifyHostnames(true)
                .setSecurity(null)
                .setPushDownEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path keystoreFile = Files.createTempFile(null, null);
        Path truststoreFile = Files.createTempFile(null, null);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("elasticsearch.host", "example.com")
                .put("elasticsearch.port", "9999")
                .put("elasticsearch.default-schema-name", "test")
                .put("elasticsearch.scroll-size", "4000")
                .put("elasticsearch.scroll-timeout", "20s")
                .put("elasticsearch.request-timeout", "1s")
                .put("elasticsearch.connect-timeout", "10s")
                .put("elasticsearch.max-retry-time", "10s")
                .put("elasticsearch.node-refresh-interval", "10m")
                .put("elasticsearch.max-http-connections", "100")
                .put("elasticsearch.http-thread-count", "30")
                .put("elasticsearch.tls.enabled", "true")
                .put("elasticsearch.tls.keystore-path", keystoreFile.toString())
                .put("elasticsearch.tls.keystore-password", "keystore-password")
                .put("elasticsearch.tls.truststore-path", truststoreFile.toString())
                .put("elasticsearch.tls.truststore-password", "truststore-password")
                .put("elasticsearch.tls.verify-hostnames", "false")
                .put("elasticsearch.security", "PASSWORD")
                .put("elasticsearch.pushdown.enabled", "true")
                .build();

        ElasticsearchConfig expected = new ElasticsearchConfig()
                .setHost("example.com")
                .setPort(9999)
                .setDefaultSchema("test")
                .setScrollSize(4000)
                .setScrollTimeout(new Duration(20, SECONDS))
                .setRequestTimeout(new Duration(1, SECONDS))
                .setConnectTimeout(new Duration(10, SECONDS))
                .setMaxRetryTime(new Duration(10, SECONDS))
                .setNodeRefreshInterval(new Duration(10, MINUTES))
                .setMaxHttpConnections(100)
                .setHttpThreadCount(30)
                .setTlsEnabled(true)
                .setKeystorePath(keystoreFile.toFile())
                .setKeystorePassword("keystore-password")
                .setTrustStorePath(truststoreFile.toFile())
                .setTruststorePassword("truststore-password")
                .setVerifyHostnames(false)
                .setSecurity(PASSWORD)
                .setPushDownEnabled(true);

        assertFullMapping(properties, expected);
    }
}
