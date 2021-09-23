/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.hetu.core.plugin.datacenter;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.plugin.jdbc.optimization.JdbcPushDownModule;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestDataCenterConfig
{
    private static final Duration READ_TIMEOUT = new Duration(30, TimeUnit.SECONDS);

    private static final Duration CONNECT_TIMEOUT = new Duration(30, TimeUnit.SECONDS);

    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(DataCenterConfig.class)
                .setConnectionUrl(null)
                .setConnectionUser(System.getProperty("user.name"))
                .setRemoteClusterId(null)
                .setConnectionPassword(null)
                .setSocksProxy(null)
                .setHttpProxy(null)
                .setApplicationNamePrefix("hetu-dc")
                .setAccessToken(null)
                .setSsl(false)
                .setSslKeyStorePath(null)
                .setSslKeyStorePassword(null)
                .setSslTrustStorePath(null)
                .setSslTrustStorePassword(null)
                .setKerberosConfigPath(null)
                .setKerberosCredentialCachePath(null)
                .setKerberosKeytabPath(null)
                .setKerberosPrincipal(null)
                .setKerberosRemoteServiceName(null)
                .setKerberosServicePrincipalPattern("${SERVICE}@${HOST}")
                .setKerberosUseCanonicalHostname(false)
                .setExtraCredentials(null)
                .setQueryPushDownEnabled(true)
                .setQueryPushDownModule(JdbcPushDownModule.DEFAULT)
                .setHttpRequestReadTimeout(READ_TIMEOUT)
                .setHttpRequestConnectTimeout(CONNECT_TIMEOUT)
                .setClientTimeout(new Duration(10, TimeUnit.MINUTES))
                .setRemoteHttpServerMaxRequestHeaderSize(DataSize.valueOf("4kB"))
                .setMetadataCacheTtl(new Duration(1, TimeUnit.SECONDS))
                .setMetadataCacheMaximumSize(10000)
                .setMetadataCacheEnabled(true)
                .setCompressionEnabled(false)
                .setMaxAnticipatedDelay(new Duration(10, TimeUnit.MINUTES))
                .setUpdateThreshold(new Duration(1, TimeUnit.MINUTES))
                .setMaxIdleConnections(20));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>().put("connection-url",
                "http://127.0.0.1:9002")
                .put("connection-user", "test")
                .put("dc.remote.cluster.id", "remote.cluster.id")
                .put("connection-password", "paxxx")
                .put("dc.accesstoken", "accesstoken")
                .put("dc.application.name.prefix", "prefix")
                .put("dc.extra.credentials", "extra.credentials")
                .put("dc.httpproxy", "httpproxy")
                .put("dc.kerberos.config.path", "kerberos.config.path")
                .put("dc.kerberos.credential.cachepath", "kerberos.credential.cachepath")
                .put("dc.kerberos.keytab.path", "kerberos.keytab.path")
                .put("dc.kerberos.principal", "kerberos.principal")
                .put("dc.kerberos.remote.service.name", "kerberos.remote.service.name")
                .put("dc.kerberos.service.principal.pattern", "kerberos.service.principal.pattern")
                .put("dc.kerberos.use.canonical.hostname", "true")
                .put("dc.socksproxy", "socksproxy")
                .put("dc.ssl", "true")
                .put("dc.ssl.keystore.password", "ssl.keystore.password")
                .put("dc.ssl.keystore.path", "ssl.keystore.path")
                .put("dc.ssl.truststore.password", "ssl.truststore.password")
                .put("dc.ssl.truststore.path", "ssl.truststore.path")
                .put("dc.query.pushdown.enabled", "false")
                .put("dc.query.pushdown.module", "FULL_PUSHDOWN")
                .put("dc.http-request-readTimeout", "5m")
                .put("dc.http-request-connectTimeout", "5m")
                .put("dc.http-client-timeout", "5m")
                .put("dc.remote-http-server.max-request-header-size", "32kB")
                .put("dc.metadata.cache.ttl", "2s")
                .put("dc.metadata.cache.maximum.size", "20000")
                .put("dc.metadata.cache.enabled", "false")
                .put("dc.http-compression", "true")
                .put("dc.max.anticipated.delay", "5s")
                .put("hetu.dc.catalog.update.threshold", "2m")
                .put("dc.httpclient.maximum.idle.connections", "10")
                .build();

        DataCenterConfig expected = new DataCenterConfig().setConnectionUrl(URI.create("http://127.0.0.1:9002"))
                .setConnectionUser("test")
                .setRemoteClusterId("remote.cluster.id")
                .setConnectionPassword("paxxx")
                .setSocksProxy("socksproxy")
                .setHttpProxy("httpproxy")
                .setApplicationNamePrefix("prefix")
                .setAccessToken("accesstoken")
                .setSsl(true)
                .setSslKeyStorePath("ssl.keystore.path")
                .setSslKeyStorePassword("ssl.keystore.password")
                .setSslTrustStorePath("ssl.truststore.path")
                .setSslTrustStorePassword("ssl.truststore.password")
                .setKerberosConfigPath("kerberos.config.path")
                .setKerberosCredentialCachePath("kerberos.credential.cachepath")
                .setKerberosKeytabPath("kerberos.keytab.path")
                .setKerberosPrincipal("kerberos.principal")
                .setKerberosRemoteServiceName("kerberos.remote.service.name")
                .setKerberosServicePrincipalPattern("kerberos.service.principal.pattern")
                .setKerberosUseCanonicalHostname(true)
                .setExtraCredentials("extra.credentials")
                .setQueryPushDownEnabled(false)
                .setQueryPushDownModule(JdbcPushDownModule.FULL_PUSHDOWN)
                .setHttpRequestReadTimeout(new Duration(5, TimeUnit.MINUTES))
                .setHttpRequestConnectTimeout(new Duration(5, TimeUnit.MINUTES))
                .setClientTimeout(new Duration(5, TimeUnit.MINUTES))
                .setRemoteHttpServerMaxRequestHeaderSize(DataSize.valueOf("32kB"))
                .setMetadataCacheTtl(new Duration(2, TimeUnit.SECONDS))
                .setMetadataCacheMaximumSize(20000)
                .setMetadataCacheEnabled(false)
                .setCompressionEnabled(true)
                .setMaxAnticipatedDelay(new Duration(5, TimeUnit.SECONDS))
                .setUpdateThreshold(new Duration(2, TimeUnit.MINUTES))
                .setMaxIdleConnections(10);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
