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

package io.hetu.core.plugin.datacenter;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.plugin.jdbc.optimization.JdbcPushDownModule;
import io.prestosql.spi.function.Mandatory;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * Data center config.
 *
 * @since 2020-02-11
 */
public class DataCenterConfig
{
    private static final int DEFAULT_HTTP_REQUEST_CONNECT_TIMEOUT_SECONDS = 30;

    private static final int DEFAULT_HTTP_REQUET_READ_TIMEOUT_SECONDS = 30;

    private static final int DEFAULT_CLIENT_TIMEOUT_MINUTES = 10;

    private static final int DEFAULT_METADATA_CACHE_MAX_SIZE = 10000;

    private static final int DEFAULT_MAX_ANTICIPATED_DELAY_MINUTES = 10;

    private static final int DEFAULT_MAX_IDLE_CONNECTIONS = 20;

    private URI connectionUrl;

    private String connectionUser = System.getProperty("user.name");

    private String connectionPassword;

    private String remoteClusterId;

    private String socksProxy;

    private String httpProxy;

    private String applicationNamePrefix = "hetu-dc";

    private String accessToken;

    private boolean isSsl;

    private String sslKeyStorePath;

    private String sslKeyStorePassword;

    private String sslTrustStorePath;

    private String sslTrustStorePassword;

    private String kerberosConfigPath;

    private String kerberosCredentialCachePath;

    private String kerberosKeytabPath;

    private String kerberosPrincipal;

    private String kerberosRemoteServiceName;

    private String kerberosServicePrincipalPattern = "${SERVICE}@${HOST}";

    private boolean isKerberosUseCanonicalHostname;

    private String extraCredentials;

    private Duration httpRequestConnectTimeout = new Duration(DEFAULT_HTTP_REQUEST_CONNECT_TIMEOUT_SECONDS,
            TimeUnit.SECONDS);

    private Duration httpRequestReadTimeout = new Duration(DEFAULT_HTTP_REQUET_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    private Duration clientTimeout = new Duration(DEFAULT_CLIENT_TIMEOUT_MINUTES, TimeUnit.MINUTES);

    /**
     * The Default value for the remote Header is 4kB
     */
    private DataSize remoteHeaderSize = DataSize.valueOf("4kB");

    private boolean isQueryPushDownEnabled = true;

    private JdbcPushDownModule queryPushDownModule = JdbcPushDownModule.DEFAULT;

    private Duration metadataCacheTtl = new Duration(1, TimeUnit.SECONDS); // DataCenter metadata cache eviction time

    private long metadataCacheMaximumSize = DEFAULT_METADATA_CACHE_MAX_SIZE; // DataCenter metadata cache max size

    private boolean isMetadataCacheEnabled = true; // DataCenter enable metadata caching

    private boolean isCompressionEnabled; // close the compression function.

    /**
     * Maximum delay expected between two requests for the same query in the cluster
     */
    private Duration maxAnticipatedDelay = new Duration(DEFAULT_MAX_ANTICIPATED_DELAY_MINUTES, TimeUnit.MINUTES);

    private int maxIdleConnections = DEFAULT_MAX_IDLE_CONNECTIONS;

    private Duration updateThreshold = new Duration(1, TimeUnit.MINUTES);

    @NotNull
    public URI getConnectionUrl()
    {
        return connectionUrl;
    }

    public Duration getUpdateThreshold()
    {
        return updateThreshold;
    }

    @Config("hetu.dc.catalog.update.threshold")
    public DataCenterConfig setUpdateThreshold(Duration updateThreshold)
    {
        this.updateThreshold = updateThreshold;
        return this;
    }

    /**
     * Set connection url, it is remote data center url.
     *
     * @param connectionUrl the connection url of data center.
     * @return DataCenterConfig object.
     */
    @Mandatory(name = "connection-url",
            description = "The connection URL of remote OpenLooKeng data center",
            defaultValue = "http://host:port",
            required = true)
    @Config("connection-url")
    public DataCenterConfig setConnectionUrl(URI connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    /**
     * Get the http request read timeout.
     *
     * @return http request read timeout.
     */
    public Duration getHttpRequestReadTimeout()
    {
        return httpRequestReadTimeout;
    }

    /**
     * Set http request read timeout.
     *
     * @param httpRequestReadTimeout the http request read timeout.
     * @return DataCenterConfig object.
     */
    @Config("dc.http-request-readTimeout")
    @ConfigDescription("http request read timeout, default value is 30s")
    public DataCenterConfig setHttpRequestReadTimeout(Duration httpRequestReadTimeout)
    {
        this.httpRequestReadTimeout = httpRequestReadTimeout;
        return this;
    }

    /**
     * Get the http request connection timeout.
     *
     * @return request connection timeout.
     */
    public Duration getHttpRequestConnectTimeout()
    {
        return httpRequestConnectTimeout;
    }

    /**
     * Set http request connection timeout.
     *
     * @param httpRequestConnectTimeout the request connection timeout.
     * @return DataCenterConfig object.
     */
    @Config("dc.http-request-connectTimeout")
    @ConfigDescription("http request connect timeout, default value is 30s")
    public DataCenterConfig setHttpRequestConnectTimeout(Duration httpRequestConnectTimeout)
    {
        this.httpRequestConnectTimeout = httpRequestConnectTimeout;
        return this;
    }

    /**
     * Get the data center client timeout.
     *
     * @return data center client timeout.
     */
    public Duration getClientTimeout()
    {
        return clientTimeout;
    }

    /**
     * Set data center client timeout.
     *
     * @param clientTimeout the data center client timeout.
     * @return DataCenterConfig object.
     */
    @Config("dc.http-client-timeout")
    @ConfigDescription("Time until the client keeps retrying to fetch the data, default value is 10min")
    public DataCenterConfig setClientTimeout(Duration clientTimeout)
    {
        this.clientTimeout = clientTimeout;
        return this;
    }

    /**
     * Get the connection user name.
     *
     * @return connection user name.
     */
    @Nullable
    public String getConnectionUser()
    {
        return connectionUser;
    }

    /**
     * Set connection user name.
     *
     * @param connectionUser the connection user name.
     * @return DataCenterConfig object.
     */
    @Config("connection-user")
    public DataCenterConfig setConnectionUser(String connectionUser)
    {
        this.connectionUser = connectionUser;
        return this;
    }

    /**
     * Get the connection password.
     *
     * @return connection password.
     */
    @Nullable
    public String getConnectionPassword()
    {
        return connectionPassword;
    }

    /**
     * Set connection password.
     *
     * @param connectionPassword the connection password.
     * @return DataCenterConfig object.
     */
    @Config("connection-password")
    @ConfigSecuritySensitive
    public DataCenterConfig setConnectionPassword(String connectionPassword)
    {
        this.connectionPassword = connectionPassword;
        return this;
    }

    /**
     * Get the remote cluster id the dcc want to connect.
     *
     * @return remote cluster id.
     */
    @Nullable
    public String getRemoteClusterId()
    {
        return remoteClusterId;
    }

    /**
     * Set the remote cluster id the dcc want to connect, when remote has multi clusters
     * We need a cluster ID to identify the cluster we want to connect to.
     *
     * @param tenant the connection user name.
     * @return DataCenterConfig object.
     */
    @Config("dc.remote.cluster.id")
    public DataCenterConfig setRemoteClusterId(String tenant)
    {
        this.remoteClusterId = tenant;
        return this;
    }

    /**
     * Get the sockets proxy.
     *
     * @return sockets proxy.
     */
    public String getSocksProxy()
    {
        return socksProxy;
    }

    /**
     * Set socksProxy
     *
     * @param socksProxy socksProxy from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.socksproxy")
    @ConfigDescription("SOCKS proxy host and port. Example: localhost:1080")
    public DataCenterConfig setSocksProxy(String socksProxy)
    {
        this.socksProxy = socksProxy;
        return this;
    }

    /**
     * Get http proxy.
     *
     * @return http proxy.
     */
    public String getHttpProxy()
    {
        return httpProxy;
    }

    /**
     * Set http proxy
     *
     * @param httpProxy httpProxy from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.httpproxy")
    @ConfigDescription("HTTP proxy host and port. Example: localhost:8888")
    public DataCenterConfig setHttpProxy(String httpProxy)
    {
        this.httpProxy = httpProxy;
        return this;
    }

    /**
     * Get application name prefix.
     *
     * @return application name prefix.
     */
    public String getApplicationNamePrefix()
    {
        return applicationNamePrefix;
    }

    /**
     * Set application name prefix
     *
     * @param applicationNamePrefix applicationNamePrefix from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.application.name.prefix")
    @ConfigDescription(
            "Prefix to append to any specified ApplicationName client info property, which is used to Set source name "
                    + "for the Hetu query. If neither this property nor ApplicationName are set, the source for "
                    + "the query will be hetu-dc")
    public DataCenterConfig setApplicationNamePrefix(String applicationNamePrefix)
    {
        this.applicationNamePrefix = applicationNamePrefix;
        return this;
    }

    /**
     * Get access token.
     *
     * @return access token.
     */
    public String getAccessToken()
    {
        return accessToken;
    }

    /**
     * Set accessToken
     *
     * @param accessToken accessToken from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.accesstoken")
    @ConfigDescription("Access token for token based authentication")
    public DataCenterConfig setAccessToken(String accessToken)
    {
        this.accessToken = accessToken;
        return this;
    }

    /**
     * Is ssl enable.
     *
     * @return is ssl enable.
     */
    public boolean isSsl()
    {
        return isSsl;
    }

    /**
     * Set ssl
     *
     * @param isSslParameter isSslParameter from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.ssl")
    @ConfigDescription("Use HTTPS for connections")
    public DataCenterConfig setSsl(boolean isSslParameter)
    {
        this.isSsl = isSslParameter;
        return this;
    }

    /**
     * Get ssl key store path.
     *
     * @return ssl key store path
     */
    public String getSslKeyStorePath()
    {
        return sslKeyStorePath;
    }

    /**
     * Set sslKeyStorePath
     *
     * @param sslKeyStorePath sslKeyStorePath from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.ssl.keystore.path")
    @ConfigDescription("The location of the Java KeyStore file that contains the certificate and private key "
            + "to use for authentication")
    public DataCenterConfig setSslKeyStorePath(String sslKeyStorePath)
    {
        this.sslKeyStorePath = sslKeyStorePath;
        return this;
    }

    public String getSslKeyStorePassword()
    {
        return sslKeyStorePassword;
    }

    /**
     * set sslKeyStorePassword
     *
     * @param sslKeyStorePassword sslKeyStorePassword from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.ssl.keystore.password")
    @ConfigSecuritySensitive
    public DataCenterConfig setSslKeyStorePassword(String sslKeyStorePassword)
    {
        this.sslKeyStorePassword = sslKeyStorePassword;
        return this;
    }

    public String getSslTrustStorePath()
    {
        return sslTrustStorePath;
    }

    /**
     * set sslTrustStorePath
     *
     * @param sslTrustStorePath sslTrustStorePath from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.ssl.truststore.path")
    @ConfigDescription(
            "The location of the Java TrustStore file that will be used to validate HTTPS server certificates")
    public DataCenterConfig setSslTrustStorePath(String sslTrustStorePath)
    {
        this.sslTrustStorePath = sslTrustStorePath;
        return this;
    }

    public String getSslTrustStorePassword()
    {
        return sslTrustStorePassword;
    }

    /**
     * set sslTrustStorePassword
     *
     * @param sslTrustStorePassword sslTrustStorePassword from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.ssl.truststore.password")
    @ConfigSecuritySensitive
    public DataCenterConfig setSslTrustStorePassword(String sslTrustStorePassword)
    {
        this.sslTrustStorePassword = sslTrustStorePassword;
        return this;
    }

    public String getKerberosConfigPath()
    {
        return kerberosConfigPath;
    }

    /**
     * set kerberosConfigPath
     *
     * @param kerberosConfigPath kerberosConfigPath from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.kerberos.config.path")
    @ConfigDescription("Kerberos configuration file")
    public DataCenterConfig setKerberosConfigPath(String kerberosConfigPath)
    {
        this.kerberosConfigPath = kerberosConfigPath;
        return this;
    }

    public String getKerberosCredentialCachePath()
    {
        return kerberosCredentialCachePath;
    }

    /**
     * set kerberosCredentialCachePath
     *
     * @param kerberosCredentialCachePath kerberosCredentialCachePath from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.kerberos.credential.cachepath")
    @ConfigDescription("Kerberos credential cache")
    public DataCenterConfig setKerberosCredentialCachePath(String kerberosCredentialCachePath)
    {
        this.kerberosCredentialCachePath = kerberosCredentialCachePath;
        return this;
    }

    public String getKerberosKeytabPath()
    {
        return kerberosKeytabPath;
    }

    /**
     * set kerberosKeytabPath
     *
     * @param kerberosKeytabPath kerberosKeytabPath from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.kerberos.keytab.path")
    @ConfigDescription("Kerberos keytab file")
    public DataCenterConfig setKerberosKeytabPath(String kerberosKeytabPath)
    {
        this.kerberosKeytabPath = kerberosKeytabPath;
        return this;
    }

    public String getKerberosPrincipal()
    {
        return kerberosPrincipal;
    }

    /**
     * set kerberosPrincipal
     *
     * @param kerberosPrincipal kerberosPrincipal from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.kerberos.principal")
    @ConfigDescription("The principal to use when authenticating to the Hetu coordinator")
    public DataCenterConfig setKerberosPrincipal(String kerberosPrincipal)
    {
        this.kerberosPrincipal = kerberosPrincipal;
        return this;
    }

    public String getKerberosRemoteServiceName()
    {
        return kerberosRemoteServiceName;
    }

    /**
     * set kerberosRemoteServiceName
     *
     * @param kerberosRemoteServiceName kerberosRemoteServiceName from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.kerberos.remote.service.name")
    @ConfigDescription(
            "Hetu coordinator Kerberos service name. This parameter is required for Kerberos authentication")
    public DataCenterConfig setKerberosRemoteServiceName(String kerberosRemoteServiceName)
    {
        this.kerberosRemoteServiceName = kerberosRemoteServiceName;
        return this;
    }

    public String getKerberosServicePrincipalPattern()
    {
        return kerberosServicePrincipalPattern;
    }

    /**
     * set kerberosServicePrincipalPattern
     *
     * @param kerberosServicePrincipalPattern kerberosServicePrincipalPattern from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.kerberos.service.principal.pattern")
    @ConfigDescription("Hetu coordinator Kerberos service principal pattern. The default is ${SERVICE}@${HOST}."
            + "${SERVICE} is replaced with the value of KerberosRemoteServiceName and ${HOST} is replaced with the "
            + "hostname of the coordinator (after canonicalization if enabled)")
    public DataCenterConfig setKerberosServicePrincipalPattern(String kerberosServicePrincipalPattern)
    {
        this.kerberosServicePrincipalPattern = kerberosServicePrincipalPattern;
        return this;
    }

    public boolean isKerberosUseCanonicalHostname()
    {
        return isKerberosUseCanonicalHostname;
    }

    /**
     * set iskerberosUseCanonicalHostname
     *
     * @param isKerberosUseCanonicalHostnameParameter isKerberosUseCanonicalHostnameParameter from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.kerberos.use.canonical.hostname")
    @ConfigDescription(
            "Use the canonical hostname of the Hetu coordinator for the Kerberos service principal by first"
                    + "resolving the hostname to an IP address and then doing a reverse DNS lookup for that IP address.")
    public DataCenterConfig setKerberosUseCanonicalHostname(boolean isKerberosUseCanonicalHostnameParameter)
    {
        this.isKerberosUseCanonicalHostname = isKerberosUseCanonicalHostnameParameter;
        return this;
    }

    public String getExtraCredentials()
    {
        return extraCredentials;
    }

    /**
     * set extraCredentials
     *
     * @param extraCredentials extraCredentials from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.extra.credentials")
    @ConfigDescription(
            "Extra credentials for connecting to external services. The extraCredentials is a list of key-value pairs. "
                    + "Example: foo:bar;abc:xyz will create credentials abc=xyz and foo=bar")
    public DataCenterConfig setExtraCredentials(String extraCredentials)
    {
        this.extraCredentials = extraCredentials;
        return this;
    }

    public boolean isQueryPushDownEnabled()
    {
        return isQueryPushDownEnabled;
    }

    /**
     * set queryPushDownEnabled
     *
     * @param isQueryPushDownEnabledParameter isQueryPushDownEnabledParameter from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.query.pushdown.enabled")
    @ConfigDescription(
            "Enable sub-query push down to this data center. If this property is not set, by default sub-queries are "
                    + "pushed down")
    public DataCenterConfig setQueryPushDownEnabled(boolean isQueryPushDownEnabledParameter)
    {
        this.isQueryPushDownEnabled = isQueryPushDownEnabledParameter;
        return this;
    }

    public JdbcPushDownModule getQueryPushDownModule()
    {
        return queryPushDownModule;
    }

    /**
     * set queryPushDownEnabled
     *
     * @param queryPushDownModule Push Down Module
     * @return DataCenterConfig object
     */
    @Config("dc.query.pushdown.module")
    @ConfigDescription("query push down module [FULL_PUSHDOWN/BASE_PUSHDOWN]")
    public DataCenterConfig setQueryPushDownModule(JdbcPushDownModule queryPushDownModule)
    {
        this.queryPushDownModule = queryPushDownModule;
        return this;
    }

    public DataSize getRemoteHttpServerMaxRequestHeaderSize()
    {
        return remoteHeaderSize;
    }

    /**
     * set remoteHeaderSize
     *
     * @param remoteHeaderSizeParameter Remote Server Max Request Header Size
     * @return DataCenterConfig object
     */
    @Config("dc.remote-http-server.max-request-header-size")
    @ConfigDescription("This property should be equivalent to the value of "
            + "http-server.max-request-header-size in the remote server")
    public DataCenterConfig setRemoteHttpServerMaxRequestHeaderSize(DataSize remoteHeaderSizeParameter)
    {
        this.remoteHeaderSize = remoteHeaderSizeParameter;
        return this;
    }

    public Duration getMetadataCacheTtl()
    {
        return metadataCacheTtl;
    }

    /**
     * set metadataCacheTtl
     *
     * @param metadataCacheTtl metadataCacheTtl from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.metadata.cache.ttl")
    @ConfigDescription("Metadata Cache Ttl")
    public DataCenterConfig setMetadataCacheTtl(Duration metadataCacheTtl)
    {
        this.metadataCacheTtl = metadataCacheTtl;
        return this;
    }

    public Duration getMaxAnticipatedDelay()
    {
        return maxAnticipatedDelay;
    }

    /**
     * set maxAnticipatedDelay
     *
     * @param maxAnticipatedDelay maxAnticipatedDelay from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.max.anticipated.delay")
    @ConfigDescription(
            "Maximum anticipated delay between two requests for a query in the cluster. If the remote dc did not receive a "
                    + "request for more than this delay, it may cancel the query.")
    public DataCenterConfig setMaxAnticipatedDelay(Duration maxAnticipatedDelay)
    {
        this.maxAnticipatedDelay = maxAnticipatedDelay;
        return this;
    }

    public long getMetadataCacheMaximumSize()
    {
        return metadataCacheMaximumSize;
    }

    /**
     * set metadataCacheMaximumSize
     *
     * @param metadataCacheMaximumSize metadataCacheMaximumSize from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.metadata.cache.maximum.size")
    @ConfigDescription("Metadata Cache Maximum Size")
    public DataCenterConfig setMetadataCacheMaximumSize(long metadataCacheMaximumSize)
    {
        this.metadataCacheMaximumSize = metadataCacheMaximumSize;
        return this;
    }

    public boolean isMetadataCacheEnabled()
    {
        return isMetadataCacheEnabled;
    }

    /**
     * set metadataCacheEnabled
     *
     * @param isMetadataCacheEnabledParameter metadataCacheEnabled from properties file
     * @return DataCenterConfig object
     */
    @Config("dc.metadata.cache.enabled")
    @ConfigDescription("Metadata Cache Enabled")
    public DataCenterConfig setMetadataCacheEnabled(boolean isMetadataCacheEnabledParameter)
    {
        this.isMetadataCacheEnabled = isMetadataCacheEnabledParameter;
        return this;
    }

    public boolean isCompressionEnabled()
    {
        return this.isCompressionEnabled;
    }

    /**
     * set compression enabled.
     *
     * @param isCompressionEnabledParameter compression enabled or not.
     * @return DataCenterConfig object
     */
    @Config("dc.http-compression")
    @ConfigDescription("whether use gzip compress response body, default value is false")
    public DataCenterConfig setCompressionEnabled(boolean isCompressionEnabledParameter)
    {
        this.isCompressionEnabled = isCompressionEnabledParameter;
        return this;
    }

    public int getMaxIdleConnections()
    {
        return this.maxIdleConnections;
    }

    /**
     * set maximum http client idle connections in connection pool.
     *
     * @param maxIdleConnectionsParameter maximum http client idle connections.
     * @return DataCenterConfig object
     */
    @Config("dc.httpclient.maximum.idle.connections")
    @ConfigDescription("http client maximum idle connections")
    public DataCenterConfig setMaxIdleConnections(int maxIdleConnectionsParameter)
    {
        this.maxIdleConnections = maxIdleConnectionsParameter;
        return this;
    }
}
