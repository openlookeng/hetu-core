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

package io.hetu.core.plugin.heuristicindex.datasource.hive;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.metastore.thrift.ThriftHiveMetastoreClient;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreUtil;
import org.apache.hadoop.hive.thrift.client.TUGIAssumingTransport;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformationShim;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.net.ssl.SSLContext;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static io.hetu.core.plugin.heuristicindex.datasource.hive.ConstantsHelper.HDFS_AUTH_PRINCIPLE;
import static io.hetu.core.plugin.heuristicindex.datasource.hive.ConstantsHelper.HDFS_KEYTAB_FILEPATH;
import static io.hetu.core.plugin.heuristicindex.datasource.hive.ConstantsHelper.HIVE_CONFIG_RESOURCES;
import static io.hetu.core.plugin.heuristicindex.datasource.hive.ConstantsHelper.HIVE_METASTORE_AUTH_TYPE;
import static io.hetu.core.plugin.heuristicindex.datasource.hive.ConstantsHelper.HIVE_METASTORE_KRB5_CONF;
import static io.hetu.core.plugin.heuristicindex.datasource.hive.ConstantsHelper.HIVE_METASTORE_SERVICE_PRINCIPAL;
import static io.hetu.core.plugin.heuristicindex.datasource.hive.ConstantsHelper.HIVE_METASTORE_URI;
import static io.hetu.core.plugin.heuristicindex.datasource.hive.ConstantsHelper.HIVE_WIRE_ENCRYPTION_ENABLED;
import static io.hetu.core.plugin.heuristicindex.datasource.hive.ConstantsHelper.KRB5_CONF_KEY;
import static java.net.Proxy.Type.SOCKS;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.security.SaslRpcServer.AuthMethod.KERBEROS;
import static org.apache.hadoop.security.SecurityUtil.getServerPrincipal;

/**
 * Factory for ThriftHiveMetaStoreService
 */
public class ThriftHiveMetaStoreService
{
    private static final String KERBEROS_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule";
    private static final String COMMA_SEPARATOR = ",";
    private static final int TIMEOUT_MILLIS = 5000;

    private static final String STRING_BOOLEAN_TRUE = "true";

    private String metaStoreHost;

    private int metaStorePort;

    private String hiveMetastoreClientPrincipal;

    private String hiveMetastoreServicePrincipal;

    private ThriftHiveMetastoreClient client;

    private boolean isAuthEnabled;

    private String krb5ConfPath;

    private String hiveConfigResources;

    private boolean isHdfsWireEncryptionEnabled;

    private String hiveUserKeytab;

    private List<String> metastoreUris;

    /**
     * Constructor for ThriftHiveMetaStoreService
     *
     * @param properties - configurations to connect to thrift hive metastore
     */
    public ThriftHiveMetaStoreService(Properties properties)
    {
        setProperties(requireNonNull(properties));

        for (String uri : metastoreUris) {
            String metastoreUriTrimmed = uri.replace("thrift://", "");
            String[] metastoreUriSplits = metastoreUriTrimmed.split(":");
            this.metaStoreHost = metastoreUriSplits[0];
            this.metaStorePort = Integer.parseInt(metastoreUriSplits[1]);
            try {
                this.client = createClient(metaStoreHost, metaStorePort, hiveMetastoreServicePrincipal);
            }
            catch (TTransportException e) {
                throw new IllegalStateException("Could not connect to metastore at " + metaStoreHost, e);
            }
        }
    }

    /**
     * Set properties for ThriftHiveMetaStoreService
     *
     * @param properties - configurations to connect to thrift hive metastore
     */
    private void setProperties(Properties properties)
    {
        this.hiveMetastoreClientPrincipal = properties.getProperty(HDFS_AUTH_PRINCIPLE);
        this.hiveMetastoreServicePrincipal = properties.getProperty(HIVE_METASTORE_SERVICE_PRINCIPAL);
        this.hiveConfigResources = properties.getProperty(HIVE_CONFIG_RESOURCES);
        this.isHdfsWireEncryptionEnabled = Boolean.valueOf(
                properties.getProperty(HIVE_WIRE_ENCRYPTION_ENABLED));
        this.hiveUserKeytab = properties.getProperty(HDFS_KEYTAB_FILEPATH);
        // hive.metastore.uri=thrift://IP_ADDRESS:PORT
        this.metastoreUris = Arrays.asList(properties.getProperty(HIVE_METASTORE_URI).split(COMMA_SEPARATOR));
        this.isAuthEnabled = !properties.getProperty(HIVE_METASTORE_AUTH_TYPE, "NONE").equals("NONE");
        if (isAuthEnabled) {
            this.krb5ConfPath = properties.getProperty(HIVE_METASTORE_KRB5_CONF, System.getProperty(KRB5_CONF_KEY));
            if (krb5ConfPath != null) {
                System.setProperty(KRB5_CONF_KEY, krb5ConfPath);
            }
        }
    }

    /**
     * Get metadata from ThriftHiveMetaStore
     *
     * @param databaseName - name of the database for requested metadata
     * @param tableName    - name of the table for requested metadata
     * @return TableMetadata
     */
    public TableMetadata getTableMetadata(String databaseName, String tableName)
    {
        return new TableMetadata(getTable(databaseName, tableName));
    }

    private Table getTable(String databaseName, String tableName)
    {
        try {
            org.apache.hadoop.hive.metastore.api.Table table = client.getTable(databaseName, tableName);
            Table table1 = ThriftMetastoreUtil.fromMetastoreApiTable(table);
            return table1;
        }
        catch (TException e) {
            throw new IllegalStateException(e);
        }
    }

    private ThriftHiveMetastoreClient createClient(String host, int port,
                                                   String principal) throws TTransportException
    {
        TTransport rawTransport = createRaw(host, port);
        if (isAuthEnabled) {
            TTransport authenticatedTransport = authenticate(rawTransport, host, principal);
            if (!authenticatedTransport.isOpen()) {
                authenticatedTransport.open();
            }
            return new ThriftHiveMetastoreClient(authenticatedTransport, host);
        }
        else {
            return new ThriftHiveMetastoreClient(rawTransport, host);
        }
    }

    private TTransport createRaw(String host, int port) throws TTransportException
    {
        HostAndPort address = HostAndPort.fromParts(host, port);
        Optional<SSLContext> sslContext = Optional.empty();
        Optional<HostAndPort> socksProxy = Optional.empty();
        Proxy proxy = socksProxy.map(socksAddress -> new Proxy(SOCKS,
                InetSocketAddress.createUnresolved(socksAddress.getHost(), socksAddress.getPort()))).orElse(Proxy.NO_PROXY);
        Socket socket = new Socket(proxy);
        try {
            socket.connect(new InetSocketAddress(address.getHost(), address.getPort()), TIMEOUT_MILLIS);
            socket.setSoTimeout(TIMEOUT_MILLIS);
            if (sslContext.isPresent()) {
                // SSL will connect to the SOCKS address when present
                HostAndPort sslConnectAddress = socksProxy.orElse(address);
                socket = sslContext.get()
                        .getSocketFactory()
                        .createSocket(socket, sslConnectAddress.getHost(), sslConnectAddress.getPort(), true);
            }
            return new TSocket(socket);
        }
        catch (SocketException t) {
            // something went wrong, close the socket and rethrow
            try {
                socket.close();
            }
            catch (IOException e) {
                t.addSuppressed(e);
            }
            throw new TTransportException(t);
        }
        catch (IOException e) {
            throw new TTransportException(e);
        }
    }

    private TTransport authenticate(TTransport rawTransport, String hiveMetastoreHost,
                                    String hiveMetastorePrincipal)
    {
        TTransport saslTransport = null;
        try {
            String serverPrincipal = getServerPrincipal(hiveMetastorePrincipal, hiveMetastoreHost);

            String[] names = SaslRpcServer.splitKerberosName(serverPrincipal);
            Map<String, String> saslProps = ImmutableMap.of(Sasl.QOP,
                    this.isHdfsWireEncryptionEnabled ? "auth-conf" : "auth", Sasl.SERVER_AUTH, STRING_BOOLEAN_TRUE);

            saslTransport = new TSaslClientTransport(KERBEROS.getMechanismName(), null, names[0], names[1], saslProps,
                    null, rawTransport);
            String[] configResources = this.hiveConfigResources.split(COMMA_SEPARATOR);
            List<String> resources = Arrays.asList(configResources);
            for (String resource : resources) {
                org.apache.hadoop.conf.Configuration.addDefaultResource(resource);
            }
            org.apache.hadoop.conf.Configuration hdfsConfiguration = new org.apache.hadoop.conf.Configuration(false);
            hdfsConfiguration.set("hadoop.security.authentication", "kerberos");
            hdfsConfiguration.set("hadoop.rpc.protection", "privacy");
            UserGroupInformation.setConfiguration(hdfsConfiguration);
            KerberosPrincipal principal = createKerberosPrincipal(hiveMetastoreClientPrincipal);
            Configuration configuration = createConfiguration(hiveMetastoreClientPrincipal, hiveUserKeytab);
            Subject subject = getSubject(principal, configuration);
            UserGroupInformation userGroupInformation =
                    UserGroupInformationShim.createUserGroupInformationForSubject(subject);
            return new TUGIAssumingTransport(saslTransport, userGroupInformation);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Subject getSubject(KerberosPrincipal principal, Configuration configuration)
    {
        Subject subject = new Subject(false, ImmutableSet.of(principal), emptySet(), emptySet());
        try {
            LoginContext loginContext = new LoginContext("", subject, null, configuration);
            loginContext.login();
            return loginContext.getSubject();
        }
        catch (LoginException e) {
            throw new IllegalStateException(e);
        }
    }

    private static KerberosPrincipal createKerberosPrincipal(String principal)
    {
        try {
            return new KerberosPrincipal(
                    getServerPrincipal(principal, InetAddress.getLocalHost().getCanonicalHostName()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Configuration createConfiguration(String principal, String keytabLocation)
    {
        ImmutableMap.Builder<String, String> optionsBuilder = ImmutableMap.<String, String>builder().put("useKeyTab",
                STRING_BOOLEAN_TRUE)
                .put("storeKey", STRING_BOOLEAN_TRUE)
                .put("doNotPrompt", STRING_BOOLEAN_TRUE)
                .put("isInitiator", STRING_BOOLEAN_TRUE)
                .put("principal", principal)
                .put("keyTab", keytabLocation);
        Map<String, String> options = optionsBuilder.build();

        return new Configuration()
        {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name)
            {
                return new AppConfigurationEntry[]{
                        new AppConfigurationEntry(KERBEROS_LOGIN_MODULE,
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options)
                };
            }
        };
    }
}
