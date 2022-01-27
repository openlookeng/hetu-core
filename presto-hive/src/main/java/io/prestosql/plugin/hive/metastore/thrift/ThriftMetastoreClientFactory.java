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
package io.prestosql.plugin.hive.metastore.thrift;

import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.hetu.core.common.util.SslSocketUtil;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.authentication.HiveMetastoreAuthentication;
import io.prestosql.plugin.hive.metastore.MetastoreClientFactory;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PrestoException;
import org.apache.thrift.transport.TTransportException;

import javax.inject.Inject;
import javax.net.ssl.SSLContext;

import java.security.GeneralSecurityException;
import java.util.Optional;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ThriftMetastoreClientFactory
        implements MetastoreClientFactory
{
    private Optional<SSLContext> sslContext;
    private Optional<HostAndPort> socksProxy;
    private int timeoutMillis;
    private HiveMetastoreAuthentication metastoreAuthentication;
    private String hostname;

    public ThriftMetastoreClientFactory(
            Optional<SSLContext> sslContext,
            Optional<HostAndPort> socksProxy,
            Duration timeout,
            HiveMetastoreAuthentication metastoreAuthentication,
            String hostname)
    {
        init(sslContext, socksProxy, timeout, metastoreAuthentication, hostname);
    }

    @Inject
    public ThriftMetastoreClientFactory(HiveConfig config, HiveMetastoreAuthentication metastoreAuthentication, NodeManager nodeManager)
    {
        try {
            init(SslSocketUtil.buildSslContext(config.isTlsEnabled()),
                    Optional.ofNullable(config.getMetastoreSocksProxy()),
                    config.getMetastoreTimeout(),
                    metastoreAuthentication,
                    nodeManager.getCurrentNode().getHost());
        }
        catch (GeneralSecurityException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e.getMessage());
        }
    }

    public ThriftMetastoreClientFactory(HiveConfig config, HiveMetastoreAuthentication metastoreAuthentication)
    {
        try {
            init(SslSocketUtil.buildSslContext(config.isTlsEnabled()),
                    Optional.ofNullable(config.getMetastoreSocksProxy()),
                    config.getMetastoreTimeout(),
                    metastoreAuthentication,
                    "localhost");
        }
        catch (GeneralSecurityException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e.getMessage());
        }
    }

    @Override
    public ThriftMetastoreClient create(HostAndPort address)
            throws TTransportException
    {
        return new ThriftHiveMetastoreClient(Transport.create(address, sslContext, socksProxy, timeoutMillis, metastoreAuthentication), hostname);
    }

    private void init(
            Optional<SSLContext> sslContext,
            Optional<HostAndPort> socksProxy,
            Duration timeout,
            HiveMetastoreAuthentication metastoreAuthentication,
            String hostname)
    {
        this.sslContext = requireNonNull(sslContext, "sslContext is null");
        this.socksProxy = requireNonNull(socksProxy, "socksProxy is null");
        this.timeoutMillis = toIntExact(timeout.toMillis());
        this.metastoreAuthentication = requireNonNull(metastoreAuthentication, "metastoreAuthentication is null");
        this.hostname = requireNonNull(hostname, "hostname is null");
    }
}
