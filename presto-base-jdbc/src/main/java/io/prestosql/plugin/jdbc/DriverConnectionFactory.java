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

import io.airlift.log.Logger;
import io.prestosql.plugin.basejdbc.ConnectionPoolFactory;
import io.prestosql.plugin.basejdbc.HetuConnectionObjectPool;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DriverConnectionFactory
        implements ConnectionFactory
{
    private static final Logger LOG = Logger.get(DriverConnectionFactory.class);

    private final Driver driver;
    private final String connectionUrl;
    private final Properties connectionProperties;
    private final Optional<String> userCredentialName;
    private final Optional<String> passwordCredentialName;
    private final ConnectionPoolFactory connectionPoolFactory;
    private GenericObjectPoolConfig genericObjectPoolConfig;
    private AbandonedConfig abandonedConfig;
    private GenericObjectPool<Connection> genericObjectPool;
    private boolean isUseConnectionPool;

    public DriverConnectionFactory(Driver driver, BaseJdbcConfig config)
    {
        this(
                driver,
                config.getConnectionUrl(),
                Optional.ofNullable(config.getUserCredentialName()),
                Optional.ofNullable(config.getPasswordCredentialName()),
                basicConnectionProperties(config));
    }

    public static Properties basicConnectionProperties(BaseJdbcConfig config)
    {
        Properties connectionProperties = new Properties();
        if (config.getConnectionUser() != null) {
            connectionProperties.setProperty("user", config.getConnectionUser());
        }
        if (config.getConnectionPassword() != null) {
            connectionProperties.setProperty("password", config.getConnectionPassword());
        }

        try {
            connectionProperties.setProperty("useConnectionPool", "" + config.isUseConnectionPool());
            connectionProperties.setProperty("maxIdle", "" + config.getMaxIdle());
            connectionProperties.setProperty("minIdle", "" + config.getMinIdle());
            connectionProperties.setProperty("maxTotal", "" + config.getMaxTotal());
            connectionProperties.setProperty("lifo", "" + config.isLifo());
            connectionProperties.setProperty("fairness", "" + config.isFairness());
            connectionProperties.setProperty("maxWaitMillis", "" + config.getMaxWaitMillis());
            connectionProperties.setProperty("softMinEvictableIdleTimeMillis", "" + config.getSoftMinEvictableIdleTimeMillis());
            connectionProperties.setProperty("numTestsPerEvictionRun", "" + config.getNumTestsPerEvictionRun());
            connectionProperties.setProperty("testOnCreate", "" + config.isTestOnCreate());
            connectionProperties.setProperty("testOnBorrow", "" + config.isTestOnBorrow());
            connectionProperties.setProperty("testOnReturn", "" + config.isTestOnReturn());
            connectionProperties.setProperty("testWhileIdle", "" + config.isTestWhileIdle());
            connectionProperties.setProperty("timeBetweenEvictionRunsMillis", "" + config.getTimeBetweenEvictionRunsMillis());
            connectionProperties.setProperty("blockWhenExhausted", "" + config.isBlockWhenExhausted());
            connectionProperties.setProperty("jmxEnabled", "" + config.isJmxEnabled());
        }
        catch (Exception e) {
            // ignore exception
            if (LOG.isDebugEnabled()) {
                LOG.debug("basicConnectionProperties : set pool config failed... cause by", e);
            }
        }
        return connectionProperties;
    }

    public DriverConnectionFactory(Driver driver, String connectionUrl, Optional<String> userCredentialName, Optional<String> passwordCredentialName, Properties connectionProperties)
    {
        this.driver = requireNonNull(driver, "driver is null");
        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
        this.connectionProperties = new Properties();
        this.connectionProperties.putAll(requireNonNull(connectionProperties, "basicConnectionProperties is null"));
        this.userCredentialName = requireNonNull(userCredentialName, "userCredentialName is null");
        this.passwordCredentialName = requireNonNull(passwordCredentialName, "passwordCredentialName is null");
        this.connectionPoolFactory = new ConnectionPoolFactory(driver, connectionUrl, connectionProperties);
        this.genericObjectPoolConfig = createGenericObjectPoolConfig(connectionProperties);
        this.isUseConnectionPool = isUseConnectionPool(connectionProperties);
        this.abandonedConfig = new AbandonedConfig();
        this.genericObjectPool = new HetuConnectionObjectPool(this.connectionPoolFactory, this.genericObjectPoolConfig, this.abandonedConfig);
    }

    public static boolean isUseConnectionPool(Properties connectionProperties)
    {
        return "true".equals(connectionProperties.get("useConnectionPool"));
    }

    public static GenericObjectPoolConfig createGenericObjectPoolConfig(Properties connectionProperties)
    {
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        try {
            genericObjectPoolConfig.setMaxIdle(Integer.parseInt(connectionProperties.getProperty("maxIdle")));
            genericObjectPoolConfig.setMaxTotal(Integer.parseInt(connectionProperties.getProperty("maxTotal")));
            genericObjectPoolConfig.setMinIdle(Integer.parseInt(connectionProperties.getProperty("minIdle")));
            genericObjectPoolConfig.setLifo(Boolean.parseBoolean(connectionProperties.getProperty("lifo")));
            genericObjectPoolConfig.setFairness(Boolean.parseBoolean(connectionProperties.getProperty("fairness")));
            genericObjectPoolConfig.setMaxWaitMillis(Long.parseLong(connectionProperties.getProperty("maxWaitMillis")));
            genericObjectPoolConfig.setSoftMinEvictableIdleTimeMillis(Long.parseLong(connectionProperties.getProperty("softMinEvictableIdleTimeMillis")));
            genericObjectPoolConfig.setSoftMinEvictableIdleTimeMillis(Long.parseLong(connectionProperties.getProperty("softMinEvictableIdleTimeMillis")));
            genericObjectPoolConfig.setNumTestsPerEvictionRun(Integer.parseInt(connectionProperties.getProperty("numTestsPerEvictionRun")));
            genericObjectPoolConfig.setTestOnCreate(Boolean.parseBoolean(connectionProperties.getProperty("testOnCreate")));
            genericObjectPoolConfig.setTestOnBorrow(Boolean.parseBoolean(connectionProperties.getProperty("testOnBorrow")));
            genericObjectPoolConfig.setTestOnReturn(Boolean.parseBoolean(connectionProperties.getProperty("testOnReturn")));
            genericObjectPoolConfig.setTestWhileIdle(Boolean.parseBoolean(connectionProperties.getProperty("testWhileIdle")));
            genericObjectPoolConfig.setTimeBetweenEvictionRunsMillis(Long.parseLong(connectionProperties.getProperty("timeBetweenEvictionRunsMillis")));
            genericObjectPoolConfig.setBlockWhenExhausted(Boolean.parseBoolean(connectionProperties.getProperty("blockWhenExhausted")));
            genericObjectPoolConfig.setJmxEnabled(Boolean.parseBoolean(connectionProperties.getProperty("jmxEnabled")));
            if (LOG.isDebugEnabled()) {
                LOG.debug("createGenericObjectPoolConfig: success... ");
            }
        }
        catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("createGenericObjectPoolConfig: failed... cause by ", e);
            }
        }
        return genericObjectPoolConfig;
    }

    @Override
    public Connection openConnection(JdbcIdentity identity)
            throws SQLException
    {
        Connection connection = null;

        if (!isUseConnectionPool) {
            userCredentialName.ifPresent(credentialName -> setConnectionProperty(connectionProperties, identity.getExtraCredentials(), credentialName, "user"));
            passwordCredentialName.ifPresent(credentialName -> setConnectionProperty(connectionProperties, identity.getExtraCredentials(), credentialName, "password"));

            // if not use connection pool, remove all connector pool relate properties before call jdbc driver's connect method.
            Properties connectionPropertiesClone = (Properties) connectionProperties.clone();
            removeConnectionPool(connectionPropertiesClone);
            connection = driver.connect(connectionUrl, connectionPropertiesClone);
            if (LOG.isDebugEnabled()) {
                LOG.debug("openConnection: create Connection success...connection[%s]", connection.toString());
            }
        }
        else {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("openConnection: getTestOnBorrow=%s, getTestOnCreate=%s", "" + this.genericObjectPool.getTestOnBorrow(), "" + this.genericObjectPool.getTestOnCreate());
                }
                connection = genericObjectPool.borrowObject();
                connection.setReadOnly(false);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("openConnection: borrow Connection success...connection[%s]", connection.toString());
                }
            }
            catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("openConnection: borrow Connection failed...cuase by %s", e.getMessage());
                }
            }
        }
        checkState(connection != null, "Driver returned null connection");
        return connection;
    }

    private static void setConnectionProperty(Properties connectionProperties, Map<String, String> extraCredentials, String credentialName, String propertyName)
    {
        String value = extraCredentials.get(credentialName);
        if (value != null) {
            connectionProperties.setProperty(propertyName, value);
        }
    }

    private static void removeConnectionPool(Properties connectionProperties)
    {
        connectionProperties.remove("useConnectionPool");
        connectionProperties.remove("maxIdle");
        connectionProperties.remove("minIdle");
        connectionProperties.remove("maxTotal");
        connectionProperties.remove("lifo");
        connectionProperties.remove("fairness");
        connectionProperties.remove("maxWaitMillis");
        connectionProperties.remove("softMinEvictableIdleTimeMillis");
        connectionProperties.remove("numTestsPerEvictionRun");
        connectionProperties.remove("testOnCreate");
        connectionProperties.remove("testOnBorrow");
        connectionProperties.remove("testOnReturn");
        connectionProperties.remove("testWhileIdle");
        connectionProperties.remove("timeBetweenEvictionRunsMillis");
        connectionProperties.remove("blockWhenExhausted");
        connectionProperties.remove("jmxEnabled");
    }
}
