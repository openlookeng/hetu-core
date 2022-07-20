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
package io.hetu.core.plugin.mariadb;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.hetu.core.plugin.mariadb.optimization.function.MariaExternalFunctionHub;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.spi.function.ExternalFunctionHub;
import org.mariadb.jdbc.Driver;
import org.mariadb.jdbc.UrlParser;

import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;

public class MariaClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).to(MariaClient.class).in(Scopes.SINGLETON);
        binder.bind(ExternalFunctionHub.class).to(MariaExternalFunctionHub.class).in(Scopes.SINGLETON);
        ensureCatalogIsEmpty(buildConfigObject(BaseJdbcConfig.class).getConnectionUrl());
        configBinder(binder).bindConfig(MariaConfig.class);
    }

    private static void ensureCatalogIsEmpty(String connectionUrl)
    {
        try {
            //The implement for parsing url of MariaDB Driver exists in the UrlParser class
            UrlParser urlParser = UrlParser.parse(connectionUrl);
            checkArgument(urlParser != null, "Invalid JDBC URL for MariaDB connector");
            checkArgument(urlParser.getDatabase() == null, "Database (catalog) must not be specified in JDBC URL for MariaDB connector");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Provides
    @Singleton
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config, MariaConfig mariaConfig)
            throws SQLException
    {
        Properties connectionProperties = basicConnectionProperties(config);
        connectionProperties.setProperty("useInformationSchema", "true");
        connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
        connectionProperties.setProperty("useUnicode", "true");
        connectionProperties.setProperty("characterEncoding", "utf8");
        connectionProperties.setProperty("tinyInt1isBit", "false");
        if (mariaConfig.isAutoReconnect()) {
            connectionProperties.setProperty("autoReconnect", String.valueOf(mariaConfig.isAutoReconnect()));
            connectionProperties.setProperty("maxReconnects", String.valueOf(mariaConfig.getMaxReconnects()));
        }
        if (mariaConfig.getConnectionTimeout() != null) {
            connectionProperties.setProperty("connectTimeout", String.valueOf(mariaConfig.getConnectionTimeout().toMillis()));
        }

        return new DriverConnectionFactory(new Driver(), config.getConnectionUrl(), Optional.ofNullable(config.getUserCredentialName()), Optional.ofNullable(config.getPasswordCredentialName()), connectionProperties);
    }
}
