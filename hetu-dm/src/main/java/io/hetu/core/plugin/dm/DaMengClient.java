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
package io.hetu.core.plugin.dm;

import io.airlift.log.Logger;
import io.hetu.core.plugin.oracle.OracleClient;
import io.hetu.core.plugin.oracle.OracleConfig;
import io.hetu.core.plugin.oracle.config.RoundingMode;
import io.hetu.core.plugin.oracle.config.UnsupportedTypeHandling;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.StatsCollecting;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

public class DaMengClient
        extends OracleClient
{
    private static final Logger log = Logger.get(DaMengClient.class);

    private final int numberDefaultScale;

    private final RoundingMode roundingMode;

    private final UnsupportedTypeHandling unsupportedTypeHandling;

    /**
     * Create dameng client using the configurations.
     *
     * @param config config
     * @param damengConfig damengConfig
     * @param connectionFactory connectionFactory
     */
    @Inject
    public DaMengClient(BaseJdbcConfig config, OracleConfig damengConfig,
                        @StatsCollecting ConnectionFactory connectionFactory)
    {
        // the empty "" is to not use a quote to create queries
        //support both auto case trans between hetu to data source
        //and mixed cases table attributes DDL in data source side
        super(config.internalsetCaseInsensitiveNameMatching(true), damengConfig, connectionFactory);
        this.numberDefaultScale = damengConfig.getNumberDefaultScale();
        this.roundingMode = requireNonNull(damengConfig.getRoundingMode(), "dameng rounding mode cannot be null");
        this.unsupportedTypeHandling = requireNonNull(damengConfig.getUnsupportedTypeHandling(),
                "dameng unsupported type handling cannot be null");
    }

    @Override
    public Connection getConnection(JdbcIdentity identity, JdbcSplit split)
            throws SQLException
    {
        Connection connection = connectionFactory.openConnection(identity);
        try {
            connection.setReadOnly(false);
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }
}
