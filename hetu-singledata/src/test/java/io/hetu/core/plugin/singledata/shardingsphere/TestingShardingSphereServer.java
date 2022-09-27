/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.hetu.core.plugin.singledata.shardingsphere;

import com.zaxxer.hikari.HikariDataSource;
import io.airlift.log.Logger;
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import io.hetu.core.plugin.singledata.SingleDataPlugin;
import io.prestosql.Session;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.DistributedQueryRunner;
import org.apache.curator.test.TestingServer;
import org.apache.shardingsphere.driver.api.ShardingSphereDataSourceFactory;
import org.apache.shardingsphere.driver.jdbc.core.datasource.ShardingSphereDataSource;
import org.apache.shardingsphere.infra.config.algorithm.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.config.mode.ModeConfiguration;
import org.apache.shardingsphere.mode.repository.cluster.ClusterPersistRepositoryConfiguration;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.strategy.sharding.StandardShardingStrategyConfiguration;

import javax.sql.DataSource;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class TestingShardingSphereServer
        implements Closeable
{
    private static final Logger LOGGER = Logger.get(TestingShardingSphereServer.class);

    private final TestingPostgreSqlServer ds0;
    private final TestingPostgreSqlServer ds1;
    private final TestingServer curatorServer;
    private final ShardingSphereDataSource dataSource;

    public TestingShardingSphereServer()
            throws Exception
    {
        this.ds0 = new TestingPostgreSqlServer("test", "ds_0");
        this.ds1 = new TestingPostgreSqlServer("test", "ds_1");
        this.curatorServer = new TestingServer();
        this.dataSource = (ShardingSphereDataSource) createDataSource();

        try (Connection connection = dataSource.getConnection()) {
            // create test data
            String createTableASql = "CREATE TABLE table_a (col_database INT, col_table INT, col_value VARCHAR(50))";
            String createTableBSql = "CREATE TABLE table_b (col_database INT, col_table INT, col_value VARCHAR(50))";

            String insertTableASql = "INSERT INTO table_a (col_database, col_table, col_value) VALUES (?, ?, ?)";
            String insertTableBSql = "INSERT INTO table_b (col_database, col_table, col_value) VALUES (?, ?, ?)";

            connection.prepareStatement(createTableASql).execute();
            connection.prepareStatement(createTableBSql).execute();

            PreparedStatement insertA = connection.prepareStatement(insertTableASql);
            PreparedStatement insertB = connection.prepareStatement(insertTableBSql);

            for (int i = 0; i < 10; i++) {
                insertA.setInt(1, i);
                insertA.setInt(2, i);
                insertA.setString(3, "TEST");
                insertA.executeUpdate();
                insertB.setInt(1, i);
                insertB.setInt(2, i);
                insertB.setString(3, "TEST");
                insertB.executeUpdate();
            }
        }
    }

    public QueryRunner createShardingSphereQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession()).setNodeCount(3).build();

            Map<String, String> connectorProperties = new HashMap<>();
            connectorProperties.put("singledata.mode", "SHARDING_SPHERE");
            connectorProperties.put("shardingsphere.type", "zookeeper");
            connectorProperties.put("shardingsphere.namespace", "test");
            connectorProperties.put("shardingsphere.server-lists", curatorServer.getConnectString());
            connectorProperties.put("shardingsphere.database-name", "test");

            queryRunner.installPlugin(new SingleDataPlugin());
            queryRunner.createCatalog("singledata", "singledata", connectorProperties);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("singledata")
                .setSchema("public")
                .build();
    }

    @Override
    public void close()
    {
        try (ShardingSphereDataSource ssDataSource = dataSource;
                TestingServer server = curatorServer;
                TestingPostgreSqlServer pg0 = ds0;
                TestingPostgreSqlServer pg1 = ds1) {
            checkState(true, "Just for close resources");
        }
        catch (Exception e) {
            // ignore exception in close
            LOGGER.error(e.getMessage());
        }
    }

    private DataSource createDataSource() throws SQLException
    {
        ClusterPersistRepositoryConfiguration clusterPersistRepositoryConfiguration = new ClusterPersistRepositoryConfiguration("ZooKeeper", "test", curatorServer.getConnectString(), new Properties());
        ModeConfiguration modeConfiguration = new ModeConfiguration("Cluster", clusterPersistRepositoryConfiguration, true);
        return ShardingSphereDataSourceFactory.createDataSource("test", modeConfiguration, createDataSourceMap(), Collections.singleton(createShardingRuleConfiguration()), new Properties());
    }

    private ShardingRuleConfiguration createShardingRuleConfiguration()
    {
        ShardingRuleConfiguration result = new ShardingRuleConfiguration();
        result.getTables().add(getTableAConfiguration());
        result.getTables().add(getTableBRuleConfiguration());
        result.getBindingTableGroups().add("table_a, table_b");
        result.setDefaultDatabaseShardingStrategy(new StandardShardingStrategyConfiguration("col_database", "database_inline"));
        Properties databaseProperties = new Properties();
        databaseProperties.put("algorithm-expression", "ds_${col_database %2}");
        result.getShardingAlgorithms().put("database_inline", new AlgorithmConfiguration("INLINE", databaseProperties));
        Properties tableAProperties = new Properties();
        tableAProperties.setProperty("algorithm-expression", "table_a_${col_table %2}");
        result.getShardingAlgorithms().put("table_a_inline", new AlgorithmConfiguration("INLINE", tableAProperties));
        Properties tableBProperties = new Properties();
        tableBProperties.setProperty("algorithm-expression", "table_b_${col_table %2}");
        result.getShardingAlgorithms().put("table_b_inline", new AlgorithmConfiguration("INLINE", tableBProperties));
        return result;
    }

    private ShardingTableRuleConfiguration getTableAConfiguration()
    {
        ShardingTableRuleConfiguration tableRuleConfiguration = new ShardingTableRuleConfiguration("table_a", "ds_${0..1}.table_a_${[0, 1]}");
        tableRuleConfiguration.setTableShardingStrategy(new StandardShardingStrategyConfiguration("col_table", "table_a_inline"));
        return tableRuleConfiguration;
    }

    private ShardingTableRuleConfiguration getTableBRuleConfiguration()
    {
        ShardingTableRuleConfiguration tableRuleConfiguration = new ShardingTableRuleConfiguration("table_b", "ds_${0..1}.table_b_${[0, 1]}");
        tableRuleConfiguration.setTableShardingStrategy(new StandardShardingStrategyConfiguration("col_table", "table_b_inline"));
        return tableRuleConfiguration;
    }

    private Map<String, DataSource> createDataSourceMap()
    {
        Map<String, DataSource> result = new HashMap<>();
        result.put("ds_0", createDataSource(ds0));
        result.put("ds_1", createDataSource(ds1));
        return result;
    }

    private DataSource createDataSource(TestingPostgreSqlServer server)
    {
        HikariDataSource result = new HikariDataSource();
        result.setDriverClassName("org.postgresql.Driver");
        result.setJdbcUrl(server.getJdbcUrl());
        result.setUsername(server.getUser());
        return result;
    }
}
