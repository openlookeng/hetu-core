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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.hetu.core.plugin.singledata.SingleDataSplit;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.SchemaTableName;
import org.apache.shardingsphere.driver.api.ShardingSphereDataSourceFactory;
import org.apache.shardingsphere.driver.jdbc.core.connection.ShardingSphereConnection;
import org.apache.shardingsphere.driver.jdbc.core.datasource.ShardingSphereDataSource;
import org.apache.shardingsphere.infra.binder.QueryContext;
import org.apache.shardingsphere.infra.binder.SQLStatementContextFactory;
import org.apache.shardingsphere.infra.config.mode.ModeConfiguration;
import org.apache.shardingsphere.infra.context.kernel.KernelProcessor;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.database.type.dialect.OpenGaussDatabaseType;
import org.apache.shardingsphere.infra.datanode.DataNode;
import org.apache.shardingsphere.infra.executor.check.SQLCheckEngine;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.rule.ShardingSphereRuleMetaData;
import org.apache.shardingsphere.infra.metadata.database.schema.QualifiedTable;
import org.apache.shardingsphere.mode.repository.cluster.ClusterPersistRepositoryConfiguration;
import org.apache.shardingsphere.parser.rule.SQLParserRule;
import org.apache.shardingsphere.readwritesplitting.rule.ReadwriteSplittingDataSourceRule;
import org.apache.shardingsphere.readwritesplitting.rule.ReadwriteSplittingRule;
import org.apache.shardingsphere.sharding.rule.ShardingRule;
import org.apache.shardingsphere.singletable.rule.SingleTableRule;
import org.apache.shardingsphere.transaction.core.TransactionType;
import org.apache.shardingsphere.transaction.core.TransactionTypeHolder;

import javax.inject.Inject;
import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static io.hetu.core.plugin.singledata.SingleDataUtils.getSingleDataSplit;
import static io.hetu.core.plugin.singledata.shardingsphere.ShardingSphereUtils.fillProperties;
import static io.hetu.core.plugin.singledata.shardingsphere.ShardingSphereUtils.loadDrivers;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ShardingSphereAdaptor
        implements ConnectionFactory
{
    private static final Logger LOGGER = Logger.get(ShardingSphereAdaptor.class);
    private static final DatabaseType DATABASE_TYPE = new OpenGaussDatabaseType();

    private final ClassLoader classLoader;
    private final ShardingSphereDataSource dataSource;
    private final KernelProcessor kernelProcessor = new KernelProcessor();

    @Inject
    public ShardingSphereAdaptor(ShardingSphereConfig shardingSphereConfig, ClassLoader classLoader)
    {
        requireNonNull(shardingSphereConfig, "shardingSphereConfig is null");
        requireNonNull(classLoader, "classLoader is null");

        this.classLoader = classLoader;
        Properties properties = new Properties();
        fillProperties(properties, shardingSphereConfig);
        ModeConfiguration modeConfiguration = new ModeConfiguration(
                "Cluster",
                new ClusterPersistRepositoryConfiguration(
                        shardingSphereConfig.getType(),
                        shardingSphereConfig.getNamespace(),
                        shardingSphereConfig.getServerLists(),
                        properties),
                false);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            loadDrivers();
            this.dataSource = (ShardingSphereDataSource) ShardingSphereDataSourceFactory
                    .createDataSource(shardingSphereConfig.getDatabaseName(), modeConfiguration);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Connection openConnection(JdbcIdentity identity)
            throws SQLException
    {
        return openConnection();
    }

    public Connection openConnection()
            throws SQLException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            TransactionTypeHolder.set(TransactionType.LOCAL);
            return dataSource.getConnection();
        }
    }

    public Connection openResourceConnection(String resourceName)
    {
        try (ShardingSphereConnection connection = (ShardingSphereConnection) openConnection()) {
            Map<String, DataSource> resourceMap = getShardingSphereDatabase(connection).getResource().getDataSources();
            if (resourceMap.containsKey(resourceName)) {
                return resourceMap.get(resourceName).getConnection();
            }
            else {
                throw new PrestoException(JDBC_ERROR, format("Unknown actual datasource [%s]", resourceName));
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    public List<String> getSchemaNames()
    {
        try (ShardingSphereConnection connection = (ShardingSphereConnection) openConnection()) {
            return ImmutableList.copyOf(getShardingSphereDatabase(connection).getSchemas().keySet());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    public List<String> getTableNames(String schemaName)
    {
        try (ShardingSphereConnection connection = (ShardingSphereConnection) openConnection()) {
            return ImmutableList.copyOf(getShardingSphereDatabase(connection).getSchema(schemaName).getAllTableNames());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    public ActualDataNode getOneActualDataNode(String logicalTableName)
    {
        try (ShardingSphereConnection connection = (ShardingSphereConnection) openConnection()) {
            Optional<DataNode> dataNode = Optional.empty();

            ShardingSphereRuleMetaData ruleMetaData = getShardingSphereDatabase(connection).getRuleMetaData();
            Optional<ShardingRule> shardingRule = ruleMetaData.findSingleRule(ShardingRule.class);
            Optional<SingleTableRule> singleTableRule = ruleMetaData.findSingleRule(SingleTableRule.class);
            Optional<ReadwriteSplittingRule> splitRule = ruleMetaData.findSingleRule(ReadwriteSplittingRule.class);

            if (shardingRule.isPresent() && (shardingRule.get().findTableRule(logicalTableName).isPresent()
                    || shardingRule.get().isBroadcastTable(logicalTableName))) {
                dataNode = Optional.of(shardingRule.get().getDataNode(logicalTableName));
            }

            if (!dataNode.isPresent() && singleTableRule.isPresent()) {
                Map<String, Collection<DataNode>> singleDataNodes = singleTableRule.get().getSingleTableDataNodes();
                if (singleDataNodes.containsKey(logicalTableName)) {
                    dataNode = singleDataNodes.get(logicalTableName).stream().findFirst();
                }
            }

            if (!dataNode.isPresent()) {
                throw new PrestoException(JDBC_ERROR, format("Can not find actual dataNode of logical table [%s]", logicalTableName));
            }

            String dataSourceName = dataNode.get().getDataSourceName();

            if (splitRule.isPresent()) {
                Optional<ReadwriteSplittingDataSourceRule> dataSourceRule = splitRule.get().findDataSourceRule(dataSourceName);
                if (dataSourceRule.isPresent() && dataSourceRule.get().getEnabledReplicaDataSources().size() > 0) {
                    dataSourceName = dataSourceRule.get().getEnabledReplicaDataSources().get(0);
                }
                else {
                    throw new PrestoException(JDBC_ERROR, format("Can not find read dataSource [%s]", dataSourceName));
                }
            }

            return new ActualDataNode(dataSourceName, dataNode.get().getSchemaName(), dataNode.get().getTableName());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    public List<SingleDataSplit> getSplitsByQuery(String query)
    {
        try (ShardingSphereConnection connection = (ShardingSphereConnection) openConnection();
                ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            String databaseName = connection.getDatabaseName();
            ShardingSphereMetaData metaData = getShardingSphereMetaData(connection);
            ShardingSphereDatabase database = getShardingSphereDatabase(connection);

            QueryContext queryContext = getQueryContext(connection, query);

            SQLCheckEngine.check(queryContext.getSqlStatementContext(),
                    queryContext.getParameters(),
                    database.getRuleMetaData().getRules(),
                    databaseName,
                    metaData.getDatabases(),
                    null);

            return kernelProcessor.generateExecutionContext(queryContext,
                        database,
                        metaData.getGlobalRuleMetaData(),
                        metaData.getProps(),
                        connection.getConnectionContext())
                    .getExecutionUnits().stream()
                    .map(unit -> getSingleDataSplit(unit.getDataSourceName(), unit.getSqlUnit().getSql()))
                    .collect(Collectors.toList());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    public boolean checkJoin(JdbcTableHandle leftTable, JdbcTableHandle rightTable, String sql)
    {
        try (ShardingSphereConnection connection = (ShardingSphereConnection) openConnection();
                ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            ShardingSphereRuleMetaData ruleMetaData = getShardingSphereDatabase(connection).getRuleMetaData();
            Optional<ShardingRule> shardingRule = ruleMetaData.findSingleRule(ShardingRule.class);
            Optional<SingleTableRule> singleTableRule = ruleMetaData.findSingleRule(SingleTableRule.class);

            Collection<String> tableNames = ImmutableList.of(leftTable.getTableName(), rightTable.getTableName());

            if (shardingRule.isPresent()) {
                if (shardingRule.get().isAllBroadcastTables(tableNames)) {
                    return true;
                }
                if (shardingRule.get().isAllShardingTables(tableNames)) {
                    QueryContext queryContext = getQueryContext(connection, sql);
                    return shardingRule.get().isAllBindingTables(getShardingSphereDatabase(connection),
                            queryContext.getSqlStatementContext(), tableNames);
                }
            }

            if (singleTableRule.isPresent()) {
                SchemaTableName leftSchemaTableName = leftTable.getSchemaTableName();
                SchemaTableName rightSchemaTableName = rightTable.getSchemaTableName();
                Collection<QualifiedTable> singleTableNames = ImmutableList.of(
                        new QualifiedTable(leftSchemaTableName.getSchemaName(), leftSchemaTableName.getTableName()),
                        new QualifiedTable(rightSchemaTableName.getSchemaName(), rightSchemaTableName.getTableName()));
                return singleTableRule.get().isSingleTablesInSameDataSource(singleTableNames);
            }

            return false;
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private QueryContext getQueryContext(ShardingSphereConnection connection, String query)
    {
        String databaseName = connection.getDatabaseName();
        ShardingSphereMetaData metaData = getShardingSphereMetaData(connection);

        return new QueryContext(
            SQLStatementContextFactory.newInstance(
                metaData.getDatabases(),
                metaData.getGlobalRuleMetaData()
                        .getSingleRule(SQLParserRule.class)
                        .getSQLParserEngine(DATABASE_TYPE.getType())
                        .parse(query, false),
                databaseName),
            query, Collections.emptyList());
    }

    private ShardingSphereMetaData getShardingSphereMetaData(ShardingSphereConnection connection)
    {
        return connection.getContextManager().getMetaDataContexts().getMetaData();
    }

    private ShardingSphereDatabase getShardingSphereDatabase(ShardingSphereConnection connection)
    {
        return connection.getContextManager().getMetaDataContexts().getMetaData().getDatabase(connection.getDatabaseName());
    }

    @Override
    public void close()
    {
        try (ShardingSphereDataSource ssDataSource = dataSource) {
            checkState(true, "Just for close resource");
        }
        catch (Exception e) {
            LOGGER.error("Ignore error in close");
        }
    }
}
