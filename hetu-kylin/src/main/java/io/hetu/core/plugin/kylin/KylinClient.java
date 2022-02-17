/*
 * Copyright (C) 2018-2020. Autohome Technologies Co., Ltd. All rights reserved.
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

package io.hetu.core.plugin.kylin;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.hetu.core.plugin.kylin.optimization.KylinQueryGenerator;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.QueryBuilder;
import io.prestosql.plugin.jdbc.StatsCollecting;
import io.prestosql.plugin.jdbc.optimization.JdbcConverterContext;
import io.prestosql.plugin.jdbc.optimization.JdbcPushDownModule;
import io.prestosql.plugin.jdbc.optimization.JdbcPushDownParameter;
import io.prestosql.plugin.jdbc.optimization.JdbcQueryGeneratorResult;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.FunctionMetadataManager;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.relation.DeterminismEvaluator;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.spi.sql.QueryGenerator;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.VarcharType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static io.hetu.core.plugin.kylin.KylinErrorCode.KYLIN_INVALID_CONNECTION_URL;
import static io.prestosql.plugin.jdbc.optimization.JdbcPushDownModule.BASE_PUSHDOWN;
import static io.prestosql.plugin.jdbc.optimization.JdbcPushDownModule.DEFAULT;
import static java.util.Locale.ENGLISH;

public class KylinClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(KylinClient.class);
    private final boolean isQueryPushDownEnabled;
    private final KylinConfig kylinConfig;
    private final boolean partialPushdownEnableWithCubePriority;
    private final String explainMatchPattern;
    private final KylinConfig.ValidateSqlMethod validateSqlMethod;
    private final BaseJdbcConfig config;
    private final FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.configBuilder()
                    .setParserFactory(SqlParserImpl.FACTORY)
                    .setCaseSensitive(false)
                    .setConformance(SqlConformanceEnum.DEFAULT)
                    .build())
            .build();
    private String restBaseUrl;
    private Optional<String> project;

    @Inject
    public KylinClient(BaseJdbcConfig config, KylinConfig kylinConfig,
            @StatsCollecting ConnectionFactory connectionFactory, TypeManager typeManager)
    {
        super(config, "", connectionFactory);
        this.kylinConfig = kylinConfig;
        this.isQueryPushDownEnabled = kylinConfig.isQueryPushDownEnabled();
        this.explainMatchPattern = kylinConfig.getExplainMatchPattern();
        this.validateSqlMethod = kylinConfig.getValidateSqlMethod();
        this.partialPushdownEnableWithCubePriority = kylinConfig.isPartialPushdownEnableWithCubePriority();
        this.config = config;

        if (kylinConfig.isCubeMetadataFilter()) {
            setRestConnectionUrl();
        }
        else {
            project = Optional.empty();
        }
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columns)
            throws SQLException
    {
        if (table.getGeneratedSql().isPresent()) {
            // Hetu: If the query is pushed down, use it as the table
            return new QueryBuilder(KylinConstants.KYLIN_IDENTIFIER_QUOTE, true).buildSql(
                    this,
                    session,
                    connection,
                    null,
                    null,
                    table.getGeneratedSql().get().getSql(),
                    columns,
                    table.getConstraint(),
                    split.getAdditionalPredicate(),
                    tryApplyLimit(table.getLimit()));
        }
        return new QueryBuilder(KylinConstants.KYLIN_IDENTIFIER_QUOTE).buildSql(
                this,
                session,
                connection,
                null,
                table.getSchemaName(),
                table.getTableName(),
                columns,
                table.getConstraint(),
                split.getAdditionalPredicate(),
                tryApplyLimit(table.getLimit()));
    }

    @Override
    public Map<String, ColumnHandle> getColumns(ConnectorSession session, String sql, Map<String, Type> types)
    {
        log.debug("begin getColumns : values are session %s, sql %s, types %s", session, sql, types);

        try {
            SqlParser parser = SqlParser.create(sql, frameworkConfig.getParserConfig());
            SqlNode sqlNode = parser.parseStmt();
            SqlSelect select = (SqlSelect) sqlNode;
            SqlNodeList nodeList = select.getSelectList();
            ImmutableMap.Builder<String, ColumnHandle> columnBuilder = new ImmutableMap.Builder<>();

            String columnName = "";
            JdbcColumnHandle columnHandle = null;
            for (SqlNode node : nodeList.getList()) {
                if (SqlKind.IDENTIFIER == node.getKind()) {
                    SqlIdentifier sqlBasicCall = (SqlIdentifier) node;
                    columnName = sqlBasicCall.getSimple();
                }
                else {
                    SqlBasicCall sqlBasicCall = (SqlBasicCall) node;
                    columnName = sqlBasicCall.operands[1].toString();
                }

                Type type = types.get(columnName.toLowerCase(Locale.ENGLISH));
                if (type instanceof BigintType) {
                    columnHandle = new JdbcColumnHandle(columnName, KylinJdbcTypeHandle.JDBC_BIGINT, BigintType.BIGINT, true);
                }
                else if (type instanceof IntegerType) {
                    columnHandle = new JdbcColumnHandle(columnName, KylinJdbcTypeHandle.JDBC_INTEGER, IntegerType.INTEGER, true);
                }
                else if (type instanceof DoubleType) {
                    columnHandle = new JdbcColumnHandle(columnName, KylinJdbcTypeHandle.JDBC_DOUBLE, DoubleType.DOUBLE, true);
                }
                else if (type instanceof VarcharType) {
                    columnHandle = new JdbcColumnHandle(columnName, KylinJdbcTypeHandle.JDBC_VARCHAR, VarcharType.VARCHAR, true);
                }
                else { //todo default VarcharType
                    columnHandle = new JdbcColumnHandle(columnName, KylinJdbcTypeHandle.JDBC_VARCHAR, VarcharType.VARCHAR, true);
                }
                if (columnHandle != null) {
                    columnBuilder.put(columnName.toLowerCase(ENGLISH), columnHandle);
                }
            }

            return columnBuilder.build();
        }
        catch (Exception e) {
            log.debug("There is a problem %s", e.getLocalizedMessage());
            log.debug("Error message: " + e.getStackTrace());
            // No need to raise an error.
            // This method is used inside applySubQuery method to extract the column types from a sub-query.
            // Returning empty map will indicate that something wrong and let the Presto to execute the query as usual.
            return Collections.emptyMap();
        }
    }

    private void setRestConnectionUrl()
    {
        if (kylinConfig.getRestUrl().isPresent()) {
            String url = kylinConfig.getRestUrl().get();
            int slash = url.indexOf(47);
            if (slash != -1) {
                this.restBaseUrl = url.substring(0, url.indexOf(47));
                this.project = Optional.of(url.substring(url.indexOf(47) + 1));
            }
            else {
                throw new PrestoException(KYLIN_INVALID_CONNECTION_URL, "Url should have project name");
            }
        }
        else {
            String url = kylinConfig.getConnectionUrl();
            if (url.startsWith("jdbc:kylin://")) {
                url = url.substring("jdbc:kylin://".length());
                int slash = url.indexOf(47);
                if (slash != -1) {
                    this.restBaseUrl = url.substring(0, url.indexOf(47));
                    this.project = Optional.of(url.substring(url.indexOf(47) + 1));
                }
                else {
                    throw new PrestoException(KYLIN_INVALID_CONNECTION_URL, "Url should have project name");
                }
            }
            else {
                throw new PrestoException(KYLIN_INVALID_CONNECTION_URL, "Url should starts with jdbc:kylin://");
            }
        }
    }

    @Override
    public Optional<QueryGenerator<JdbcQueryGeneratorResult, JdbcConverterContext>> getQueryGenerator(DeterminismEvaluator determinismEvaluator, RowExpressionService rowExpressionService, FunctionMetadataManager functionManager, StandardFunctionResolution functionResolution)
    {
        // In most cases, the running efficiency of kylin is not satisfactory, so just base push down by default.
        JdbcPushDownModule mysqlPushDownModule = config.getPushDownModule() == DEFAULT ? BASE_PUSHDOWN : config.getPushDownModule();
        JdbcPushDownParameter pushDownParameter = new JdbcPushDownParameter(getIdentifierQuote(), this.caseInsensitiveNameMatching, mysqlPushDownModule, functionResolution);
        return Optional.of(new KylinQueryGenerator(determinismEvaluator, rowExpressionService, functionManager, functionResolution, pushDownParameter, config));
    }

    @Override
    public boolean isLimitGuaranteed()
    {
        return true;
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }
}
