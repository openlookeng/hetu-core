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
package io.hetu.core.plugin.clickhouse;

import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.spi.PrestoException;
import io.prestosql.sql.builder.functioncall.JdbcExternalFunctionHub;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.prestosql.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;

public class DataBaseTest
        implements AutoCloseable
{
    private final Connection connection;

    private final ClickHouseClient clickHouseClient;

    private ClickHouseServerTest clickHouseServerTest;

    private List<String> tables = new ArrayList<>();

    /**
     * constructor
     */
    public DataBaseTest(ClickHouseServerTest clickHouseServer)
            throws SQLException
    {
        this.clickHouseServerTest = clickHouseServer;

        BaseJdbcConfig jdbcConfig = new BaseJdbcConfig();
        ClickHouseConfig clickHouseConfig = new ClickHouseConfig();

        jdbcConfig.setConnectionUrl(clickHouseServer.getJdbcUrl());
        jdbcConfig.setConnectionUser(clickHouseServer.getUser());
        jdbcConfig.setConnectionPassword(clickHouseServer.getPassword());

        clickHouseConfig.setTableTypes("TABLE,VIEW");
        clickHouseConfig.setSchemaPattern(clickHouseServer.getSchema());
        clickHouseConfig.setQueryPushDownEnabled(false);

        Driver driver = null;
        try {
            driver = (Driver) Class.forName(ClickHouseConstants.CLICKHOUSE_JDBC_DRIVER_CLASS_NAME).getConstructor(((Class<?>[]) null)).newInstance();
        }
        catch (InstantiationException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        catch (IllegalAccessException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        catch (ClassNotFoundException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        catch (InvocationTargetException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        catch (NoSuchMethodException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        ConnectionFactory connectionFactory = new DriverConnectionFactory(driver, jdbcConfig.getConnectionUrl(), Optional.ofNullable(jdbcConfig.getUserCredentialName()), Optional.ofNullable(jdbcConfig.getPasswordCredentialName()), basicConnectionProperties(jdbcConfig));

        clickHouseClient = new ClickHouseClient(jdbcConfig, clickHouseConfig, connectionFactory, new JdbcExternalFunctionHub());
        connection =
                DriverManager.getConnection(clickHouseServer.getJdbcUrl(), clickHouseServer.getUser(), clickHouseServer.getPassword());

        connection.createStatement()
                .execute(buildCreateTableSql("example", "(text varchar,text_short varchar(32), value bigint)"));
        connection.createStatement().execute(buildCreateTableSql("student", "(id varchar)"));
        connection.createStatement()
                .execute(buildCreateTableSql("table_with_float_col", "(col1 bigint, col2 double, col3 float, col4 real)"));
        connection.createStatement()
                .execute(buildCreateTableSql("number", "(text varchar, text_short varchar(32), value bigint)"));
    }

    @Override
    public void close()
            throws SQLException
    {
        for (String table : tables) {
            String sql = "DROP TABLE " + clickHouseServerTest.getSchema() + "." + table;
            connection.createStatement().execute(sql);
        }

        ClickHouseServerTest.shutDown();
    }

    public Connection getConnection()
    {
        return connection;
    }

    public ClickHouseClient getClickHouseClient()
    {
        return clickHouseClient;
    }

    public List<String> getTables()
    {
        return tables;
    }

    public String getSchema()
    {
        return clickHouseServerTest.getSchema();
    }

    public String getActualTable(String tablePattern)
    {
        return ClickHouseServerTest.getActualTable(tables, tablePattern);
    }

    private String buildCreateTableSql(String tableName, String columnInfo)
    {
        String newTableName = ClickHouseServerTest.generateNewTableName(tableName);
        tables.add(newTableName);
        String sql = "CREATE TABLE " + clickHouseServerTest.getSchema() + "." + newTableName + columnInfo + "engine=MergeTree() order by tuple()";
        System.out.println(sql);

        return sql;
    }
}
