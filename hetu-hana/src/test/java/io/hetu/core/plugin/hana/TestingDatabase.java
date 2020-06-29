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
package io.hetu.core.plugin.hana;

import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.spi.PrestoException;

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

public class TestingDatabase
        implements AutoCloseable
{
    private final Connection connection;

    private final HanaClient hanaClient;

    private TestingHanaServer testingHanaServer;

    private List<String> tables = new ArrayList<>();

    /**
     * constructor
     */
    public TestingDatabase(TestingHanaServer hanaServer) throws SQLException
    {
        this.testingHanaServer = hanaServer;

        BaseJdbcConfig jdbcConfig = new BaseJdbcConfig();
        HanaConfig hanaConfig = new HanaConfig();

        jdbcConfig.setConnectionUrl(hanaServer.getJdbcUrl());
        jdbcConfig.setConnectionUser(hanaServer.getUser());
        jdbcConfig.setConnectionPassword(hanaServer.getPassword());

        hanaConfig.setTableTypes("TABLE,VIEW");
        hanaConfig.setSchemaPattern(hanaServer.getSchema());
        hanaConfig.setQueryPushDownEnabled(false);

        Driver driver = null;
        try {
            driver = (Driver) Class.forName(HanaConstants.SAP_HANA_JDBC_DRIVER_CLASS_NAME).getConstructor(((Class<?>[]) null)).newInstance();
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

        hanaClient = new HanaClient(jdbcConfig, hanaConfig, connectionFactory);
        connection =
            DriverManager.getConnection(hanaServer.getJdbcUrl(), hanaServer.getUser(), hanaServer.getPassword());

        connection.createStatement()
            .execute(buildCreateTableSql("example", "(text varchar primary key,text_short varchar(32), value bigint)"));
        connection.createStatement().execute(buildCreateTableSql("student", "(id varchar primary key)"));
        connection.createStatement()
            .execute(buildCreateTableSql("table_with_float_col", "(col1 bigint, col2 double, col3 float, col4 real)"));
        connection.createStatement()
            .execute(buildCreateTableSql("number", "(text varchar primary key, text_short varchar(32), value bigint)"));
    }

    @Override
    public void close() throws SQLException
    {
        for (String table : tables) {
            String sql = "DROP TABLE " + testingHanaServer.getSchema() + "." + table;
            connection.createStatement().execute(sql);
        }

        TestingHanaServer.shutDown();
    }

    public Connection getConnection()
    {
        return connection;
    }

    public HanaClient getHanaClient()
    {
        return hanaClient;
    }

    public List<String> getTables()
    {
        return tables;
    }

    public String getSchema()
    {
        return testingHanaServer.getSchema();
    }

    public String getActualTable(String tablePattern)
    {
        return TestingHanaServer.getActualTable(tables, tablePattern);
    }

    private String buildCreateTableSql(String tableName, String columnInfo)
    {
        String newTableName = TestingHanaServer.generateNewTableName(tableName);
        tables.add(newTableName);

        return "CREATE TABLE " + testingHanaServer.getSchema() + "." + newTableName + columnInfo;
    }
}
