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
package io.hetu.core.plugin.kylin;

import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcClient;
import org.h2.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

final class TestingDatabase
        implements AutoCloseable
{
    private final Connection connection;
    private final JdbcClient jdbcClient;

    public TestingDatabase(String user, String pwd)
            throws SQLException
    {
        String connectionUrl = "jdbc:h2:mem:test" + System.nanoTime() + ThreadLocalRandom.current().nextLong();
        jdbcClient = new BaseJdbcClient(
                new BaseJdbcConfig(),
                "\"",
                new DriverConnectionFactory(new Driver(), connectionUrl, Optional.empty(), Optional.empty(), new Properties()));

        connection = DriverManager.getConnection(connectionUrl, user, pwd);
        executeQuery("CREATE SCHEMA example");

        executeQuery("CREATE TABLE example.numbers(text varchar primary key, text_short varchar(32), value bigint)");
        executeQuery("INSERT INTO example.numbers(text, text_short, value) VALUES "
                + "('one', 'one', 1),"
                + "('two', 'two', 2),"
                + "('three', 'three', 3),"
                + "('ten', 'ten', 10),"
                + "('eleven', 'eleven', 11),"
                + "('twelve', 'twelve', 12)"
                + "");
        executeQuery("CREATE TABLE example.view_source(id varchar primary key)");
        executeQuery("CREATE VIEW example.view AS SELECT id FROM example.view_source");
        executeQuery("CREATE SCHEMA tpch");
        executeQuery("CREATE TABLE tpch.orders(orderkey bigint primary key, custkey bigint)");
        executeQuery("CREATE TABLE tpch.lineitem(orderkey bigint primary key, partkey bigint)");

        executeQuery("CREATE SCHEMA exa_ple");
        executeQuery("CREATE TABLE exa_ple.num_ers(te_t varchar primary key, \"VA%UE\" bigint)");
        executeQuery("CREATE TABLE exa_ple.table_with_float_col(col1 bigint, col2 double, col3 float, col4 real)");

        connection.commit();
    }

    private void executeQuery(String query)
            throws SQLException
    {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(query);
        }
        finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    @Override
    public void close()
            throws SQLException
    {
        connection.close();
    }

    public Connection getConnection()
    {
        return connection;
    }
}
