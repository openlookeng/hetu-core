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

package io.hetu.core.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.docker.DockerContainer;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static java.lang.String.format;

/**
 * TestingOracleServer
 *
 * @since 2019-08-28
 */

public class TestingOracleServer
        implements Closeable
{
    /**
     * user
     */
    public static final String USER = "test";

    /**
     * password
     */
    public static final String PASSWORD = "test";

    private static final int ORACLE_SERVER_PORT = 1521;
    private static final long MEMORY_SIZE = 15 * 1024 * 1024 * 1024L;
    private static final String SQL_STATEMENT = "select 1 from dual";
    private final DockerContainer dockerContainer;

    /**
     * constructor
     */
    public TestingOracleServer()
    {
        this.dockerContainer = new DockerContainer(
                "oracle-12c:latest",
                ImmutableList.of(ORACLE_SERVER_PORT),
                ImmutableMap.of(
                        "USER", USER,
                        "PASSWORD", PASSWORD),
                portProvider -> TestingOracleServer.helthCheck(portProvider), MEMORY_SIZE);
    }

    private static void helthCheck(DockerContainer.HostPortProvider hostPortProvider)
            throws Exception
    {
        String jdbcUrl = getJdbcUrl(hostPortProvider);
        try {
            Class.forName(Constants.ORACLE_JDBC_DRIVER_CLASS_NAME);
            Connection conn = DriverManager.getConnection(jdbcUrl, USER, PASSWORD);
            if (conn != null) {
                Statement stmt = conn.createStatement();
                stmt.execute(SQL_STATEMENT);
            }
        }
        catch (ClassNotFoundException e) {
            throw new ClassNotFoundException("Class not found: " + Constants.ORACLE_JDBC_DRIVER_CLASS_NAME, e);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to execute statement: " + SQL_STATEMENT, e);
        }
    }

    private static String getJdbcUrl(DockerContainer.HostPortProvider hostPortProvider)
    {
        return format("jdbc:oracle:thin:@//localhost:%s/orcl", hostPortProvider.getHostPort(ORACLE_SERVER_PORT));
    }

    /**
     * getJdbcUrl
     *
     * @return String
     */
    public String getJdbcUrl()
    {
        return getJdbcUrl(dockerContainer::getHostPort);
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
