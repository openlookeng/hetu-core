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

package io.hetu.core.plugin.opengauss;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.plugin.postgresql.TestPostgreSqlExtendServer;
import io.prestosql.testing.docker.DockerContainer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.hetu.core.plugin.opengauss.TestOpenGaussConstants.OPENGAUSS_UT_CONFIG_FILE_PATH;
import static java.lang.String.format;

public class TestOpenGaussServer
        extends TestPostgreSqlExtendServer
        implements Closeable
{
    private static final Logger LOG = Logger.get(TestOpenGaussServer.class);

    private static String user;
    private static String db;
    private static String passWd;
    private static String connectionUrl;
    private static int port;
    private static String dockerImage;
    private static final long MEMORY_SIZE = 15 * 1024 * 1024 * 1024L;
    private static final String SQL_STATEMENT = "select 1 from dual";
    private final DockerContainer dockerContainer;

    static {
        File file = new File(OPENGAUSS_UT_CONFIG_FILE_PATH);

        try {
            Map<String, String> properties = new HashMap<>(loadPropertiesFrom(file.getPath()));
            connectionUrl = properties.get("connection.url");
            db = properties.get("connection.db");
            user = properties.get("connection.user");
            passWd = properties.get("connection.password");
            dockerImage = properties.get("docker.image");
            port = Integer.parseInt(properties.get("connection.port"));
        }
        catch (IOException e) {
            LOG.warn("Failed to load properties for file %s", file);
        }
    }

    public TestOpenGaussServer()
    {
        this.dockerContainer = new DockerContainer(
                dockerImage,
                ImmutableList.of(port),
                ImmutableMap.of(
                        "GS_PASSWORD", passWd,
                        "GS_DB", db),
                this::healthCheck, MEMORY_SIZE);
    }

    private void healthCheck(DockerContainer.HostPortProvider hostPortProvider)
            throws Exception
    {
        String jdbcUrl = getJdbcUrl(hostPortProvider);
        LOG.info("connection jdbcurl: " + jdbcUrl);
        execute(SQL_STATEMENT, jdbcUrl);
    }

    public void execute(String sql) throws Exception
    {
        execute(sql, getJdbcUrl());
    }

    public void execute(String sql, String jdbcUrl) throws Exception
    {
        try {
            Class.forName("org.postgresql.Driver");
            try (Connection conn = DriverManager.getConnection(jdbcUrl, user, passWd)) {
                LOG.info("Connection succeed!");
                if (conn != null) {
                    conn.setReadOnly(false);
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute(sql);
                    }
                }
            }
        }
        catch (ClassNotFoundException e) {
            throw new ClassNotFoundException("Class not found: ", e);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to execute statement: " + SQL_STATEMENT, e);
        }
    }

    private String getJdbcUrl(DockerContainer.HostPortProvider hostPortProvider)
    {
        return format(connectionUrl, hostPortProvider.getHostPort(port), db);
    }

    /**
     * getJdbcU
     *
     * @return String
     */
    public String getJdbcUrl()
    {
        return format(connectionUrl, dockerContainer.getHostPort(port), db);
    }

    /**
     * get connection user name
     *
     * @return String
     */
    public String getUser()
    {
        return user;
    }

    /**
     * get connection pass word
     *
     * @return String
     */
    public String getPassWd()
    {
        return passWd;
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
