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

package io.hetu.core.plugin.carbondata.server;

import com.google.common.collect.ImmutableMap;
import io.hetu.core.plugin.carbondata.CarbondataPlugin;
import io.prestosql.Session;
import io.prestosql.execution.QueryIdGenerator;
import io.prestosql.jdbc.PrestoStatement;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.plugin.hive.HivePlugin;
import io.prestosql.spi.security.Identity;
import io.prestosql.sql.parser.SqlParserOptions;
import io.prestosql.tests.DistributedQueryRunner;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;

public class HetuTestServer
{
    private static final String carbonDataCatalog = "carbondata";
    private static final String carbonDataCatalogLocationDisabled = "carbondatacataloglocationdisabled";
    private static final String carbonDataConnector = "carbondata";
    private static final String carbonDataSource = "carbondata";

    private static final Logger logger =
            LogServiceFactory.getLogService(HetuTestServer.class.getName());
    private ThreadLocalRandom random = ThreadLocalRandom.current();
    private final int port = random.nextInt(1024, 15000);

    private Map<String, String> carbonProperties = new HashMap<>();
    private Map<String, String> hetuProperties = new HashMap<String, String>() {{
        put("http-server.http.port", String.valueOf(port));
    }};

    private String dbName;
    private DistributedQueryRunner queryRunner;
    private PrestoStatement statement;

    public HetuTestServer() throws Exception
    {
    }

    public void startServer(String dbName, Map<String, String> properties, Map<String, String> configProperties) throws Exception
    {
        queryRunner = new DistributedQueryRunner(createSession(), 4, hetuProperties, configProperties,  new SqlParserOptions(), "testing", Optional.empty());
        this.dbName = dbName;
        startServer(properties);
    }

    public void startServer(String dbName, Map<String, String> properties) throws Exception
    {
        queryRunner = new DistributedQueryRunner(createSession(), 4, hetuProperties);
        this.dbName = dbName;
        startServer(properties);
    }

    public void startServer(Map<String, String> properties) throws Exception
    {
        carbonProperties.putAll(properties);

        logger.info("------------ Starting Presto Server -------------");
        DistributedQueryRunner queryRunner = createQueryRunner(hetuProperties);
        Connection connection = createJdbcConnection(dbName);
        statement = (PrestoStatement) connection.createStatement();

        logger.info("STARTED SERVER AT :" + queryRunner.getCoordinator().getBaseUrl());
    }

    public void stopServer() throws SQLException
    {
        queryRunner.close();
        statement.close();
        logger.info("------------ Stopping Presto Server -------------");
    }

    public boolean execute(String query) throws SQLException
    {
        logger.info(">>>>> Executing Statement: " + query);
        boolean result = false;
        try {
            result = statement.execute(query);
        }
        catch (SQLException e) {
            logger.error("Exception Occured: " + e.getMessage() + "\n Failed Query: " + query);
            throw e;
        }

        return result;
    }

    public List<Map<String, Object>> executeQuery(String query) throws SQLException
    {
        logger.info(">>>>> Executing Query: " + query);
        try {
            ResultSet rs = statement.executeQuery(query);
            return convertResultSetToList(rs);
        }
        catch (SQLException e) {
            logger.error("Exception Occured: " + e.getMessage() + "\n Failed Query: " + query);
            throw e;
        }
    }

    private List<Map<String, Object>> convertResultSetToList(ResultSet rs) throws SQLException
    {
        List<Map<String, Object>> result = new LinkedList<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
            result.add(new TreeMap<String, Object>()
            {{
                for (int i = 1; i <= columnCount; i++) {
                    put(metaData.getColumnName(i), rs.getObject(i));
                }
            }});
        }

        return result;
    }

    private Session createSession()
    {
        logger.info("\nCreating THe Hetu Server Session");
        return Session.builder(new SessionPropertyManager())
                .setQueryId(new QueryIdGenerator().createNextQueryId())
                .setIdentity(new Identity("user", Optional.empty()))
                .setSource(carbonDataSource).setCatalog(carbonDataCatalog)
                .setTimeZoneKey(UTC_KEY).setLocale(Locale.ENGLISH)
                .setRemoteUserAddress("address")
                .setUserAgent("agent").build();
    }

    private Connection createJdbcConnection(String dbName) throws ClassNotFoundException, SQLException
    {
        String driver = "io.prestosql.jdbc.PrestoDriver";
        String url = null;

        if (StringUtils.isEmpty(dbName)) {
            url = "jdbc:presto://localhost:" + port + "/carbondata/default";
        }
        else {
            url = "jdbc:presto://localhost:" + port + "/carbondata/" + dbName;
        }

        Properties properties = new Properties();
        properties.setProperty("user", "test");

        Class.forName(driver);

        return DriverManager.getConnection(url, properties);
    }

    private DistributedQueryRunner createQueryRunner(Map<String, String> hetuProperties)
    {
        try {
            queryRunner.installPlugin(new CarbondataPlugin());
            Map<String, String> carbonProperties = ImmutableMap.<String, String>builder()
                    .putAll(this.carbonProperties)
                    .put("carbon.unsafe.working.memory.in.mb", "512")
                    .build();
            Map<String, String> carbonPropertiesLocationDisabled = ImmutableMap.<String, String>builder()
                    .putAll(this.carbonProperties)
                    .put("carbon.unsafe.working.memory.in.mb", "512")
                    .put("hive.table-creates-with-location-allowed", "false")
                    .build();

            // CreateCatalog will create a catalog for CarbonData in etc/catalog.
            queryRunner.createCatalog(carbonDataCatalog, carbonDataConnector, carbonProperties);
            queryRunner.createCatalog(carbonDataCatalogLocationDisabled, carbonDataConnector, carbonPropertiesLocationDisabled);
        }
        catch (RuntimeException e) {
            queryRunner.close();
            throw e;
        }

        return queryRunner;
    }

    public void addHiveCatalogToQueryRunner(Map<String, String> hiveProperties)
    {
        queryRunner.installPlugin(new HivePlugin("hive"));
        queryRunner.createCatalog("hive", "hive", hiveProperties);
    }

    public CatalogManager getCatalog()
    {
        return queryRunner.getCatalogManager();
    }
}
