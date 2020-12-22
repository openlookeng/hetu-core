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

import io.airlift.log.Logger;
import io.airlift.tpch.TpchTable;

import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.hetu.core.plugin.hana.TestHanaConstants.TEST_PROPERTY_FILE_PATH;

public final class TestingHanaServer
{
    private static final Logger LOG = Logger.get(TestingHanaServer.class);

    @GuardedBy("this")
    private static int referenceCount;

    @GuardedBy("this")
    private static TestingHanaServer instance;

    private final AtomicBoolean tpchLoaded = new AtomicBoolean(false);
    private final CountDownLatch tpchLoadComplete = new CountDownLatch(1);
    private static List<String> actualTables = new ArrayList<>();
    private final AtomicBoolean isServerUsable = new AtomicBoolean(false);
    private final AtomicInteger atomicIntegerTimes = new AtomicInteger(0);
    private String jdbcUrl;
    private String user;
    private String password;
    private String schema;

    public static synchronized TestingHanaServer getInstance()
    {
        if (referenceCount == 0) {
            instance = new TestingHanaServer();
            instance.startup();
        }
        referenceCount++;
        return instance;
    }

    public static synchronized void shutDown() throws SQLException
    {
        referenceCount--;
        if (referenceCount == 0) {
            instance.shutdown();
            instance = null;
        }
    }

    private void startup()
    {
        for (TpchTable<?> table : TpchTable.getTables()) {
            String newTable = generateNewTableName(table.getTableName());
            actualTables.add(newTable);
        }
    }

    private void shutdown() throws SQLException
    {
        if (isHanaServerAvailable() && tpchLoaded.get()) {
            for (String table : actualTables) {
                String sql = "DROP TABLE " + schema + "." + table;
                executeInHana(sql);
            }
        }
    }

    private void executeInHana(String sql) throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private TestingHanaServer()
    {
        File file = new File(TEST_PROPERTY_FILE_PATH);
        try {
            Map<String, String> properties = new HashMap<>(loadPropertiesFrom(file.getPath()));
            LOG.info("test-hana properties: %s", properties);

            String connectionUrl = properties.get("connection.url");
            String connectionUser = properties.get("connection.user");
            String connectionPass = properties.get("connection.password");
            String connectionSchema = properties.get("connection.schema");

            if (connectionUrl != null) {
                this.jdbcUrl = connectionUrl;
            }

            if (connectionUser != null) {
                this.user = connectionUser;
            }

            if (connectionPass != null) {
                this.password = connectionPass;
            }

            if (connectionSchema != null) {
                this.schema = connectionSchema;
            }
        }
        catch (IOException e) {
            LOG.warn("Failed to load properties for file %s", file);
        }
    }

    public boolean isHanaServerAvailable()
    {
        if (atomicIntegerTimes.getAndIncrement() > 0) {
            return isServerUsable.get();
        }

        try {
            Connection connection = DriverManager.getConnection(this.jdbcUrl, this.user, this.password);
            isServerUsable.set(true);
            return isServerUsable.get();
        }
        catch (SQLException e) {
            isServerUsable.set(false);
            return isServerUsable.get();
        }
    }

    public String getJdbcUrl()
    {
        return jdbcUrl;
    }

    public String getUser()
    {
        return user;
    }

    public String getPassword()
    {
        return password;
    }

    public String getSchema()
    {
        return schema;
    }

    public boolean isTpchLoaded()
    {
        return tpchLoaded.getAndSet(true);
    }

    public void setTpchLoaded()
    {
        tpchLoadComplete.countDown();
    }

    public void waitTpchLoaded()
            throws InterruptedException
    {
        tpchLoadComplete.await(2, TimeUnit.MINUTES);
    }

    public static String generateNewTableName(String tableName)
    {
        return tableName + "_" + UUID.randomUUID().toString().replace("-", "");
    }

    public static String getActualTable(String tablePattern)
    {
        return getActualTable(actualTables, tablePattern);
    }

    public static String getActualTable(List<String> tables, String tablePattern)
    {
        String actualTable = tablePattern;

        for (String table : tables) { //tableName + _ + UUID
            int lastIndex = table.lastIndexOf("_");
            if (lastIndex == -1) {
                continue;
            }

            if (table.substring(0, lastIndex).equalsIgnoreCase(tablePattern)) {
                actualTable = tablePattern + table.substring(lastIndex);
                break;
            }
        }
        return actualTable;
    }
}
