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
package io.prestosql.plugin.jdbc;

import io.airlift.log.Logger;
import io.prestosql.plugin.basejdbc.ConnectionPoolFactory;
import io.prestosql.plugin.basejdbc.HetuConnectionObjectPool;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

/**
 * test for connection pool
 */
@Test(singleThreaded = true)
public class TestConnectionPool
{
    private static final Logger LOG = Logger.get(TestConnectionPool.class);
    private String connectionUrl = "mock:jdbc://localhost:port";

    private Driver driver = new MockDriver();

    private Properties connectionProperties;

    private ConnectionPoolFactory poolFactory;

    private HetuConnectionObjectPool genericObjectPool;

    @BeforeClass
    private void setUp()
    {
        connectionProperties = new Properties();
        connectionProperties.setProperty("user", "user");
        connectionProperties.setProperty("password", "password");
        connectionProperties.setProperty("useConnectionPool", "true");
        connectionProperties.setProperty("maxIdle", "2");
        connectionProperties.setProperty("minIdle", "0");
        connectionProperties.setProperty("maxTotal", "5");
        connectionProperties.setProperty("lifo", "true");
        connectionProperties.setProperty("fairness", "true");
        connectionProperties.setProperty("maxWaitMillis", "100");
        connectionProperties.setProperty("softMinEvictableIdleTimeMillis", "100");
        connectionProperties.setProperty("numTestsPerEvictionRun", "3");
        connectionProperties.setProperty("testOnCreate", "false");
        connectionProperties.setProperty("testOnBorrow", "true");
        connectionProperties.setProperty("testOnReturn", "false");
        connectionProperties.setProperty("testWhileIdle", "true");
        connectionProperties.setProperty("timeBetweenEvictionRunsMillis", "100");
        connectionProperties.setProperty("blockWhenExhausted", "true");
        connectionProperties.setProperty("jmxEnabled", "true");
        poolFactory = new ConnectionPoolFactory(driver, connectionUrl, connectionProperties);
        genericObjectPool = new HetuConnectionObjectPool(poolFactory,
                DriverConnectionFactory.createGenericObjectPoolConfig(connectionProperties), new AbandonedConfig());
    }

    @Test
    public void testConnectionPool()
    {
        try {
            Connection con1 = this.genericObjectPool.borrowObject();
            Connection con2 = this.genericObjectPool.borrowObject();
            Connection con3 = this.genericObjectPool.borrowObject();

            assertEquals(this.genericObjectPool.getNumActive(), 3);
            assertEquals(this.genericObjectPool.getCreatedCount(), 3);
            assertEquals(this.genericObjectPool.getReturnedCount(), 0);
            assertEquals(this.genericObjectPool.getNumIdle(), 0);

            con1.close();
            assertEquals(this.genericObjectPool.getNumActive(), 2);
            assertEquals(this.genericObjectPool.getCreatedCount(), 3);
            assertEquals(this.genericObjectPool.getReturnedCount(), 1);
            assertEquals(this.genericObjectPool.getNumIdle(), 1);

            con2.close();
            assertEquals(this.genericObjectPool.getNumActive(), 1);
            assertEquals(this.genericObjectPool.getCreatedCount(), 3);
            assertEquals(this.genericObjectPool.getReturnedCount(), 2);
            assertEquals(this.genericObjectPool.getNumIdle(), 2);

            con3.close();
            assertEquals(this.genericObjectPool.getNumActive(), 0);
            assertEquals(this.genericObjectPool.getCreatedCount(), 3);
            assertEquals(this.genericObjectPool.getReturnedCount(), 3);
            assertEquals(this.genericObjectPool.getNumIdle(), 2);
        }
        catch (Exception e) {
            throw new RuntimeException("testConnectionPool failed");
        }
    }
}
