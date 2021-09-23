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
package io.hetu.core.metastore;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.hetu.core.metastore.jdbc.JdbcMetastoreModule;
import io.prestosql.plugin.base.jmx.MBeanServerModule;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.metastore.HetuMetastore;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.hetu.core.metastore.MetaStoreConstants.LOCAL;
import static io.prestosql.spi.metastore.HetuErrorCode.HETU_METASTORE_CODE;
import static org.testng.Assert.assertTrue;

/**
 * test jdbc hetu metastore
 *
 * @since 2020-03-18
 */
@Test(singleThreaded = true)
public class TestHetuMetastoreModule
{
    private TestingMysqlDatabase database;
    private final String user = "user";
    private final String password = "testpass";

    /**
     * setUp
     *
     * @throws Exception Exception
     */
    @BeforeClass
    public void setUp()
            throws Exception
    {
        database = new TestingMysqlDatabase(user, password, "metastoremodule");
    }

    /**
     * tearDown
     *
     * @throws Exception Exception
     */
    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        database.close();
    }

    /**
     * test jdbc metastore
     */
    @Test
    public void testJdbcMetastore()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hetu.metastore.db.url", database.getUrl())
                .put("hetu.metastore.db.user", user)
                .put("hetu.metastore.db.password", password)
                .put("hetu.metastore.cache.ttl", "0s")
                .build();
        try {
            String type = LOCAL;
            Bootstrap app = new Bootstrap(
                    new MBeanModule(),
                    new MBeanServerModule(),
                    new JdbcMetastoreModule(type));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(properties)
                    .initialize();
            assertTrue(injector.getInstance(HetuMetastore.class) instanceof HetuMetastoreCache);
        }
        catch (Exception ex) {
            throwIfUnchecked(ex);
            throw new PrestoException(HETU_METASTORE_CODE,
                    "init jdbc metastore module failed.");
        }
    }

    /**
     * test hetu metastore
     *
     * @since 2020-03-19
     */
    static class TestPrestoMetastore
    {
        private HetuMetastore hetuMetastore;

        @Inject
        TestPrestoMetastore(HetuMetastore hetuMetastore)
        {
            this.hetuMetastore = hetuMetastore;
        }

        public HetuMetastore getHetuMetastore()
        {
            return hetuMetastore;
        }
    }
}
