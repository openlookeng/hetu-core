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

package io.hetu.core.plugin.heuristicindex.datasource.hive;

import org.apache.hadoop.security.UserGroupInformation;
import org.testng.SkipException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestThriftHiveMetaStoreService
{
    private DockerizedHive container;

    Properties properties;

    public TestThriftHiveMetaStoreService()
    {
    }

    @BeforeClass
    public void setup()
            throws SQLException, IOException
    {
        container = DockerizedHive.getInstance(this.getClass().getName());

        if (container == null) {
            throw new SkipException("Docker environment for this test not setup");
        }

        properties = container.getHiveProperties();
        // Create a table
        try (Connection con = DriverManager.getConnection(container.getHiveJdbcUri(), "hive", "")) {
            Statement stmt = con.createStatement();
            stmt.execute("CREATE SCHEMA IF NOT EXISTS unittest");
            stmt.execute("CREATE TABLE unittest.person (name string) STORED AS ORC");
        }
    }

    @Test
    public void testGetTableMetadata()
    {
        ThriftHiveMetaStoreService hiveMetaStoreService = new ThriftHiveMetaStoreService(properties);
        TableMetadata tableMetadata = hiveMetaStoreService.getTableMetadata("unittest", "person");
        assertNotNull(tableMetadata.getUri());
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testGetNonExistentTable()
    {
        ThriftHiveMetaStoreService hiveMetaStoreService = new ThriftHiveMetaStoreService(properties);
        TableMetadata tableMetadata = hiveMetaStoreService.getTableMetadata("unittest", "i_am_not_there");
        assertNotNull(tableMetadata.getUri());
    }

    @Test
    public void testNonExistentMetadata()
    {
        ThriftHiveMetaStoreService hiveMetaStoreService = new ThriftHiveMetaStoreService(properties);
        assertThrows(IllegalStateException.class, () -> hiveMetaStoreService.getTableMetadata("bogus_db", "bogus_table"));
    }

    @Test
    public void testMissingConfigs()
    {
        assertThrows(NullPointerException.class, () -> new ThriftHiveMetaStoreService(null));
    }

    @Test
    public void testKerberizedMetaStore()
            throws IOException
    {
        // This needs to be synchronized because Kerberos authentication methods are static.
        // There might be issues when unit tests run in parallel if it's not synchronized.
        synchronized (UserGroupInformation.class) {
            // This only tests whether the set up is correctly because we don't have running kerberized instance
            metaStoreConnectionExceptionHelper(
                    TestConstantsHelper.DOCKER_KERBERIZED_HIVE_PROPERTIES_FILE_LOCATION,
                    container.getHiveProperties().getProperty(ConstantsHelper.HIVE_METASTORE_URI),
                    RuntimeException.class);
            assertTrue(UserGroupInformation.isSecurityEnabled());
            UserGroupInformation.reset();
        }
    }

    @Test
    public void testInvalidThriftMetaStoreUri()
            throws IOException
    {
        // Port out of range should be thrown
        metaStoreConnectionExceptionHelper(
                TestConstantsHelper.DOCKER_KERBERIZED_HIVE_PROPERTIES_FILE_LOCATION,
                "thrift://this-host-name-should-not-exist:999999",
                IllegalArgumentException.class);

        // Should fail to connect to the uri
        metaStoreConnectionExceptionHelper(
                TestConstantsHelper.DOCKER_KERBERIZED_HIVE_PROPERTIES_FILE_LOCATION,
                "thrift://this-host-name-should-not-exist:9000",
                IllegalStateException.class);

        String thriftUri = container.getHiveProperties().getProperty(ConstantsHelper.HIVE_METASTORE_URI);
        Integer port = Integer.valueOf(thriftUri.substring(thriftUri.lastIndexOf(":") + 1));
        thriftUri = thriftUri.replace(port.toString(), String.valueOf(port + 1));
        metaStoreConnectionExceptionHelper(
                TestConstantsHelper.DOCKER_KERBERIZED_HIVE_PROPERTIES_FILE_LOCATION,
                thriftUri,
                IllegalStateException.class);
    }

    public void metaStoreConnectionExceptionHelper(String hivePropertiesFileLocation, String thriftUri, Class exception)
            throws IOException
    {
        Properties props = new Properties();
        try (InputStream is = new FileInputStream(hivePropertiesFileLocation)) {
            props.load(is);
        }
        props.setProperty(ConstantsHelper.HIVE_METASTORE_URI, thriftUri);
        expectThrows(exception, () -> new ThriftHiveMetaStoreService(props));
    }

    @AfterTest
    public void cleanUp()
    {
        if (container != null) {
            container.close();
        }
    }
}
