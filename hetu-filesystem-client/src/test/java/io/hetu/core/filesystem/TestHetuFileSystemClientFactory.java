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
package io.hetu.core.filesystem;

import com.google.common.io.Resources;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.filesystem.HetuFileSystemClientFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

import static org.testng.Assert.assertSame;

/**
 * Test for client factories
 *
 * @since 2020-04-30
 */
public class TestHetuFileSystemClientFactory
{
    private static final String testFSConfigPath = "/tmp/test-fs.properties";

    private static String getResourcePath(String resourceName)
    {
        return Resources.getResource(resourceName).getPath();
    }

    /**
     * Test creating HetuLocalFileSystemClient
     *
     * @throws IOException
     */
    @Test
    public void testCreateLocalFS()
            throws IOException
    {
        HetuFileSystemClientFactory factory = new LocalFileSystemClientFactory();
        Properties properties = new Properties();
        properties.setProperty("fs.client.type", "local");
        try (HetuFileSystemClient fs = factory.getFileSystemClient(properties, Paths.get("/"))) {
            assertSame(fs.getClass(), HetuLocalFileSystemClient.class);
        }
    }

    /**
     * Test creating HetuHdfsFileSystemClient
     *
     * @throws IOException
     */
    @Test
    public void testCreateHdfsFS()
            throws IOException
    {
        HetuFileSystemClientFactory factory = new HdfsFileSystemClientFactory();
        Properties properties = new Properties();
        properties.setProperty("fs.client.type", "hdfs");
        properties.setProperty("hdfs.config.resources",
                getResourcePath("docker_config/core-site.xml") + "," + getResourcePath("docker_config/hdfs-site.xml"));
        properties.setProperty("hdfs.authentication.type", "KERBEROS");
        properties.setProperty("hdfs.krb5.conf.path", getResourcePath("docker_config/docker_krb5.conf"));
        properties.setProperty("hdfs.krb5.keytab.path", getResourcePath("docker_config/user.keytab"));
        properties.setProperty("hdfs.krb5.principal", "user@HADOOP.COM");

        try (HetuFileSystemClient fs = factory.getFileSystemClient((properties), Paths.get("/"))) {
            assertSame(fs.getClass(), HetuHdfsFileSystemClient.class);
        }
    }
}
