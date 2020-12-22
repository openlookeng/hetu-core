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

package io.prestosql.catalog;

import com.google.common.io.Resources;
import com.google.inject.Key;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static com.google.common.collect.Maps.fromProperties;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestCatalogStore
        extends TestDynamicCatalogRunner
{
    public TestCatalogStore()
            throws Exception
    {
    }

    private Properties createProperties()
    {
        Properties properties = new Properties();
        properties.put("hive.hdfs.impersonation.enabled", "false");
        properties.put("hive.hdfs.authentication.type", "KERBEROS");
        properties.put("hive.collect-column-statistics-on-write", "true");
        properties.put("hive.metastore.service.principal", "hive/hadoop.hadoop.com@HADOOP.COM");
        properties.put("hive.metastore.authentication.type", "KERBEROS");
        properties.put("hive.metastore.uri", "thrift://localhost:21088,thrift://localhost:21088");
        properties.put("hive.allow-drop-table", "true");
        properties.put("hive.config.resources", "core-site.xml, hdfs-site.xml");
        properties.put("hive.hdfs.hetu.keytab", "user.keytab");
        properties.put("hive.metastore.krb5.conf.path", "krb5.conf");
        properties.put("hive.metastore.client.keytab", "user.keytab");
        properties.put("hive.metastore.client.principal", "test@HADOOP.COM");
        properties.put("hive.hdfs.wire-encryption.enabled", "true");
        properties.put("hive.hdfs.hetu.principal", "test@HADOOP.COM");
        properties.put("connector.name", "hive-hadoop2");

        return properties;
    }

    private Properties createMetadata()
    {
        Properties metadata = new Properties();
        metadata.put("version", "9d3fe902-a0f0-4556-81b7-2d5cf5e21518");
        metadata.put("createdTime", "0");
        metadata.put("catalogFiles", "[ \"hdfs-site.xml\", \"core-site.xml\", \"user.keytab\" ]");
        metadata.put("globalFiles", "[\"krb5.conf\"]");
        return metadata;
    }

    private boolean isFileEqual(Path first, Path second)
            throws IOException
    {
        try (InputStream oldInStream = Files.newInputStream(first);
                InputStream newInStream = Files.newInputStream(second)) {
            int firstChar;
            int secondChar;
            while (true) {
                firstChar = oldInStream.read();
                secondChar = newInStream.read();
                if (firstChar != -1 && secondChar != -1) {
                    if (firstChar != secondChar) {
                        return false;
                    }
                }
                else {
                    break;
                }
            }
            return true;
        }
    }

    private Map<String, String> difference(Map<String, String> first, Map<String, String> second)
    {
        Map<String, String> diff = new HashMap<>();
        first.entrySet().stream()
                .forEach(entry -> {
                    if (!second.keySet().contains(entry.getKey())) {
                        diff.put(entry.getKey(), entry.getValue());
                    }
                });
        return diff;
    }

    private boolean isPropertiesEqual(Path first, Path second)
            throws IOException
    {
        Map<String, String> firstProperties = loadPropertiesFrom(first.toFile().getPath());
        Map<String, String> secondProperties = loadPropertiesFrom(second.toFile().getPath());
        if (!difference(secondProperties, firstProperties).isEmpty()) {
            return false;
        }

        if (!difference(firstProperties, secondProperties).isEmpty()) {
            return false;
        }
        return true;
    }

    @Test
    public void testCreateCatalog()
            throws IOException
    {
        String catalogName = "hive";

        String orgBaseDirectory = Resources.getResource("dynamiccatalog").getPath();

        // create file system client.
        FileSystemClientManager fileSystemClientManager = server.getInstance(Key.get(FileSystemClientManager.class));
        HetuFileSystemClient client = fileSystemClientManager.getFileSystemClient("local-config-catalog", Paths.get(orgBaseDirectory));

        // create hive.properties file.
        CatalogFilePath orgCatalogPath = new CatalogFilePath(orgBaseDirectory, catalogName);
        Properties properties = createProperties();
        try (OutputStream outputStream = client.newOutputStream(orgCatalogPath.getPropertiesPath())) {
            properties.store(outputStream, "dynamic catalog store");
        }

        // create hive.metadata file.
        Properties metadata = createMetadata();
        try (OutputStream outputStream = client.newOutputStream(orgCatalogPath.getMetadataPath())) {
            metadata.store(outputStream, "dynamic catalog store");
        }

        // create catalog.
        CatalogInfo catalogInfo = new CatalogInfo("hive",
                "hadoop-hive2",
                "",
                0,
                UUID.randomUUID().toString(),
                fromProperties(properties));
        CatalogFileInputStream configFiles = new LocalCatalogStore(orgBaseDirectory, client, 128 * 1024).getCatalogFiles("hive");
        CatalogStore localCatalogStore = new LocalCatalogStore(localPath, client, 128 * 1024);
        localCatalogStore.createCatalog(catalogInfo, configFiles);

        // check the result
        CatalogFilePath localCatalogPath = new CatalogFilePath(localPath, catalogName);
        String catalogDirectory = localCatalogPath.getCatalogDirPath().toString();
        String globalDirectory = localCatalogPath.getGlobalDirPath().toString();
        assertTrue(Files.exists(Paths.get(catalogDirectory, "hdfs-site.xml")));
        assertTrue(isFileEqual(Paths.get(catalogDirectory, "hdfs-site.xml"),
                Paths.get(orgCatalogPath.getCatalogDirPath().toString(), "hdfs-site.xml")));
        assertTrue(Files.exists(Paths.get(catalogDirectory, "core-site.xml")));
        assertTrue(isFileEqual(Paths.get(catalogDirectory, "core-site.xml"),
                Paths.get(orgCatalogPath.getCatalogDirPath().toString(), "core-site.xml")));
        assertTrue(Files.exists(Paths.get(catalogDirectory, "user.keytab")));
        assertTrue(isFileEqual(Paths.get(catalogDirectory, "user.keytab"),
                Paths.get(orgCatalogPath.getCatalogDirPath().toString(), "user.keytab")));
        assertTrue(Files.exists(Paths.get(globalDirectory, "krb5.conf")));
        assertTrue(isFileEqual(Paths.get(globalDirectory, "krb5.conf"),
                Paths.get(orgCatalogPath.getGlobalDirPath().toString(), "krb5.conf")));
        assertTrue(Files.exists(localCatalogPath.getPropertiesPath()));
        assertTrue(isPropertiesEqual(localCatalogPath.getPropertiesPath(),
                Paths.get(orgBaseDirectory, "catalog", "hive.properties")));
        assertTrue(Files.exists(Paths.get(catalogDirectory, "hive.metadata")));
        assertTrue(isPropertiesEqual(Paths.get(catalogDirectory, "hive.metadata"),
                Paths.get(orgCatalogPath.getCatalogDirPath().toString(), "hive.metadata")));

        // delete catalog files from file store.
        localCatalogStore.deleteCatalog("hive", true);

        // check the result.
        assertFalse(Files.exists(Paths.get(catalogDirectory, "hdfs-site.xml")));
        assertFalse(Files.exists(Paths.get(catalogDirectory, "core-site.xml")));
        assertFalse(Files.exists(Paths.get(catalogDirectory, "user.keytab")));
        assertFalse(Files.exists(Paths.get(globalDirectory, "krb5.conf")));
        assertFalse(Files.exists(Paths.get(localPath, "hive.properties")));
        assertFalse(Files.exists(Paths.get(catalogDirectory, "hive.metadata")));
    }
}
