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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Key;
import io.airlift.json.JsonCodec;
import io.hetu.core.filesystem.HetuFileSystemClientPlugin;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.security.SecurityKeyManager;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.prestosql.client.PrestoHeaders.PRESTO_USER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.NO_CONTENT;
import static javax.ws.rs.core.Response.Status.OK;

public class TestDynamicCatalogRunner
{
    private static final JsonCodec<CatalogInfo> CATALOG_INFO_CODEC = JsonCodec.jsonCodec(CatalogInfo.class);

    final Map<String, String> tpchProperties = new ImmutableMap.Builder<String, String>()
            .put("tpch.splits-per-node", "4")
            .build();
    final Map<String, String> hiveProperties = new ImmutableMap.Builder<String, String>()
            .put("hive.hdfs.impersonation.enabled", "false")
            .put("hive.hdfs.authentication.type", "KERBEROS")
            .put("hive.collect-column-statistics-on-write", "true")
            .put("hive.metastore.service.principal", "hive/hadoop.hadoop.com@HADOOP.COM")
            .put("hive.metastore.authentication.type", "KERBEROS")
            .put("hive.metastore.uri", "thrift://localhost:21088,thrift://localhost:21088")
            .put("hive.allow-drop-table", "true")
            .put("hive.config.resources", "core-site.xml,hdfs-site.xml")
            .put("hive.hdfs.hetu.keytab", "user.keytab")
            .put("hive.metastore.krb5.conf.path", "krb5.conf")
            .put("hive.metastore.client.keytab", "user.keytab")
            .put("hive.metastore.client.principal", "test@HADOOP.COM")
            .put("hive.hdfs.wire-encryption.enabled", "true")
            .put("hive.hdfs.hetu.principal", "test@HADOOP.COM")
            .build();

    final String testResources = Resources.getResource("dynamiccatalog").getPath();
    String localPath;
    String sharePath;
    String storePath;

    TestingPrestoServer server;
    SecurityKeyManager securityKeyManager;
    CatalogStoreUtil catalogStoreUtil;

    public TestDynamicCatalogRunner()
            throws Exception
    {
        localPath = testResources + File.separator + "local";
        sharePath = testResources + File.separator + "share";
        storePath = testResources + File.separator + "keys" + File.separator + "keystore.jks";

        prepareDirectory(localPath);
        prepareDirectory(sharePath);
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("catalog.share.filesystem.profile", "local-config-default")
                .put("catalog.dynamic-enabled", "true")
                .put("catalog.config-dir", localPath)
                .put("catalog.share.config-dir", sharePath)
                .put("catalog.valid-file-suffixes", "jks,keytab,conf,xml")
                .put("security.share.filesystem.profile", "local-config-default")
                .put("security.key.manager-type", "keystore")
                .put("security.key.keystore-password", "password")
                .put("security.key.store-file-path", storePath)
                .put("security.password.decryption-type", "RSA")
                .build();

        server = new TestingPrestoServer(properties);
        server.installPlugin(new TpchPlugin());

        // File system clients
        try {
            server.getInstance(Key.get(FileSystemClientManager.class)).loadFactoryConfigs();
        }
        catch (IllegalStateException e) {
            server.installPlugin(new HetuFileSystemClientPlugin());
            server.getInstance(Key.get(FileSystemClientManager.class)).loadFactoryConfigs();
        }
        securityKeyManager = server.getInstance(Key.get(SecurityKeyManager.class));
        catalogStoreUtil = server.getInstance(Key.get(CatalogStoreUtil.class));
        server.getInstance(Key.get(DynamicCatalogStore.class)).loadCatalogStores(new FileSystemClientManager());
    }

    private void prepareDirectory(String directory)
            throws IOException
    {
        Path path = FileSystems.getDefault().getPath(directory);
        FileUtils.deleteQuietly(path.toFile());
        Files.createDirectory(path);
    }

    private MultipartEntity createMultipartEntity(String catalogName, String connectorName, Map<String, String> properties, List<URL> catalogFileUris, List<URL> globalFileUris)
            throws UnsupportedEncodingException
    {
        MultipartEntity multipartEntity = new MultipartEntity();

        catalogFileUris.forEach(catalogFileUri ->
        {
            FileBody propertiesFile = new FileBody(new File(catalogFileUri.getPath()));
            multipartEntity.addPart("catalogConfigurationFiles", propertiesFile);
        });

        globalFileUris.forEach(globalFileUri ->
        {
            FileBody propertiesFile = new FileBody(new File(globalFileUri.getPath()));
            multipartEntity.addPart("globalConfigurationFiles", propertiesFile);
        });

        CatalogInfo catalogInfo = new CatalogInfo(catalogName, connectorName, "", 0, UUID.randomUUID().toString(), properties);
        StringBody catalogInfoBody = new StringBody(CATALOG_INFO_CODEC.toJson(catalogInfo));
        multipartEntity.addPart("catalogInformation", catalogInfoBody);

        return multipartEntity;
    }

    boolean executeAddCatalogCall(String catalogName, String connectorName, Map<String, String> properties, List<URL> catalogFileUris, List<URL> globalFileUris)
            throws Exception
    {
        HttpClient httpclient = new DefaultHttpClient();
        HttpPost httpPost = new HttpPost(server.getBaseUrl() + "/v1/catalog/");

        MultipartEntity multipartEntity = createMultipartEntity(catalogName, connectorName, properties, catalogFileUris, globalFileUris);

        httpPost.setEntity(multipartEntity);
        httpPost.setHeader(PRESTO_USER, "admin");

        HttpResponse response = httpclient.execute(httpPost);
        if (response != null) {
            if (response.getStatusLine().getStatusCode() == CREATED.getStatusCode()) {
                return true;
            }
            else {
                throw new RuntimeException(EntityUtils.toString(response.getEntity(), UTF_8));
            }
        }
        return false;
    }

    boolean executeUpdateCatalogCall(String catalogName, String connectorName, Map<String, String> properties, List<URL> catalogFileUris, List<URL> globalFileUris)
            throws Exception
    {
        HttpClient httpclient = new DefaultHttpClient();
        HttpPut httpPut = new HttpPut(server.getBaseUrl() + "/v1/catalog/");

        MultipartEntity multipartEntity = createMultipartEntity(catalogName, connectorName, properties, catalogFileUris, globalFileUris);

        httpPut.setEntity(multipartEntity);
        httpPut.setHeader(PRESTO_USER, "admin");

        HttpResponse response = httpclient.execute(httpPut);
        if (response != null) {
            if (response.getStatusLine().getStatusCode() == CREATED.getStatusCode()) {
                return true;
            }
            else {
                throw new RuntimeException(EntityUtils.toString(response.getEntity(), UTF_8));
            }
        }
        return false;
    }

    boolean executeDeleteCatalogCall(String catalogName)
            throws Exception
    {
        HttpClient httpclient = new DefaultHttpClient();
        HttpDelete httpDelete = new HttpDelete(server.getBaseUrl() + "/v1/catalog/" + catalogName);
        httpDelete.setHeader(PRESTO_USER, "admin");

        HttpResponse response = httpclient.execute(httpDelete);
        if (response != null) {
            if (response.getStatusLine().getStatusCode() == NO_CONTENT.getStatusCode()) {
                return true;
            }
            else {
                throw new RuntimeException(EntityUtils.toString(response.getEntity(), UTF_8));
            }
        }
        return false;
    }

    List<String> executeShowCatalogCall()
            throws Exception
    {
        HttpClient httpclient = new DefaultHttpClient();
        HttpGet httpGet = new HttpGet(server.getBaseUrl() + "/v1/catalog");
        httpGet.setHeader(PRESTO_USER, "admin");

        HttpResponse response = httpclient.execute(httpGet);
        if (response != null) {
            JsonCodec<List<String>> catalogNameListCodec = JsonCodec.listJsonCodec(String.class);
            if (response.getStatusLine().getStatusCode() == OK.getStatusCode()) {
                return catalogNameListCodec.fromJson(EntityUtils.toString(response.getEntity(), UTF_8));
            }
            else {
                throw new RuntimeException(EntityUtils.toString(response.getEntity(), UTF_8));
            }
        }
        throw new RuntimeException("execute failed");
    }
}
