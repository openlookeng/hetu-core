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

package io.hetu.core.plugin.datacenter;

import com.google.common.collect.ImmutableMap;
import io.airlift.security.pem.PemReader;
import io.hetu.core.plugin.datacenter.client.DataCenterClient;
import io.hetu.core.plugin.datacenter.client.DataCenterStatementClientFactory;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.testing.TestingTypeManager;
import okhttp3.OkHttpClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.security.PrivateKey;
import java.sql.SQLException;
import java.util.Base64;
import java.util.Optional;
import java.util.Set;

import static com.google.common.io.Files.asCharSource;
import static com.google.common.io.Resources.getResource;
import static io.jsonwebtoken.JwsHeader.KEY_ID;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Base64.getMimeDecoder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Testing authentication similar to io.hetu.core.jdbc.TestHetuDriverAuth
 */
public class TestDataCenterClientAuth
{
    private TypeManager typeManager = new TestingTypeManager();

    private TestingPrestoServer server;

    private byte[] defaultKey;

    private byte[] hmac222;

    private PrivateKey privateKey33;

    @BeforeClass
    public void setup()
            throws Exception
    {
        URL resource = getClass().getClassLoader().getResource("33.privateKey");
        assertNotNull(resource, "key directory not found");
        File keyDir = new File(resource.getFile()).getAbsoluteFile().getParentFile();

        defaultKey = getMimeDecoder().decode(
                asCharSource(new File(keyDir, "default-key.key"), US_ASCII).read().getBytes(US_ASCII));
        hmac222 = getMimeDecoder().decode(
                asCharSource(new File(keyDir, "222.key"), US_ASCII).read().getBytes(US_ASCII));
        privateKey33 = PemReader.loadPrivateKey(new File(keyDir, "33.privateKey"), Optional.empty());

        server = new TestingPrestoServer(
                ImmutableMap.<String, String>builder().put("http-server.authentication.type", "JWT")
                        .put("http.authentication.jwt.key-file", new File(keyDir, "${KID}.key").toString())
                        .put("http-server.https.enabled", "true")
                        .put("http-server.https.keystore.path", getResource("localhost.keystore").getPath())
                        .put("http-server.https.keystore.key", "changeit")
                        .build());
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
            throws IOException
    {
        server.close();
    }

    @Test
    public void testSuccessDefaultKey()
            throws SQLException
    {
        String accessToken = Jwts.builder().setSubject("test").signWith(SignatureAlgorithm.HS512, defaultKey).compact();
        assertToken(accessToken);
    }

    @Test
    public void testSuccessHmac()
            throws SQLException
    {
        String accessToken = Jwts.builder()
                .setSubject("test")
                .setHeaderParam(KEY_ID, "222")
                .signWith(SignatureAlgorithm.HS512, hmac222)
                .compact();

        assertToken(accessToken);
    }

    @Test
    public void testSuccessPublicKey()
            throws SQLException
    {
        String accessToken = Jwts.builder()
                .setSubject("test")
                .setHeaderParam(KEY_ID, "33")
                .signWith(SignatureAlgorithm.RS256, privateKey33)
                .compact();

        assertToken(accessToken);
    }

    @Test(expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = "tpch not found, failed to get schema names")
    public void testFailedNoToken()
            throws SQLException
    {
        assertToken(null);
    }

    @Test(expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = "tpch not found, failed to get schema names")
    public void testFailedUnsigned()
            throws SQLException
    {
        String accessToken = Jwts.builder().setSubject("test").compact();

        assertToken(accessToken);
    }

    @Test(expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = "tpch not found, failed to get schema names")
    public void testFailedBadHmacSignature()
            throws Exception
    {
        String accessToken = Jwts.builder()
                .setSubject("test")
                .signWith(SignatureAlgorithm.HS512, Base64.getEncoder().encodeToString("bad-key".getBytes(US_ASCII)))
                .compact();

        assertToken(accessToken);
    }

    @Test(expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = "tpch not found, failed to get schema names")
    public void testFailedWrongPublicKey()
            throws Exception
    {
        String accessToken = Jwts.builder()
                .setSubject("test")
                .setHeaderParam(KEY_ID, "42")
                .signWith(SignatureAlgorithm.RS256, privateKey33)
                .compact();

        assertToken(accessToken);
    }

    @Test(expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = "tpch not found, failed to get schema names")
    public void testFailedUnknownPublicKey()
            throws Exception
    {
        String accessToken = Jwts.builder()
                .setSubject("test")
                .setHeaderParam(KEY_ID, "unknown")
                .signWith(SignatureAlgorithm.RS256, privateKey33)
                .compact();

        assertToken(accessToken);
    }

    private void assertToken(String accessToken)
            throws SQLException
    {
        String serverUri = "https://localhost:" + this.server.getHttpsAddress().getPort();
        DataCenterConfig config = new DataCenterConfig().setConnectionUrl(URI.create(serverUri))
                .setConnectionUser("test")
                .setSsl(true)
                .setAccessToken(accessToken)
                .setSslTrustStorePath(getResource("localhost.truststore").getPath())
                .setSslTrustStorePassword("changeit");
        OkHttpClient httpClient = DataCenterStatementClientFactory.newHttpClient(config);
        try {
            DataCenterClient client = new DataCenterClient(config, httpClient, typeManager);
            Set<String> schemaNames = client.getSchemaNames("tpch");
            assertTrue(schemaNames.contains("tiny"));
            assertEquals(schemaNames.size(), 9);
        }
        catch (Throwable t) {
            if (t.getCause() instanceof SQLException) {
                throw (SQLException) t.getCause();
            }
            throw t;
        }
        finally {
            httpClient.dispatcher().executorService().shutdown();
            httpClient.connectionPool().evictAll();
        }
    }
}
