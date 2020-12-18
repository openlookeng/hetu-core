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

import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static io.prestosql.spi.HetuConstant.ENCRYPTED_PROPERTIES;
import static org.testng.Assert.assertTrue;

public class TestPasswordDecryption
        extends TestDynamicCatalogRunner
{
    public TestPasswordDecryption()
            throws Exception
    {
    }

    @Test
    public void testDecryptionPassword()
            throws Exception
    {
        // save the key
        String privateKey = "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDE9YRkOjcXKPKxCTUfojpl3prOPgprGkibQc62V8CnvoL7wI3I8v+JAPn+3sLOl2rNN8ziQxW6eCThbvnIo0ID6i+FeNcv5DngPMLHnUkTPs8uTXsc055WaNhx/6YwQa6dlnmaiOhIPoLmHqmZYGrlwLB9O5qpW2wOeFnQ/JbdeeUPIMy0srFia+j/MklXwm+9hGG+Cn0h7EnzIR6rrCrkai4Axqma4pDWPCAOI1G34H2BntSjqMGifhyHS2U6QuO7MC8fsYSVMvkkk7F6dR3jhQpjd3dmPZeK2o8CRTrymLG86wgaqDbz/DCnZaSJuGmcWPTAfl5GNicwJ/MSMJO7AgMBAAECggEAJgkDuBdF7EMMGwQcLi+191Y1rv5sJKK+wrzCnpPzsHEp+lQFDXlfv6VgoceC90JmbZsJBejOmWP6O06jDgv5A6iF5NChPa5lPth9BO9q3TyT5e0wiTCUszssEVe9UDRe9C/K6/zkXo8z2Byzw9rKyfOcIZMRGwN8qo6zSZh9yecZVLesiE+d9nNJguypLQOnJlDH5U73qpWJHW0s2bZfNfgq3WPZGR5/pVXPd8MbQkNNdo71FMumCV6Xdu2kLkZVObDGg90bJmB4dzvH0grDB8HVeoQ3mkrQjRK3d5XW99SGaANNSqT1jeUXMBLBQONKhElqYNBOVXT9Hw7H2QGDgQKBgQDwynd1R13zvGFQgEZ6thwonXDkxTgmoyi9GIxMVsxQtbMJIo0211ndZ4ZCoeXm5DsTw7C+lzE/Bb+pkCuSUFbYpKnEOLOVgSKsaxKiV7NVr+i2rZgVZdTxPTwwDTekPUZo8ZMUX4ryNzjG02k9E/pNyq84iZJnlzlLoF3+cWzy7QKBgQDRZkzBQgqACLoChj1xjQAqcXklyhQZIkJXt4A5bwYvX8J0Xw8+ZTNn8lJo9g8TVcdmoTLJRRbYmWn2QHzOyf4YrjD44wj1KDfOrCYimShGDAcvNnxOSiZoVAZ90YHOGRgQjQIUdc9EWyHQUVuHGmJJn2tMrNWmTdQG2QO/ZLOERwKBgDV+xctob5cW4wffd8kLbHYZhFtO9Yqf9Q0Nxx2uqvXDaGM/KeBlN7HYrhtfnJQPYJCjiUDOlkJKJKqnPQbkkmbPLmhJsJwmdG2Z3Cn1EgKXcjwjlQYr+YMe96A1T4dNlbb69JIyJ8xoOFTz4w2Owq1Fumf1KHGbRz9mAXvl9y6BAoGBAM/JO5Dp/5FdF3c5ze7Hg8qaHyUoiRkHrd1s8YgMa47G7yaazX2U3pXfF3ef8wW0sNFeVA70x97XHRaGl1J7jfDfqSjP4SukZPxoRs8+O4CGnvpyjpcBbWtJNcfUBYKF1CNYSSAUZz/lToBwuuotfCqiTs8fEkKcKDFDVzysqJYxAoGBAMZny9cs/AZH33iZVedtHQZclcs1PJBMm8ZyTR9uSmDQYSeujO/5BFOQ5jNdpKEKxirm5ZNuPqDubbJW78aHpjRl/uUvmDZDY9fYbo9NVZXNn805RWT59zn1cAlRr629lHn7bbyXBVKDN9/z2fqaUkI7qj5Xbi/C+prVhJS15V2p";
        String catalogName = "mysql001";
        securityKeyManager.saveKey(privateKey.toCharArray(), catalogName);

        // set parameters
        String connectionPasswordCipherText = "ov4otIMT9ZF9OU7NCTm+kie9GDAdxYiD+CGfROhzQfVXg7+clwyZ36wCONgFYOa/lDw6POLQGKbdsJzFn3SGfJPgEl/nvoseAjefXNBFGEppf+9bqMSRvw98SQJnhsogv5likU7MLeM8FzknNZBr0J6dtm7Y6pJMeWxJp7nJWFndJ2ba7NiRw7v8zsCt/VFMa3juCbpqyLhUBfUOMQI06k4O1HlYrdJL4g4EEmWuWg0HrIIb00SWrpBJb31jOSAy2FdKqW47t28z234uePBWz94hrTuWq/zdebbXVQBnFCaxCwLfdaJ//K8FCnrWoAOzzv9B5CRBcCbx2IF/4oyckQ==";
        String sslPasswordCipherText = "VDZK8r+u5BNw9Ihzomk0aHlSLuumDmSRq6asZjGiwsygnUbHBWQn4SsZd/oQ+9y4LefPCXx53Txz8H04tMzzd23+J4TYa+zc7PxHLkUwxTqR03dQTLlJVlDlc/dM7aEKapHZYSL3kzUWL1Qt0laocbx0hYpw0eF24J1QPpcEVv92MGy0WeqMwpdIu7FQbfhjixaY/Zsaz3V9mMM+RHjhgOnyMyx4MwrdEWCrJzTUz1wBaqt9LzO4rba5UD8VQM2K4fXpMrdBgHLSdZr5FQNisqiw1eEs/94pnAWWjlsRdlpwVDsN+niXvrJfyqKo3FC8DAhder+Ighxdluu3WSZORA==";
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("connection-url", "jdbc:mysql://localhost:3306");
        properties.put("connection-user", "root");
        properties.put("connection-password", connectionPasswordCipherText);
        properties.put("ssl-password", sslPasswordCipherText);
        properties.put("ssl.password", sslPasswordCipherText);
        properties.put(ENCRYPTED_PROPERTIES, "connection-password,ssl-password, ssl.password");

        catalogStoreUtil.decryptEncryptedProperties(catalogName, properties);

        // check the result
        assertTrue(properties.get("connection-url").equals("jdbc:mysql://localhost:3306"));
        assertTrue(properties.get("connection-user").equals("root"));
        assertTrue(properties.get("connection-password").equals("CONNECTION_PASSWORD"));
        assertTrue(properties.get("ssl-password").equals("SSL_PASSWORD"));
        assertTrue(properties.get("ssl.password").equals("SSL_PASSWORD"));
        assertTrue(properties.get(ENCRYPTED_PROPERTIES) == null);
    }
}
