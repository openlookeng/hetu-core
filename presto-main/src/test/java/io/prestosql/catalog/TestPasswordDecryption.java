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
        String publicKey = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEArEhOFoAk46GQzet7C/tmod5Sju91+dDl28TtuFXqOvSz5vvjuEkIK6NI411Lb11vF2P1a9J4ctcA3V3VvXk+H9IsW2jzuXZ774RaACNfMyslmHB+JOoPbTzUSVLyNfb3eSmEhkfYYUrKqlz4tvXUm5FJRX2Xh83yYX1FAnOpuMwzbwaMVLqb8k7LP2uI9+2dTTocE51K3SDH0KoFMyW/uSF9Jj9HLQ1rI9DxCH8UIf7SEwSZ0Pq42fxNW1qjjanYF5tHIp/5l2/O4LUNnZaMTISmT9m9lYt8FTudMtWCMNWarTk8JNkui4x7cSbmE5QaP+s994LK2umBXl95esUSsQIDAQAB";
        String catalogName = "mysql001";
        securityKeyManager.saveKey(publicKey.toCharArray(), catalogName);

        // set parameters
        String connectionPasswordCipherText = "huOJCr35k6K2Dgn4InAMJHTyzGvcg+bgOvG+eUxcefML5jPBc3nWc4Wm0nedt1c6g/CKbQksUps/6KP1bcirdkBVwlfpmABwdfGMWuqAQLSUKyi4N1i0zexwDGRgTz0/FuTudlPFDF0XM3iJODB5XfStjxptBPGNAwnRxlmVYnxP2hXHxmY0wmFzk9ZbOj8fLM88dft5T03zBSnErftZ71MNyyskmksI+l9CVB7QDGEaMvhVDb9gSz1t8wTDrxlb0MTmJVl7n7Kw0B1MGmMqWKJA49YhfJJG8ZUPSMkrgTeIGwMqCDs1y2KOZORx5IXYdMJtL01td25OjcKenTuNwQ==";
        String sslPasswordCipherText = "cjLGNZadxWTIZTQRHkDeu4dBD79M4sKZ1fgWf39NvgBCiYflr/rvcUyCVlNQYV7WXFpl853bInMnzc+FaGk8039T+k7haK3uTk0o3itCAom2HtYcJzh3dhy8ZzsPLXsEldarYlTpFUbCNl3gepiZY98253Nn8J4n+eZidyfSGzecJC00Snfj6Ry7QiwqP90w5/CszMkAg97Ri0z28e5RN+xeM+wlXut2ap7hptpj5sbvJzaU9tjUJJZVxmkPdUk1jy48EpnG7YAtJftxLo6AcEaV7iK/7QaXOLGWUWeOAvGM4e8YIgJdxk8ptqvTAGw+9agRkKLP6Ujic+pPoHnnNA==";
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
