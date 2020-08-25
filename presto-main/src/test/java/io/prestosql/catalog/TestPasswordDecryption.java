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
        String publicKey = "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCKRJEeAPEuVjwTRB8htuPxMJ7K/HVs8PmtWwsPlwF3YCQX37aICblM7/rCbaEDd1B6kWI0D2QVp9vPnjsme951FmuNtzsN9lxfwCXyKp0Zus9YncUuZimztb+EYjm9GHGBYIgk+BgJ8y6imSzkI/d0H3twtSGVSC/YSnmYBc3LpwIDAQAB";
        String catalogName = "mysql001";
        securityKeyManager.saveKey(publicKey, catalogName);

        // set parameters
        String connectionPasswordCipherText = "U3Z3bfIxliTU7Avh+Uzu30Whc9rb2fqQN23SF0gz0U+L0+jrfrke6ONzzZsdzqUIBlod6Y4D0ESS9wJFdMeRX+YPofsoxOWSDjhxY4IoxNFGQqYK0xxgxpuekBbJwetLHFBmdFGXKfFUAL6EdPbsWbH/cXfot+3FJfziisxXJhE=";
        String sslPasswordCipherText = "cAI9ZAbdS5QvTUf3ix6m4CPU3XNIy4HjoJhj1chucH7U2J5+CfJ+xi+nf+jwUWD4VJMGvRDxbYr5XNlZsYqUz0/uekcRu8z5c7kKNo1KJalkufQp0OSqFEgGzKaAOxlDcP33oSxo+/Ea3XXxiX4pBfpqw6wsGXDq3X/678pU7fM=";
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
