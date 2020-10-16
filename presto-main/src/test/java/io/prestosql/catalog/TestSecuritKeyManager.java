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

import io.prestosql.spi.security.SecurityKeyException;
import org.testng.annotations.Test;

import static io.prestosql.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestSecuritKeyManager
        extends TestDynamicCatalogRunner
{
    private static final String TESTCATALOG = "test";

    public TestSecuritKeyManager()
            throws Exception
    {
        // empty
    }

    @Test
    public void testSecurityKey()
            throws SecurityKeyException
    {
        String publicKey = "fkasdlkf-erjlskdf-lkf234-werjd-fsdf23-df23-sdfgdfa3-4dsfksdlf-4234s-fjk234";
        String catalogName = "testCatalog";

        securityKeyManager.saveKey(publicKey.toCharArray(), catalogName);
        securityKeyManager.saveKey(publicKey.toCharArray(), TESTCATALOG);

        String key = new String(securityKeyManager.getKey(catalogName));
        String test2Key = new String(securityKeyManager.getKey(TESTCATALOG));

        assertEquals(key, publicKey);
        assertEquals(test2Key, publicKey);

        securityKeyManager.deleteKey(TESTCATALOG);
        securityKeyManager.deleteKey(TESTCATALOG);
        assertNull(securityKeyManager.getKey(TESTCATALOG));
    }
}
