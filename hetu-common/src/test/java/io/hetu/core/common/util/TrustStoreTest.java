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
package io.hetu.core.common.util;

import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Optional;

import static org.testng.Assert.assertThrows;

public class TrustStoreTest
{
    @Test
    public void testLoadTrustStore() throws Exception
    {
        // Setup
        String path = "/media/kong/70202495202463F6/workspace/hetu-core/presto-main/etc/es.yml";
        // Run the test
        File file = new File(path);
        final KeyStore result = TrustStore.loadTrustStore(file, Optional.of("value"));
    }

    @Test
    public void testLoadTrustStore_ThrowsIOException()
    {
        // Setup
        final File trustStorePath = new File("filename.txt");

        // Run the test
        assertThrows(IOException.class, () -> TrustStore.loadTrustStore(trustStorePath, Optional.of("value")));
    }

    @Test
    public void testLoadTrustStore_ThrowsGeneralSecurityException()
    {
        // Setup
        final File trustStorePath = new File("filename.txt");

        // Run the test
        assertThrows(GeneralSecurityException.class,
                () -> TrustStore.loadTrustStore(trustStorePath, Optional.of("value")));
    }
}
