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
package io.hetu.core.plugin.hbase.connector;

import org.testng.annotations.Test;

import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

/**
 * TestHBaseTransactionHandle
 *
 * @since 2020-03-20
 */
public class TestHBaseTransactionHandle
{
    private HBaseTransactionHandle hBaseTransactionHandle;
    private HBaseTransactionHandle hBaseTransactionHandle2;

    private TestHBaseTransactionHandle() {}

    /**
     * testHBaseTransactionHandle
     */
    @Test
    public void testHBaseTransactionHandle()
    {
        UUID uuid = UUID.randomUUID();
        hBaseTransactionHandle = new HBaseTransactionHandle();
        hBaseTransactionHandle2 = new HBaseTransactionHandle(uuid);
        assertEquals(true, uuid.equals(hBaseTransactionHandle2.getUuid()));

        assertEquals(false, hBaseTransactionHandle.equals(hBaseTransactionHandle2));
        assertNotEquals(0, hBaseTransactionHandle2.hashCode());
        hBaseTransactionHandle.toString();
        assertEquals(true, hBaseTransactionHandle.equals(hBaseTransactionHandle));
        assertEquals(false, hBaseTransactionHandle.equals(null));
    }
}
