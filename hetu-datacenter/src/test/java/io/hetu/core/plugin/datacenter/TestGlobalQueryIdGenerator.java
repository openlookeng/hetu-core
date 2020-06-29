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

import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestGlobalQueryIdGenerator
{
    @Test
    public void testGlobalQueryIdGenerator1()
    {
        GlobalQueryIdGenerator generator = new GlobalQueryIdGenerator(Optional.empty());
        String id1 = generator.createId();
        String id2 = generator.createId();
        assertNotEquals(id1, id2, "returning the same id");
    }

    @Test
    public void testGlobalQueryIdGenerator2()
    {
        GlobalQueryIdGenerator generator = new GlobalQueryIdGenerator(Optional.of("cluster-a"));
        String id1 = generator.createId();
        String id2 = generator.createId();
        assertNotEquals(id1, id2, "returning the same id");
        assertTrue(id1.startsWith("cluster-a-"), "cluster id not included in the query id");
        assertTrue(id2.startsWith("cluster-a-"), "cluster id not included in the query id");
    }
}
