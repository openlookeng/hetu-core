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

import io.airlift.json.JsonCodec;
import io.airlift.testing.EquivalenceTester;
import org.testng.annotations.Test;

import java.util.OptionalLong;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestDataCenterTableHandle
{
    private final DataCenterTableHandle tableHandle = new DataCenterTableHandle("tpch", "schemaName", "tableName",
            OptionalLong.empty());

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<DataCenterTableHandle> codec = jsonCodec(DataCenterTableHandle.class);
        String json = codec.toJson(tableHandle);
        DataCenterTableHandle copy = codec.fromJson(json);
        assertEquals(copy, tableHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(new DataCenterTableHandle("tpch", "schema", "table", OptionalLong.empty()),
                        new DataCenterTableHandle("tpch", "schema", "table", OptionalLong.empty()))
                .addEquivalentGroup(new DataCenterTableHandle("tpch", "schemaX", "table", OptionalLong.empty()),
                        new DataCenterTableHandle("tpch", "schemaX", "table", OptionalLong.empty()))
                .addEquivalentGroup(new DataCenterTableHandle("tpch", "schema", "tableX", OptionalLong.empty()),
                        new DataCenterTableHandle("tpch", "schema", "tableX", OptionalLong.empty()))
                .check();
    }
}
