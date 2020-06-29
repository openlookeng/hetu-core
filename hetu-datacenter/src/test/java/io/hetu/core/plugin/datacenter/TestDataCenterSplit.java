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
import org.testng.annotations.Test;

import java.util.UUID;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestDataCenterSplit
{
    String queryId = UUID.randomUUID().toString();

    private final DataCenterSplit split = new DataCenterSplit(queryId);

    @Test
    public void testAddresses()
    {
        // http split with default port
        DataCenterSplit httpSplit = new DataCenterSplit(queryId);
        assertEquals(httpSplit.getQueryId(), queryId);
        assertEquals(httpSplit.isRemotelyAccessible(), true);
    }

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<DataCenterSplit> codec = jsonCodec(DataCenterSplit.class);
        String json = codec.toJson(split);
        DataCenterSplit copy = codec.fromJson(json);
        assertEquals(copy.getQueryId(), split.getQueryId());

        assertEquals(copy.isRemotelyAccessible(), true);
    }
}
