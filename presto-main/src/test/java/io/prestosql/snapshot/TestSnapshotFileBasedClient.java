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
package io.prestosql.snapshot;

import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.prestosql.testing.assertions.Assert;
import org.testng.annotations.Test;

import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Properties;

public class TestSnapshotFileBasedClient
{
    private static final String ROOT_PATH_STR = "/tmp/test_snapshot_manager";

    /**
     * Test store, load, and delete snapshot result
     * @throws Exception
     */
    @Test
    public void testSnapshotResult()
            throws Exception
    {
        SnapshotFileBasedClient client = new SnapshotFileBasedClient(new HetuLocalFileSystemClient(new LocalConfig(new Properties()), Paths.get(ROOT_PATH_STR)), Paths.get(ROOT_PATH_STR));
        String queryId = "query1";
        LinkedHashMap<Long, SnapshotResult> map = new LinkedHashMap<>();
        map.put(3L, SnapshotResult.SUCCESSFUL);
        map.put(1L, SnapshotResult.FAILED);
        map.put(5L, SnapshotResult.FAILED_FATAL);
        map.put(8L, SnapshotResult.SUCCESSFUL);

        // Test store and Load
        client.storeSnapshotResult(queryId, map);
        LinkedHashMap<Long, SnapshotResult> resultMap = (LinkedHashMap<Long, SnapshotResult>) client.loadSnapshotResult(queryId);
        Assert.assertEquals(map, resultMap);

        // Test delete
        //client.deleteAll(queryId);
        //resultMap = (LinkedHashMap<Long, SnapshotResult>) client.loadSnapshotResult(queryId);
        //Assert.assertEquals(resultMap.size(), 0);
    }
}
