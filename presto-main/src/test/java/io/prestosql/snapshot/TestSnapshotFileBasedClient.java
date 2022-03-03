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
import io.prestosql.execution.TaskId;
import io.prestosql.filesystem.FileSystemClientManager;
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
        SnapshotFileBasedClient client = new SnapshotFileBasedClient(new HetuLocalFileSystemClient(new LocalConfig(new Properties()), Paths.get(ROOT_PATH_STR)), Paths.get(ROOT_PATH_STR), new FileSystemClientManager(), null, false, false);
        String queryId = "query1";
        LinkedHashMap<Long, SnapshotInfo> map = new LinkedHashMap<>();
        map.put(3L, SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL));
        map.put(1L, SnapshotInfo.withStatus(SnapshotResult.FAILED));
        map.put(5L, SnapshotInfo.withStatus(SnapshotResult.FAILED_FATAL));
        map.put(8L, SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL));

        // Test store and Load
        client.storeSnapshotResult(queryId, map);
        LinkedHashMap<Long, SnapshotInfo> resultMap = (LinkedHashMap<Long, SnapshotInfo>) client.loadSnapshotResult(queryId);
        Assert.assertEquals(map.get(3L).getSnapshotResult(), resultMap.get(3L).getSnapshotResult());
        Assert.assertEquals(map.get(1L).getSnapshotResult(), resultMap.get(1L).getSnapshotResult());
        Assert.assertEquals(map.get(5L).getSnapshotResult(), resultMap.get(5L).getSnapshotResult());
        Assert.assertEquals(map.get(8L).getSnapshotResult(), resultMap.get(8L).getSnapshotResult());
    }

    /**
     * Test store, load snapshot state
     * @throws Exception
     */
    @Test
    public void testSnapshotStateWithKryo()
            throws Exception
    {
        SnapshotFileBasedClient client = new SnapshotFileBasedClient(new HetuLocalFileSystemClient(new LocalConfig(new Properties()), Paths.get(ROOT_PATH_STR)), Paths.get(ROOT_PATH_STR), new FileSystemClientManager(), null, false, true);
        String queryId = "query1";
        TaskId taskId = new TaskId(queryId, 1, 1);
        SnapshotStateId snapshotStateId = new SnapshotStateId(2, taskId, 10);
        LinkedHashMap<Long, SnapshotResult> map = new LinkedHashMap<>();
        map.put(3L, SnapshotResult.SUCCESSFUL);
        map.put(1L, SnapshotResult.FAILED);
        map.put(5L, SnapshotResult.FAILED_FATAL);
        map.put(8L, SnapshotResult.SUCCESSFUL);

        // Test store and Load
        client.storeState(snapshotStateId, map, null);
        client.loadState(snapshotStateId, null);
        Assert.assertEquals(map, client.loadState(snapshotStateId, null).get());
    }

    /**
     * Test store, load snapshot state
     * @throws Exception
     */
    @Test
    public void testSnapshotStateWithJava()
            throws Exception
    {
        SnapshotFileBasedClient client = new SnapshotFileBasedClient(new HetuLocalFileSystemClient(new LocalConfig(new Properties()), Paths.get(ROOT_PATH_STR)), Paths.get(ROOT_PATH_STR), new FileSystemClientManager(), null, false, false);
        String queryId = "query1";
        TaskId taskId = new TaskId(queryId, 1, 1);
        SnapshotStateId snapshotStateId = new SnapshotStateId(2, taskId, 10);
        LinkedHashMap<Long, SnapshotResult> map = new LinkedHashMap<>();
        map.put(3L, SnapshotResult.SUCCESSFUL);
        map.put(1L, SnapshotResult.FAILED);
        map.put(5L, SnapshotResult.FAILED_FATAL);
        map.put(8L, SnapshotResult.SUCCESSFUL);

        // Test store and Load
        client.storeState(snapshotStateId, map, null);
        client.loadState(snapshotStateId, null);
        Assert.assertEquals(map, client.loadState(snapshotStateId, null).get());
    }
}
