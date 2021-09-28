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

import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;

import static io.prestosql.SessionTestUtils.TEST_SNAPSHOT_SESSION;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Test(singleThreaded = true)
public class TestSnapshotUtils
{
    HetuFileSystemClient fileSystemClient;
    SnapshotUtils snapshotUtils;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        // Set up snapshot config
        SnapshotConfig snapshotConfig = new SnapshotConfig();

        // Set up mock file system client manager
        FileSystemClientManager fileSystemClientManager = mock(FileSystemClientManager.class);
        fileSystemClient = mock(HetuFileSystemClient.class);
        when(fileSystemClientManager.getFileSystemClient(any(Path.class))).thenReturn(fileSystemClient);

        snapshotUtils = new SnapshotUtils(fileSystemClientManager, snapshotConfig, new InMemoryNodeManager());
        snapshotUtils.initialize();
    }

    @Test
    public void testDeleteSnapshot()
            throws IOException
    {
        QueryId queryId1 = new QueryId("query1");
        snapshotUtils.getOrCreateQuerySnapshotManager(queryId1, TEST_SNAPSHOT_SESSION);
        QueryId queryId2 = new QueryId("query2");
        snapshotUtils.getOrCreateQuerySnapshotManager(queryId2, TEST_SNAPSHOT_SESSION);

        when(fileSystemClient.deleteRecursively(anyObject()))
                .thenReturn(true) // OK to remove query 1
                .thenThrow(new IOException()) // Fail to remove query 2
                .thenReturn(true) // OK to remove query 1-or-2 (in cleanup)
                .thenThrow(new IOException()) // Fail to remove query 1-or-2 (in cleanup)
                .thenReturn(true); // OK to remove query 1-or-2 (in cleanup)

        snapshotUtils.removeQuerySnapshotManager(queryId1); // OK
        verify(fileSystemClient).deleteRecursively(anyObject());

        snapshotUtils.removeQuerySnapshotManager(queryId2); // Fail
        verify(fileSystemClient, times(2)).deleteRecursively(anyObject());

        snapshotUtils.cleanupSnapshots(); // 1 OK 1 Fail
        verify(fileSystemClient, times(4)).deleteRecursively(anyObject());

        snapshotUtils.cleanupSnapshots(); // OK
        verify(fileSystemClient, times(6)).deleteRecursively(anyObject());

        snapshotUtils.cleanupSnapshots(); // No-op
        verify(fileSystemClient, times(8)).deleteRecursively(anyObject());
    }
}
