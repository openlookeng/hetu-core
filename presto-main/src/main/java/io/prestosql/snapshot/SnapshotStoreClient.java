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

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * SnapshotStoreClient is an client(interface) to connect to snapshot store
 */
public interface SnapshotStoreClient
{
    /**
     * Store state in snapshot store
     */
    void storeState(SnapshotStateId snapshotStateId, Object state)
            throws Exception;

    /**
     * Load state from snapshot store. Optional.empty() is returned if state doesn't exist.
     */
    Optional<Object> loadState(SnapshotStateId snapshotStateId)
            throws Exception;

    /**
     * Store file from sourcePath to snapshotStateId of snapshot store
     */
    void storeFile(SnapshotStateId snapshotStateId, Path sourcePath)
            throws Exception;

    /**
     * Load file from snapshotStateId of snapshot store to targetPath
     */
    boolean loadFile(SnapshotStateId snapshotStateId, Path targetPath)
            throws Exception;

    /**
     * Delete everything under query
     */
    void deleteAll(String queryId)
            throws Exception;

    /**
     * Store snapshot result of query
     */
    void storeSnapshotResult(String queryId, Map<Long, SnapshotResult> result)
            throws Exception;

    /**
     * Load snapshot result of query
     */
    Map<Long, SnapshotResult> loadSnapshotResult(String queryId)
            throws Exception;

    /**
     * Adds a consolidated file for verification later
     */
    void storeConsolidatedFileList(String queryId, Set<String> path)
            throws Exception;

    /**
     * Checks if the consolidated file has been created
     */
    Set<String> loadConsolidatedFiles(String queryId)
            throws Exception;
}
