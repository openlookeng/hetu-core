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
package io.prestosql.snapshot;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * This is utility class for snapshot
 */
public class SnapshotUtils
{
    private final boolean isCoordinator;
    private final FileSystemClientManager fileSystemClientManager;
    private final SnapshotConfig snapshotConfig;
    private SnapshotStoreClient snapshotStoreClient;
    //TODO-cp-I2D63N hardcoded 'storeType' and 'rootPath' for now, may change to configurable after done switching to state-store
    private final SnapshotStoreType storeType = SnapshotStoreType.FILESYSTEM;
    // TODO-cp-I2D63N: use /tmp for now to avoid permission issues with writing to /opt
    @VisibleForTesting
    String rootPath = "/tmp/hetu/snapshot/";

    @Inject
    public SnapshotUtils(FileSystemClientManager fileSystemClientManager, SnapshotConfig snapshotConfig, InternalNodeManager nodeManager)
    {
        this.isCoordinator = nodeManager.getCurrentNode().isCoordinator();
        this.fileSystemClientManager = requireNonNull(fileSystemClientManager);
        this.snapshotConfig = requireNonNull(snapshotConfig);
    }

    public boolean isCoordinator()
    {
        return isCoordinator;
    }

    public void initialize()
            throws Exception
    {
        snapshotStoreClient = buildSnapshotStoreClient();
    }

    private SnapshotStoreClient buildSnapshotStoreClient()
            throws Exception
    {
        if (storeType == SnapshotStoreType.FILESYSTEM) {
            String profile = snapshotConfig.getSnapshotProfile();
            Path root = Paths.get(rootPath);
            HetuFileSystemClient fs = profile == null ?
                    fileSystemClientManager.getFileSystemClient(root) : fileSystemClientManager.getFileSystemClient(profile, root);
            return new SnapshotFileBasedClient(fs, root);
        }
        else {
            throw new UnsupportedOperationException("Not valid snapshot store type: " + storeType);
            // TODO-cp-I2D63N add different snapshot store client
        }
    }

    /**
     * Store the state of snapshotStateId in snapshot store
     */
    public void storeState(SnapshotStateId snapshotStateId, Object state)
            throws Exception
    {
        requireNonNull(snapshotStoreClient);
        requireNonNull(state);

        snapshotStoreClient.storeState(snapshotStateId, state);
    }

    /**
     * Load the state of snapshotStateId from snapshot store. Returns:
     * - Empty: state file doesn't exist
     * - NO_STATE: bug situation
     * - Other object: previously saved state
     */
    public Optional<Object> loadState(SnapshotStateId snapshotStateId)
            throws Exception
    {
        requireNonNull(snapshotStoreClient);
        return snapshotStoreClient.loadState(snapshotStateId);
    }

    public void storeFile(SnapshotStateId snapshotStateId, Path sourceFile)
            throws Exception
    {
        requireNonNull(snapshotStoreClient);
        requireNonNull(sourceFile);

        snapshotStoreClient.storeFile(snapshotStateId, sourceFile);
    }

    public Boolean loadFile(SnapshotStateId snapshotStateId, Path targetFile)
            throws Exception
    {
        requireNonNull(snapshotStoreClient);
        requireNonNull(targetFile);

        return snapshotStoreClient.loadFile(snapshotStateId, targetFile);
    }

    public void storeSnapshotResult(String queryId, Map<Long, SnapshotResult> result)
            throws Exception
    {
        snapshotStoreClient.storeSnapshotResult(queryId, result);
    }

    public Map<Long, SnapshotResult> loadSnapshotResult(String queryId)
            throws Exception
    {
        return snapshotStoreClient.loadSnapshotResult(queryId);
    }

    public void deleteAll(String queryId)
            throws Exception
    {
        snapshotStoreClient.deleteAll(queryId);
    }

    /**
     * Serialize state to outputStream
     */
    public static void serializeState(Object state, OutputStream outputStream)
            throws IOException
    {
        // java serialization
        ObjectOutputStream oos = new ObjectOutputStream(outputStream);
        oos.writeObject(state);
        oos.flush();
    }

    /**
     * Deserialize state from inputStream
     */
    public static Object deserializeState(InputStream inputStream)
            throws IOException, ClassNotFoundException
    {
        // java deserialization
        ObjectInputStream ois = new ObjectInputStream(inputStream);
        return ois.readObject();
    }

    /**
     * Create state path
     *
     * @param root root path of state path
     * @param subpaths a collection of sub paths
     * @return subpaths appended to root
     */
    public static Path createStatePath(Path root, Collection<String> subpaths)
    {
        for (String sub : subpaths) {
            root = root.resolve(sub);
        }
        return root;
    }

    public static Path createStatePath(Path root, String... subpaths)
    {
        for (String sub : subpaths) {
            root = root.resolve(sub);
        }
        return root;
    }

    public static Object captureHelper(Object obj, BlockEncodingSerdeProvider serdeProvider)
    {
        if (obj instanceof Slice) {
            return ((Slice) obj).getBytes();
        }
        else if (obj instanceof Block) {
            SliceOutput output = new DynamicSliceOutput(1);
            serdeProvider.getBlockEncodingSerde().writeBlock(output, (Block<?>) obj);
            return output.getUnderlyingSlice().getBytes();
        }
        else {
            return obj;
        }
    }

    public static Object restoreHelper(Object obj, Class<?> type, BlockEncodingSerdeProvider serdeProvider)
    {
        if (obj == null) {
            return null;
        }
        if (type == Slice.class) {
            return Slices.wrappedBuffer((byte[]) obj);
        }
        else if (type == Block.class) {
            Slice input = Slices.wrappedBuffer((byte[]) obj);
            return serdeProvider.getBlockEncodingSerde().readBlock(input.getInput());
        }
        else {
            return obj;
        }
    }
}
