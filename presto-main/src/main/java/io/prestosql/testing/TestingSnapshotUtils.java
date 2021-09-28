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
package io.prestosql.testing;

import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.snapshot.SnapshotConfig;
import io.prestosql.snapshot.SnapshotUtils;
import io.prestosql.spi.filesystem.HetuFileSystemClient;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.stream.Stream;

public class TestingSnapshotUtils
{
    public static final SnapshotUtils NOOP_SNAPSHOT_UTILS;

    static {
        SnapshotConfig snapshotConfig = new SnapshotConfig();
        FileSystemClientManager fileSystemClientManager = new NoopFileSystemClientManager();
        NOOP_SNAPSHOT_UTILS = new SnapshotUtils(fileSystemClientManager, snapshotConfig, new InMemoryNodeManager());
        NOOP_SNAPSHOT_UTILS.initialize();
    }

    private TestingSnapshotUtils()
    {
    }

    private static class NoopFileSystemClientManager
            extends FileSystemClientManager
    {
        @Override
        public HetuFileSystemClient getFileSystemClient(String name, Path root)
        {
            return new NoopFileSystemClient();
        }
    }

    private static class NoopFileSystemClient
            implements HetuFileSystemClient
    {
        @Override
        public Path createDirectories(Path dir)
        {
            return dir;
        }

        @Override
        public Path createDirectory(Path dir)
        {
            return dir;
        }

        @Override
        public void delete(Path path)
        {
        }

        @Override
        public boolean deleteIfExists(Path path)
        {
            return false;
        }

        @Override
        public boolean deleteRecursively(Path path)
        {
            return false;
        }

        @Override
        public boolean exists(Path path)
        {
            return false;
        }

        @Override
        public void move(Path source, Path target)
        {
        }

        @Override
        public InputStream newInputStream(Path path)
        {
            return new InputStream()
            {
                @Override
                public int read()
                {
                    return -1;
                }
            };
        }

        @Override
        public OutputStream newOutputStream(Path path, OpenOption... options)
        {
            return new OutputStream()
            {
                @Override
                public void write(int b)
                {
                }
            };
        }

        @Override
        public Object getAttribute(Path path, String attribute)
        {
            return null;
        }

        @Override
        public boolean isDirectory(Path path)
        {
            return false;
        }

        @Override
        public Stream<Path> list(Path dir)
        {
            return Stream.empty();
        }

        @Override
        public Stream<Path> walk(Path dir)
        {
            return Stream.empty();
        }

        @Override
        public void close()
        {
        }
    }
}
