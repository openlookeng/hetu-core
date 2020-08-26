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
package io.hetu.core.filesystem;

import io.prestosql.spi.filesystem.HetuFileSystemClient;

import java.nio.file.AccessDeniedException;
import java.nio.file.Path;

/**
 * This abstract class provides a HetuFilesystemClient that only allows operations within a certain directory.
 */
public abstract class AbstractWorkspaceFileSystemClient
        implements HetuFileSystemClient
{
    protected Path root;

    public AbstractWorkspaceFileSystemClient(Path root)
    {
        this.root = root;
    }

    /**
     * Getter of the workspace root path. Only child path within the workspace will be allowed to access.
     *
     * @return The workspace root path of current filesystem client.
     */
    public Path getRoot()
    {
        return root;
    }

    /**
     * Validate a given path. Check if it is accessible with current HetuFileSystemClient.
     *
     * @param path Path to validate.
     * @throws AccessDeniedException if the given path is not in the workspace scope.
     */
    public void validate(Path path)
            throws AccessDeniedException
    {
        if (!path.toAbsolutePath().startsWith(root)) {
            throw new AccessDeniedException(String.format("%s is not in workspace %s. Access has been denied.", path, root));
        }
    }
}
