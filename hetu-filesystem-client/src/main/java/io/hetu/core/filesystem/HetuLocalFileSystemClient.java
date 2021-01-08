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

import io.prestosql.spi.filesystem.SupportedFileAttributes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Locale;
import java.util.stream.Stream;

/**
 * HetuFileSystemClient implementation for local file system
 *
 * @since 2020-03-30
 */
public class HetuLocalFileSystemClient
        extends AbstractWorkspaceFileSystemClient
{
    public HetuLocalFileSystemClient(LocalConfig config, Path allowAccessRoot)
    {
        super(allowAccessRoot);
    }

    @Override
    public Path createDirectories(Path dir)
            throws IOException
    {
        validate(dir);
        return Files.createDirectories(dir);
    }

    @Override
    public Path createDirectory(Path dir)
            throws IOException
    {
        validate(dir);
        return Files.createDirectory(dir);
    }

    /**
     * Delete a given file or directory. If the given path is a directory it must be empty.
     *
     * @param path Path to delete.
     * @throws IOException Other exceptions.
     */
    @Override
    public void delete(Path path)
            throws IOException
    {
        validate(path);
        Files.delete(path);
    }

    /**
     * Delete a given file or directory. If the given path is a directory it must be empty.
     * Return the result of deletion.
     *
     * @param path Path to delete.
     * @return Whether the deletion is successful. If the file does not exist, return {@code false}.
     * @throws IOException Other exceptions.
     */
    @Override
    public boolean deleteIfExists(Path path)
            throws IOException
    {
        validate(path);
        return Files.deleteIfExists(path);
    }

    @Override
    public boolean deleteRecursively(Path path)
            throws FileSystemException
    {
        validate(path);
        if (!exists(path)) {
            return false;
        }
        Collection<IOException> exceptions = new LinkedList<>();
        deleteRecursivelyCore(path, exceptions);
        if (!exceptions.isEmpty()) {
            FileSystemException exceptionToThrow = new FileSystemException(path.toString(), null,
                    "Failed to delete one or more files. Please checked suppressed exceptions for details");
            for (IOException ex : exceptions) {
                exceptionToThrow.addSuppressed(ex);
            }
            throw exceptionToThrow;
        }
        return true;
    }

    private void deleteRecursivelyCore(Path path, Collection<IOException> exceptions)
    {
        if (!exists(path)) {
            exceptions.add(new FileNotFoundException(path.toString()));
            return;
        }
        if (!Files.isDirectory(path)) {
            try {
                delete(path);
            }
            catch (IOException ex) {
                exceptions.add(ex);
            }
        }
        else {
            try (Stream<Path> children = list(path)) {
                if (children != null) {
                    children.forEach(child -> deleteRecursivelyCore(child, exceptions));
                }
                delete(path);
            }
            catch (IOException ex) {
                exceptions.add(ex);
            }
        }
    }

    @Override
    public boolean exists(Path path)
    {
        return Files.exists(path);
    }

    @Override
    public void move(Path source, Path target)
            throws IOException
    {
        validate(source);
        validate(target);
        Files.move(source, target);
    }

    @Override
    public InputStream newInputStream(Path path)
            throws IOException
    {
        // Need inline check to pass security check
        if (!path.toAbsolutePath().startsWith(root)) {
            throw new AccessDeniedException(String.format("%s is not in workspace %s. Access has been denied.", path, root));
        }
        return Files.newInputStream(path);
    }

    @Override
    public OutputStream newOutputStream(Path path, OpenOption... options)
            throws IOException
    {
        // Need inline check to pass security check
        if (!path.toAbsolutePath().startsWith(root)) {
            throw new AccessDeniedException(String.format("%s is not in workspace %s. Access has been denied.", path, root));
        }
        return Files.newOutputStream(path, options);
    }

    @Override
    public Object getAttribute(Path path, String attribute)
            throws IOException
    {
        validate(path);
        if (!SupportedFileAttributes.SUPPORTED_ATTRIBUTES.contains(attribute)) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Attribute [%s] is not supported.", attribute));
        }
        // Get time in millis instead of date time format
        if (attribute.equalsIgnoreCase(SupportedFileAttributes.LAST_MODIFIED_TIME)) {
            return Files.getLastModifiedTime(path).toMillis();
        }
        return Files.getAttribute(path, attribute);
    }

    @Override
    public boolean isDirectory(Path path)
    {
        return Files.isDirectory(path);
    }

    @Override
    public Stream<Path> list(Path dir)
            throws IOException
    {
        validate(dir);
        return Files.list(dir);
    }

    @Override
    public Stream<Path> walk(Path dir)
            throws IOException
    {
        validate(dir);
        return Files.walk(dir);
    }

    @Override
    public void close()
    {
    }
}
