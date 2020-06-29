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

package io.hetu.core.heuristicindex.base;

import io.hetu.core.spi.heuristicindex.IndexStore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Persistence class for storing data into local file
 *
 * @since 2019-09-18
 */
public class LocalIndexStore
        implements IndexStore
{
    private static final String ID = "LOCAL";

    private Properties properties;

    @Override
    public String getId()
    {
        return ID;
    }

    @Override
    public boolean delete(String path) throws IOException
    {
        File file = new File(path);
        if (!file.exists()) {
            // If path is not valid, do nothing
            return false;
        }
        if (file.isFile()) {
            return file.delete();
        }
        else {
            // When it's a directory
            File[] files = file.listFiles();
            if (files != null) {
                for (File fileInDir : files) {
                    if (!delete(fileInDir.getPath())) {
                        return false;
                    }
                }
            }
            return file.delete();
        }
    }

    @Override
    public void write(String content, String path, boolean isOverwritable) throws IOException
    {
        File file = new File(path);
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new IOException("Failed to create file: " + file.getParentFile().getCanonicalPath());
        }
        try (OutputStreamWriter bout =
                     new OutputStreamWriter(new FileOutputStream(file, !isOverwritable), StandardCharsets.UTF_8)) {
            bout.append(content);
        }
    }

    @Override
    public OutputStream getOutputStream(String path, boolean isOverwritable) throws IOException
    {
        File file = new File(path);
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new IOException("Failed to create file: " + file.getParentFile().getCanonicalPath());
        }
        // FileOutputStream constructor will throw FileNotFoundException if the file points to a directory
        // or cannot be created
        return new FileOutputStream(file, !isOverwritable);
    }

    @Override
    public InputStream read(String path) throws IOException
    {
        File file = new File(path);
        // FileInputStream constructor will throw FileNotFoundException
        return new FileInputStream(file);
    }

    @Override
    public boolean exists(String path)
    {
        File file = new File(path);
        return file.exists();
    }

    @Override
    public boolean renameTo(String curPath, String newPath)
    {
        try {
            checkFileValidity(curPath);
        }
        catch (FileNotFoundException ex) {
            return false;
        }
        File curFile = new File(curPath);
        File newFile = new File(newPath);
        return curFile.renameTo(newFile);
    }

    @Override
    public long getLastModifiedTime(String path)
    {
        File file = new File(path);
        return file.lastModified();
    }

    @Override
    public Collection<String> listChildren(String path, boolean isRecursive) throws IOException
    {
        Set<String> files = new HashSet<>(1);
        if (isRecursive) {
            Files.walk(Paths.get(path)).filter(Files::isRegularFile).forEach(e -> files.add(e.toString()));
        }
        else {
            Files.list(Paths.get(path)).forEach(e -> files.add(e.toString()));
        }
        return files;
    }

    /**
     * Helper method for checking whether the path points to a file that exists
     *
     * @param path
     * @throws FileNotFoundException
     */
    private void checkFileValidity(String path) throws FileNotFoundException
    {
        if (!exists(path)) {
            throw new FileNotFoundException("File to be appended does not exist or is not a file");
        }
    }

    public Properties getProperties()
    {
        return properties;
    }

    public void setProperties(Properties properties)
    {
        this.properties = properties;
    }
}
