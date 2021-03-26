/*
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
package io.hetu.core.common.filesystem;

import io.airlift.log.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.UUID;

/**
 * A temporary folder used for testing. If used with try-with-resource pattern, it destroys itself after each usage.
 */
public class TempFolder
        implements AutoCloseable
{
    private static final Logger LOG = Logger.get(TempFolder.class);
    private final String prefix;
    private File root;

    public TempFolder()
    {
        this("");
    }

    public TempFolder(String user)
    {
        this.prefix = "hetu-tmp-folder-" + user;
    }

    public TempFolder create()
            throws IOException
    {
        root = Files.createTempDirectory(prefix).toFile();
        return this;
    }

    public File getRoot()
    {
        return root;
    }

    public File newFile()
            throws IOException
    {
        return newFile("file-" + UUID.randomUUID().toString());
    }

    public File newFile(String relativePath)
            throws IOException
    {
        File newFile = root.toPath().resolve(relativePath).toFile();
        if (newFile.createNewFile()) {
            return newFile;
        }
        throw new IOException("Not able to create file " + relativePath);
    }

    public File newFolder()
            throws IOException
    {
        return newFolder("folder-" + UUID.randomUUID().toString());
    }

    public File newFolder(String relativePath)
            throws IOException
    {
        File newFolder = root.toPath().resolve(relativePath).toFile();
        if (newFolder.mkdir()) {
            return newFolder;
        }
        throw new IOException("Not able to create folder " + relativePath);
    }

    @Override
    public void close()
    {
        if (root != null && root.exists()) {
            // retry deletion for 3 times
            for (int i = 0; i < 3; i++) {
                if (deleteRecursively(root)) {
                    return;
                }
            }

            if (!root.exists()) {
                return;
            }

            LOG.warn("Temporary folder can not be deleted. Shutdown hook added: " + root.toPath().toAbsolutePath());
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    Files.walk(root.toPath())
                            .sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(File::delete);
                }
                catch (IOException e) {
                    LOG.warn("Temporary folder not deleted. Manual deletion required: " + root.toPath().toAbsolutePath());
                }
            }));
        }
    }

    private boolean deleteRecursively(File fileToDelete)
    {
        if (fileToDelete.delete()) {
            return true;
        }
        File[] files = fileToDelete.listFiles();
        if (files != null) {
            for (File fileInDir : files) {
                if (!deleteRecursively(fileInDir)) {
                    return false;
                }
            }
        }
        return fileToDelete.delete();
    }
}
