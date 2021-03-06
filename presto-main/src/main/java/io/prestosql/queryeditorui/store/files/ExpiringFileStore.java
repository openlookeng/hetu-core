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
package io.prestosql.queryeditorui.store.files;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.units.DataSize;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class ExpiringFileStore
{
    private static final Logger LOG = LoggerFactory.getLogger(ExpiringFileStore.class);

    private LoadingCache<String, FileWithMetadata> fileWithMetadataCache;
    private File basePath = new File(System.getProperty("java.io.tmpdir"));

    public ExpiringFileStore(int maxFileCount)
    {
        this.fileWithMetadataCache = CacheBuilder.newBuilder().maximumSize(maxFileCount)
                .removalListener(notification -> {
                    FileWithMetadata value = (FileWithMetadata) notification.getValue();
                    File f = value.getFile();
                    if (f != null && f.exists()) {
                        f.delete();
                    }
                }).build(new CacheLoader<String, FileWithMetadata>()
                {
                    @Override
                    public FileWithMetadata load(String key) throws Exception
                    {
                        throw new PrestoException(StandardErrorCode.PERMISSION_DENIED, "No permission");
                    }
                });
    }

    public File get(String key, Optional<String> user)
    {
        try {
            FileWithMetadata fileWithMetadata = fileWithMetadataCache.get(key);
            if (user.isPresent()) {
                return user.get().equals(fileWithMetadata.getUser()) ? fileWithMetadata.getFile() : null;
            }
            return fileWithMetadata.getFile();
        }
        catch (ExecutionException e) {
            return null;
        }
    }

    public void addFile(String key, String user, File file)
            throws IOException
    {
        long fileSize = file.length();
        fileWithMetadataCache.put(key, new FileWithMetadata(file, user, new DataSize(fileSize, DataSize.Unit.BYTE), DateTime.now()));
    }

    private static class FileWithMetadata
    {
        private final File file;
        private final String user;
        private final DataSize size;
        private final DateTime createdAt;

        public FileWithMetadata(File file, String user, DataSize size, DateTime createdAt)
        {
            this.file = file;
            this.user = user;
            this.size = size;
            this.createdAt = createdAt;
        }

        public File getFile()
        {
            return file;
        }

        public String getUser()
        {
            return user;
        }

        public DataSize getSize()
        {
            return size;
        }

        public DateTime getCreatedAt()
        {
            return createdAt;
        }
    }
}
