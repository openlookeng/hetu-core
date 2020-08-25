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

package io.prestosql.catalog;

import io.airlift.log.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CatalogFileInputStream
        implements Closeable
{
    private static final Logger log = Logger.get(CatalogFileInputStream.class);
    private Map<String, InputStreamWithType> inputStreamWithTypeMap;

    private CatalogFileInputStream(Builder builder)
    {
        this.inputStreamWithTypeMap = builder.inputStreamWithTypeMap;
    }

    @Override
    public void close()
            throws IOException
    {
        inputStreamWithTypeMap.forEach((name, inputStream) -> {
            try {
                inputStream.getInputStream().close();
            }
            catch (IOException ignore) {
            }
        });
    }

    public Map<String, InputStreamWithType> getInputStreams()
    {
        return this.inputStreamWithTypeMap;
    }

    public void mark()
    {
        inputStreamWithTypeMap.forEach((name, inputStream) -> {
            if (inputStream.getInputStream().markSupported()) {
                inputStream.getInputStream().mark(0);
            }
            else {
                throw new RuntimeException("InputStream can not support mark");
            }
        });
    }

    public void reset()
    {
        inputStreamWithTypeMap.forEach((name, inputStream) -> {
            if (inputStream.getInputStream().markSupported()) {
                try {
                    inputStream.getInputStream().reset();
                }
                catch (IOException ignore) {
                    log.warn("Reset inputStream of %s failed", name);
                    throw new RuntimeException("InputStream reset failed");
                }
            }
            else {
                throw new RuntimeException("InputStream can not support reset");
            }
        });
    }

    public List<String> getCatalogFileNames()
    {
        return inputStreamWithTypeMap.entrySet().stream()
                .filter(entry -> entry.getValue().getFileType() == CatalogFileType.CATALOG_FILE)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    public List<String> getGlobalFileNames()
    {
        return inputStreamWithTypeMap.entrySet().stream()
                .filter(entry -> entry.getValue().getFileType() == CatalogFileType.GLOBAL_FILE)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    public static class InputStreamWithType
    {
        private CatalogFileType fileType;
        private InputStream inputStream;

        InputStreamWithType(CatalogFileType fileType, InputStream inputStream)
        {
            this.fileType = fileType;
            this.inputStream = inputStream;
        }

        InputStream getInputStream()
        {
            return inputStream;
        }

        CatalogFileType getFileType()
        {
            return fileType;
        }
    }

    public static class Builder
            implements Closeable
    {
        private Map<String, InputStreamWithType> inputStreamWithTypeMap = new HashMap<>();
        private final int maxFileSizeInBytes;

        public Builder(int maxFileSizeInBytes)
        {
            this.maxFileSizeInBytes = maxFileSizeInBytes;
        }

        private ByteArrayOutputStream cloneInputStream(InputStream inputStream, int maxFileSizeInBytes)
                throws IOException
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int len;
            int totalLen = 0;
            while ((len = inputStream.read(buffer)) > -1) {
                baos.write(buffer, 0, len);
                totalLen += len;
            }
            if (totalLen > maxFileSizeInBytes) {
                throw new IOException("The file size exceeds the upper limit");
            }
            baos.flush();
            return baos;
        }

        public Builder putAll(CatalogFileInputStream catalogFileInputStream)
                throws IOException
        {
            for (Map.Entry<String, InputStreamWithType> entry : catalogFileInputStream.getInputStreams().entrySet()) {
                String name = entry.getKey();
                InputStreamWithType inputStreamWithType = entry.getValue();
                try (ByteArrayOutputStream outputStream = cloneInputStream(inputStreamWithType.getInputStream(), maxFileSizeInBytes)) {
                    InputStream cloneInputStream = new ByteArrayInputStream(outputStream.toByteArray());
                    inputStreamWithTypeMap.put(name, new InputStreamWithType(inputStreamWithType.getFileType(), cloneInputStream));
                }
                catch (IOException ex) {
                    close();
                    throw ex;
                }
            }
            return this;
        }

        public Builder put(String fileName, CatalogFileType fileType, InputStream inputStream)
                throws IOException
        {
            try (ByteArrayOutputStream outputStream = cloneInputStream(inputStream, maxFileSizeInBytes)) {
                InputStream cloneInputStream = new ByteArrayInputStream(outputStream.toByteArray());
                inputStreamWithTypeMap.put(fileName, new InputStreamWithType(fileType, cloneInputStream));
            }
            catch (IOException ex) {
                close();
                throw ex;
            }
            return this;
        }

        public CatalogFileInputStream build()
        {
            return new CatalogFileInputStream(this);
        }

        @Override
        public void close()
        {
            inputStreamWithTypeMap.forEach((name, inputStream) -> {
                try {
                    inputStream.getInputStream().close();
                }
                catch (IOException ignore) {
                }
            });
        }
    }

    public enum CatalogFileType
    {
        UNKNOWN_TYPE,
        CATALOG_FILE,
        GLOBAL_FILE
    }
}
