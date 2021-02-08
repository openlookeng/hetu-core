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
package io.hetu.core.common.heuristicindex;

import java.util.Objects;

public class IndexCacheKey
{
    private String path;
    private long lastModifiedTime;

    /**
     * @param path path to the file the index files should be read for
     * @param lastModifiedTime lastModifiedTime of the file, used to validate the indexes
     */
    public IndexCacheKey(String path, long lastModifiedTime)
    {
        this.path = path;
        this.lastModifiedTime = lastModifiedTime;
    }

    public String getPath()
    {
        return path;
    }

    public long getLastModifiedTime()
    {
        return lastModifiedTime;
    }

    // only the path should be used as the key
    // the lastModifiedTime time is only used to check if index is valid
    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexCacheKey that = (IndexCacheKey) o;
        return Objects.equals(path, that.path);
    }

    @Override
    public String toString()
    {
        return "IndexCacheKey{" +
                "path='" + path + '\'' +
                '}';
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path);
    }
}
