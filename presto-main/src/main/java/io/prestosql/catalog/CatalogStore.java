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

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.locks.Lock;

public interface CatalogStore
        extends AutoCloseable
{
    /**
     * Stores files based on the file-related attribute values in the catalogName.properties file.
     *
     * @param catalogInfo catalog properties.
     * @param configFiles catalog config files.
     * @throws IOException when create catalog failed.
     */
    void createCatalog(CatalogInfo catalogInfo, CatalogFileInputStream configFiles)
            throws IOException;

    /**
     * Delete catalog related files.
     *
     * @param catalogName the name of catalog that to be deleted.
     * @param totalDelete if delete all files include global files.
     */
    void deleteCatalog(String catalogName, boolean totalDelete)
            throws IOException;

    /**
     * Get catalog related files from file system.
     *
     * @param catalogName catalog name.
     * @return catalog file input stream.
     * @throws IOException when get catalog failed.
     */
    CatalogFileInputStream getCatalogFiles(String catalogName)
            throws IOException;

    /**
     * Get catalog information from file system.
     *
     * @param catalogName catalog name.
     * @return catalog properties.
     * @throws IOException when get catalog information failed.
     */
    CatalogInfo getCatalogInformation(String catalogName)
            throws IOException;

    /**
     * List catalog names of file system.
     *
     * @return catalog name list.
     * @throws IOException when list catalog names failed.
     */
    Set<String> listCatalogNames()
            throws IOException;

    /**
     * Get catalog lock, other nodes can not operate this catalog.
     *
     * @param catalogName catalog name.
     * @return lock object.
     * @throws IOException lock failed.
     */
    Lock getCatalogLock(String catalogName)
            throws IOException;

    /**
     * Release lock, and delete lock directory.
     * @param catalogName
     */
    void releaseCatalogLock(String catalogName);

    void close();
}
