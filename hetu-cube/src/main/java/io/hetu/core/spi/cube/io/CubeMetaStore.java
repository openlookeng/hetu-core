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

package io.hetu.core.spi.cube.io;

import io.hetu.core.spi.cube.CubeMetadata;
import io.hetu.core.spi.cube.CubeMetadataBuilder;

import java.util.List;
import java.util.Optional;

/**
 * CubeMetaStore provides APIs to retrieve, update the Cube metadata
 * from the underlying metastore.
 */
public interface CubeMetaStore
{
    /**
     * Persist cube metadata in the underlying metastore
     * @param cubeMetadata cube metadata
     */
    void persist(CubeMetadata cubeMetadata);

    /**
     * Create a new Metadata builder
     * @param cubeName Name of the cube
     * @param originalTableName Name of the original table
     * @return a metadata builder
     */
    CubeMetadataBuilder getBuilder(String cubeName, String originalTableName);

    /**
     * Create new metadata builder from the existing metadata
     * @param existingMetadata existing metadata
     * @return a metadata builder
     */
    CubeMetadataBuilder getBuilder(CubeMetadata existingMetadata);

    /**
     * Returns the list of cube metadata associated with the given table.
     * @param tableName fully qualified name of the table
     * @return list of cube metadata
     */
    List<CubeMetadata> getMetadataList(String tableName);

    /**
     * Find a cube metadata associated with the given name
     * @param cubeName name of the cube
     * @return optional cube metadata
     */
    Optional<CubeMetadata> getMetadataFromCubeName(String cubeName);

    /**
     * Return all cube information
     * @return list of cube metadata
     */
    List<CubeMetadata> getAllCubes();

    /**
     * Remove cube metadata
     */
    void removeCube(CubeMetadata cubeMetadata);
}
