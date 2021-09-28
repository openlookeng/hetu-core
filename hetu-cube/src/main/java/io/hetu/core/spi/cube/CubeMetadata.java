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

package io.hetu.core.spi.cube;

import io.hetu.core.spi.cube.aggregator.AggregationSignature;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public interface CubeMetadata
        extends Serializable
{
    /**
     * Returns name of the cube
     */
    String getCubeName();

    /**
     * Returns the name of the source table
     */
    String getSourceTableName();

    /**
     * Returns the last updated time of the cube
     */
    long getLastUpdatedTime();

    /**
     * Returns the last updated time of the source table
     */
    long getSourceTableLastUpdatedTime();

    /**
     * Return the names of the dimension columns
     */
    List<String> getDimensions();

    /**
     * Return the names of the aggregation columns
     */
    List<String> getAggregations();

    /**
     * Cube selection filter
     */
    CubeFilter getCubeFilter();

    /**
     * Return the group by columns
     */
    Set<String> getGroup();

    /**
     * Checks if metadata matches the CubeStatement
     * @param statement cube statement
     * @return true - if metadata matches CubeStatement
     *         false - otherwise
     */
    boolean matches(CubeStatement statement);

    /**
     * Filters all metadata that matches the cube statement
     * @param metadataList metadata list
     * @param statement cube statement
     * @return all metadata that is matching the cube statement
     */
    static List<CubeMetadata> filter(List<CubeMetadata> metadataList, CubeStatement statement)
    {
        return metadataList.stream()
                .filter(metadata -> metadata.matches(statement))
                .collect(Collectors.toList());
    }

    /**
     * Retrieves the cube column matching the given aggregation signature
     * @return name of the aggregation column if found
     */
    Optional<String> getColumn(AggregationSignature aggSignature);

    /**
     * Return the aggregation function associated with given cube column
     * @return name of the aggregation function
     */
    Optional<String> getAggregationFunction(String column);

    /**
     * Get the aggregation information of the given cube column
     * @param column name of the cube column
     */
    Optional<AggregationSignature> getAggregationSignature(String column);

    /**
     * Return all aggregation column information
     */
    List<AggregationSignature> getAggregationSignatures();

    /**
     * Return the status of the cube
     */
    CubeStatus getCubeStatus();
}
