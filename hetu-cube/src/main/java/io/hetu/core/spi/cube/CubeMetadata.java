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
    String getCubeTableName();

    String getOriginalTableName();

    long getLastUpdated();

    List<String> getDimensions();

    List<String> getAggregations();

    List<String> getAggregationsAsString();

    Set<String> getGroup();

    boolean matches(CubeStatement statement);

    static List<CubeMetadata> filter(List<CubeMetadata> metadataList, CubeStatement statement)
    {
        return metadataList.stream()
                .filter(metadata -> metadata.matches(statement))
                .collect(Collectors.toList());
    }

    Optional<String> getColumn(AggregationSignature aggSignature);

    Optional<String> getAggregationFunction(String starTableColumn);

    Optional<String> getAggregationColumn(String aggFunction, String originalColumn, boolean distinct);

    Optional<AggregationSignature> getAggregationSignature(String column);

    List<AggregationSignature> getAggregationSignatures();

    String getPredicateString();

    String getGroupString();

    CubeStatus getCubeStatus();
}
