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

package io.prestosql.spi.cube;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class CubeUpdateMetadata
{
    private final String cubeName;
    private final long tableLastUpdatedTime;
    private final String dataPredicateString;
    private final boolean overwrite;

    @JsonCreator
    public CubeUpdateMetadata(
            @JsonProperty("cubeName") String cubeName,
            @JsonProperty("tableLastUpdatedTime") long tableLastUpdatedTime,
            @JsonProperty("dataPredicate") String dataPredicateString,
            @JsonProperty("overwrite") boolean overwrite)
    {
        this.cubeName = requireNonNull(cubeName, "cubeName is null");
        this.tableLastUpdatedTime = tableLastUpdatedTime;
        this.dataPredicateString = dataPredicateString;
        this.overwrite = overwrite;
    }

    @JsonProperty
    public String getCubeName()
    {
        return cubeName;
    }

    @JsonProperty
    public long getTableLastUpdatedTime()
    {
        return tableLastUpdatedTime;
    }

    @JsonProperty
    public String getDataPredicateString()
    {
        return dataPredicateString;
    }

    @JsonProperty
    public boolean isOverwrite()
    {
        return overwrite;
    }

    @Override
    public String toString()
    {
        return "CubeUpdateMetadata{" +
                "cubeName='" + cubeName + '\'' +
                ", tableLastUpdatedTime=" + tableLastUpdatedTime +
                ", dataPredicateString='" + dataPredicateString + '\'' +
                ", overwrite=" + overwrite +
                '}';
    }
}
