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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class CubeFilter
{
    private final String sourceTablePredicate;
    private final String cubePredicate;

    @JsonCreator
    public CubeFilter(
            @JsonProperty("sourceTablePredicate") String sourceTablePredicate,
            @JsonProperty("cubePredicate") String cubePredicate)
    {
        this.sourceTablePredicate = sourceTablePredicate;
        this.cubePredicate = cubePredicate;
    }

    public CubeFilter(String sourceTablePredicate)
    {
        this(sourceTablePredicate, null);
    }

    public String getSourceTablePredicate()
    {
        return sourceTablePredicate;
    }

    public String getCubePredicate()
    {
        return cubePredicate;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CubeFilter that = (CubeFilter) o;
        return Objects.equals(sourceTablePredicate, that.sourceTablePredicate)
                && Objects.equals(cubePredicate, that.cubePredicate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sourceTablePredicate, cubePredicate);
    }

    @Override
    public String toString()
    {
        return "CubeFilter{" +
                "sourceTablePredicate='" + sourceTablePredicate + '\'' +
                ", cubePredicate='" + cubePredicate + '\'' +
                '}';
    }
}
