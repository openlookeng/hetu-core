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

package io.hetu.core.spi.cube.aggregator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

import static io.hetu.core.spi.cube.CubeAggregateFunction.AVG;
import static io.hetu.core.spi.cube.CubeAggregateFunction.COUNT;
import static io.hetu.core.spi.cube.CubeAggregateFunction.MAX;
import static io.hetu.core.spi.cube.CubeAggregateFunction.MIN;
import static io.hetu.core.spi.cube.CubeAggregateFunction.SUM;

public class AggregationSignature
        implements Serializable, Comparable<AggregationSignature>
{
    private static final AggregationSignature COUNT_SIGNATURE = new AggregationSignature(COUNT.getName(), "*", false);

    private String function;
    private String dimension;
    private boolean distinct;

    @JsonCreator
    public AggregationSignature(
            @JsonProperty("function") String function,
            @JsonProperty("dimension") String dimension,
            @JsonProperty("distinct") boolean distinct)
    {
        this.function = function;
        this.dimension = dimension;
        this.distinct = distinct;
    }

    public static AggregationSignature count()
    {
        return COUNT_SIGNATURE;
    }

    public static AggregationSignature count(String dimension, boolean distinct)
    {
        return new AggregationSignature(COUNT.getName(), dimension, distinct);
    }

    public static AggregationSignature sum(String dimension, boolean distinct)
    {
        return new AggregationSignature(SUM.toString(), dimension, distinct);
    }

    public static AggregationSignature avg(String dimension, boolean distinct)
    {
        return new AggregationSignature(AVG.toString(), dimension, distinct);
    }

    public static AggregationSignature min(String dimension, boolean distinct)
    {
        return new AggregationSignature(MIN.getName(), dimension, distinct);
    }

    public static AggregationSignature max(String dimension, boolean distinct)
    {
        return new AggregationSignature(MAX.getName(), dimension, distinct);
    }

    @JsonProperty
    public String getFunction()
    {
        return function;
    }

    @JsonProperty
    public String getDimension()
    {
        return dimension;
    }

    @JsonProperty
    public boolean isDistinct()
    {
        return distinct;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(function, dimension);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AggregationSignature other = (AggregationSignature) obj;
        return Objects.equals(this.function, other.function) &&
                Objects.equals(this.dimension, other.dimension) &&
                Objects.equals(this.distinct, other.distinct);
    }

    @Override
    public String toString()
    {
        return this.function + "(" + (this.distinct ? "distinct " : "") + this.dimension + ")";
    }

    @Override
    public int compareTo(AggregationSignature aggregationSignature)
    {
        int nameComparison = function.compareTo(aggregationSignature.function);
        if (0 != nameComparison) {
            return nameComparison;
        }
        else {
            int dimensionComparison = dimension.compareTo(aggregationSignature.dimension);
            if (0 != dimensionComparison) {
                return dimensionComparison;
            }
            else {
                return Boolean.compare(distinct, aggregationSignature.distinct);
            }
        }
    }
}
