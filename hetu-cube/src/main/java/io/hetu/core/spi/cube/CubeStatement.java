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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public class CubeStatement
{
    private final String from;
    private final Set<String> groupBy;
    private final Set<String> selection;
    private final List<AggregationSignature> aggregations;

    private CubeStatement(
            Set<String> selection,
            String from,
            Set<String> groupBy,
            List<AggregationSignature> aggregations)
    {
        this.selection = requireNonNull(selection, "selection is null");
        this.from = requireNonNull(from, "from is null");
        this.groupBy = requireNonNull(groupBy, "groupBy is null");
        this.aggregations = requireNonNull(aggregations, "aggregations is null");
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public Set<String> getSelection()
    {
        return selection;
    }

    public String getFrom()
    {
        return from;
    }

    public List<AggregationSignature> getAggregations()
    {
        return aggregations;
    }

    public Set<String> getGroupBy()
    {
        return groupBy;
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
        CubeStatement that = (CubeStatement) o;
        return Objects.equals(selection, that.selection) &&
                Objects.equals(from, that.from) &&
                Objects.equals(groupBy, that.groupBy) &&
                Objects.equals(aggregations, that.aggregations);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(selection, from, groupBy, aggregations);
    }

    @Override
    public String toString()
    {
        StringJoiner columns = new StringJoiner(", ");
        selection.forEach(columns::add);
        aggregations.forEach(agg -> columns.add(agg.toString()));

        StringJoiner groupingColumns = new StringJoiner(", ");
        groupBy.forEach(groupingColumns::add);

        return "SELECT " + columns +
                " FROM " + from +
                (groupBy.isEmpty() ? "" : " GROUP BY " + groupingColumns);
    }

    public static class Builder
    {
        private String from;
        private final Set<String> groupBy = new HashSet<>();
        private final Set<String> selection = new HashSet<>();
        private final List<AggregationSignature> aggregations = new ArrayList<>();

        private Builder()
        {
            // Do nothing
        }

        public Builder select(String column, String... columns)
        {
            this.selection.add(column);
            this.selection.addAll(Arrays.asList(columns));
            return this;
        }

        public Builder aggregate(AggregationSignature signature)
        {
            this.aggregations.add(signature);
            return this;
        }

        public Builder from(String from)
        {
            this.from = from;
            return this;
        }

        public Builder groupByAddString(String column)
        {
            this.groupBy.add(column);
            return this;
        }

        public Builder groupByAddStringList(String... columns)
        {
            this.groupBy.addAll(Arrays.asList(columns));
            return this;
        }

        public CubeStatement build()
        {
            if (this.aggregations.isEmpty() && this.selection.isEmpty()) {
                throw new UnsupportedOperationException("Cannot construct a cube statement without selection and aggregation");
            }
            return new CubeStatement(Collections.unmodifiableSet(selection), from, Collections.unmodifiableSet(groupBy), Collections.unmodifiableList(aggregations));
        }
    }
}
