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

package io.hetu.core.cube.startree.tree;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.hetu.core.spi.cube.CubeMetadata;
import io.hetu.core.spi.cube.CubeStatement;
import io.hetu.core.spi.cube.CubeStatus;
import io.hetu.core.spi.cube.aggregator.AggregationSignature;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static io.hetu.core.cube.startree.tree.StarTreeColumn.ColumnType.AGGREGATE;
import static io.hetu.core.cube.startree.tree.StarTreeColumn.ColumnType.DIMENSION;
import static java.util.Objects.requireNonNull;

public class StarTreeMetadata
        implements CubeMetadata
{
    private final String starTreeName;

    private final String originalTableName;

    private final List<StarTreeColumn> columns;

    private final List<Set<String>> groups;

    private final String predicateString;

    private final long lastUpdated;

    private CubeStatus cubeStatus;

    public static final String COLUMN_DELIMITER = ",";
    public static final String GROUP_DELIMITER = "\t";

    @JsonCreator
    public StarTreeMetadata(
            @JsonProperty("starTreeName") String starTreeName,
            @JsonProperty("originalTableName") String originalTableName,
            @JsonProperty("columns") List<StarTreeColumn> columns,
            @JsonProperty("groups") List<Set<String>> groups,
            @JsonProperty("predicateString") String predicateString,
            @JsonProperty("lastUpdated") long lastUpdated,
            @JsonProperty("cubeStatus") CubeStatus cubeStatus)
    {
        this.starTreeName = requireNonNull(starTreeName, "starTreeName is null").toLowerCase(Locale.ENGLISH);
        this.originalTableName = requireNonNull(originalTableName, "tableName is null").toLowerCase(Locale.ENGLISH);
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.groups = new ArrayList<>();
        requireNonNull(groups, "groups is null").forEach(group -> {
            this.groups.add(new TreeSet<>(group));
        });
        this.predicateString = predicateString;
        this.lastUpdated = lastUpdated;
        this.cubeStatus = cubeStatus;
    }

    @JsonProperty
    @Override
    public String getCubeTableName()
    {
        return starTreeName;
    }

    @JsonProperty
    @Override
    public String getOriginalTableName()
    {
        return originalTableName;
    }

    @JsonProperty
    public List<StarTreeColumn> getColumns()
    {
        return columns;
    }

    @JsonProperty
    @Override
    public Set<String> getGroup()
    {
        return Iterables.getOnlyElement(groups);
    }

    @JsonProperty
    public String getPredicateString()
    {
        return predicateString;
    }

    @JsonProperty
    @Override
    public long getLastUpdated()
    {
        return lastUpdated;
    }

    @JsonIgnore
    @Override
    public List<String> getDimensions()
    {
        return Collections.unmodifiableList(this.columns.stream()
                .filter(column -> DIMENSION == column.getType())
                .map(StarTreeColumn::getName).collect(Collectors.toList()));
    }

    @JsonIgnore
    @Override
    public List<String> getAggregations()
    {
        return Collections.unmodifiableList(this.columns.stream()
                .filter(column -> AGGREGATE == column.getType())
                .map(StarTreeColumn::getName).collect(Collectors.toList()));
    }

    @Override
    public List<String> getAggregationsAsString()
    {
        return Collections.unmodifiableList(this.columns.stream()
                .filter(column -> AGGREGATE == column.getType())
                .map(StarTreeColumn::getUserFriendlyName).collect(Collectors.toList()));
    }

    @Override
    public boolean matches(CubeStatement statement)
    {
        return this.originalTableName.equals(statement.getFrom()) &&
                hasDimensions(statement.getSelection()) &&
                hasGroup(statement.getGroupBy()) &&
                supportAggregations(statement.getAggregations()) &&
                getCubeStatus() != CubeStatus.INACTIVE;
    }

    private boolean hasDimensions(Collection<String> dimensions)
    {
        return getDimensions().containsAll(dimensions);
    }

    private boolean hasGroup(Set<String> group)
    {
        Set<String> sorted = new TreeSet<>(group);
        return groups.stream().anyMatch(cubeGroup -> cubeGroup.equals(sorted));
    }

    private boolean supportAggregations(Collection<AggregationSignature> aggregations)
    {
        Collection<AggregationSignature> decomposedAggregations = new ArrayList<>();
        aggregations.forEach(aggregationSignature -> {
            if (AggregationSignature.AVG_FUNCTION_NAME.equals(aggregationSignature.getFunction())) {
                decomposedAggregations.add(AggregationSignature.sum(aggregationSignature.getDimension(), false));
                decomposedAggregations.add(AggregationSignature.count(aggregationSignature.getDimension(), false));
            }
            else {
                decomposedAggregations.add(aggregationSignature);
            }
        });
        return decomposedAggregations.stream().allMatch(aggregationSignature -> this.columns.stream()
                .filter(column -> AGGREGATE == column.getType())
                .map(AggregateColumn.class::cast)
                .anyMatch(column -> signatureMatchesColumn(column, aggregationSignature)));
    }

    private boolean signatureMatchesColumn(AggregateColumn column, AggregationSignature aggregationSignature)
    {
        return column.getAggregateFunction().equals(aggregationSignature.getFunction())
                && column.getOriginalColumn().equals(aggregationSignature.getDimension())
                && column.isDistinct() == aggregationSignature.isDistinct();
    }

    @JsonProperty
    @Override
    public CubeStatus getCubeStatus()
    {
        return cubeStatus;
    }

    public void setCubeStatus(CubeStatus cubeStatus)
    {
        this.cubeStatus = cubeStatus;
    }

    @JsonIgnore
    @Override
    public Optional<String> getColumn(AggregationSignature aggSignature)
    {
        return getAggregationColumn(aggSignature.getFunction(), aggSignature.getDimension(), aggSignature.isDistinct());
    }

    @JsonIgnore
    @Override
    public Optional<String> getAggregationColumn(String aggFunction, String originalColumn, boolean distinct)
    {
        return this.columns.stream()
                .filter(column -> AGGREGATE == column.getType())
                .map(AggregateColumn.class::cast)
                .filter(column -> aggFunction.equals(column.getAggregateFunction()) && originalColumn.equals(column.getOriginalColumn()) && distinct == column.isDistinct())
                .findFirst()
                .map(AggregateColumn::getName);
    }

    @JsonIgnore
    @Override
    public List<AggregationSignature> getAggregationSignatures()
    {
        return this.columns.stream()
                .filter(starTreeColumn -> AGGREGATE == starTreeColumn.getType())
                .map(AggregateColumn.class::cast)
                .map(aggregateColumn -> new AggregationSignature(aggregateColumn.getAggregateFunction(), aggregateColumn.getOriginalColumn(), aggregateColumn.isDistinct()))
                .collect(Collectors.toList());
    }

    @JsonIgnore
    @Override
    public Optional<AggregationSignature> getAggregationSignature(String column)
    {
        return this.columns.stream()
                .filter(starTreeColumn -> column.equalsIgnoreCase(starTreeColumn.getName()))
                .filter(starTreeColumn -> AGGREGATE == starTreeColumn.getType())
                .map(AggregateColumn.class::cast)
                .map(aggregateColumn -> new AggregationSignature(aggregateColumn.getAggregateFunction(), aggregateColumn.getOriginalColumn(), aggregateColumn.isDistinct()))
                .findFirst();
    }

    @JsonIgnore
    @Override
    public Optional<String> getAggregationFunction(String starTreeColumn)
    {
        return this.columns.stream()
                .filter(column -> AGGREGATE == column.getType())
                .map(AggregateColumn.class::cast)
                .filter(column -> starTreeColumn.equals(column.getName()))
                .findFirst()
                .map(AggregateColumn::getAggregateFunction);
    }

    @Override
    public String getGroupString()
    {
        StringBuilder stringBuilder = new StringBuilder();
        groups.forEach(group -> {
            if (!group.isEmpty()) {
                group.forEach(column -> {
                    stringBuilder.append(column).append(COLUMN_DELIMITER);
                });
                stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            }
            stringBuilder.append(GROUP_DELIMITER);
        });
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        return stringBuilder.toString();
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
        StarTreeMetadata that = (StarTreeMetadata) o;
        return lastUpdated == that.lastUpdated &&
                starTreeName.equals(that.starTreeName) &&
                originalTableName.equals(that.originalTableName) &&
                columns.equals(that.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(starTreeName, originalTableName, columns, lastUpdated);
    }

    @Override
    public String toString()
    {
        return "StarTreeMetadata{" +
                "starTreeName='" + starTreeName + '\'' +
                ", originalTableName='" + originalTableName + '\'' +
                ", columns=" + columns +
                ", predicateString='" + predicateString + '\'' +
                ", lastUpdated=" + lastUpdated +
                '}';
    }
}
