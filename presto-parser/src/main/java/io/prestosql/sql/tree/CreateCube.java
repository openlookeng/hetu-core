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

package io.prestosql.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateCube
        extends Statement
{
    private final QualifiedName cubeName;
    private final boolean notExists;
    private final QualifiedName tableName;
    private final List<Identifier> groupingSet;
    private final Set<FunctionCall> aggregations;
    private final List<Property> properties;

    public CreateCube(QualifiedName cubeName, QualifiedName tableName, List<Identifier> groupingSet,
            Set<FunctionCall> aggregations, boolean notExists, List<Property> properties)
    {
        this(Optional.empty(), cubeName, tableName, groupingSet, aggregations, notExists, properties);
    }

    public CreateCube(NodeLocation location, QualifiedName cubeName, QualifiedName tableName, List<Identifier> groupingSet,
            Set<FunctionCall> aggregations, boolean notExists, List<Property> properties)
    {
        this(Optional.of(location), cubeName, tableName, groupingSet, aggregations, notExists, properties);
    }

    private CreateCube(Optional<NodeLocation> location, QualifiedName cubeName, QualifiedName tableName, List<Identifier> groupingSet,
            Set<FunctionCall> aggregations, boolean notExists, List<Property> properties)
    {
        super(location);
        this.cubeName = requireNonNull(cubeName, "cube name is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.groupingSet = groupingSet;
        this.aggregations = aggregations;
        this.notExists = notExists;
        this.properties = properties;
    }

    public QualifiedName getCubeName()
    {
        return cubeName;
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public List<Property> getProperties()
    {
        return properties;
    }

    public List<Identifier> getGroupingSet()
    {
        return groupingSet;
    }

    public Set<FunctionCall> getAggregations()
    {
        return aggregations;
    }

    public boolean isNotExists()
    {
        return notExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateCube(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .addAll(aggregations)
                .addAll(properties)
                .build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                cubeName,
                tableName,
                groupingSet,
                aggregations,
                notExists,
                properties);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("cubeName", cubeName)
                .add("tableName", tableName)
                .add("groupingSet", groupingSet)
                .add("aggregations", aggregations)
                .add("notExists", notExists)
                .add("properties", properties)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CreateCube)) {
            return false;
        }
        CreateCube that = (CreateCube) o;
        return Objects.equals(cubeName, that.cubeName) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(groupingSet, that.groupingSet) &&
                Objects.equals(aggregations, that.aggregations) &&
                Objects.equals(notExists, that.notExists) &&
                Objects.equals(properties, that.properties);
    }
}
