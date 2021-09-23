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
    private final QualifiedName sourceTableName;
    private final Optional<Expression> where;
    private final List<Identifier> groupingSet;
    private final Set<FunctionCall> aggregations;
    private final List<Property> properties;
    private final Expression sourceFilter;

    public CreateCube(QualifiedName cubeName, QualifiedName sourceTableName, List<Identifier> groupingSet,
            Set<FunctionCall> aggregations, boolean notExists, List<Property> properties, Optional<Expression> where, Expression sourceFilter)
    {
        this(Optional.empty(), cubeName, sourceTableName, groupingSet, aggregations, notExists, properties, where, sourceFilter);
    }

    public CreateCube(NodeLocation location, QualifiedName cubeName, QualifiedName sourceTableName, List<Identifier> groupingSet,
            Set<FunctionCall> aggregations, boolean notExists, List<Property> properties, Optional<Expression> where, Expression sourceFilter)
    {
        this(Optional.of(location), cubeName, sourceTableName, groupingSet, aggregations, notExists, properties, where, sourceFilter);
    }

    private CreateCube(Optional<NodeLocation> location, QualifiedName cubeName, QualifiedName sourceTableName, List<Identifier> groupingSet,
            Set<FunctionCall> aggregations, boolean notExists, List<Property> properties, Optional<Expression> where, Expression sourceFilter)
    {
        super(location);
        this.cubeName = requireNonNull(cubeName, "cube name is null");
        this.sourceTableName = requireNonNull(sourceTableName, "table name is null");
        this.groupingSet = groupingSet;
        this.aggregations = aggregations;
        this.notExists = notExists;
        this.properties = properties;
        this.where = where;
        this.sourceFilter = sourceFilter;
    }

    public QualifiedName getCubeName()
    {
        return cubeName;
    }

    public QualifiedName getSourceTableName()
    {
        return sourceTableName;
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

    public Optional<Expression> getWhere()
    {
        return where;
    }

    public boolean isNotExists()
    {
        return notExists;
    }

    public Optional<Expression> getSourceFilter()
    {
        return Optional.ofNullable(sourceFilter);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateCube(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.addAll(groupingSet);
        nodes.addAll(aggregations);
        nodes.addAll(properties);
        where.ifPresent(nodes::add);
        if (sourceFilter != null) {
            nodes.add(sourceFilter);
        }
        return nodes.build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                cubeName,
                sourceTableName,
                groupingSet,
                aggregations,
                notExists,
                properties,
                where,
                sourceFilter);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("cubeName", cubeName)
                .add("tableName", sourceTableName)
                .add("groupingSet", groupingSet)
                .add("aggregations", aggregations)
                .add("notExists", notExists)
                .add("properties", properties)
                .add("where", where)
                .add("sourceFilterPredicate", sourceFilter)
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
                Objects.equals(sourceTableName, that.sourceTableName) &&
                Objects.equals(groupingSet, that.groupingSet) &&
                Objects.equals(aggregations, that.aggregations) &&
                Objects.equals(notExists, that.notExists) &&
                Objects.equals(properties, that.properties) &&
                Objects.equals(where, that.where) &&
                Objects.equals(sourceFilter, that.sourceFilter);
    }
}
