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
    private final QualifiedName sourceTableName;
    private final Optional<Expression> where;
    private final List<Identifier> groupingSet;
    private final Set<FunctionCall> aggregations;
    private final List<Property> properties;

    public CreateCube(QualifiedName cubeName, QualifiedName sourceTableName, List<Identifier> groupingSet,
            Set<FunctionCall> aggregations, boolean notExists, List<Property> properties, Optional<Expression> where)
    {
        this(Optional.empty(), cubeName, sourceTableName, groupingSet, aggregations, notExists, properties, where);
    }

    public CreateCube(NodeLocation location, QualifiedName cubeName, QualifiedName sourceTableName, List<Identifier> groupingSet,
            Set<FunctionCall> aggregations, boolean notExists, List<Property> properties, Optional<Expression> where)
    {
        this(Optional.of(location), cubeName, sourceTableName, groupingSet, aggregations, notExists, properties, where);
    }

    private CreateCube(Optional<NodeLocation> location, QualifiedName cubeName, QualifiedName sourceTableName, List<Identifier> groupingSet,
            Set<FunctionCall> aggregations, boolean notExists, List<Property> properties, Optional<Expression> where)
    {
        super(location);
        this.cubeName = requireNonNull(cubeName, "cube name is null");
        this.sourceTableName = requireNonNull(sourceTableName, "table name is null");
        this.groupingSet = groupingSet;
        this.aggregations = aggregations;
        this.notExists = notExists;
        this.properties = properties;
        this.where = where;
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
                sourceTableName,
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
                .add("tableName", sourceTableName)
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
                Objects.equals(sourceTableName, that.sourceTableName) &&
                Objects.equals(groupingSet, that.groupingSet) &&
                Objects.equals(aggregations, that.aggregations) &&
                Objects.equals(notExists, that.notExists) &&
                Objects.equals(properties, that.properties);
    }
}
