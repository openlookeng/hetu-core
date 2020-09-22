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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateIndex
        extends Statement
{
    private final QualifiedName indexName;
    private final QualifiedName tableName;
    private final List<Identifier> columnAliases;
    private final String indexType;
    private final boolean notExists;
    private final List<Property> properties;
    private final Optional<Expression> where;

    public CreateIndex(QualifiedName indexName, QualifiedName tableName, List<Identifier> columnAliases,
                       String indexType, boolean notExists, List<Property> properties)
    {
        this(Optional.empty(), indexName, tableName, columnAliases, indexType, notExists, properties, Optional.empty());
    }

    public CreateIndex(NodeLocation location, QualifiedName indexName, QualifiedName tableName, List<Identifier> columnAliases,
                       String indexType, boolean notExists, List<Property> properties, Optional<Expression> where)
    {
        this(Optional.of(location), indexName, tableName, columnAliases, indexType, notExists, properties, where);
    }

    private CreateIndex(Optional<NodeLocation> location, QualifiedName indexName, QualifiedName tableName, List<Identifier> columnAliases,
                        String indexType, boolean notExists, List<Property> properties, Optional<Expression> where)
    {
        super(location);
        this.indexName = requireNonNull(indexName, "indexName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columnAliases = columnAliases;
        this.indexType = requireNonNull(indexType, "indexType is null");
        this.notExists = notExists;
        this.properties = ImmutableList.copyOf(requireNonNull(properties, "properties is null"));
        this.where = where;
    }

    public QualifiedName getIndexName()
    {
        return indexName;
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public List<Identifier> getColumnAliases()
    {
        return columnAliases;
    }

    public String getIndexType()
    {
        return indexType;
    }

    public boolean isNotExists()
    {
        return notExists;
    }

    public List<Property> getProperties()
    {
        return properties;
    }

    public Optional<Expression> getExpression()
    {
        return where;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateIndex(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .addAll(columnAliases)
                .addAll(properties)
                .build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(indexName, tableName, columnAliases, indexType, notExists, properties, where);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        CreateIndex o = (CreateIndex) obj;
        return Objects.equals(indexName, o.indexName)
                && Objects.equals(tableName, o.tableName)
                && Objects.equals(columnAliases, o.columnAliases)
                && Objects.equals(indexType, o.indexType)
                && Objects.equals(notExists, o.notExists)
                && Objects.equals(properties, o.properties)
                && Objects.equals(where, o.where);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("indexName", indexName)
                .add("tableName", tableName)
                .add("columnAliases", columnAliases)
                .add("indexType", indexType)
                .add("notExists", notExists)
                .add("properties", properties)
                .add("where", where)
                .toString();
    }
}
