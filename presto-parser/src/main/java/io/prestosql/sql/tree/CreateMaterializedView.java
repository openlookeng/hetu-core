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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateMaterializedView
        extends Statement
{
    private final QualifiedName name;
    private final Query query;
    private final boolean notExists;
    private final List<Property> properties;
    private final Optional<List<Identifier>> columnAliases;
    private final Optional<String> comment;
    public static final String CATALOG_NAME_PROPERTY = "catalog";

    public CreateMaterializedView(QualifiedName name, Query query, boolean notExists, List<Property> properties, Optional<List<Identifier>> columnAliases, Optional<String> comment)
    {
        this(Optional.empty(), name, query, notExists, properties, columnAliases, comment);
    }

    public CreateMaterializedView(NodeLocation location, QualifiedName name, Query query, boolean notExists, List<Property> properties, Optional<List<Identifier>> columnAliases, Optional<String> comment)
    {
        this(Optional.of(location), name, query, notExists, properties, columnAliases, comment);
    }

    private CreateMaterializedView(Optional<NodeLocation> location, QualifiedName name, Query query, boolean notExists, List<Property> properties, Optional<List<Identifier>> columnAliases, Optional<String> comment)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.query = requireNonNull(query, "query is null");
        this.notExists = notExists;
        this.properties = new ArrayList<Property>(requireNonNull(properties, "properties is null"));
        this.columnAliases = columnAliases;
        this.comment = requireNonNull(comment, "comment is null");
    }

    public QualifiedName getName()
    {
        return name;
    }

    public Query getQuery()
    {
        return query;
    }

    public boolean isNotExists()
    {
        return notExists;
    }

    public List<Property> getProperties()
    {
        return properties;
    }

    public Optional<List<Identifier>> getColumnAliases()
    {
        return columnAliases;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateMaterializedView(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .add(query)
                .addAll(properties)
                .build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, query, properties, columnAliases, comment);
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
        io.prestosql.sql.tree.CreateMaterializedView o = (io.prestosql.sql.tree.CreateMaterializedView) obj;
        return Objects.equals(name, o.name)
                && Objects.equals(query, o.query)
                && Objects.equals(notExists, o.notExists)
                && Objects.equals(properties, o.properties)
                && Objects.equals(columnAliases, o.columnAliases)
                && Objects.equals(comment, o.comment);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("query", query)
                .add("notExists", notExists)
                .add("properties", properties)
                .add("columnAliases", columnAliases)
                .add("comment", comment)
                .toString();
    }

    public String getTargetCatalogName(String configuredCatalogName)
    {
        for (Property property : properties) {
            if (property.getName().toString().equals(io.prestosql.sql.tree.CreateMaterializedView.CATALOG_NAME_PROPERTY)) {
                String name = property.getValue().toString().replaceAll("'", "");
                properties.remove(property);
                return name;
            }
        }
        return configuredCatalogName;
    }
}
