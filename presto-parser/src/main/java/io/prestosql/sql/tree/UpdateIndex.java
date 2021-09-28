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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class UpdateIndex
        extends Statement
{
    private final QualifiedName indexName;
    private final boolean exists;
    // Properties are removed from grammar but passed as empty for potential future usage
    private final List<Property> properties;

    public UpdateIndex(QualifiedName indexName, boolean exists, List<Property> properties)
    {
        this(Optional.empty(), indexName, exists, properties);
    }

    public UpdateIndex(NodeLocation location, QualifiedName indexName, boolean exists, List<Property> properties)
    {
        this(Optional.of(location), indexName, exists, properties);
    }

    private UpdateIndex(Optional<NodeLocation> location, QualifiedName indexName, boolean exists, List<Property> properties)
    {
        super(location);
        this.indexName = requireNonNull(indexName, "indexName is null");
        this.exists = exists;
        this.properties = properties;
    }

    public QualifiedName getIndexName()
    {
        return indexName;
    }

    public List<Property> getProperties()
    {
        return properties;
    }

    public boolean isExists()
    {
        return exists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitUpdateIndex(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add((Node) properties);
        return nodes.build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(indexName, exists, properties);
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
        UpdateIndex o = (UpdateIndex) obj;
        return Objects.equals(indexName, o.indexName) &&
                Objects.equals(exists, o.exists) &&
                Objects.equals(properties, o.properties);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("indexName", indexName)
                .add("notExists", exists)
                .add("properties", properties)
                .toString();
    }
}
