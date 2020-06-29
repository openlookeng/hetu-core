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

import static java.util.Objects.requireNonNull;

public class Cache
        extends Statement
{
    private final QualifiedName tableName;
    private final Optional<Expression> where;

    public Cache(QualifiedName tableName)
    {
        this(Optional.empty(), tableName, Optional.empty());
    }

    public Cache(QualifiedName tableName, Optional<Expression> where)
    {
        this(Optional.empty(), tableName, where);
    }

    public Cache(NodeLocation location, QualifiedName tableName, Optional<Expression> where)
    {
        this(Optional.of(location), tableName, where);
    }

    private Cache(Optional<NodeLocation> location, QualifiedName tableName, Optional<Expression> where)
    {
        super(location);
        this.tableName = requireNonNull(tableName, "table is null");
        this.where = requireNonNull(where, "where is null");
    }

    public Identifier getName()
    {
        return new Identifier(tableName.toString() + ".cache");
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public Optional<Expression> getWhere()
    {
        return where;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCache(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .add(getName())
                .build();
    }

    @Override
    public String toString()
    {
        return "Cache{" +
                "tableName=" + tableName +
                ", where=" + (where.isPresent() ? where.get().toString() : "null") +
                '}';
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
        Cache cache = (Cache) o;
        return Objects.equals(tableName, cache.tableName) &&
                Objects.equals(where, cache.where);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, where);
    }
}
