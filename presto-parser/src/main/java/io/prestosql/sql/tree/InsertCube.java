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

public class InsertCube
        extends Statement
{
    private final QualifiedName cubeName;
    private final Optional<Expression> where;
    private final List<Identifier> columns;
    private final Query query;
    private final boolean overwrite;

    public InsertCube(QualifiedName cubeName, Optional<Expression> where, boolean overwrite)
    {
        this(cubeName, where, null, overwrite);
    }

    public InsertCube(NodeLocation location, QualifiedName cubeName, Optional<Expression> where, boolean overwrite)
    {
        this(location, cubeName, where, null, overwrite);
    }

    public InsertCube(QualifiedName cubeName, Optional<Expression> where, List<Identifier> columns, boolean overwrite)
    {
        this(cubeName, where, columns, overwrite, null);
    }

    public InsertCube(NodeLocation location, QualifiedName cubeName, Optional<Expression> where, List<Identifier> columns, boolean overwrite)
    {
        this(location, cubeName, where, columns, overwrite, null);
    }

    public InsertCube(QualifiedName cubeName, Optional<Expression> where, List<Identifier> columns, boolean overwrite, Query query)
    {
        super(Optional.empty());
        this.cubeName = cubeName;
        this.where = where;
        this.columns = columns;
        this.overwrite = overwrite;
        this.query = query;
    }

    public InsertCube(NodeLocation location, QualifiedName cubeName, Optional<Expression> where, List<Identifier> columns, boolean overwrite, Query query)
    {
        super(Optional.of(location));
        this.cubeName = cubeName;
        this.where = where;
        this.columns = columns;
        this.overwrite = overwrite;
        this.query = query;
    }

    public QualifiedName getCubeName()
    {
        return cubeName;
    }

    public Optional<Expression> getWhere()
    {
        return where;
    }

    public List<Identifier> getColumns()
    {
        return this.columns;
    }

    public Query getQuery()
    {
        return query;
    }

    public boolean isOverwrite()
    {
        return overwrite;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitInsertCube(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cubeName, where);
    }

    @Override
    public String toString()
    {
        return "InsertCube{" +
                "cubeName=" + cubeName +
                ", where=" + where +
                ", columns=" + columns +
                ", query=" + query +
                ", overwrite=" + overwrite +
                '}';
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
        InsertCube o = (InsertCube) obj;
        return Objects.equals(cubeName, o.cubeName) &&
                Objects.equals(where, o.where) &&
                Objects.equals(overwrite, o.overwrite);
    }
}
