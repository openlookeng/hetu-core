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

public class DropCube
        extends Statement
{
    private final QualifiedName cubeName;
    private final boolean exists;

    public DropCube(QualifiedName cubeName, boolean exists)
    {
        this(Optional.empty(), cubeName, exists);
    }

    public DropCube(NodeLocation location, QualifiedName cubeName, boolean exists)
    {
        this(Optional.of(location), cubeName, exists);
    }

    private DropCube(Optional<NodeLocation> location, QualifiedName cubeName, boolean exists)
    {
        super(location);
        this.cubeName = cubeName;
        this.exists = exists;
    }

    public QualifiedName getCubeName()
    {
        return cubeName;
    }

    public boolean isExists()
    {
        return exists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropCube(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cubeName, exists);
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
        DropCube o = (DropCube) obj;
        return Objects.equals(cubeName, o.cubeName)
                && (exists == o.exists);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("cubeName", cubeName)
                .add("exists", exists)
                .toString();
    }
}
