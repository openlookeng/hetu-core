/*
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

public class DropCache
        extends Statement
{
    private final QualifiedName tableName;
    private final boolean exists;
    private final Optional<Expression> where;

    public DropCache(QualifiedName tableName, boolean exists)
    {
        this(Optional.empty(), tableName, exists, Optional.empty());
    }

    public DropCache(NodeLocation location, QualifiedName tableName, boolean exists, Optional<Expression> where)
    {
        this(Optional.of(location), tableName, exists, where);
    }

    private DropCache(Optional<NodeLocation> location, QualifiedName tableName, boolean exists, Optional<Expression> where)
    {
        super(location);
        this.tableName = tableName;
        this.exists = exists;
        this.where = where;
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public boolean isExists()
    {
        return exists;
    }

    public Optional<Expression> getWhere()
    {
        return where;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropCache(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, exists);
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
        DropCache o = (DropCache) obj;
        return Objects.equals(tableName, o.tableName)
                && (exists == o.exists)
                && Objects.equals(where, o.where);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableName", tableName)
                .add("exists", exists)
                .add("where", (where.isPresent() ? where.get().toString() : "null"))
                .toString();
    }
}
