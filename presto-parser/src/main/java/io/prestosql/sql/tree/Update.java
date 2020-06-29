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
import static java.util.Objects.requireNonNull;

public class Update
        extends Statement
{
    private final Table table;
    private final List<AssignmentItem> assignmentItems;
    private final Optional<Expression> where;

    public Update(Table table, List<AssignmentItem> assignmentItems, Optional<Expression> where)
    {
        this(Optional.empty(), table, assignmentItems, where);
    }

    public Update(NodeLocation location, Table table, List<AssignmentItem> assignmentItems, Optional<Expression> where)
    {
        this(Optional.of(location), table, assignmentItems, where);
    }

    private Update(Optional<NodeLocation> location, Table table, List<AssignmentItem> assignmentItems, Optional<Expression> where)
    {
        super(location);
        this.table = requireNonNull(table, "table is null");
        this.assignmentItems = requireNonNull(assignmentItems, "assignmentItems is null");
        this.where = requireNonNull(where, "where is null");
    }

    public Table getTable()
    {
        return table;
    }

    public List<AssignmentItem> getAssignmentItems()
    {
        return assignmentItems;
    }

    public Optional<Expression> getWhere()
    {
        return where;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitUpdate(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(table);
        nodes.add((Node) assignmentItems);
        where.ifPresent(nodes::add);
        return nodes.build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, assignmentItems, where);
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
        Update o = (Update) obj;
        return Objects.equals(table, o.table) &&
                Objects.equals(assignmentItems, o.assignmentItems) &&
                Objects.equals(where, o.where);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("assignmentItems", assignmentItems)
                .add("where", where)
                .toString();
    }
}
