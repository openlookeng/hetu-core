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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class VacuumTable
        extends Statement
{
    private final Table table;

    private final boolean isFull;
    private final boolean isUnify;
    private final Optional<String> partition;
    private final boolean isAsync;

    public VacuumTable(Optional<NodeLocation> location, Table table, boolean isFull, boolean isUnify, Optional<String> partition, boolean isAsync)
    {
        super(location);
        this.table = requireNonNull(table, "table is null");
        this.isFull = isFull;
        this.isUnify = isUnify;
        this.partition = requireNonNull(partition, "partition is null");
        this.isAsync = isAsync;
    }

    public Table getTable()
    {
        return table;
    }

    public boolean isFull()
    {
        return isFull;
    }

    public boolean isUnify()
    {
        return isUnify;
    }

    public boolean isAsync()
    {
        return isAsync;
    }

    public Optional<String> getPartition()
    {
        return partition;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return null;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, isFull, partition);
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
        VacuumTable o = (VacuumTable) obj;
        return Objects.equals(table, o.table) &&
                isFull == o.isFull &&
                Objects.equals(partition, o.partition);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("full", isFull)
                .add("partition", partition)
                .toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitVacuumTable(this, context);
    }
}
