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

public class DropPartition
        extends Statement
{
    private final QualifiedName tableName;
    private final boolean ifExists;
    private final List<List<ComparisonExpression>> partitionSpec;

    public DropPartition(QualifiedName tableName, boolean ifExists, List<List<ComparisonExpression>> partitionSpec)
    {
        this (Optional.empty(), tableName, ifExists, partitionSpec);
    }

    public DropPartition(NodeLocation nodeLocation, QualifiedName tableName, boolean ifExists, List<List<ComparisonExpression>> partitionSpec)
    {
        this(Optional.of(nodeLocation), tableName, ifExists, partitionSpec);
    }

    private DropPartition(Optional<NodeLocation> nodeLocation, QualifiedName tableName, boolean ifExists, List<List<ComparisonExpression>> partitionSpec)
    {
        super(nodeLocation);
        this.tableName = requireNonNull(tableName);
        this.ifExists = ifExists;
        this.partitionSpec = partitionSpec;
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public List<List<ComparisonExpression>> getPartitionSpec()
    {
        return partitionSpec;
    }

    public boolean isIfExists()
    {
        return ifExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropPartition(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, ifExists, partitionSpec);
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
        DropPartition o = (DropPartition) obj;
        return Objects.equals(tableName, o.tableName)
                && Objects.equals(ifExists, o.ifExists)
                && Objects.equals(partitionSpec, o.partitionSpec);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", tableName)
                .add("ifExists", ifExists)
                .add("partitionSpec", partitionSpec)
                .toString();
    }
}
