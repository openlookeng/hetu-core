/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.plugin.hive;

import io.prestosql.spi.connector.ConnectorTableHandle;

import java.util.Objects;

public class HiveTableState
        extends HiveTableHandle
{
    private final long referenceCount; // refcount or writeId

    public HiveTableState(HiveTableHandle th, long referenceCount)
    {
        super(th.getSchemaName(), th.getTableName(), th.getTableParameters(),
                th.getPartitionColumns(), th.getPartitions(), th.getCompactEffectivePredicate(),
                th.getEnforcedConstraint(), th.getBucketHandle(), th.getBucketFilter(),
                th.getAnalyzePartitionValues(), th.getPredicateColumns(),
                th.getDisjunctCompactEffectivePredicate(), th.isSuitableToPush());
        this.referenceCount = referenceCount;
    }

    public long getReferenceCount()
    {
        return referenceCount;
    }

    @Override
    public boolean basicEquals(ConnectorTableHandle o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HiveTableState)) {
            return false;
        }
        HiveTableState that = (HiveTableState) o;
        return referenceCount == that.referenceCount &&
                super.basicEquals(that);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HiveTableState)) {
            return false;
        }
        HiveTableState that = (HiveTableState) o;
        return referenceCount == that.referenceCount &&
                super.equals(that);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(referenceCount, getSchemaName(), getTableName());
    }
}
