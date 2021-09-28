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
package io.prestosql.orc;

import io.prestosql.orc.metadata.OrcColumnId;

import java.util.Objects;

public class OrcRowDataCacheKey
{
    private OrcDataSourceIdWithTimeStamp orcDataSourceId;
    private long stripeOffset;
    private long rowGroupOffset;
    private OrcColumnId columnId;

    public OrcRowDataCacheKey()
    {
        //default constructor
    }

    public OrcDataSourceIdWithTimeStamp getOrcDataSourceId()
    {
        return orcDataSourceId;
    }

    public void setOrcDataSourceId(OrcDataSourceIdWithTimeStamp orcDataSourceId)
    {
        this.orcDataSourceId = orcDataSourceId;
    }

    public long getStripeOffset()
    {
        return stripeOffset;
    }

    public void setStripeOffset(long stripeOffset)
    {
        this.stripeOffset = stripeOffset;
    }

    public long getRowGroupOffset()
    {
        return rowGroupOffset;
    }

    public void setRowGroupOffset(long rowGroupOffset)
    {
        this.rowGroupOffset = rowGroupOffset;
    }

    public OrcColumnId getColumnId()
    {
        return columnId;
    }

    public void setColumnId(OrcColumnId columnId)
    {
        this.columnId = columnId;
    }

    @Override
    public String toString()
    {
        return "OrcRowDataCacheKey{" +
                "orcDataSourceId=" + orcDataSourceId +
                ", stripeOffset=" + stripeOffset +
                ", rowGroupOffset=" + rowGroupOffset +
                ", columnId=" + columnId +
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
        OrcRowDataCacheKey that = (OrcRowDataCacheKey) o;
        return stripeOffset == that.stripeOffset &&
                rowGroupOffset == that.rowGroupOffset &&
                Objects.equals(orcDataSourceId, that.orcDataSourceId) &&
                Objects.equals(columnId, that.columnId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(orcDataSourceId, stripeOffset, rowGroupOffset, columnId);
    }
}
