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
package io.prestosql.plugin.hive.orc;

import io.prestosql.spi.Page;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;

public class OrcAcidRowId
        implements Comparable
{
    private static final int ORIGINAL_TRANSACTION_INDEX = 0;
    private static final int BUCKET_ID_INDEX = 1;
    private static final int ROW_ID_INDEX = 2;

    private long originalTransaction;
    private int bucket;
    private long rowId;

    public OrcAcidRowId(long originalTransaction, int bucket, long rowId)
    {
        set(originalTransaction, bucket, rowId);
    }

    void set(Page page, int position)
    {
        set(BIGINT.getLong(page.getBlock(ORIGINAL_TRANSACTION_INDEX), position),
                (int) INTEGER.getLong(page.getBlock(BUCKET_ID_INDEX), position),
                BIGINT.getLong(page.getBlock(ROW_ID_INDEX), position));
    }

    void set(long originalTransaction, int bucket, long rowId)
    {
        this.originalTransaction = originalTransaction;
        this.bucket = bucket;
        this.rowId = rowId;
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

        OrcAcidRowId other = (OrcAcidRowId) o;
        return originalTransaction == other.originalTransaction &&
                bucket == other.bucket &&
                rowId == other.rowId;
    }

    public long getOriginalTransaction()
    {
        return originalTransaction;
    }

    public int getBucket()
    {
        return bucket;
    }

    public long getRowId()
    {
        return rowId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(originalTransaction, bucket, rowId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("originalTransaction", originalTransaction)
                .add("bucket", bucket)
                .add("rowId", rowId)
                .toString();
    }

    @Override
    public int compareTo(Object o)
    {
        OrcAcidRowId other = (OrcAcidRowId) o;
        if (equals(other)) {
            return 0;
        }
        //For transactions deleted from original files, ignore bucket field during comparison
        if (originalTransaction == other.originalTransaction && originalTransaction == 0) {
            return rowId < other.rowId ? -1 : rowId > other.rowId ? 1 : 0;
        }
        if (originalTransaction != other.originalTransaction) {
            return Long.compare(originalTransaction, other.originalTransaction);
        }
        else if (bucket != other.bucket) {
            return Integer.compare(bucket, other.bucket);
        }
        else {
            return Long.compare(rowId, other.rowId);
        }
    }
}
