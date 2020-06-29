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
package io.prestosql.orc.stream;

import io.prestosql.orc.OrcDataSourceId;

public class StreamSourceMeta
{
    private OrcDataSourceId dataSourceId;
    private long stripeOffset = -1;
    private long rowGroupOffset = -1;
    private long rowCount;

    public OrcDataSourceId getDataSourceId()
    {
        return dataSourceId;
    }

    public void setDataSourceId(OrcDataSourceId dataSourceId)
    {
        this.dataSourceId = dataSourceId;
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

    public long getRowCount()
    {
        return rowCount;
    }

    public void setRowCount(long rowCount)
    {
        this.rowCount = rowCount;
    }
}
