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

import java.util.Objects;

public class OrcStripeFooterCacheKey
{
    private OrcDataSourceIdWithTimeStamp orcDataSourceId;
    private long stripeOffset;

    public OrcStripeFooterCacheKey()
    {
        //default constructor
    }

    public OrcStripeFooterCacheKey(OrcDataSourceIdWithTimeStamp orcDataSourceId, long stripeOffset)
    {
        this.orcDataSourceId = orcDataSourceId;
        this.stripeOffset = stripeOffset;
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OrcStripeFooterCacheKey that = (OrcStripeFooterCacheKey) o;
        return stripeOffset == that.stripeOffset &&
                orcDataSourceId.equals(that.orcDataSourceId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(orcDataSourceId, stripeOffset);
    }
}
