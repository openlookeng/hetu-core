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
package io.prestosql.orc;

import java.util.Objects;

public class OrcRowIndexCacheKey
{
    private OrcDataSourceId orcDataSourceId;
    private long stripeOffset;
    private StreamId streamId;

    public OrcDataSourceId getOrcDataSourceId()
    {
        return orcDataSourceId;
    }

    public void setOrcDataSourceId(OrcDataSourceId orcDataSourceId)
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

    public StreamId getStreamId()
    {
        return streamId;
    }

    public void setStreamId(StreamId streamId)
    {
        this.streamId = streamId;
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
        OrcRowIndexCacheKey that = (OrcRowIndexCacheKey) o;
        return stripeOffset == that.stripeOffset &&
                orcDataSourceId.equals(that.orcDataSourceId) &&
                streamId.equals(that.streamId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(orcDataSourceId, stripeOffset, streamId);
    }
}
