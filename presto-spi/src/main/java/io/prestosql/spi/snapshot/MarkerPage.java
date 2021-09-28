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
package io.prestosql.spi.snapshot;

import com.google.common.primitives.Longs;
import io.prestosql.spi.Page;

import java.nio.ByteBuffer;

/**
 * A marker page is a signal for components (e.g. operators) within a task to take a snapshot of its internal state,
 * or to restore to a previous snapshot of its internal state.
 */
public class MarkerPage
        extends Page
{
    private final long snapshotId;
    private final boolean isResuming;

    public static MarkerPage snapshotPage(long snapshotId)
    {
        return new MarkerPage(snapshotId, false);
    }

    public static MarkerPage resumePage(long snapshotId)
    {
        return new MarkerPage(snapshotId, true);
    }

    public MarkerPage(long snapshotId, boolean isResuming)
    {
        // positionCount can't be 0, to maintain assumptions about what a page does; blocks can't be null
        super(1);
        this.snapshotId = snapshotId;
        this.isResuming = isResuming;
    }

    public long getSnapshotId()
    {
        return snapshotId;
    }

    public boolean isResuming()
    {
        return isResuming;
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(snapshotId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if (obj instanceof MarkerPage) {
            MarkerPage other = (MarkerPage) obj;
            // equals() doesn't need to consider "taskCount"
            return other.snapshotId == snapshotId && other.isResuming == isResuming;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return new StringBuilder("MarkerPage{")
                .append("snapshotId=").append(snapshotId)
                .append(",isResuming=").append(isResuming)
                .append("}")
                .toString();
    }

    public byte[] serialize()
    {
        final int size = Longs.BYTES + 1;
        return ByteBuffer.allocate(size)
                .putLong(snapshotId)
                .put((byte) (isResuming ? 1 : 0))
                .array();
    }

    public static MarkerPage deserialize(byte[] bytes)
    {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long snapshotId = buffer.getLong();
        boolean isResuming = buffer.get() != 0;
        return new MarkerPage(snapshotId, isResuming);
    }
}
