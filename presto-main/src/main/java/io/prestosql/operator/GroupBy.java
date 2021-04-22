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
package io.prestosql.operator;

import com.google.common.annotations.VisibleForTesting;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.snapshot.Restorable;
import io.prestosql.spi.type.Type;

import java.util.List;

public interface GroupBy
        extends Restorable
{
    List<Type> getTypes();

    long getEstimatedSize();

    default long getHashCollisions()
    {
        return 0;
    }

    default double getExpectedHashCollisions()
    {
        return 0;
    }

    int getGroupCount();

    void appendValuesTo(int groupId, PageBuilder pageBuilder, int outputChannelOffset);

    Work<?> addPage(Page page);

    Work<GroupByIdBlock> getGroupIds(Page page);

    boolean contains(int position, Page page, int[] hashChannels);

    default boolean contains(int position, Page page, int[] hashChannels, long rawHash)
    {
        return contains(position, page, hashChannels);
    }

    default long getRawHash(int groupyId)
    {
        throw new UnsupportedOperationException();
    }

    @VisibleForTesting
    default int getCapacity()
    {
        throw new UnsupportedOperationException();
    }

    default boolean needMoreCapacity()
    {
        throw new UnsupportedOperationException();
    }

    default boolean tryToIncreaseCapacity()
    {
        throw new UnsupportedOperationException();
    }

    default int putIfAbsent(int position, Block block)
    {
        throw new UnsupportedOperationException("does not support putIfAbsent");
    }

    default int putIfAbsent(int position, Page page)
    {
        throw new UnsupportedOperationException("does not support putIfAbsent");
    }

    default int putIfAbsent(int position, Page page, long rawHash)
    {
        throw new UnsupportedOperationException("does not support putIfAbsent");
    }
}
