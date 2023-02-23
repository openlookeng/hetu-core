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

import io.prestosql.operator.aggregation.builder.AggregationBuilder;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;

@NotThreadSafe
public interface LookupSource
        extends Closeable
{
    int getChannelCount();

    long getInMemorySizeInBytes();

    long getJoinPositionCount();

    long joinPositionWithinPartition(long joinPosition);

    long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash);

    long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage);

    long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage);

    default long getCountForJoinPosition(long position, int channel)
    {
        throw new UnsupportedOperationException("Only supported for Group Join usage");
    }

    default AggregationBuilder getAggregationBuilder()
    {
        throw new UnsupportedOperationException("Only supported for Group Join usage");
    }

    void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset);

    boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage);

    boolean isEmpty();

    @Override
    void close();

    /**
     * The `rawHashes` and `result` arrays are global to the entire processed page (thus, the same size),
     * while the `positions` array may hold any number of selected positions from this page
     */
    default void getJoinPosition(int[] positions, Page hashChannelsPage, Page allChannelsPage, long[] rawHashes, long[] result)
    {
        for (int i = 0; i < positions.length; i++) {
            result[positions[i]] = getJoinPosition(positions[i], hashChannelsPage, allChannelsPage, rawHashes[positions[i]]);
        }
    }

    /**
     * The `result` array is global to the entire processed page, while the `positions` array may hold
     * any number of selected positions from this page
     */
    default void getJoinPosition(int[] positions, Page hashChannelsPage, Page allChannelsPage, long[] result)
    {
        for (int i = 0; i < positions.length; i++) {
            result[positions[i]] = getJoinPosition(positions[i], hashChannelsPage, allChannelsPage);
        }
    }
}
