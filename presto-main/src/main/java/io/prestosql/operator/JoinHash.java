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
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

// This implementation assumes arrays used in the hash are always a power of 2
public final class JoinHash
        implements LookupSource
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(JoinHash.class).instanceSize();
    private final IPagesHash pagesHash;

    // we unwrap Optional<JoinFilterFunction> to actual verifier or null in constructor for performance reasons
    // we do quick check for `filterFunction == null` in `isJoinPositionEligible` to avoid calls to applyFilterFunction
    @Nullable
    private final JoinFilterFunction filterFunction;

    // we unwrap Optional<PositionLinks> to actual position links or null in constructor for performance reasons
    // we do quick check for `positionLinks == null` to avoid calls to positionLinks
    @Nullable
    private final PositionLinks positionLinks;

    public JoinHash(IPagesHash pagesHash, Optional<JoinFilterFunction> filterFunction, Optional<PositionLinks> positionLinks)
    {
        this.pagesHash = requireNonNull(pagesHash, "pagesHash is null");
        this.filterFunction = requireNonNull(filterFunction, "filterFunction can not be null").orElse(null);
        this.positionLinks = requireNonNull(positionLinks, "positionLinks is null").orElse(null);
    }

    @Override
    public boolean isEmpty()
    {
        return getJoinPositionCount() == 0;
    }

    @Override
    public final int getChannelCount()
    {
        return pagesHash.getChannelCount();
    }

    @Override
    public long getJoinPositionCount()
    {
        return pagesHash.getPositionCount();
    }

    @Override
    public long getInMemorySizeInBytes()
    {
        return INSTANCE_SIZE + pagesHash.getInMemorySizeInBytes() + (positionLinks == null ? 0 : positionLinks.getSizeInBytes());
    }

    @Override
    public long joinPositionWithinPartition(long joinPosition)
    {
        return joinPosition;
    }

    @Override
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage)
    {
        int addressIndex = pagesHash.getAddressIndex(position, hashChannelsPage);
        return startJoinPosition(addressIndex, position, allChannelsPage);
    }

    @Override
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash)
    {
        int addressIndex = pagesHash.getAddressIndex(position, hashChannelsPage, rawHash);
        return startJoinPosition(addressIndex, position, allChannelsPage);
    }

    private long startJoinPosition(int currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        if (currentJoinPosition == -1) {
            return -1;
        }
        if (positionLinks == null) {
            return currentJoinPosition;
        }
        return positionLinks.start(currentJoinPosition, probePosition, allProbeChannelsPage);
    }

    @Override
    public final long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        if (positionLinks == null) {
            return -1;
        }
        return positionLinks.next(toIntExact(currentJoinPosition), probePosition, allProbeChannelsPage);
    }

    @Override
    public boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        return filterFunction == null || filterFunction.filter(toIntExact(currentJoinPosition), probePosition, allProbeChannelsPage);
    }

    @Override
    public long getCountForJoinPosition(long position, int channel)
    {
        return pagesHash.getCountForJoinPosition(position, channel);
    }

    @Override
    public AggregationBuilder getAggregationBuilder()
    {
        return pagesHash.getAggregationBuilder();
    }

    @Override
    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        pagesHash.appendTo(toIntExact(position), pageBuilder, outputChannelOffset);
    }

    @Override
    public void close()
    {
    }

    @Override
    public void getJoinPosition(int[] positions, Page hashChannelsPage, Page allChannelsPage, long[] rawHashes, long[] result)
    {
        int[] addressIndexex = pagesHash.getAddressIndex(positions, hashChannelsPage, rawHashes);
        startJoinPosition(addressIndexex, positions, allChannelsPage, result);
    }

    @Override
    public void getJoinPosition(int[] positions, Page hashChannelsPage, Page allChannelsPage, long[] result)
    {
        int[] addressIndexex = pagesHash.getAddressIndex(positions, hashChannelsPage);
        startJoinPosition(addressIndexex, positions, allChannelsPage, result);
    }

    private long[] startJoinPosition(int[] currentJoinPositions, int[] probePositions, Page allProbeChannelsPage, long[] result)
    {
        checkArgument(currentJoinPositions.length == probePositions.length,
                "currentJoinPositions and probePositions arrays must have the same size, %s != %s",
                currentJoinPositions.length,
                probePositions.length);
        int positionCount = currentJoinPositions.length;

        if (positionLinks == null) {
            for (int i = 0; i < positionCount; i++) {
                result[probePositions[i]] = currentJoinPositions[i];
            }
            return result;
        }

        for (int i = 0; i < positionCount; i++) {
            if (currentJoinPositions[i] == -1) {
                result[probePositions[i]] = -1;
            }
            else {
                result[probePositions[i]] = positionLinks.start(currentJoinPositions[i], probePositions[i], allProbeChannelsPage);
            }
        }

        return result;
    }
}
