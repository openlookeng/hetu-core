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

import io.airlift.units.DataSize;
import io.prestosql.operator.aggregation.builder.AggregationBuilder;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.prestosql.operator.SyntheticAddress.decodePosition;
import static io.prestosql.operator.SyntheticAddress.decodeSliceIndex;
import static io.prestosql.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

// This implementation assumes arrays used in the hash are always a power of 2
public final class DefaultPagesHash
        implements IPagesHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DefaultPagesHash.class).instanceSize();
    private static final DataSize CACHE_SIZE = new DataSize(128, KILOBYTE);
    private final LongArrayList addresses;
    private final PagesHashStrategy pagesHashStrategy;

    private final int channelCount;
    private final int mask;
    private final int[] key;
    private final long size;

    // Native array of hashes for faster collisions resolution compared
    // to accessing values in blocks. We use bytes to reduce memory foot print
    // and there is no performance gain from storing full hashes
    private final byte[] positionToHashes;
    private final long hashCollisions;
    private final double expectedHashCollisions;

    public DefaultPagesHash(
            LongArrayList addresses,
            PagesHashStrategy pagesHashStrategy,
            PositionLinks.FactoryBuilder positionLinks)
    {
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.pagesHashStrategy = requireNonNull(pagesHashStrategy, "pagesHashStrategy is null");
        this.channelCount = pagesHashStrategy.getChannelCount();

        // reserve memory for the arrays
        int hashSize = HashCommon.arraySize(addresses.size(), 0.75f);

        mask = hashSize - 1;
        key = new int[hashSize];
        Arrays.fill(key, -1);

        positionToHashes = new byte[addresses.size()];

        // We will process addresses in batches, to save memory on array of hashes.
        int positionsInStep = Math.min(addresses.size() + 1, (int) CACHE_SIZE.toBytes() / Integer.SIZE);
        long[] positionToFullHashes = new long[positionsInStep];
        long hashCollisionsLocal = 0;

        for (int step = 0; step * positionsInStep <= addresses.size(); step++) {
            int stepBeginPosition = step * positionsInStep;
            int stepEndPosition = Math.min((step + 1) * positionsInStep, addresses.size());
            int stepSize = stepEndPosition - stepBeginPosition;

            // First extract all hashes from blocks to native array.
            // Somehow having this as a separate loop is much faster compared
            // to extracting hashes on the fly in the loop below.
            for (int position = 0; position < stepSize; position++) {
                int realPosition = position + stepBeginPosition;
                long hash = readHashPosition(realPosition);
                positionToFullHashes[position] = hash;
                positionToHashes[realPosition] = (byte) hash;
            }

            // index pages
            for (int position = 0; position < stepSize; position++) {
                int realPosition = position + stepBeginPosition;
                if (isPositionNull(realPosition)) {
                    continue;
                }

                long hash = positionToFullHashes[position];
                int pos = getHashPosition(hash, mask);

                // look for an empty slot or a slot containing this key
                while (key[pos] != -1) {
                    int currentKey = key[pos];
                    if (((byte) hash) == positionToHashes[currentKey] && positionEqualsPositionIgnoreNulls(currentKey, realPosition)) {
                        // found a slot for this key
                        // link the new key position to the current key position
                        realPosition = positionLinks.link(realPosition, currentKey);

                        // key[pos] updated outside of this loop
                        break;
                    }
                    // increment position and mask to handler wrap around
                    pos = (pos + 1) & mask;
                    hashCollisionsLocal++;
                }

                key[pos] = realPosition;
            }
        }

        size = sizeOf(addresses.elements()) + pagesHashStrategy.getSizeInBytes() +
                sizeOf(key) + sizeOf(positionToHashes);
        hashCollisions = hashCollisionsLocal;
        expectedHashCollisions = estimateNumberOfHashCollisions(addresses.size(), hashSize);
    }

    @Override
    public int getChannelCount()
    {
        return channelCount;
    }

    public int getPositionCount()
    {
        return addresses.size();
    }

    public long getInMemorySizeInBytes()
    {
        return INSTANCE_SIZE + size;
    }

    public long getHashCollisions()
    {
        return hashCollisions;
    }

    public double getExpectedHashCollisions()
    {
        return expectedHashCollisions;
    }

    public int getAddressIndex(int position, Page hashChannelsPage)
    {
        return getAddressIndex(position, hashChannelsPage, pagesHashStrategy.hashRow(position, hashChannelsPage));
    }

    public int getAddressIndex(int rightPosition, Page hashChannelsPage, long rawHash)
    {
        int pos = getHashPosition(rawHash, mask);

        while (key[pos] != -1) {
            if (positionEqualsCurrentRowIgnoreNulls(key[pos], (byte) rawHash, rightPosition, hashChannelsPage)) {
                return key[pos];
            }
            // increment position and mask to handler wrap around
            pos = (pos + 1) & mask;
        }
        return -1;
    }

    @Override
    public long getCountForJoinPosition(long position, int channel)
    {
        long pageAddress = addresses.getLong(toIntExact(position));
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);
        return pagesHashStrategy.getCountForJoinPosition(blockIndex, blockPosition, channel);
    }

    @Override
    public AggregationBuilder getAggregationBuilder()
    {
        return pagesHashStrategy.getAggregationBuilder();
    }

    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        long pageAddress = addresses.getLong(toIntExact(position));
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        pagesHashStrategy.appendTo(blockIndex, blockPosition, pageBuilder, outputChannelOffset);
    }

    private boolean isPositionNull(int position)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.isPositionNull(blockIndex, blockPosition);
    }

    private long readHashPosition(int position)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.hashPosition(blockIndex, blockPosition);
    }

    private boolean positionEqualsCurrentRowIgnoreNulls(int leftPosition, byte rawHash, int rightPosition, Page rightPage)
    {
        if (positionToHashes[leftPosition] != rawHash) {
            return false;
        }

        long pageAddress = addresses.getLong(leftPosition);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.positionEqualsRowIgnoreNulls(blockIndex, blockPosition, rightPosition, rightPage);
    }

    private boolean positionEqualsPositionIgnoreNulls(int leftPosition, int rightPosition)
    {
        long leftPageAddress = addresses.getLong(leftPosition);
        int leftBlockIndex = decodeSliceIndex(leftPageAddress);
        int leftBlockPosition = decodePosition(leftPageAddress);

        long rightPageAddress = addresses.getLong(rightPosition);
        int rightBlockIndex = decodeSliceIndex(rightPageAddress);
        int rightBlockPosition = decodePosition(rightPageAddress);

        return pagesHashStrategy.positionEqualsPositionIgnoreNulls(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition);
    }

    public int[] getAddressIndex(int[] positions, Page hashChannelsPage)
    {
        long[] hashes = new long[positions[positions.length - 1] + 1];
        for (int i = 0; i < positions.length; i++) {
            hashes[positions[i]] = pagesHashStrategy.hashRow(positions[i], hashChannelsPage);
        }

        return getAddressIndex(positions, hashChannelsPage, hashes);
    }

    public int[] getAddressIndex(int[] positions, Page hashChannelsPage, long[] rawHashes)
    {
        int positionCount = positions.length;
        int[] hashPositions = new int[positionCount];

        for (int i = 0; i < positionCount; i++) {
            hashPositions[i] = getHashPosition(rawHashes[positions[i]], mask);
        }

        int[] found = new int[positionCount];
        int foundCount = 0;
        int[] result = new int[positionCount];
        Arrays.fill(result, -1);
        int[] foundKeys = new int[positionCount];

        // Search for positions in the hash array. This is the most CPU-consuming part as
        // it relies on random memory accesses
        for (int i = 0; i < positionCount; i++) {
            foundKeys[i] = key[hashPositions[i]];
        }
        // Found positions are put into `found` array
        for (int i = 0; i < positionCount; i++) {
            if (foundKeys[i] != -1) {
                found[foundCount++] = i;
            }
        }

        // At this step we determine if the found keys were indeed the proper ones or it is a hash collision.
        // The result array is updated for the found ones, while the collisions land into `remaining` array.
        int[] remaining = found; // Rename for readability
        int remainingCount = 0;
        for (int i = 0; i < foundCount; i++) {
            int index = found[i];
            if (positionEqualsCurrentRowIgnoreNulls(foundKeys[index], (byte) rawHashes[positions[index]], positions[index], hashChannelsPage)) {
                result[index] = foundKeys[index];
            }
            else {
                remaining[remainingCount++] = index;
            }
        }

        // At this point for any reasoable load factor of a hash array (< .75), there is no more than
        // 10 - 15% of positions left. We search for them in a sequential order and update the result array.
        for (int i = 0; i < remainingCount; i++) {
            int index = remaining[i];
            int position = (hashPositions[index] + 1) & mask; // hashPositions[index] position has already been checked

            while (key[position] != -1) {
                if (positionEqualsCurrentRowIgnoreNulls(key[position], (byte) rawHashes[positions[index]], positions[index], hashChannelsPage)) {
                    result[index] = key[position];
                    break;
                }
                // increment position and mask to handler wrap around
                position = (position + 1) & mask;
            }
        }

        return result;
    }
}
