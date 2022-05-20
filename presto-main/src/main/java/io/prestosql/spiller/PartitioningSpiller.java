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
package io.prestosql.spiller;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.spi.Page;
import io.prestosql.spi.snapshot.Restorable;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.IntPredicate;

import static java.util.Objects.requireNonNull;

public interface PartitioningSpiller
        extends Closeable, Restorable
{
    /**
     * Partition page and enqueue partitioned pages to spill writers.
     * {@link PartitioningSpillResult#getSpillingFuture} is completed when spilling is finished.
     * <p>
     * This method may not be called if previously initiated spilling is not finished yet.
     */
    PartitioningSpillResult partitionAndSpill(Page page, IntPredicate spillPartitionMask);

    PartitioningSpillResult partitionAndSpill(Page page, IntPredicate spillPartitionMask, BiPredicate<Integer, Long> spillPartitionMatcher);

    /**
     * Returns iterator of previously spilled pages from given partition. Callers are expected to call
     * this method once. Calling multiple times can results in undefined behavior.
     * <p>
     * This method may not be called if previously initiated spilling is not finished yet.
     * <p>
     * This method may perform blocking I/O to flush internal buffers.
     */
    // TODO getSpilledPages should not need flush last buffer to disk
    Iterator<Page> getSpilledPages(int partition);

    Set<Integer> getSpilledPartitions();

    default ListenableFuture<?> flushPartition(int partition)
    {
        return Futures.immediateFuture(null);
    }

    void verifyAllPartitionsRead();

    /**
     * Closes and removes all underlying resources used during spilling.
     */
    @Override
    void close()
            throws IOException;

    default List<Path> getSpilledFilePaths(boolean isStoreSpillFiles)
    {
        return ImmutableList.of();
    }

    default List<Pair<Path, Long>> getSpilledFileInfo()
    {
        return ImmutableList.of();
    }

    class PartitioningSpillResult
    {
        private final ListenableFuture<?> spillingFuture;
        private final Page retained;

        public PartitioningSpillResult(ListenableFuture<?> spillingFuture, Page retained)
        {
            this.spillingFuture = requireNonNull(spillingFuture, "spillingFuture is null");
            this.retained = requireNonNull(retained, "retained is null");
        }

        public ListenableFuture<?> getSpillingFuture()
        {
            return spillingFuture;
        }

        public Page getRetained()
        {
            return retained;
        }
    }

    default void closeSessionSpiller(int partition)
    {
    }
}
