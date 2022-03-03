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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.operator.PartitionFunction;
import io.prestosql.operator.SpillContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

@ThreadSafe
@RestorableConfig(uncapturedFields = {"types", "partitionFunction", "closer",
        "spillerFactory", "spillContext", "memoryContext", "pageBuilders", "getRawHash"})
public class GenericPartitioningSpiller
        implements PartitioningSpiller
{
    private final List<Type> types;
    private final PartitionFunction partitionFunction;
    private final Closer closer = Closer.create();
    private final SingleStreamSpillerFactory spillerFactory;
    private final SpillContext spillContext;
    private final AggregatedMemoryContext memoryContext;

    private final List<PageBuilder> pageBuilders;
    private final List<Optional<SingleStreamSpiller>> spillers;
    private final BiFunction<Integer, Page, Long> getRawHash;

    private boolean readingStarted;
    private final Set<Integer> spilledPartitions = new HashSet<>();

    public GenericPartitioningSpiller(
            List<Type> types,
            PartitionFunction partitionFunction,
            SpillContext spillContext,
            AggregatedMemoryContext memoryContext,
            SingleStreamSpillerFactory spillerFactory,
            BiFunction<Integer, Page, Long> getRawHash)
    {
        requireNonNull(spillContext, "spillContext is null");

        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.spillContext = closer.register(requireNonNull(spillContext, "spillContext is null"));

        requireNonNull(memoryContext, "memoryContext is null");
        closer.register(memoryContext::close);
        this.memoryContext = memoryContext;
        int partitionCount = partitionFunction.getPartitionCount();

        ImmutableList.Builder<PageBuilder> tmpPageBuilders = ImmutableList.builder();
        spillers = new ArrayList<>(partitionCount);
        for (int partition = 0; partition < partitionCount; partition++) {
            tmpPageBuilders.add(new PageBuilder(types));
            spillers.add(Optional.empty());
        }
        this.pageBuilders = tmpPageBuilders.build();
        this.getRawHash = getRawHash;
    }

    @Override
    public synchronized Iterator<Page> getSpilledPages(int partition)
    {
        readingStarted = true;
        getFutureValue(flush(partition));
        spilledPartitions.remove(partition);
        return getSpiller(partition).getSpilledPages();
    }

    @Override
    public synchronized void verifyAllPartitionsRead()
    {
        verify(spilledPartitions.isEmpty(), "Some partitions were spilled but not read: %s", spilledPartitions);
    }

    @Override
    public synchronized PartitioningSpillResult partitionAndSpill(Page page, IntPredicate spillPartitionMask)
    {
        return partitionAndSpill(page, spillPartitionMask, (ign1, ign2) -> true);
    }

    @Override
    public PartitioningSpillResult partitionAndSpill(Page page, IntPredicate spillPartitionMask, BiPredicate<Integer, Long> spillPartitionMatcher)
    {
        requireNonNull(page, "page is null");
        requireNonNull(spillPartitionMask, "spillPartitionMask is null");
        checkArgument(page.getChannelCount() == types.size(), "Wrong page channel count, expected %s but got %s", types.size(), page.getChannelCount());

        checkState(!readingStarted, "reading already started");
        IntArrayList unspilledPositions = partitionPage(page, spillPartitionMask, spillPartitionMatcher);
        ListenableFuture<?> future = flushFullBuilders();

        return new PartitioningSpillResult(future, page.getPositions(unspilledPositions.elements(), 0, unspilledPositions.size()));
    }

    private synchronized IntArrayList partitionPage(Page page, IntPredicate spillPartitionMask, BiPredicate<Integer, Long> spillPartitionMatcher)
    {
        IntArrayList unspilledPositions = new IntArrayList();

        for (int position = 0; position < page.getPositionCount(); position++) {
            int partition = partitionFunction.getPartition(page, position);

            if (!spillPartitionMask.test(partition)) {
                unspilledPositions.add(position);
                continue;
            }

            if (getRawHash != null && !spillPartitionMatcher.test(partition, getRawHash.apply(position, page))) {
                continue;
            }

            spilledPartitions.add(partition);
            PageBuilder pageBuilder = pageBuilders.get(partition);
            pageBuilder.declarePosition();
            for (int channel = 0; channel < types.size(); channel++) {
                Type type = types.get(channel);
                type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
            }
        }

        return unspilledPositions;
    }

    private ListenableFuture<?> flushFullBuilders()
    {
        return flush(PageBuilder::isFull);
    }

    @VisibleForTesting
    ListenableFuture<?> flush()
    {
        return flush(pageBuilder -> true);
    }

    private synchronized ListenableFuture<?> flush(Predicate<PageBuilder> flushCondition)
    {
        requireNonNull(flushCondition, "flushCondition is null");
        ImmutableList.Builder<ListenableFuture<?>> futures = ImmutableList.builder();

        for (int partition = 0; partition < spillers.size(); partition++) {
            PageBuilder pageBuilder = pageBuilders.get(partition);
            if (flushCondition.test(pageBuilder)) {
                futures.add(flush(partition));
            }
        }

        return Futures.allAsList(futures.build());
    }

    private synchronized ListenableFuture<?> flush(int partition)
    {
        PageBuilder pageBuilder = pageBuilders.get(partition);
        if (pageBuilder.isEmpty()) {
            return Futures.immediateFuture(null);
        }
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return getSpiller(partition).spill(page);
    }

    private synchronized SingleStreamSpiller getSpiller(int partition)
    {
        Optional<SingleStreamSpiller> spiller = spillers.get(partition);
        if (!spiller.isPresent()) {
            spiller = Optional.of(closer.register(spillerFactory.create(types, spillContext, memoryContext.newLocalMemoryContext(GenericPartitioningSpiller.class.getSimpleName()))));
            spillers.set(partition, spiller);
        }
        return spiller.get();
    }

    @Override
    public synchronized void close()
            throws IOException
    {
        closer.close();
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        GenericPartitioningSpillerState myState = new GenericPartitioningSpillerState();
        myState.spilledPartitions = spilledPartitions;
        myState.readingStarted = readingStarted;
        myState.spillers = new ArrayList<>(Collections.nCopies(spillers.size(), null));
        for (int i = 0; i < spillers.size(); i++) {
            if (spillers.get(i).isPresent()) {
                myState.spillers.set(i, spillers.get(i).get().capture(serdeProvider));
            }
        }
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        GenericPartitioningSpillerState myState = (GenericPartitioningSpillerState) state;
        this.readingStarted = myState.readingStarted;
        for (int partition : myState.spilledPartitions) {
            this.spilledPartitions.add(partition);
        }
        for (int i = 0; i < spillers.size(); i++) {
            if (myState.spillers.get(i) != null) {
                SingleStreamSpiller spiller = spillerFactory.create(types, spillContext, memoryContext.newLocalMemoryContext(GenericPartitioningSpiller.class.getSimpleName()));
                spiller.restore(myState.spillers.get(i), serdeProvider);
                this.spillers.set(i, Optional.of(closer.register(spiller)));
            }
        }
    }

    private static class GenericPartitioningSpillerState
            implements Serializable
    {
        Set<Integer> spilledPartitions;
        boolean readingStarted;
        List<Object> spillers;
    }
}
