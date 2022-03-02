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

import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.operator.SpillContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@NotThreadSafe
@RestorableConfig(uncapturedFields = {"types", "spillContext", "aggregatedMemoryContext", "singleStreamSpillerFactory", "closer", "previousSpill", "spillCommitted"})
public class GenericSpiller
        implements Spiller
{
    private final List<Type> types;
    private final SpillContext spillContext;
    private final AggregatedMemoryContext aggregatedMemoryContext;
    private final SingleStreamSpillerFactory singleStreamSpillerFactory;
    private final Closer closer = Closer.create();
    private ListenableFuture<?> previousSpill = Futures.immediateFuture(null);
    private final List<SingleStreamSpiller> singleStreamSpillers = new ArrayList<>();
    private final List<AtomicBoolean> spillCommitted = new ArrayList<>();

    public GenericSpiller(
            List<Type> types,
            SpillContext spillContext,
            AggregatedMemoryContext aggregatedMemoryContext,
            SingleStreamSpillerFactory singleStreamSpillerFactory)
    {
        this.types = requireNonNull(types, "types can not be null");
        this.spillContext = requireNonNull(spillContext, "spillContext can not be null");
        this.aggregatedMemoryContext = requireNonNull(aggregatedMemoryContext, "aggregatedMemoryContext can not be null");
        this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory can not be null");
    }

    @Override
    public ListenableFuture<?> spill(Iterator<Page> pageIterator)
    {
        SingleStreamSpiller singleStreamSpiller = singleStreamSpillerFactory.create(types, spillContext, aggregatedMemoryContext.newLocalMemoryContext(GenericSpiller.class.getSimpleName()));
        closer.register(singleStreamSpiller);
        singleStreamSpillers.add(singleStreamSpiller);
        spillCommitted.add(new AtomicBoolean(true));
        previousSpill = singleStreamSpiller.spill(pageIterator);
        return previousSpill;
    }

    /**
     * Initiate spilling of pages stream. Returns completed future once spilling has finished with commit function.
     *
     * @param pageIterator
     */
    @Override
    public Pair<ListenableFuture<?>, Runnable> spillUnCommit(Iterator<Page> pageIterator)
    {
        SingleStreamSpiller singleStreamSpiller = singleStreamSpillerFactory.create(types, spillContext, aggregatedMemoryContext.newLocalMemoryContext(GenericSpiller.class.getSimpleName()));
        closer.register(singleStreamSpiller);
        singleStreamSpillers.add(singleStreamSpiller);
        AtomicBoolean isCommitted = new AtomicBoolean(false);
        spillCommitted.add(isCommitted);
        previousSpill = singleStreamSpiller.spill(pageIterator);
        return ImmutablePair.of(previousSpill, () -> isCommitted.set(true));
    }

    @Override
    public List<Iterator<Page>> getSpills()
    {
        checkNoSpillInProgress();
        return singleStreamSpillers.stream()
                .map(SingleStreamSpiller::getSpilledPages)
                .collect(toList());
    }

    public SingleStreamSpillerFactory getSingleStreamSpillerFactory()
    {
        return singleStreamSpillerFactory;
    }

    @Override
    public void close()
    {
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new RuntimeException("could not close some single stream spillers", e);
        }
    }

    private void checkNoSpillInProgress()
    {
        checkState(previousSpill.isDone(), "previous spill still in progress");
    }

    public void deleteAllStreams()
    {
        singleStreamSpillers.stream().forEach(SingleStreamSpiller::deleteFile);
    }

    @Override
    public List<Path> getSpilledFilePaths()
    {
        return IntStream.range(0, spillCommitted.size()).filter(i -> spillCommitted.get(i).get()).mapToObj(o -> singleStreamSpillers.get(o).getFile()).collect(toList());
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        GenericSpillerState myState = new GenericSpillerState();
        for (int i = 0; i < singleStreamSpillers.size(); i++) {
            if (spillCommitted.get(i).get()) {
                SingleStreamSpiller s = singleStreamSpillers.get(i);
                myState.singleStreamSpillers.add(s.capture(serdeProvider));
            }
        }
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        GenericSpillerState myState = (GenericSpillerState) state;
        for (Object s : myState.singleStreamSpillers) {
            SingleStreamSpiller singleStreamSpiller = singleStreamSpillerFactory.create(types, spillContext, aggregatedMemoryContext.newLocalMemoryContext(GenericSpiller.class.getSimpleName()));
            singleStreamSpiller.restore(s, serdeProvider);
            this.singleStreamSpillers.add(singleStreamSpiller);
            this.spillCommitted.add(new AtomicBoolean(true));
            this.closer.register(singleStreamSpiller);
        }
    }

    private static class GenericSpillerState
            implements Serializable
    {
        List<Object> singleStreamSpillers = new ArrayList<>();
    }
}
