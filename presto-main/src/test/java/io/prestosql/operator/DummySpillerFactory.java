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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.Spiller;
import io.prestosql.spiller.SpillerFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class DummySpillerFactory
        implements SpillerFactory
{
    private long spillsCount;

    @Override
    public Spiller create(List<Type> types, SpillContext spillContext, AggregatedMemoryContext memoryContext)
    {
        return new Spiller()
        {
            @RestorableConfig(unsupported = true)
            private final RestorableConfig restorableConfig = null;

            private final List<Iterable<Page>> spills = new ArrayList<>();
            private final List<AtomicBoolean> spillCommitted = new ArrayList<>();

            @Override
            public ListenableFuture<?> spill(Iterator<Page> pageIterator)
            {
                spillsCount++;
                spills.add(ImmutableList.copyOf(pageIterator));
                spillCommitted.add(new AtomicBoolean(true));
                return immediateFuture(null);
            }

            @Override
            public Pair<ListenableFuture<?>, Runnable> spillUnCommit(Iterator<Page> pageIterator)
            {
                spillsCount++;
                spills.add(ImmutableList.copyOf(pageIterator));
                AtomicBoolean isCommitted = new AtomicBoolean(false);
                spillCommitted.add(isCommitted);
                return ImmutablePair.of(immediateFuture(null), () -> isCommitted.set(true));
            }

            @Override
            public List<Iterator<Page>> getSpills()
            {
                return spills.stream()
                        .map(Iterable::iterator)
                        .collect(toImmutableList());
            }

            @Override
            public void close()
            {
                spills.clear();
            }

            /**
             * Capture this object's internal state, so it can be used later to restore to the same state.
             *
             * @param serdeProvider
             * @return An object representing internal state of the current object
             */
            @Override
            public Object capture(BlockEncodingSerdeProvider serdeProvider)
            {
                DummySpillerState myState = new DummySpillerState();
                for (int i = 0; i < spills.size(); i++) {
                    if (spillCommitted.get(i).get()) {
                        List<Page> pages = new ArrayList<>();
                        spills.get(i).forEach(pg -> pages.add(pg));
                        myState.spills.add(pages);
                    }
                }
                return myState;
            }

            /**
             * Restore this object's internal state according to the snapshot
             *
             * @param state         an object that represents this object's snapshot state
             * @param serdeProvider
             */
            @Override
            public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
            {
                DummySpillerState myState = (DummySpillerState) state;
                this.spills.clear();
                for (List<Page> s : myState.spills) {
                    this.spills.add(s);
                    this.spillCommitted.add(new AtomicBoolean(true));
                }
            }

            class DummySpillerState
                    implements Serializable
            {
                List<List<Page>> spills = new ArrayList<>();
            }
        };
    }

    public long getSpillsCount()
    {
        return spillsCount;
    }
}
