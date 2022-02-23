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
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.spi.Page;
import io.prestosql.spi.snapshot.Restorable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

public interface Spiller
        extends Closeable, Restorable
{
    /**
     * Initiate spilling of pages stream. Returns completed future once spilling has finished.
     */
    ListenableFuture<?> spill(Iterator<Page> pageIterator);

    /**
     * Initiate spilling of pages stream. Returns completed future once spilling has finished with commit function.
     */
    default Pair<ListenableFuture<?>, Runnable> spillUnCommit(Iterator<Page> pageIterator)
    {
        return ImmutablePair.of(spill(pageIterator), () -> {});
    }

    /**
     * Returns list of previously spilled Pages streams.
     */
    List<Iterator<Page>> getSpills();

    /**
     * Returns spilled files paths
     */
    default List<Path> getSpilledFilePaths()
    {
        return ImmutableList.of();
    }

    /**
     * Close releases/removes all underlying resources used during spilling
     * like for example all created temporary files.
     */
    @Override
    void close();
}
