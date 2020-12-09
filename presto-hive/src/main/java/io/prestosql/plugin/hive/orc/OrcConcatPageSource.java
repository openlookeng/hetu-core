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
package io.prestosql.plugin.hive.orc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import io.airlift.log.Logger;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Iterators.concat;
import static java.util.stream.Collectors.toList;

public class OrcConcatPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(OrcConcatPageSource.class);

    private final List<ConnectorPageSource> pageSources;
    private final Iterator<Page> concatPageIterator;

    private boolean closed;

    public OrcConcatPageSource(List<ConnectorPageSource> pargeSources)
    {
        this.pageSources = pargeSources;
        List<Iterator<Page>> sourceIterators = pageSources.stream().map(source -> new AbstractIterator<Page>()
        {
            @Override
            protected Page computeNext()
            {
                Page nextPage;
                do {
                    nextPage = source.getNextPage();
                    if (nextPage == null) {
                        if (source.isFinished()) {
                            return endOfData();
                        }
                        return null;
                    }
                }
                while (nextPage.getPositionCount() == 0);

                /* Todo(Nitin) Check if loaded block needed here! */
                return nextPage;
            }
        }).collect(toList());

        concatPageIterator = concat(sourceIterators.iterator());
        closed = false;
    }

    @Override
    public long getCompletedBytes()
    {
        return pageSources.stream().mapToLong(ConnectorPageSource::getCompletedBytes).sum();
    }

    @Override
    public long getReadTimeNanos()
    {
        return pageSources.get(0).getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        if (!concatPageIterator.hasNext()) {
            close();
            return null;
        }

        return concatPageIterator.next();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return pageSources.get(0).getSystemMemoryUsage();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }

        pageSources.stream().forEach(ps -> {
            try {
                ps.close();
            }
            catch (IOException | RuntimeException e) {
                log.warn("failed to close page source");
            }
        });
        closed = true;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).toString();
    }

    @VisibleForTesting
    public ConnectorPageSource getConnectorPageSource()
    {
        return pageSources.get(0);
    }
}
