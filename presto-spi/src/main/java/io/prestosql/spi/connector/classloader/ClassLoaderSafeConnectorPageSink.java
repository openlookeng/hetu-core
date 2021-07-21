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
package io.prestosql.spi.connector.classloader;

import io.airlift.slice.Slice;
import io.prestosql.spi.Page;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"classLoader"})
public class ClassLoaderSafeConnectorPageSink
        implements ConnectorPageSink
{
    private final ConnectorPageSink delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeConnectorPageSink(ConnectorPageSink delegate, ClassLoader classLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public long getCompletedBytes()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getCompletedBytes();
        }
    }

    @Override
    public long getRowsWritten()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getRowsWritten();
        }
    }

    @Override
    public long getSystemMemoryUsage()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getSystemMemoryUsage();
        }
    }

    @Override
    public long getValidationCpuNanos()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getValidationCpuNanos();
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.appendPage(page);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.finish();
        }
    }

    @Override
    public void abort()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.abort();
        }
    }

    @Override
    public void cancelToResume()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.cancelToResume();
        }
    }

    @Override
    public VacuumResult vacuum(ConnectorPageSourceProvider pageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorTableHandle connectorTableHandle,
            List<ConnectorSplit> splits)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.vacuum(pageSourceProvider, transactionHandle, connectorTableHandle, splits);
        }
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.capture(serdeProvider);
        }
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider, long resumeCount)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.restore(state, serdeProvider, resumeCount);
        }
    }
}
