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
package io.prestosql.operator.exchange;

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.spi.Page;

import java.util.function.LongConsumer;

import static java.util.Objects.requireNonNull;

public class PassthroughExchanger
        implements LocalExchanger
{
    private final LocalExchangeSource localExchangeSource;
    private final LocalExchangeMemoryManager bufferMemoryManager;
    private final LongConsumer memoryTracker;
    private final boolean isForMerge;

    public PassthroughExchanger(LocalExchangeSource localExchangeSource, long bufferMaxMemory, LongConsumer memoryTracker, boolean isForMerge)
    {
        this.localExchangeSource = requireNonNull(localExchangeSource, "localExchangeSource is null");
        this.bufferMemoryManager = new LocalExchangeMemoryManager(bufferMaxMemory);
        this.memoryTracker = requireNonNull(memoryTracker, "memoryTracker is null");
        this.isForMerge = isForMerge;
    }

    @Override
    public void accept(Page page, String origin)
    {
        long retainedSizeInBytes = page.getRetainedSizeInBytes();
        bufferMemoryManager.updateMemoryUsage(retainedSizeInBytes);
        memoryTracker.accept(retainedSizeInBytes);

        PageReference pageReference = new PageReference(page, 1, () -> {
            bufferMemoryManager.updateMemoryUsage(-retainedSizeInBytes);
            memoryTracker.accept(-retainedSizeInBytes);
        });

        localExchangeSource.addPage(pageReference, origin);
    }

    @Override
    public ListenableFuture<?> waitForWriting()
    {
        return bufferMemoryManager.getNotFullFuture();
    }

    @Override
    public void finish()
    {
        // If PassthroughExchanger serves LocalMergeSourceOperator, it needs to call finish on its localExchangeSource
        // otherwise the entire pipeline will be blocked, but if it's used for LocalExchangeSourceOperator,
        // DO NOT call finish() on the localExchangeSource in this case, because other sinks may still need to send markers to it
        if (isForMerge) {
            localExchangeSource.finish();
        }
    }

    public LocalExchangeSource getLocalExchangeSource()
    {
        return localExchangeSource;
    }
}
