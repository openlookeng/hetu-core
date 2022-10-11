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
package io.prestosql.exchange;

import io.airlift.slice.Slice;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.prestosql.exchange.FileSystemExchangeConfig.DirectSerialisationType;
import io.prestosql.spi.Page;

import java.util.concurrent.CompletableFuture;

public interface ExchangeSink
{
    CompletableFuture<Void> NOT_BLOCKED = CompletableFuture.completedFuture(null);

    CompletableFuture<Void> isBlocked();

    void add(int partitionId, Slice page);

    void add(int partitionId, Page page, PagesSerde directSerde);

    long getMemoryUsage();

    CompletableFuture<Void> finish();

    CompletableFuture<Void> abort();

    default DirectSerialisationType getDirectSerialisationType()
    {
        return DirectSerialisationType.OFF;
    }
}
