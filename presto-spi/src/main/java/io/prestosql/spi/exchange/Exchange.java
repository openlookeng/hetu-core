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
package io.prestosql.spi.exchange;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface Exchange
        extends Closeable
{
    /**
     * Add a new sink
     *
     * @param taskPartitionId unique partition written to a sink
     * @return {@link ExchangeSinkHandle} associated with the <code>taskPartitionId</code>
     */
    ExchangeSinkHandle addSink(int taskPartitionId);

    /**
     * Called when no more sinks will be added with {@link #addSink(int)}
     */
    void noMoreSinks();

    /**
     * Registers a sink instance for a task attempt.
     *
     * @param sinkHandle - handle returned by {@link #addSink(int)}
     * @param taskAttemptId - attempt id (how many times attempted)
     * @return ExchangeSinkInstanceHandle to be sent to a worker that is needed to create an {@link ExchangeSink} instance
     * with {@link ExchangeManager#createSink(ExchangeSinkInstanceHandle, boolean)}
     */
    ExchangeSinkInstanceHandle instantiateSink(ExchangeSinkHandle sinkHandle, int taskAttemptId);

    void sinkFinished(ExchangeSinkInstanceHandle handle);

    CompletableFuture<List<ExchangeSourceHandle>> getSourceHandles();

    ExchangeSourceSplitter split(ExchangeSourceHandle handle, long targetSizeInBytes);

    ExchangeSourceStatistics getExchangeSourceStatistics(ExchangeSourceHandle handle);

    @Override
    void close();
}
