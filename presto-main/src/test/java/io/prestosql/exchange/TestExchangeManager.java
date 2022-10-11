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

import io.prestosql.exchange.FileSystemExchangeConfig.DirectSerialisationType;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public abstract class TestExchangeManager
{
    public static ExchangeManager createExchangeManager()
    {
        return new ExchangeManager()
        {
            @Override
            public Exchange createExchange(ExchangeContext context, int outputPartitionCount)
            {
                return new Exchange()
                {
                    @Override
                    public ExchangeSinkHandle addSink(int taskPartitionId)
                    {
                        return null;
                    }

                    @Override
                    public void noMoreSinks()
                    {
                    }

                    @Override
                    public ExchangeSinkInstanceHandle instantiateSink(ExchangeSinkHandle sinkHandle, int taskAttemptId)
                    {
                        return null;
                    }

                    @Override
                    public void sinkFinished(ExchangeSinkInstanceHandle handle)
                    {
                    }

                    @Override
                    public CompletableFuture<List<ExchangeSourceHandle>> getSourceHandles()
                    {
                        return null;
                    }

                    @Override
                    public ExchangeSourceSplitter split(ExchangeSourceHandle handle, long targetSizeInBytes)
                    {
                        return null;
                    }

                    @Override
                    public ExchangeSourceStatistics getExchangeSourceStatistics(ExchangeSourceHandle handle)
                    {
                        return null;
                    }

                    @Override
                    public void close()
                    {
                    }
                };
            }

            @Override
            public ExchangeSink createSink(ExchangeSinkInstanceHandle handle, boolean preserveRecordsOrder)
            {
                return null;
            }

            @Override
            public ExchangeSink createSink(ExchangeSinkInstanceHandle handle, DirectSerialisationType serType, boolean preserveRecordsOrder)
            {
                return null;
            }

            @Override
            public ExchangeSource createSource(List<ExchangeSourceHandle> handles)
            {
                return null;
            }
        };
    }
}
