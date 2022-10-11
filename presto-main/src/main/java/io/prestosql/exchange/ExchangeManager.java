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

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

/**
 * Service provider interface for an external exchange
 * It's used to exchange data between stages
 */
@ThreadSafe
public interface ExchangeManager
{
    /**
     * create an external exchange between a pair of stages
     *
     * @param context              information about the query and stage being executed
     * @param outputPartitionCount number of distinct partitions to be created by the exchange
     * @return {@link Exchange} instance to be used by coordinator to interact with the external exchange
     */
    Exchange createExchange(ExchangeContext context, int outputPartitionCount);

    ExchangeSink createSink(ExchangeSinkInstanceHandle handle, boolean preserveRecordsOrder);

    ExchangeSink createSink(ExchangeSinkInstanceHandle handle, DirectSerialisationType serType, boolean preserveRecordsOrder);

    ExchangeSource createSource(List<ExchangeSourceHandle> handles);
}
