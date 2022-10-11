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

import io.prestosql.exchange.ExchangeId;
import io.prestosql.exchange.RetryPolicy;
import io.prestosql.execution.TaskFailureListener;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.spi.QueryId;

public interface ExchangeClientSupplier
{
    ExchangeClient get(
            LocalMemoryContext systemMemoryContext,
            TaskFailureListener taskFailureListener,
            RetryPolicy retryPolicy,
            ExchangeId exchangeId,
            QueryId queryId);
}
