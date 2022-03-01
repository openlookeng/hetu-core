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

import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.operator.PartitionFunction;
import io.prestosql.operator.SpillContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.function.BiFunction;

public interface PartitioningSpillerFactory
{
    PartitioningSpiller create(
            List<Type> types,
            PartitionFunction partitionFunction,
            SpillContext spillContext,
            AggregatedMemoryContext memoryContext);

    default PartitioningSpiller create(
            List<Type> types,
            PartitionFunction partitionFunction,
            SpillContext spillContext,
            AggregatedMemoryContext memoryContext,
            BiFunction<Integer, Page, Long> getRawHash)
    {
        return create(types, partitionFunction, spillContext, memoryContext);
    }

    static PartitioningSpillerFactory unsupportedPartitioningSpillerFactory()
    {
        return (types, partitionFunction, spillContext, memoryContext) -> {
            throw new UnsupportedOperationException();
        };
    }
}
