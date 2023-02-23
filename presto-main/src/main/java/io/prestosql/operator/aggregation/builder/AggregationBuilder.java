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
package io.prestosql.operator.aggregation.builder;

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.operator.HashCollisionsCounter;
import io.prestosql.operator.Work;
import io.prestosql.operator.WorkProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.snapshot.Restorable;

public interface AggregationBuilder
        extends AutoCloseable, Restorable
{
    Work<?> processPage(Page page);

    WorkProcessor<Page> buildResult();

    default WorkProcessor<Page> buildResult(AggregationNode.Step step, boolean isFinalizedValuePresent)
    {
        return buildResult();
    }

    boolean isFull();

    void updateMemory();

    void recordHashCollisions(HashCollisionsCounter hashCollisionsCounter);

    @Override
    void close();

    ListenableFuture<?> startMemoryRevoke();

    void finishMemoryRevoke();

    default AggregationBuilder duplicate()
    {
        throw new UnsupportedOperationException("Only supported for Group Join flow");
    }

    default int getAggregationCount()
    {
        throw new UnsupportedOperationException("Only supported for Group Join flow");
    }
}
