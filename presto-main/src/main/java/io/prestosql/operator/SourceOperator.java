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

import io.prestosql.metadata.Split;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.plan.PlanNodeId;

import java.util.Optional;
import java.util.function.Supplier;

public interface SourceOperator
        extends Operator
{
    @Override
    default void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " can not take input");
    }

    @Override
    default boolean needsInput()
    {
        return false;
    }

    PlanNodeId getSourceId();

    Supplier<Optional<UpdatablePageSource>> addSplit(Split split);

    void noMoreSplits();
}
