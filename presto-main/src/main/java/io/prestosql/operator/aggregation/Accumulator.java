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

package io.prestosql.operator.aggregation;

import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.WindowIndex;
import io.prestosql.spi.snapshot.Restorable;
import io.prestosql.spi.type.Type;

import java.util.List;

public interface Accumulator
        extends Restorable
{
    long getEstimatedSize();

    Type getFinalType();

    Type getIntermediateType();

    void addInput(Page page);

    void addInput(WindowIndex index, List<Integer> channels, int startPosition, int endPosition);

    void addIntermediate(Block block);

    void evaluateIntermediate(BlockBuilder blockBuilder);

    void evaluateFinal(BlockBuilder blockBuilder);
}
