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

import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;
import io.prestosql.spi.snapshot.RestorableConfig;

import java.io.Serializable;
import java.util.function.Supplier;

import static io.prestosql.operator.HashCollisionsInfo.createHashCollisionsInfo;

@RestorableConfig(uncapturedFields = {"operatorContext"})
public class HashCollisionsCounter
        implements Supplier<OperatorInfo>, Restorable
{
    private final OperatorContext operatorContext;

    private long hashCollisions;
    private double expectedHashCollisions;

    public HashCollisionsCounter(OperatorContext operatorContext)
    {
        this.operatorContext = operatorContext;
    }

    public void recordHashCollision(long hashCollisions, double expectedHashCollisions)
    {
        this.hashCollisions += hashCollisions;
        this.expectedHashCollisions += expectedHashCollisions;
    }

    @Override
    public HashCollisionsInfo get()
    {
        return createHashCollisionsInfo(operatorContext.getInputPositions().getTotalCount(), hashCollisions, expectedHashCollisions);
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        HashCollisionsCounterState myState = new HashCollisionsCounterState();
        myState.hashCollisions = hashCollisions;
        myState.expectedHashCollisions = expectedHashCollisions;
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        HashCollisionsCounterState myState = (HashCollisionsCounterState) state;
        this.hashCollisions = myState.hashCollisions;
        this.expectedHashCollisions = myState.expectedHashCollisions;
    }

    private static class HashCollisionsCounterState
            implements Serializable
    {
        private long hashCollisions;
        private double expectedHashCollisions;
    }
}
