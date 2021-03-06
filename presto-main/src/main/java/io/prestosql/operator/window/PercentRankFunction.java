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
package io.prestosql.operator.window;

import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.RankingWindowFunction;
import io.prestosql.spi.function.WindowFunctionSignature;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;

import java.io.Serializable;

import static io.prestosql.spi.type.DoubleType.DOUBLE;

@WindowFunctionSignature(name = "percent_rank", returnType = "double")
public class PercentRankFunction
        extends RankingWindowFunction
{
    private long totalCount;
    private long rank;
    private long count;

    @Override
    public void reset()
    {
        totalCount = windowIndex.size();
        rank = 0;
        count = 1;
    }

    @Override
    public void processRow(BlockBuilder output, boolean newPeerGroup, int peerGroupCount, int currentPosition)
    {
        if (totalCount == 1) {
            DOUBLE.writeDouble(output, 0.0);
            return;
        }

        if (newPeerGroup) {
            rank += count;
            count = 1;
        }
        else {
            count++;
        }

        DOUBLE.writeDouble(output, ((double) (rank - 1)) / (totalCount - 1));
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        PercentRankFunctionState myState = new PercentRankFunctionState();
        myState.totalCount = totalCount;
        myState.rank = rank;
        myState.count = count;
        myState.baseState = super.capture(serdeProvider);
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        PercentRankFunctionState myState = (PercentRankFunctionState) state;
        this.totalCount = myState.totalCount;
        this.rank = myState.rank;
        this.count = myState.count;
        super.restore(myState.baseState, serdeProvider);
    }

    private static class PercentRankFunctionState
            implements Serializable
    {
        private long totalCount;
        private long rank;
        private long count;
        private Object baseState;
    }
}
