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
package io.prestosql.tests;

import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.RankingWindowFunction;
import io.prestosql.spi.function.WindowFunctionSignature;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;

import java.io.Serializable;

import static io.prestosql.spi.type.BigintType.BIGINT;

@WindowFunctionSignature(name = "custom_rank", returnType = "bigint")
public class CustomRank
        extends RankingWindowFunction
{
    private long rank;
    private long count;

    @Override
    public void reset()
    {
        rank = 0;
        count = 1;
    }

    @Override
    public void processRow(BlockBuilder output, boolean newPeerGroup, int peerGroupCount, int currentPosition)
    {
        if (newPeerGroup) {
            rank += count;
            count = 1;
        }
        else {
            count++;
        }
        BIGINT.writeLong(output, rank);
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        CustomRankState myState = new CustomRankState();
        myState.rank = rank;
        myState.count = count;
        myState.baseState = super.capture(serdeProvider);
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        CustomRankState myState = (CustomRankState) state;
        this.rank = myState.rank;
        this.count = myState.count;
        super.restore(myState.baseState, serdeProvider);
    }

    private static class CustomRankState
            implements Serializable
    {
        private long rank;
        private long count;
        private Object baseState;
    }
}
