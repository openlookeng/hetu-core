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
package io.prestosql.spi.function;

import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;

import java.io.Serializable;

@RestorableConfig(uncapturedFields = {"windowIndex"})
public abstract class RankingWindowFunction
        implements WindowFunction
{
    // Snapshot: all windowIndex operations revolves around pagesIndex which is passed in and captured/restored outside
    // windowIndex fields in all window functions are reset when WindowPartition is created(see WindowPartition line 71)
    // so it doesn't need to be captured.
    protected WindowIndex windowIndex;

    private int currentPeerGroupStart;
    private int currentPosition;

    @Override
    public final void reset(WindowIndex windowIndex)
    {
        this.windowIndex = windowIndex;
        this.currentPeerGroupStart = -1;
        this.currentPosition = 0;

        reset();
    }

    @Override
    public final void processRow(BlockBuilder output, int peerGroupStart, int peerGroupEnd, int frameStart, int frameEnd)
    {
        boolean newPeerGroup = false;
        if (peerGroupStart != currentPeerGroupStart) {
            currentPeerGroupStart = peerGroupStart;
            newPeerGroup = true;
        }

        int peerGroupCount = (peerGroupEnd - peerGroupStart) + 1;

        processRow(output, newPeerGroup, peerGroupCount, currentPosition);

        currentPosition++;
    }

    /**
     * Reset state for a new partition (including the first one).
     */
    public void reset()
    {
        // subclasses can override
    }

    /**
     * Process a row by outputting the result of the window function.
     * <p/>
     * This method provides information about the ordering peer group. A peer group is all
     * of the rows that are peers within the specified ordering. Rows are peers if they
     * compare equal to each other using the specified ordering expression. The ordering
     * of rows within a peer group is undefined (otherwise they would not be peers).
     *
     * @param output the {@link BlockBuilder} to use for writing the output row
     * @param newPeerGroup if this row starts a new peer group
     * @param peerGroupCount the total number of rows in this peer group
     * @param currentPosition the current position for this row
     */
    public abstract void processRow(BlockBuilder output, boolean newPeerGroup, int peerGroupCount, int currentPosition);

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        RankingWindowFunctionState myState = new RankingWindowFunctionState();
        myState.currentPeerGroupStart = currentPeerGroupStart;
        myState.currentPosition = currentPosition;
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        RankingWindowFunctionState myState = (RankingWindowFunctionState) state;
        this.currentPeerGroupStart = myState.currentPeerGroupStart;
        this.currentPosition = myState.currentPosition;
    }

    private static class RankingWindowFunctionState
            implements Serializable
    {
        private int currentPeerGroupStart;
        private int currentPosition;
    }
}
