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

@RestorableConfig(uncapturedFields = {"windowIndex"})
public abstract class ValueWindowFunction
        implements WindowFunction
{
    // Snapshot: all windowIndex operations revolves around pagesIndex which is passed in and captured/restored outside
    // windowIndex fields in all window functions are reset when WindowPartition is created(see WindowPartition line 71)
    // so it doesn't need to be captured.
    protected WindowIndex windowIndex;

    private int currentPosition;

    @Override
    public final void reset(WindowIndex windowIndex)
    {
        this.windowIndex = windowIndex;
        this.currentPosition = 0;

        reset();
    }

    @Override
    public final void processRow(BlockBuilder output, int peerGroupStart, int peerGroupEnd, int frameStart, int frameEnd)
    {
        processRow(output, frameStart, frameEnd, currentPosition);

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
     *
     * @param output the {@link BlockBuilder} to use for writing the output row
     * @param frameStart the position of the first row in the window frame
     * @param frameEnd the position of the last row in the window frame
     * @param currentPosition the current position for this row
     */
    public abstract void processRow(BlockBuilder output, int frameStart, int frameEnd, int currentPosition);

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        return currentPosition;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        this.currentPosition = (int) state;
    }
}
