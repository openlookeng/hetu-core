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
package io.prestosql.orc.reader;

import io.prestosql.orc.TupleDomainFilter;
import io.prestosql.spi.block.Block;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

public interface SelectiveColumnReader<T>
        extends AbstractColumnReader
{
    /**
     ** Extract values at the specified positions, apply filter and buffer the values that pass
     ** the filter.
     ** Each datatype reader logic is as below:
     *  1. Allocates enough storage to store potential matching position count.
     *  2. Allocate enough storage to store all matching values.
     *  3. Allocates enough storage to store null values.
     *  4. Traverse through all positions in the position array, if there is hollow in position array, then skip
     *     corresponding number of offset within the column file stream.
     *  5. Apply filter, if it matches then
     *      a. Then store the corresponding position in outputPositionCount.
     *      b. If column is part of projection, then corresponding values also.
     *  6. Finally return number of matching positionCount to be used by next column reader.
     ** @param positions Monotonically increasing positions to read
     ** @param positionCount Number of valid positions in the positions array; may be less than the
     **                      size of the array
     ** @param filter
     * @return the number of positions that passed the filter
     **/
    int read(int offset, int[] positions, int positionCount, TupleDomainFilter filter)
            throws IOException;

    /**
     ** @return an array of positions that passed the filter during most recent read(); the return
     **      value of read() is the number of valid entries in this array; the return value is a strict
     **      subset of positions passed into read()
     **/
    int[] getReadPositions();

    /**
     ** Return a subset of the values extracted during most recent read() for the specified positions
     * @param positions Monotonically increasing positions to return; must be a strict subset of both
     **                  the list of positions passed into read() and the list of positions returned
     **                  from getReadPositions()
     ** @param positionCount Number of valid positions in the positions array; may be less than the
     **/
    Block<T> getBlock(int[] positions, int positionCount);

    /**
     * Similar to read but it applies OR filter.
     * @param offset Offset within the row group.
     * @param positions Monotonically increasing positions to read
     * @param positionCount Number of valid positions in the positions array; may be less than the
     *                     size of the array
     * @param filter      Filter to be applied.
     * @param accumulator  Used to set bit corresponding to position which matched filter.
     * @return
     * @throws IOException
     */
    default int readOr(int offset, int[] positions, int positionCount, List<TupleDomainFilter> filter, BitSet accumulator)
            throws IOException
    {
        return read(offset, positions, positionCount, null);
    }

    default Block<T> mergeBlocks(List<Block<T>> blocks, int positionCount)
    {
        return blocks.get(0);
    }
}
