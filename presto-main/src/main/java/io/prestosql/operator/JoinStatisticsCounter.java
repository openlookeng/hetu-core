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

import io.prestosql.operator.LookupJoinOperators.JoinType;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;

import java.io.Serializable;
import java.util.Optional;
import java.util.function.Supplier;

import static io.prestosql.operator.JoinOperatorInfo.createJoinOperatorInfo;
import static java.util.Objects.requireNonNull;

public class JoinStatisticsCounter
        implements Supplier<OperatorInfo>, Restorable
{
    public static final int HISTOGRAM_BUCKETS = 8;

    private static final int INDIVIDUAL_BUCKETS = 4;

    private final JoinType joinType;
    // Logarithmic histogram. Regular histogram (or digest) is too expensive, because of memory manipulations. Also, we don't need their guarantees of precision.
    // To make it maximally fast by reducing indirections (it will fit in cache L1 anyways) counters are packed in one array.
    // Layout (here "bucket" is histogram bucket):
    //      [2*bucket]      count probe positions that produced "bucket" rows on source side,
    //      [2*bucket + 1]  total count of rows that were produces by probe rows in this bucket.
    private final long[] logHistogramCounters = new long[HISTOGRAM_BUCKETS * 2];

    /**
     * Estimated number of positions in on the build side
     */
    private Optional<Long> lookupSourcePositions = Optional.empty();

    public JoinStatisticsCounter(JoinType joinType)
    {
        this.joinType = requireNonNull(joinType, "joinType is null");
    }

    public void updateLookupSourcePositions(long lookupSourcePositionsDelta)
    {
        this.lookupSourcePositions = Optional.of(this.lookupSourcePositions.orElse(0L) + lookupSourcePositionsDelta);
    }

    public void recordProbe(int numSourcePositions)
    {
        int bucket;
        if (numSourcePositions <= INDIVIDUAL_BUCKETS) {
            bucket = numSourcePositions;
        }
        else if (numSourcePositions <= 10) {
            bucket = INDIVIDUAL_BUCKETS + 1;
        }
        else if (numSourcePositions <= 100) {
            bucket = INDIVIDUAL_BUCKETS + 2;
        }
        else {
            bucket = INDIVIDUAL_BUCKETS + 3;
        }
        logHistogramCounters[2 * bucket]++;
        logHistogramCounters[2 * bucket + 1] += numSourcePositions;
    }

    @Override
    public JoinOperatorInfo get()
    {
        return createJoinOperatorInfo(joinType, logHistogramCounters, lookupSourcePositions);
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        JoinStatisticsCounterState myState = new JoinStatisticsCounterState();
        myState.logHistogramCounters = logHistogramCounters.clone();
        if (lookupSourcePositions.isPresent()) {
            myState.lookupSourcePositions = lookupSourcePositions.get();
        }
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        JoinStatisticsCounterState myState = (JoinStatisticsCounterState) state;
        System.arraycopy(myState.logHistogramCounters, 0, this.logHistogramCounters, 0, myState.logHistogramCounters.length);
        this.lookupSourcePositions = Optional.empty();
        if (myState.lookupSourcePositions != null) {
            this.lookupSourcePositions = Optional.of(myState.lookupSourcePositions);
        }
    }

    private static class JoinStatisticsCounterState
            implements Serializable
    {
        private long[] logHistogramCounters;
        private Long lookupSourcePositions;
    }
}
