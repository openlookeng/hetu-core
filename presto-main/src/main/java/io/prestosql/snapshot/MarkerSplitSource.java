/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.snapshot;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.prestosql.execution.Lifespan;
import io.prestosql.metadata.Split;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.split.SplitSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

public class MarkerSplitSource
        implements SplitSource
{
    private static final Logger LOG = Logger.get(MarkerSplitSource.class);

    // Actual split source being wrapped
    private final SplitSource source;
    private final MarkerAnnouncer announcer;
    // Don't send data splits (only send marker splits) until all dependencies have finished sending their data splits.
    // Dependencies are those that are on the "build" side of joins where the current source is on the "probe" side.
    // Probe side is blocked until build side finishes. The above ensures that, while probe is blocked,
    // marker pages are not blocked by data pages, so they can still pass through join operators, to make their corresponding snapshots complete.
    private final Set<MarkerSplitSource> allDependencies;
    // When a dependency finishes, then remove it from the "remaining" set. Only send data splits when this set becomes empty.
    private final Set<MarkerSplitSource> remainingDependencies;

    // When resuming from a snapshot, the first split sent from a source is a resume marker split
    private OptionalLong resumingSnapshotId = OptionalLong.empty();

    // Keep track of all splits sent from this source, so that they can be replayed after a resume.
    // The bufferPosition indicates the next split to send. If it's not at the end of the buffer, and split source is not exhausted,
    // then need to ask for more splits from the source.
    private boolean sourceExhausted;
    private final List<Split> splitBuffer = new ArrayList<>();
    private int bufferPosition;
    private boolean sentFinalMarker;

    // Keep track of how many splits are before each snapshot. Key is snapshotId.
    // This is used to determine the value of bufferPosition above.
    private final Map<Long, Integer> snapshotBufferPositions = new HashMap<>();
    // The first snapshot id sent from this source. When resuming to a snapshot that's not in the above map,
    // this helps to determine what to do. See "resumeSnapshot" method for details.
    private OptionalLong firstSnapshot = OptionalLong.empty();

    public MarkerSplitSource(SplitSource source, MarkerAnnouncer announcer)
    {
        this.source = source;
        this.announcer = announcer;
        allDependencies = new HashSet<>();
        remainingDependencies = new HashSet<>();
        // Snapshot 0 indicates the beginning of the query.
        // In case there are no complete snapshot to restore to, then go back to the very beginning.
        snapshotBufferPositions.put(0L, 0);
    }

    @Override
    public CatalogName getCatalogName()
    {
        return source.getCatalogName();
    }

    public void addDependency(MarkerSplitSource dependency)
    {
        allDependencies.add(dependency);
        remainingDependencies.add(dependency);
    }

    public void finishDependency(MarkerSplitSource dependency)
    {
        remainingDependencies.remove(dependency);
    }

    @Override
    public ListenableFuture<SplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, Lifespan lifespan, int maxSize)
    {
        checkArgument(maxSize > 0, "Cannot fetch a batch of zero size");

        if (resumingSnapshotId.isPresent()) {
            long snapshotId = resumingSnapshotId.getAsLong();
            resumingSnapshotId = OptionalLong.empty();
            Split marker = new Split(getCatalogName(), MarkerSplit.resumeSplit(getCatalogName(), snapshotId), lifespan);
            List<Split> splits = Collections.singletonList(marker);
            boolean lastBatch = sourceExhausted && bufferPosition == splitBuffer.size();
            if (lastBatch) {
                sentFinalMarker = true;
            }
            LOG.debug("Sending out resuming marker %d after %d splits for source: %s (%s)", snapshotId, bufferPosition, getCatalogName(), toString());
            return Futures.immediateFuture(new SplitBatch(splits, lastBatch));
        }

        if (sourceExhausted && bufferPosition == splitBuffer.size()) {
            // Force send last-batch marker
            long sid = announcer.forceGenerateMarker(this);
            SplitBatch batch = recordSnapshot(lifespan, sid, true);
            return Futures.immediateFuture(batch);
        }

        OptionalLong snapshotId = announcer.shouldGenerateMarker(this);
        if (snapshotId.isPresent()) {
            SplitBatch batch = recordSnapshot(lifespan, snapshotId.getAsLong(), false);
            return Futures.immediateFuture(batch);
        }

        if (!remainingDependencies.isEmpty()) {
            // There are other sources that block this one through a join operator.
            // Wait until those sources to finish before sending data splits from this source.
            // The result is that, while this source is blocked, only marker splits are sent,
            // which will pass through join operators, and can be completed.
            return Futures.immediateFuture(new SplitBatch(Collections.emptyList(), false));
        }

        // Get next batch of "data" splits, then determine if marker should be added.
        ListenableFuture<SplitBatch> result = prepareNextBatch(partitionHandle, lifespan, maxSize);
        result = Futures.transform(result, batch ->
        {
            if (batch != null) {
                List<Split> splits = batch.getSplits();
                incrementSplitCount(splits.size());
                if (batch.isLastBatch()) {
                    if (splits.size() == 0) {
                        // Force generate a marker for last batch. Marker can't be mixed with data splits.
                        long sid = announcer.forceGenerateMarker(this);
                        batch = recordSnapshot(lifespan, sid, true);
                    }
                    else {
                        // Don't send last-batch signal yet. Next call will generate a marker with last-batch.
                        batch = new SplitBatch(splits, false);
                    }
                }
            }
            return batch;
        }, directExecutor());
        return result;
    }

    private ListenableFuture<SplitBatch> prepareNextBatch(ConnectorPartitionHandle partitionHandle, Lifespan lifespan, int maxSize)
    {
        int remaining = splitBuffer.size() - bufferPosition;
        if (remaining >= maxSize || sourceExhausted) {
            // Request can be served entirely by the buffer
            int including = Math.min(remaining, maxSize);
            List<Split> splits = splitBuffer.subList(bufferPosition, bufferPosition + including);
            bufferPosition += including;
            boolean lastBatch = sourceExhausted && including == remaining;
            return Futures.immediateFuture(new SplitBatch(splits, lastBatch));
        }

        List<Split> existing = null;
        if (remaining > 0) {
            existing = new ArrayList<>(splitBuffer.subList(bufferPosition, bufferPosition + remaining));
            bufferPosition += remaining;
            maxSize -= remaining;
        }
        // Lambda below requires this to be effectively final, so can't use "existing" directly
        final List<Split> existingSplits = existing;

        // Retrieve additional splits from source, and combine with remaining splits in the buffer, if any
        ListenableFuture<SplitBatch> result = source.getNextBatch(partitionHandle, lifespan, maxSize);
        return Futures.transform(result, batch ->
        {
            if (batch != null) {
                List<Split> splits = batch.getSplits();
                splitBuffer.addAll(splits);
                bufferPosition += splits.size();
                checkState(bufferPosition == splitBuffer.size());

                if (batch.isLastBatch()) {
                    sourceExhausted = true;
                }

                if (existingSplits != null) {
                    existingSplits.addAll(splits);
                    batch = new SplitBatch(existingSplits, batch.isLastBatch());
                }
            }
            else if (existingSplits != null) {
                batch = new SplitBatch(existingSplits, false);
            }
            return batch;
        }, directExecutor());
    }

    private SplitBatch recordSnapshot(Lifespan lifespan, long snapshotId, boolean lastBatch)
    {
        if (!firstSnapshot.isPresent()) {
            firstSnapshot = OptionalLong.of(snapshotId);
        }
        snapshotBufferPositions.put(snapshotId, bufferPosition);

        LOG.debug("Generating snapshot %d after %d splits for source: %s (%s)", snapshotId, bufferPosition, source.getCatalogName(), source.toString());

        Split marker = new Split(getCatalogName(), MarkerSplit.snapshotSplit(getCatalogName(), snapshotId), lifespan);
        SplitBatch batch = new SplitBatch(Collections.singletonList(marker), lastBatch);
        if (lastBatch) {
            sentFinalMarker = true;
            deactivate();
        }
        return batch;
    }

    public void resumeSnapshot(long snapshotId)
    {
        Integer position = snapshotBufferPositions.get(snapshotId);
        if (position != null) {
            bufferPosition = position;
        }
        else {
            if (!firstSnapshot.isPresent()) {
                // Never took a snapshot for this source, then start from beginning.
                bufferPosition = 0;
            }
            else if (snapshotId < firstSnapshot.getAsLong()) {
                // Restoring to a snapshot before the first one, then also start from beginning
                bufferPosition = 0;
                firstSnapshot = OptionalLong.empty();
            }
            else {
                // The snapshot must have been created after the last snapshot taken for this source.
                // Then restoring to that snapshot should leave the pointer at the end of the buffer.
                checkState(sourceExhausted && bufferPosition == splitBuffer.size());
            }
        }

        // Don't send a resume marker if resuming from the beginning
        resumingSnapshotId = snapshotId == 0 ? OptionalLong.empty() : OptionalLong.of(snapshotId);

        sentFinalMarker = false;
        remainingDependencies.clear();
        remainingDependencies.addAll(allDependencies);
    }

    private void incrementSplitCount(int count)
    {
        announcer.incrementSplitCount(count);
    }

    private void deactivate()
    {
        announcer.deactivateSplitSource(this);
    }

    @Override
    public void close()
    {
        announcer.deactivateSplitSource(this);
        source.close();
    }

    @Override
    public boolean isFinished()
    {
        return sentFinalMarker;
    }
}
