/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.split.SplitSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Each query has an instance of MarkerAnnouncer. It's responsible for coordinating marker generation from all split sources of the query.
 */
public class MarkerAnnouncer
{
    private static final Logger LOG = Logger.get(MarkerAnnouncer.class);

    private final SnapshotConfig.IntervalType intervalType;
    private long currentSnapshotId;

    // Used when snapshot interval is based on split-count
    private long querySplitCount;
    private final long splitCountInterval;

    // Used when snapshot interval is based on time
    private long queryTimeStamp;
    private final Duration timeInterval;

    // All split sources. It's keyed by plan node id, so that after reschedule, the same split sources can be reused based on the plan node.
    private final Map<PlanNodeId, MarkerSplitSource> allSplitSources;
    // A subset of all split sources, excluding those that have finished sending all their splits.
    private final List<MarkerSplitSource> activeSplitSources;
    // A subset of actives split sources, for those that have sent out markers for the current snapshot id.
    private final List<MarkerSplitSource> markerSplitSent;

    //Used to store snapshotId for split sources that are behind the main snapshotId.
    //Having split source manage snapshotId was also considered but after considering potential future bypassing mechanism,
    //this method of recording slow/blocked split source is chosen.
    private final Map<MarkerSplitSource, LinkedList<Long>> pendingSnapshot;

    // Whenever MarkerAnnouncer decides to initiate a new snapshot, it needs to inform the QuerySnapshotManager about its action.
    // QuerySnapshotManager will keep track of all snapshots because snapshotId won't be able to tell us the absolute index after
    // restore. (eg. have completed snapshot 1-10, restored to 1, snapshot 11 that's generated after restore is actually 2nd snapshot)
    // It is important for the management of snapshot sub-files written by TableWriterOperator.
    private QuerySnapshotManager snapshotManager;

    public MarkerAnnouncer(Duration timeInterval)
    {
        this(SnapshotConfig.IntervalType.TIME, 0L, timeInterval);
    }

    public MarkerAnnouncer(long splitCountInterval)
    {
        this(SnapshotConfig.IntervalType.SPLIT_COUNT, splitCountInterval, null);
    }

    private MarkerAnnouncer(SnapshotConfig.IntervalType intervalType, long splitCountInterval, Duration timeInterval)
    {
        this.intervalType = intervalType;
        // Reserve snapshot 0 for special case of "restart from beginning"
        this.currentSnapshotId = 1L;
        this.querySplitCount = 0L;
        this.splitCountInterval = splitCountInterval;
        this.queryTimeStamp = System.currentTimeMillis();
        this.timeInterval = timeInterval;
        this.allSplitSources = new HashMap<>();
        this.activeSplitSources = new ArrayList<>();
        this.markerSplitSent = new ArrayList<>();
        this.pendingSnapshot = new HashMap<>();
    }

    public void setSnapshotManager(QuerySnapshotManager snapshotManager)
    {
        this.snapshotManager = snapshotManager;
    }

    public void informSnapshotInitiation(long snapshotId)
    {
        snapshotManager.snapshotInitiated(snapshotId);
    }

    public MarkerSplitSource createMarkerSplitSource(SplitSource splitSource, PlanNodeId nodeId)
    {
        checkArgument(!(splitSource instanceof MarkerSplitSource));
        // Wrap a MarkerSplitSource around the original split source, to add marker generation functionality
        MarkerSplitSource markerSplitSource = new MarkerSplitSource(splitSource, this);
        allSplitSources.put(nodeId, markerSplitSource);
        activeSplitSources.add(markerSplitSource);
        pendingSnapshot.put(markerSplitSource, new LinkedList<>());
        //Use the timestamp of the last registered splitSource as the query's first significant timestamp
        if (intervalType == SnapshotConfig.IntervalType.TIME) {
            queryTimeStamp = System.currentTimeMillis();
        }
        return markerSplitSource;
    }

    public SplitSource getSplitSource(PlanNodeId nodeId)
    {
        return allSplitSources.get(nodeId);
    }

    //When a split source got its last batch, announcer will remove it from active splitSources
    public synchronized void deactivateSplitSource(MarkerSplitSource splitSource)
    {
        if (activeSplitSources.remove(splitSource)) {
            for (MarkerSplitSource source : activeSplitSources) {
                source.finishDependency(splitSource);
            }
            LOG.debug("Finished split source: %s (%s)", splitSource.getCatalogName(), splitSource.toString());
            if (!markerSplitSent.contains(splitSource)) {
                checkMarkerSplitCompleteness();
            }
        }
    }

    //To see if the last splitSource who sent out a marker split is the last one for the query of this snapshotId.
    private void checkMarkerSplitCompleteness()
    {
        if (markerSplitSent.size() >= activeSplitSources.size()) {
            if (markerSplitSent.containsAll(activeSplitSources)) {
                setupNewSnapshotId();
            }
        }
    }

    private void setupNewSnapshotId()
    {
        //Reset interval: update the query significant timestamp or reset the split count.
        queryTimeStamp = System.currentTimeMillis();
        querySplitCount = 0L;
        //Move to next snapshot id
        currentSnapshotId++;
        markerSplitSent.clear();
    }

    public long currentSnapshotId()
    {
        return currentSnapshotId;
    }

    // Check if a split source should generate a snapshot marker,
    // - Either a marker has been sent by some sources but not from the specified source
    // - Or a new snapshot id was established and the snapshot interval has been reached
    public synchronized OptionalLong shouldGenerateMarker(MarkerSplitSource source)
    {
        //check for previously uncompleted snapshotId
        Long pendingId = pendingSnapshot.get(source).pollFirst();
        if (pendingId != null) {
            return OptionalLong.of(pendingId);
        }

        if (!markerSplitSent.isEmpty()) {
            // Sending markers
            if (markerSplitSent.contains(source)) {
                // Already sent for this source
                return OptionalLong.empty();
            }
        }
        else if (!(intervalType == SnapshotConfig.IntervalType.TIME && System.currentTimeMillis() - queryTimeStamp >= timeInterval.toMillis() ||
                intervalType == SnapshotConfig.IntervalType.SPLIT_COUNT && querySplitCount >= splitCountInterval)) {
            // Not already sending markers, and haven't reached threshold
            return OptionalLong.empty();
        }

        long upcomingSnapshotId = generateMarker(source);
        informSnapshotInitiation(upcomingSnapshotId);
        return OptionalLong.of(upcomingSnapshotId);
    }

    // Generate a new marker for this source, e.g. after the source has sent all its data splits.
    public synchronized long forceGenerateMarker(MarkerSplitSource source)
    {
        OptionalLong should = shouldGenerateMarker(source);
        if (should.isPresent()) {
            return should.getAsLong();
        }
        if (!markerSplitSent.isEmpty()) {
            // Current snapshot id has been sent to some sources. Force it to finish and start a new snapshot id.
            recordUncompletedSplitSource();
            setupNewSnapshotId();
        }
        long upcomingSnapshotId = generateMarker(source);
        informSnapshotInitiation(upcomingSnapshotId);
        return upcomingSnapshotId;
    }

    private void recordUncompletedSplitSource()
    {
        //Determine which active split sources have not received the marker
        List<MarkerSplitSource> uncompleted = new ArrayList<>(activeSplitSources);
        uncompleted.removeAll(markerSplitSent);
        //record current snapshotId as pending for these uncompleted split sources
        for (MarkerSplitSource source : uncompleted) {
            pendingSnapshot.get(source).add(currentSnapshotId);
        }
    }

    private long generateMarker(MarkerSplitSource source)
    {
        long ret = currentSnapshotId;
        markerSplitSent.add(source);
        checkMarkerSplitCompleteness();
        return ret;
    }

    public synchronized void incrementSplitCount(int count)
    {
        if (intervalType == SnapshotConfig.IntervalType.SPLIT_COUNT) {
            querySplitCount += count;
        }
    }

    // Resume to specified snapshot id. Also forward this request to all the split sources.
    public synchronized void resumeSnapshot(long snapshotId)
    {
        LOG.debug("Resuming to snapshot %d", snapshotId);
        activeSplitSources.clear();
        activeSplitSources.addAll(allSplitSources.values());
        for (MarkerSplitSource source : activeSplitSources) {
            source.resumeSnapshot(snapshotId);
        }
        // Abandon current snapshot id, to ensure snapshots taken after resume to not overlap with those from before resume
        setupNewSnapshotId();
        //Previously missed snapshotId shouldn't be carried out in the future.
        for (List<Long> pendingIds : pendingSnapshot.values()) {
            pendingIds.clear();
        }
    }
}
